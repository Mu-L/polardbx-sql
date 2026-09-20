package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.omc.OmcStorageInfo;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.cdc.entity.LogicMeta;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.sql.SqlIdentifier;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

/**
 * MCE Task: physically add the addr column to every DN partition and register it in MetaDB.
 * <p>
 * Uses a dedicated MCE task so the externalize pipeline controls physical DDL probing and MetaDB
 * registration directly.
 * <p>
 * The addr column is placed {@code AFTER} the original content column so that after the content
 * column is dropped in {@link MceDropContentColumnTask}, the addr column naturally occupies the
 * original column's physical position — preserving column order.
 * <p>
 * Physical ADD COLUMN algorithm (probed per partition, no version guessing):
 * <ol>
 *   <li>Try {@code ALGORITHM=INSTANT} (8.0.29+ supports AFTER-position instant add).</li>
 *   <li>On DN error, try {@code ALGORITHM=INPLACE, LOCK=NONE} (online, non-locking).</li>
 *   <li>On DN error again: if {@code MCE_ADD_COLUMN_ALLOW_LOCK} is false (default), throw to the
 *       user (never silently lock the table); if true, drop the algorithm clause and let the DN
 *       pick its default (may lock the table).</li>
 * </ol>
 * <p>
 * When the addr column needs to be created, this task runs at the beginning of the externalize
 * pipeline. It must register the column in MetaDB here because the later
 * {@link MceChangeWriteModeTask} sets flags on this column and would fail if it were not present.
 */
@Getter
@TaskName(name = "MceAddAddrColumnTask")
public class MceAddAddrColumnTask extends BaseGmsTask {

    private final String tableName;
    private final String columnName;      // original content column, e.g. "body"
    private final String addrColumnName;  // addr column, e.g. "body_addr_"
    private final String originalType;    // original column type, for the ext_type comment
    private final String userCommentOverride; // null means preserve the source column comment

    private static final com.alibaba.polardbx.common.utils.logger.Logger LOG =
        com.alibaba.polardbx.common.utils.logger.LoggerFactory.getLogger(MceAddAddrColumnTask.class);

    public MceAddAddrColumnTask(String schemaName, String tableName, String columnName, String originalType) {
        this(schemaName, tableName, columnName, originalType, null);
    }

    @JSONCreator
    public MceAddAddrColumnTask(String schemaName, String tableName, String columnName, String originalType,
                                String userCommentOverride) {
        super(schemaName, tableName);
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
        this.originalType = originalType != null ? originalType : "LONGTEXT";
        this.userCommentOverride = userCommentOverride;
    }

    /**
     * Physical ADD COLUMN on all DN partitions. Outside the MetaDB transaction because physical
     * DDL cannot run in the same txn.
     */
    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        // NOTE: deliberately do NOT call super.beforeTransaction() here. BaseGmsTask's version
        // check (checkTableMetaVersion) fails inside the MCE pipeline because earlier steps have
        // already bumped the table version. MceDropContentColumnTask overrides the same way.
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        MceExternalizeTargetValidator.validate(tableMeta, schemaName, tableName, columnName, originalType);

        boolean allowLock = executionContext.getParamManager().getBoolean(ConnectionParams.MCE_ADD_COLUMN_ALLOW_LOCK);
        List<McePhysicalTableResolver.PhysicalTableTarget> physicalTableTargets =
            McePhysicalTableResolver.resolve(schemaName, tableName, tableMeta, executionContext);
        validateNoPhysicalAddrColumn(physicalTableTargets);
        String comment = resolveStoredComment();

        updateTaskStateInNewTxn(DdlTaskState.DIRTY);

        MceTaskRuntimeConfig runtimeConfig = new MceTaskRuntimeConfig(
            executionContext, LOG, getJobId(), getTaskId(), "add-addr-physical-ddl");
        McePhysicalDdlExecutor.execute(
            physicalTableTargets,
            runtimeConfig,
            target -> addAddrColumnToPhysicalTable(
                target.storageInfo, target.physicalDb, target.physicalTable, comment, allowLock),
            executionContext,
            LOG,
            "add-addr-physical-ddl job=" + getJobId() + " task=" + getTaskId(),
            "Concurrent MCE addr-column ADD failed: ");

        LOG.info(String.format("[MCE] MceAddAddrColumnTask added column '%s' AFTER '%s' for %s.%s",
            addrColumnName, columnName, schemaName, tableName));
    }

    private void validateNoPhysicalAddrColumn(List<McePhysicalTableResolver.PhysicalTableTarget> targets) {
        for (McePhysicalTableResolver.PhysicalTableTarget target : targets) {
            if (findPhysicalAddrColumn(target.storageInfo, target.physicalTable) != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                    "MCE addr column '%s' already exists on physical table %s.%s",
                    addrColumnName, target.physicalDb, target.physicalTable));
            }
        }
    }

    private String resolveStoredComment() {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(connection);
            if (!tableInfoManager.queryOneColumn(schemaName, tableName, addrColumnName).isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                    "MCE addr column '%s' already exists in MetaDB columns for table %s.%s",
                    addrColumnName, schemaName, tableName));
            }
            List<ColumnsRecord> sourceColumns =
                tableInfoManager.queryOneColumn(schemaName, tableName, columnName);
            if (sourceColumns.size() != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                    "Expected exactly one source column '%s' in MetaDB for table %s.%s, found %d",
                    columnName, schemaName, tableName, sourceColumns.size()));
            }
            String effectiveUserComment =
                userCommentOverride != null ? userCommentOverride : sourceColumns.get(0).columnComment;
            String storedComment = ExternalizedColumnInfo.buildComment(originalType, effectiveUserComment);
            LimitValidator.validateColumnComment(addrColumnName, storedComment);
            return storedComment;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("Failed to check MCE addr column '%s' in MetaDB for table %s.%s: %s",
                    addrColumnName, schemaName, tableName, e.getMessage()), e);
        }
    }

    /**
     * Add the addr column to one physical table, probing algorithms in order:
     * INSTANT → INPLACE/LOCK=NONE → (allowLock ? default : error).
     */
    private void addAddrColumnToPhysicalTable(OmcStorageInfo storageInfo, String phyDb, String phyTable,
                                              String comment, boolean allowLock) {
        String base = String.format("ALTER TABLE %s.%s ADD COLUMN %s VARCHAR(%d) DEFAULT '' COMMENT '%s' AFTER %s",
            SqlIdentifier.surroundWithBacktick(phyDb),
            SqlIdentifier.surroundWithBacktick(phyTable),
            SqlIdentifier.surroundWithBacktick(addrColumnName),
            ExternalizedColumnInfo.ADDR_VARCHAR_LENGTH,
            comment,
            SqlIdentifier.surroundWithBacktick(columnName));

        // 1) INSTANT
        if (tryExecute(storageInfo, base + ", ALGORITHM=INSTANT")) {
            return;
        }
        // 2) INPLACE, LOCK=NONE
        if (tryExecute(storageInfo, base + ", ALGORITHM=INPLACE, LOCK=NONE")) {
            return;
        }
        // 3) allow lock (switch on) → default algorithm; otherwise error out
        if (allowLock) {
            execute(storageInfo, base);
            return;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
            "MCE add column '%s' on %s.%s cannot run online (neither ALGORITHM=INSTANT nor "
                + "ALGORITHM=INPLACE,LOCK=NONE is supported by the DN). Set MCE_ADD_COLUMN_ALLOW_LOCK=true "
                + "to allow a locking DDL.", addrColumnName, phyDb, phyTable));
    }

    private ColumnsInfoSchemaRecord findPhysicalAddrColumn(OmcStorageInfo storageInfo, String phyTable) {
        List<ColumnsInfoSchemaRecord> columns = OmcUtils.getColumnsInfo(storageInfo, phyTable);
        if (columns == null) {
            return null;
        }
        return columns.stream()
            .filter(c -> c.columnName != null && c.columnName.equalsIgnoreCase(addrColumnName))
            .findFirst()
            .orElse(null);
    }

    /**
     * Try one physical DDL; return true on success, false on DN error (so the caller can try the
     * next algorithm). Errors are swallowed here on purpose — the final fallback decides whether
     * to throw.
     */
    private boolean tryExecute(OmcStorageInfo storageInfo, String sql) {
        try (Connection conn = OmcUtils.getPhysicalConnection(storageInfo);
            Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            return true;
        } catch (Exception e) {
            LOG.warn(String.format("[MCE] physical add column probe failed, will try next algorithm. sql=%s, err=%s",
                sql, e.getMessage()));
            return false;
        }
    }

    private void execute(OmcStorageInfo storageInfo, String sql) {
        try (Connection conn = OmcUtils.getPhysicalConnection(storageInfo);
            Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("Failed to add column via '%s': %s", sql, e.getMessage()), e);
        }
    }

    /**
     * Register the addr column in MetaDB (columns table). Reuses {@link TableInfoManager#addColumns}
     * which pulls the authoritative jdbcType / length / ordinal_position from the DN
     * information_schema. Table version is bumped automatically by {@link BaseGmsTask}.
     * <p>
     * Deliberately does NOT call showTable (would reset all columns to PUBLIC) — the addr column's
     * WRITE_ONLY status / MCE flag / mapping name are set by the following MceChangeWriteModeTask.
     */
    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        McePhysicalTableResolver.PhysicalTableTarget sampleTarget =
            McePhysicalTableResolver.resolve(schemaName, tableName, tableMeta, executionContext).get(0);

        TableInfoManager.PhyInfoSchemaContext context =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, tableName, sampleTarget.groupName,
                sampleTarget.physicalTable);

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        LogicMeta.LogicalTableMetaDetail detail = tableInfoManager.fetchLogicalTableMetaFromInfoSchema(context);
        tableInfoManager.addColumns(context, detail.getColumnsJdbcExtInfo(),
            Collections.singletonList(addrColumnName), detail);
        tableInfoManager.updateColumnMappingName(schemaName, tableName, addrColumnName, columnName);
        registerMceColumnState(metaDbConnection);

        LOG.info(String.format("[MCE] Registered MetaDB column record: %s.%s.%s",
            schemaName, tableName, addrColumnName));
    }

    private void registerMceColumnState(Connection metaDbConnection) {
        com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor accessor =
            new com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor();
        accessor.setConnection(metaDbConnection);
        com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord record =
            new com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord();
        record.setJobId(getJobId() == null ? 0L : getJobId());
        record.setTaskId(getTaskId() == null ? 0L : getTaskId());
        record.setTableSchema(schemaName);
        record.setTableName(tableName);
        record.setColumnName(columnName);
        record.setAddrColumnName(addrColumnName);
        record.setState(com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord.STATE_NONE);
        record.setStatus(com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord.STATUS_RUNNING);
        record.setPhysicalDb("");
        record.setPhysicalTable("");
        record.setPartitionName("");
        MceControlStateHelper.insertOrValidate(accessor, record);
    }

    /**
     * Drop the addr column from all physical partitions before opening the MetaDB rollback
     * transaction. A missing column is an idempotent retry; any real DROP failure pauses rollback
     * and keeps the metadata intact for the next attempt.
     */
    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        List<McePhysicalTableResolver.PhysicalTableTarget> physicalTableTargets =
            McePhysicalTableResolver.resolve(schemaName, tableName, tableMeta, executionContext);
        MceTaskRuntimeConfig runtimeConfig = new MceTaskRuntimeConfig(
            executionContext, LOG, getJobId(), getTaskId(), "rollback-add-addr-physical-ddl");
        McePhysicalDdlExecutor.executeRollback(
            physicalTableTargets,
            runtimeConfig,
            this::dropAddrColumnFromPhysicalTable,
            executionContext,
            LOG,
            "rollback-add-addr-physical-ddl job=" + getJobId() + " task=" + getTaskId(),
            "Concurrent MCE addr-column rollback DROP failed: ");
    }

    /**
     * Remove the addr column metadata and MCE control data in one MetaDB transaction after every
     * physical partition has been cleaned successfully.
     */
    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.removeColumns(schemaName, tableName, Collections.singletonList(addrColumnName));

        com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor accessor =
            new com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor();
        accessor.setConnection(metaDbConnection);
        long currentJobId = getJobId() == null ? 0L : getJobId();
        MceControlStateHelper.deleteIfPresent(accessor, currentJobId, schemaName, tableName, columnName,
            addrColumnName, com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord.STATE_NONE);
        accessor.deleteCheckpointsByJobAndColumn(currentJobId, schemaName, tableName, columnName);
    }

    private void dropAddrColumnFromPhysicalTable(McePhysicalTableResolver.PhysicalTableTarget target) {
        if (findPhysicalAddrColumn(target.storageInfo, target.physicalTable) == null) {
            return;
        }

        String dropSql = String.format("ALTER TABLE %s.%s DROP COLUMN %s",
            SqlIdentifier.surroundWithBacktick(target.physicalDb),
            SqlIdentifier.surroundWithBacktick(target.physicalTable),
            SqlIdentifier.surroundWithBacktick(addrColumnName));
        try (Connection conn = OmcUtils.getPhysicalConnection(target.storageInfo);
            Statement stmt = conn.createStatement()) {
            stmt.execute(dropSql);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("Failed to rollback MCE addr column '%s' on %s.%s: %s",
                    addrColumnName, target.physicalDb, target.physicalTable, e.getMessage()), e);
        }
    }

    @Override
    protected String remark() {
        return String.format("|MCE ADD addr column (physical AFTER %s + meta), table=%s, column=%s",
            columnName, tableName, addrColumnName);
    }

}
