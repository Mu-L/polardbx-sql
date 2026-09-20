package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.omc.OmcStorageInfo;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.sql.SqlIdentifier;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Internalize Task: physically re-add the plaintext content column to every DN partition of a
 * terminal externalized column. Mirrors {@link MceAddAddrColumnTask} for the reverse direction.
 * <p>
 * The content column is placed {@code AFTER} the addr column so that after the addr column is
 * dropped by the final internalize task, the content column naturally occupies the addr column's
 * physical position — restoring the original column order.
 * <p>
 * This task is PHYSICAL ONLY. It deliberately registers NOTHING in MetaDB:
 * <ul>
 *   <li>Registering a {@code body} columns record here would clash with the terminal
 *       {@code body_addr_} record, which TableMeta loading already translates to logical
 *       {@code body} ({@code GmsTableMetaManager.buildColumnMeta}) — two ColumnMeta with the same
 *       logical name.</li>
 *   <li>{@code MceColumnStateResolver} fail-closes table meta loading when a control row exists
 *       without a content+addr record pair, so the control row must be created in the SAME MetaDB
 *       transaction that atomically flips the terminal single-record layout to the standard
 *       READ_ADDR two-record layout (the following internalize ChangeWriteMode task).</li>
 * </ul>
 * Until that flip commits, the physical content column stays invisible to CN.
 * <p>
 * Physical ADD COLUMN algorithm probing (INSTANT → INPLACE,LOCK=NONE → allowLock default) is the
 * same as the forward addr-column task.
 */
@Getter
@TaskName(name = "MceAddContentColumnTask")
public class MceAddContentColumnTask extends BaseDdlTask {

    private final String tableName;
    private final String columnName;      // logical content column to restore, e.g. "body"
    private final String addrColumnName;  // existing physical addr column, e.g. "body_addr_"
    private final String originalType;    // original logical type from the addr ext_type comment
    private final String userComment;     // user comment restored from COMMENT_B64, may be null

    private static final com.alibaba.polardbx.common.utils.logger.Logger LOG =
        com.alibaba.polardbx.common.utils.logger.LoggerFactory.getLogger(MceAddContentColumnTask.class);

    @JSONCreator
    public MceAddContentColumnTask(String schemaName, String tableName, String columnName, String originalType,
                                   String userComment) {
        super(schemaName);
        onExceptionTryRecoveryThenPause();
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
        this.originalType = originalType;
        this.userComment = userComment;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        MceInternalizeTargetValidator.validate(tableMeta, schemaName, tableName, columnName, originalType);

        boolean allowLock = executionContext.getParamManager().getBoolean(ConnectionParams.MCE_ADD_COLUMN_ALLOW_LOCK);
        List<McePhysicalTableResolver.PhysicalTableTarget> physicalTableTargets =
            McePhysicalTableResolver.resolve(schemaName, tableName, tableMeta, executionContext);

        boolean retry = getState() == DdlTaskState.DIRTY;
        if (!retry) {
            validateNoPhysicalContentColumn(physicalTableTargets);
            validateNoMetaDbContentColumn();
            updateTaskStateInNewTxn(DdlTaskState.DIRTY);
        }

        MceTaskRuntimeConfig runtimeConfig = new MceTaskRuntimeConfig(
            executionContext, LOG, getJobId(), getTaskId(), "add-content-physical-ddl");
        McePhysicalDdlExecutor.execute(
            physicalTableTargets,
            runtimeConfig,
            target -> {
                if (retry && findPhysicalColumn(target.storageInfo, target.physicalTable, columnName) != null) {
                    LOG.info(String.format("[MCE] Physical column '%s' already exists on %s.%s; "
                            + "skip it while retrying DIRTY task",
                        columnName, target.physicalDb, target.physicalTable));
                    return;
                }
                addContentColumnToPhysicalTable(
                    target.storageInfo, target.physicalDb, target.physicalTable, allowLock);
            },
            executionContext,
            LOG,
            "add-content-physical-ddl job=" + getJobId() + " task=" + getTaskId(),
            "Concurrent MCE content-column ADD failed: ");

        injectFailPointAfterInternalizeAddContent(executionContext);

        LOG.info(String.format("[MCE] MceAddContentColumnTask added column '%s' AFTER '%s' for %s.%s",
            columnName, addrColumnName, schemaName, tableName));
    }

    private static void injectFailPointAfterInternalizeAddContent(ExecutionContext executionContext) {
        MceTaskFailPoint.pauseWhileEnabled(MceTaskFailPoint.FP_MCE_INTERNALIZE_AFTER_ADD_CONTENT, executionContext);
    }

    private void validateNoPhysicalContentColumn(List<McePhysicalTableResolver.PhysicalTableTarget> targets) {
        for (McePhysicalTableResolver.PhysicalTableTarget target : targets) {
            if (findPhysicalColumn(target.storageInfo, target.physicalTable, columnName) != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                    "Internalize content column '%s' already exists on physical table %s.%s",
                    columnName, target.physicalDb, target.physicalTable));
            }
            if (findPhysicalColumn(target.storageInfo, target.physicalTable, addrColumnName) == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                    "Internalize addr column '%s' is missing on physical table %s.%s",
                    addrColumnName, target.physicalDb, target.physicalTable));
            }
        }
    }

    /**
     * The terminal layout must have exactly one columns record (the physical addr record) — a
     * stray {@code body} record would collide with the loader's addr→logical translation.
     */
    private void validateNoMetaDbContentColumn() {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(connection);
            if (!tableInfoManager.queryOneColumn(schemaName, tableName, columnName).isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                    "Internalize content column '%s' already exists in MetaDB columns for table %s.%s",
                    columnName, schemaName, tableName));
            }
            if (tableInfoManager.queryOneColumn(schemaName, tableName, addrColumnName).size() != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                    "Expected exactly one addr column record '%s' in MetaDB for table %s.%s",
                    addrColumnName, schemaName, tableName));
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("Failed to check internalize content column '%s' in MetaDB for table %s.%s: %s",
                    columnName, schemaName, tableName, e.getMessage()), e);
        }
    }

    /**
     * Add the content column to one physical table, probing algorithms in order:
     * INSTANT → INPLACE/LOCK=NONE → (allowLock ? default : error).
     * <p>
     * TEXT-family columns are pinned to utf8mb4: the forward EXTERNALIZE gate only accepted
     * utf8/utf8mb3/utf8mb4 sources, all of whose bytes round-trip through utf8mb4 unchanged,
     * while the table default charset may be something else entirely. The MD5 checker before
     * read cutover still verifies the restored bytes independently.
     */
    private void addContentColumnToPhysicalTable(OmcStorageInfo storageInfo, String phyDb, String phyTable,
                                                 boolean allowLock) {
        boolean isTextFamily = !originalType.toUpperCase().endsWith("BLOB");
        StringBuilder base = new StringBuilder();
        base.append(String.format("ALTER TABLE %s.%s ADD COLUMN %s %s",
            SqlIdentifier.surroundWithBacktick(phyDb),
            SqlIdentifier.surroundWithBacktick(phyTable),
            SqlIdentifier.surroundWithBacktick(columnName),
            originalType));
        if (isTextFamily) {
            base.append(" CHARACTER SET utf8mb4");
        }
        base.append(" NULL");
        if (TStringUtil.isNotEmpty(userComment)) {
            base.append(" COMMENT '").append(escapeStringLiteral(userComment)).append('\'');
        }
        base.append(" AFTER ").append(SqlIdentifier.surroundWithBacktick(addrColumnName));

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
            execute(storageInfo, base.toString());
            return;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
            "Internalize add column '%s' on %s.%s cannot run online (neither ALGORITHM=INSTANT nor "
                + "ALGORITHM=INPLACE,LOCK=NONE is supported by the DN). Set MCE_ADD_COLUMN_ALLOW_LOCK=true "
                + "to allow a locking DDL.", columnName, phyDb, phyTable));
    }

    private static String escapeStringLiteral(String value) {
        return value.replace("\\", "\\\\").replace("'", "\\'");
    }

    private ColumnsInfoSchemaRecord findPhysicalColumn(OmcStorageInfo storageInfo, String phyTable,
                                                       String targetColumn) {
        List<ColumnsInfoSchemaRecord> columns = OmcUtils.getColumnsInfo(storageInfo, phyTable);
        if (columns == null) {
            return null;
        }
        return columns.stream()
            .filter(c -> c.columnName != null && c.columnName.equalsIgnoreCase(targetColumn))
            .findFirst()
            .orElse(null);
    }

    private boolean tryExecute(OmcStorageInfo storageInfo, String sql) {
        try (Connection conn = OmcUtils.getPhysicalConnection(storageInfo);
            Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            return true;
        } catch (Exception e) {
            LOG.warn(String.format(
                "[MCE] physical add content column probe failed, will try next algorithm. sql=%s, err=%s",
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
                String.format("Failed to add content column via '%s': %s", sql, e.getMessage()), e);
        }
    }

    /**
     * Drop the physical content column from every partition on rollback. A missing column is an
     * idempotent retry; any real DROP failure pauses rollback for the next attempt. The terminal
     * externalized layout (metadata and objects) is untouched by this task, so a successful
     * rollback restores the exact pre-DDL state.
     */
    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        List<McePhysicalTableResolver.PhysicalTableTarget> physicalTableTargets =
            McePhysicalTableResolver.resolve(schemaName, tableName, tableMeta, executionContext);
        MceTaskRuntimeConfig runtimeConfig = new MceTaskRuntimeConfig(
            executionContext, LOG, getJobId(), getTaskId(), "rollback-add-content-physical-ddl");
        McePhysicalDdlExecutor.executeRollback(
            physicalTableTargets,
            runtimeConfig,
            this::dropContentColumnFromPhysicalTable,
            executionContext,
            LOG,
            "rollback-add-content-physical-ddl job=" + getJobId() + " task=" + getTaskId(),
            "Concurrent MCE content-column rollback DROP failed: ");
    }

    private void dropContentColumnFromPhysicalTable(
        McePhysicalTableResolver.PhysicalTableTarget target) {
        if (findPhysicalColumn(target.storageInfo, target.physicalTable, columnName) == null) {
            return;
        }
        String dropSql = String.format("ALTER TABLE %s.%s DROP COLUMN %s",
            SqlIdentifier.surroundWithBacktick(target.physicalDb),
            SqlIdentifier.surroundWithBacktick(target.physicalTable),
            SqlIdentifier.surroundWithBacktick(columnName));
        try (Connection conn = OmcUtils.getPhysicalConnection(target.storageInfo);
            Statement stmt = conn.createStatement()) {
            stmt.execute(dropSql);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("Failed to rollback internalize content column '%s' on %s.%s: %s",
                    columnName, target.physicalDb, target.physicalTable, e.getMessage()), e);
        }
    }

    @Override
    protected String remark() {
        return String.format("|MCE internalize ADD content column (physical AFTER %s), table=%s, column=%s",
            addrColumnName, tableName, columnName);
    }
}
