package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.omc.OmcStorageInfo;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.sql.SqlIdentifier;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MCE Task: Drop the original content column from physical tables AND fix MetaDB metadata.
 * <p>
 * After cutover, the physical table has: [..., content_addr_, ..., content (at end)].
 * This task:
 * 1. Drops the physical 'content' column from all DN partitions
 * 2. Removes the 'content' row from MetaDB columns table (it's now replaced by content_addr_)
 * <p>
 * Runs after {@link MceChangeWriteOnlyAddrTask} and its table sync, when all CNs are in ADDR_ONLY
 * and no longer write the content column. A final table sync follows this task.
 * <p>
 * Must use physical DDL (not logical DDL SubJob) because after metadata registration,
 * CN maps logical 'body' to physical 'body_addr_', causing wrong column to be dropped.
 */
@Getter
@TaskName(name = "MceDropContentColumnTask")
public class MceDropContentColumnTask extends BaseGmsTask {

    /**
     * MCE state-transition timeline goes to tddl.log (SLS-collected) instead of the
     * ddl-engine log, so migration troubleshooting survives log collection.
     */
    private static final Logger MCE_LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private final String tableName;
    private final String columnName;  // e.g. "body" — the original content column to remove
    private final String addrColumnName; // e.g. "body_addr_" — becomes the externalized backing column
    private final long versionId;

    private static final com.alibaba.polardbx.common.utils.logger.Logger LOG =
        com.alibaba.polardbx.common.utils.logger.LoggerFactory.getLogger(MceDropContentColumnTask.class);

    @JSONCreator
    public MceDropContentColumnTask(String schemaName, String tableName, String columnName, long versionId) {
        super(schemaName, tableName);
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
        this.versionId = versionId;
    }

    @Override
    protected int getMetaDbDeadlockMaxAttempts() {
        return 3;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        long t0 = System.currentTimeMillis();
        MCE_LOGGER.info(String.format("[MCE] MceDropContentColumnTask.executeImpl START table=%s.%s",
            schemaName, tableName));
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        // Step 1: Query body's current ordinal_position (to assign to body_addr_ later)
        com.alibaba.polardbx.gms.metadb.table.ColumnsAccessor columnsAccessor =
            new com.alibaba.polardbx.gms.metadb.table.ColumnsAccessor();
        columnsAccessor.setConnection(metaDbConnection);

        List<com.alibaba.polardbx.gms.metadb.table.ColumnsRecord> bodyRecords =
            columnsAccessor.query(schemaName, tableName, columnName);
        if (bodyRecords.isEmpty()) {
            setExceptionAction(DdlExceptionAction.PAUSE);
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                "[MCE] cannot determine ordinal_position before externalize cutover: "
                    + "schema=%s, table=%s, column=%s, addrColumn=%s, jobId=%s. "
                    + "The content column record is missing from MetaDB; pause instead of using "
                    + "a guessed ordinal and corrupting column order.",
                schemaName, tableName, columnName, addrColumnName, jobId));
        }
        long bodyOrdinalPosition = bodyRecords.get(0).ordinalPosition;
        LOG.info(String.format("[MCE] Column '%s' ordinal_position=%d, will assign to '%s_addr_'",
            columnName, bodyOrdinalPosition, columnName));

        // Step 2: Remove body column record from MetaDB (columns + indexes + column_evolution + column_mapping)
        tableInfoManager.removeColumns(schemaName, tableName, java.util.Collections.singletonList(columnName));
        LOG.info(String.format("[MCE] Removed MetaDB column record: %s.%s.%s",
            schemaName, tableName, columnName));

        // Step 3: Update body_addr_'s ordinal_position to body's original position.
        // After cutover, body_addr_ physically occupies body's original slot.
        columnsAccessor.updateNewColumnPosition(schemaName, tableName, addrColumnName, bodyOrdinalPosition);
        LOG.info(String.format("[MCE] Updated '%s' ordinal_position to %d in MetaDB",
            addrColumnName, bodyOrdinalPosition));

        // Step 4-6: Register externalize metadata IN THE SAME METADB TRANSACTION so that
        // CN sees an atomic transition from "body is a regular column" to "body is
        // externalized on top of body_addr_". The blob-column mapping was already registered
        // by RegisterBlobColumnMappingTask earlier in the pipeline.
        tableInfoManager.updateColumnMappingName(schemaName, tableName, addrColumnName, null);
        tableInfoManager.setExternalizedColumnFlag(schemaName, tableName, addrColumnName);
        tableInfoManager.updateColumnStatus(schemaName, tableName,
            Collections.singletonList(addrColumnName), ColumnStatus.PUBLIC.getValue());
        TableMetaChanger.changeColumnarTableMeta(metaDbConnection, schemaName, tableName,
            Collections.singletonList(addrColumnName), Collections.singletonList(columnName), Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList(), versionId, jobId);

        com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor mceAccessor =
            new com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor();
        mceAccessor.setConnection(metaDbConnection);
        long currentJobId = getJobId() == null ? 0L : getJobId();
        MceControlStateHelper.delete(mceAccessor, currentJobId, schemaName, tableName, columnName, addrColumnName,
            com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord.STATE_EXTERNALIZED);
        mceAccessor.deleteCheckpointsByJobAndColumn(currentJobId, schemaName, tableName, columnName);
        LOG.info(String.format(
            "[MCE] Registered externalize metadata (addr=%s → logical %s) and refreshed columnar schema",
            addrColumnName, columnName));
        MCE_LOGGER.info(
            String.format("[MCE] MceDropContentColumnTask.executeImpl DONE table=%s.%s cleanup complete (cost %sms)",
                schemaName, tableName, System.currentTimeMillis() - t0));
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        injectFailPointBeforeDropContent(executionContext);
        long t0 = System.currentTimeMillis();
        MCE_LOGGER.info(
            String.format("[MCE] MceDropContentColumnTask.beforeTransaction START (physical DROP body) table=%s.%s",
                schemaName, tableName));
        // Step 2: Drop the physical 'content' column from all DN partitions.
        // This must happen outside the MetaDB transaction (physical DDL cannot be in the same txn).
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        List<McePhysicalTableResolver.PhysicalTableTarget> physicalTableTargets =
            McePhysicalTableResolver.resolve(schemaName, tableName, tableMeta, executionContext);

        LOG.info(String.format("[MCE] MceDropContentColumnTask: dropping column '%s' from %d physical tables",
            columnName, physicalTableTargets.size()));

        boolean retry = getState() == DdlTaskState.DIRTY;
        if (!retry) {
            validateAllPhysicalContentColumnsPresent(physicalTableTargets);
            updateTaskStateInNewTxn(DdlTaskState.DIRTY);
        }

        AtomicInteger processedPhysicalTables = new AtomicInteger();
        MceTaskRuntimeConfig runtimeConfig = new MceTaskRuntimeConfig(
            executionContext, LOG, getJobId(), getTaskId(), "drop-content-physical-ddl");
        McePhysicalDdlExecutor.execute(
            physicalTableTargets,
            runtimeConfig,
            target -> dropContentColumnFromPhysicalTable(
                target, retry, processedPhysicalTables, executionContext),
            executionContext,
            LOG,
            "drop-content-physical-ddl job=" + getJobId() + " task=" + getTaskId(),
            "Concurrent MCE content-column DROP failed: ");

        LOG.info(String.format("[MCE] MceDropContentColumnTask completed for %s.%s", schemaName, tableName));
        MCE_LOGGER.info(String.format(
            "[MCE] MceDropContentColumnTask.beforeTransaction DONE (physical DROP body) table=%s.%s cost=%sms",
            schemaName, tableName, System.currentTimeMillis() - t0));
    }

    private static void injectFailPointBeforeDropContent(ExecutionContext executionContext) {
        MceTaskFailPoint.pauseWhileEnabled(MceTaskFailPoint.FP_MCE_BEFORE_DROP_CONTENT, executionContext);
    }

    private static void injectFailPointAfterDropContentPartition(int processedPhysicalTables,
                                                                 ExecutionContext executionContext) {
        MceTaskFailPoint.failAfterPhysicalTables(
            MceTaskFailPoint.FP_MCE_AFTER_DROP_CONTENT_PARTITION, processedPhysicalTables, executionContext);
    }

    private void dropContentColumnFromPhysicalTable(
        McePhysicalTableResolver.PhysicalTableTarget target,
        boolean retry,
        AtomicInteger processedPhysicalTables,
        ExecutionContext executionContext) {
        if (retry && !isPhysicalContentColumnPresent(target.storageInfo, target.physicalTable)) {
            LOG.info(String.format("[MCE] Physical column '%s' is already absent from %s.%s; "
                    + "skip it while retrying DIRTY task",
                columnName, target.physicalDb, target.physicalTable));
            return;
        }

        String dropColSql = String.format("ALTER TABLE %s.%s DROP COLUMN %s",
            SqlIdentifier.surroundWithBacktick(target.physicalDb),
            SqlIdentifier.surroundWithBacktick(target.physicalTable),
            SqlIdentifier.surroundWithBacktick(columnName));
        try (Connection conn = OmcUtils.getPhysicalConnection(target.storageInfo);
            Statement stmt = conn.createStatement()) {
            stmt.execute(dropColSql);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("Failed to drop column '%s' from %s.%s: %s",
                    columnName, target.physicalDb, target.physicalTable, e.getMessage()), e);
        }

        LOG.info(String.format("[MCE] Dropped physical column '%s' from %s.%s",
            columnName, target.physicalDb, target.physicalTable));
        injectFailPointAfterDropContentPartition(processedPhysicalTables.incrementAndGet(), executionContext);
    }

    private void validateAllPhysicalContentColumnsPresent(
        List<McePhysicalTableResolver.PhysicalTableTarget> physicalTableTargets) {
        for (McePhysicalTableResolver.PhysicalTableTarget target : physicalTableTargets) {
            if (!isPhysicalContentColumnPresent(target.storageInfo, target.physicalTable)) {
                throw new com.alibaba.polardbx.common.exception.TddlRuntimeException(
                    com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("MCE content column '%s' is missing from physical table %s.%s before "
                            + "MceDropContentColumnTask starts",
                        columnName, target.physicalDb, target.physicalTable));
            }
        }
    }

    private boolean isPhysicalContentColumnPresent(OmcStorageInfo storageInfo, String phyTable) {
        List<ColumnsInfoSchemaRecord> columns = OmcUtils.getColumnsInfo(storageInfo, phyTable);
        return columns != null && columns.stream()
            .anyMatch(c -> c.columnName != null && c.columnName.equalsIgnoreCase(columnName));
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // Not rollbackable after ADDR_ONLY state
    }

    @Override
    protected String remark() {
        return String.format("|MCE DROP content column (physical+meta), table=%s, column=%s",
            tableName, columnName);
    }
}
