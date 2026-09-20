package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.ExtColumnMappingManager;
import com.alibaba.polardbx.executor.columnar.ExternalColumnTableIdResolver;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.omc.OmcStorageInfo;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
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

/**
 * Internalize Task: drop the addr column from physical tables AND commit the final MetaDB cutover
 * to a plain column — the reverse of {@link MceDropContentColumnTask}.
 * <p>
 * READY preflight (before DIRTY, before any physical mutation):
 * <ol>
 *   <li>every physical addr column must still exist;</li>
 * </ol>
 * Then DIRTY + idempotent per-partition physical DROP, and one final MetaDB transaction: remove
 * the addr record, move the content record to the addr's original ordinal, delete the PUBLIC
 * {@code ext_column_mapping} row, refresh the columnar evolution back to the original type, and
 * delete the control row / checkpoints. Staging rows remain independently flushable because they
 * carry both table_id and blob_addr and the flusher does not resolve ext_column_mapping.
 */
@Getter
@TaskName(name = "MceDropAddrColumnTask")
public class MceDropAddrColumnTask extends BaseGmsTask {

    /**
     * MCE state-transition timeline goes to tddl.log (SLS-collected) instead of the
     * ddl-engine log, so migration troubleshooting survives log collection.
     */
    private static final Logger MCE_LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private final String tableName;
    private final String columnName;      // restored plaintext column, e.g. "body"
    private final String addrColumnName;  // physical addr column to drop, e.g. "body_addr_"
    private final long versionId;

    private static final com.alibaba.polardbx.common.utils.logger.Logger LOG =
        com.alibaba.polardbx.common.utils.logger.LoggerFactory.getLogger(MceDropAddrColumnTask.class);

    @JSONCreator
    public MceDropAddrColumnTask(String schemaName, String tableName, String columnName, long versionId) {
        super(schemaName, tableName);
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
        this.versionId = versionId;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        injectFailPointBeforeInternalizeDropAddr(executionContext);
        long t0 = System.currentTimeMillis();
        MCE_LOGGER.info(
            String.format("[MCE] MceDropAddrColumnTask.beforeTransaction START (physical DROP addr) table=%s.%s",
                schemaName, tableName));

        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        List<McePhysicalTableResolver.PhysicalTableTarget> physicalTableTargets =
            McePhysicalTableResolver.resolve(schemaName, tableName, tableMeta, executionContext);

        boolean retry = getState() == DdlTaskState.DIRTY;
        if (!retry) {
            validateAllPhysicalAddrColumnsPresent(physicalTableTargets);
            updateTaskStateInNewTxn(DdlTaskState.DIRTY);
        }

        MceTaskRuntimeConfig runtimeConfig = new MceTaskRuntimeConfig(
            executionContext, LOG, getJobId(), getTaskId(), "drop-addr-physical-ddl");
        McePhysicalDdlExecutor.execute(
            physicalTableTargets,
            runtimeConfig,
            target -> dropAddrColumnFromPhysicalTable(target, retry),
            executionContext,
            LOG,
            "drop-addr-physical-ddl job=" + getJobId() + " task=" + getTaskId(),
            "Concurrent MCE addr-column DROP failed: ");

        MCE_LOGGER.info(String.format(
            "[MCE] MceDropAddrColumnTask.beforeTransaction DONE (physical DROP addr) table=%s.%s cost=%sms",
            schemaName, tableName, System.currentTimeMillis() - t0));
    }

    private static void injectFailPointBeforeInternalizeDropAddr(ExecutionContext executionContext) {
        MceTaskFailPoint.pauseWhileEnabled(MceTaskFailPoint.FP_MCE_INTERNALIZE_BEFORE_DROP_ADDR, executionContext);
    }

    private void dropAddrColumnFromPhysicalTable(
        McePhysicalTableResolver.PhysicalTableTarget target, boolean retry) {
        if (retry && !isPhysicalAddrColumnPresent(target.storageInfo, target.physicalTable)) {
            LOG.info(String.format("[MCE] Physical addr column '%s' is already absent from %s.%s; "
                    + "skip it while retrying DIRTY task",
                addrColumnName, target.physicalDb, target.physicalTable));
            return;
        }

        String dropColSql = String.format("ALTER TABLE %s.%s DROP COLUMN %s",
            SqlIdentifier.surroundWithBacktick(target.physicalDb),
            SqlIdentifier.surroundWithBacktick(target.physicalTable),
            SqlIdentifier.surroundWithBacktick(addrColumnName));
        try (Connection conn = OmcUtils.getPhysicalConnection(target.storageInfo);
            Statement stmt = conn.createStatement()) {
            stmt.execute(dropColSql);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("Failed to drop addr column '%s' from %s.%s: %s",
                    addrColumnName, target.physicalDb, target.physicalTable, e.getMessage()), e);
        }
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        long t0 = System.currentTimeMillis();
        MCE_LOGGER.info(String.format("[MCE] MceDropAddrColumnTask.executeImpl START table=%s.%s",
            schemaName, tableName));
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        ColumnsAccessor columnsAccessor = new ColumnsAccessor();
        columnsAccessor.setConnection(metaDbConnection);

        // Step 1: the addr record's ordinal is the column's original position; the restored
        // content column takes it back after the addr record is removed.
        List<ColumnsRecord> addrRecords = columnsAccessor.query(schemaName, tableName, addrColumnName);
        if (addrRecords.isEmpty()) {
            setExceptionAction(DdlExceptionAction.PAUSE);
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                "[MCE] cannot determine ordinal_position before internalize cutover: "
                    + "schema=%s, table=%s, column=%s, addrColumn=%s, jobId=%s. "
                    + "The addr column record is missing from MetaDB; pause instead of using "
                    + "a guessed ordinal and corrupting column order.",
                schemaName, tableName, columnName, addrColumnName, jobId));
        }
        long addrOrdinalPosition = addrRecords.get(0).ordinalPosition;

        // Step 2: remove the addr record and give its ordinal to the content record.
        tableInfoManager.removeColumns(schemaName, tableName, Collections.singletonList(addrColumnName));
        columnsAccessor.updateNewColumnPosition(schemaName, tableName, columnName, addrOrdinalPosition);
        // Internalize registered content immediately after addr and shifted all following columns by one. Compact the
        // remaining records after addr is removed so the terminal logical order matches the terminal physical order.
        tableInfoManager.resetColumnOrder(schemaName, tableName);

        // Step 3: the terminal plain column no longer needs an external-column namespace. Staging
        // flush is self-contained (table_id + blob_addr + payload), so it remains valid after this
        // row is gone. A future unified purge mechanism will own orphan object discovery.
        ExtColumnMappingManager.getInstance()
            .deletePublicByColumn(metaDbConnection, schemaName, tableName, columnName);

        // Step 4: refresh the columnar evolution back to the original plain form.
        TableMetaChanger.changeColumnarTableMeta(metaDbConnection, schemaName, tableName,
            Collections.singletonList(columnName), Collections.singletonList(addrColumnName),
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), versionId, jobId);

        // Step 5: drop the control row and checkpoints.
        MceColumnStateAccessor mceAccessor = new MceColumnStateAccessor();
        mceAccessor.setConnection(metaDbConnection);
        long currentJobId = getJobId() == null ? 0L : getJobId();
        MceControlStateHelper.delete(mceAccessor, currentJobId, schemaName, tableName, columnName,
            addrColumnName, MceColumnStateRecord.STATE_NONE);
        mceAccessor.deleteCheckpointsByJobAndColumn(currentJobId, schemaName, tableName, columnName);

        ExternalColumnTableIdResolver.getInstance().invalidateForTable(schemaName, tableName);
        MCE_LOGGER.info(String.format("[MCE] MceDropAddrColumnTask.executeImpl DONE table=%s.%s (cost %sms)",
            schemaName, tableName, System.currentTimeMillis() - t0));
    }

    private void validateAllPhysicalAddrColumnsPresent(
        List<McePhysicalTableResolver.PhysicalTableTarget> physicalTableTargets) {
        for (McePhysicalTableResolver.PhysicalTableTarget target : physicalTableTargets) {
            if (!isPhysicalAddrColumnPresent(target.storageInfo, target.physicalTable)) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("Internalize addr column '%s' is missing from physical table %s.%s before "
                            + "MceDropAddrColumnTask starts",
                        addrColumnName, target.physicalDb, target.physicalTable));
            }
        }
    }

    private boolean isPhysicalAddrColumnPresent(OmcStorageInfo storageInfo, String phyTable) {
        List<ColumnsInfoSchemaRecord> columns = OmcUtils.getColumnsInfo(storageInfo, phyTable);
        return columns != null && columns.stream()
            .anyMatch(c -> c.columnName != null && c.columnName.equalsIgnoreCase(addrColumnName));
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // Not rollbackable; DIRTY + CONTINUE converges forward.
    }

    @Override
    protected String remark() {
        return String.format("|MCE internalize DROP addr column (physical+meta), table=%s, column=%s",
            tableName, addrColumnName);
    }
}
