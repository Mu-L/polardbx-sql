package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.ExternalColumnTableIdResolver;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.cdc.entity.LogicMeta;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

/**
 * Internalize Task: atomically flip the terminal externalized single-record layout to the standard
 * READ_ADDR two-record layout in ONE MetaDB transaction, and move the control state
 * EXTERNALIZED → READ_ADDR. After the following TableSyncTask every CN dual-writes the column
 * (content plaintext + addr BlobRef) while reads keep going through FETCH_BLOB(addr).
 * <p>
 * The atomicity is mandatory: registering the content record in an earlier task would publish a
 * TableMeta version where the terminal {@code body_addr_} record (loader-translated to logical
 * {@code body}) coexists with the new {@code body} record — two ColumnMeta under one logical name.
 * {@code MceColumnStateResolver} also fail-closes any control row without a complete content+addr
 * record pair, so the control row must be born in the same transaction as the pair.
 * <p>
 * Layout before: {@code body_addr_} (PUBLIC, externalized flag, mapping=NULL) — single record.
 * Layout after (identical to forward READ_ADDR): {@code body} (PUBLIC, externalized flag,
 * mapping→addr) + {@code body_addr_} (WRITE_ONLY, no flag, mapping→content), control=READ_ADDR.
 * The {@code ext_column_mapping} row stays PUBLIC: staging rows keep resolving through it until
 * the final drop-addr task.
 */
@Getter
@TaskName(name = "MceInternalizeChangeWriteModeTask")
public class MceInternalizeChangeWriteModeTask extends BaseGmsTask {

    /**
     * MCE state-transition timeline goes to tddl.log (SLS-collected) instead of the
     * ddl-engine log, so migration troubleshooting survives log collection.
     */
    private static final Logger MCE_LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private final String tableName;
    private final String columnName;
    private final String addrColumnName;

    private static final com.alibaba.polardbx.common.utils.logger.Logger LOG =
        com.alibaba.polardbx.common.utils.logger.LoggerFactory.getLogger(MceInternalizeChangeWriteModeTask.class);

    @JSONCreator
    public MceInternalizeChangeWriteModeTask(String schemaName, String tableName, String columnName) {
        super(schemaName, tableName);
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        // Deliberately no super.beforeTransaction(): the version check fails inside the MCE
        // pipeline after earlier tasks bumped the table version (same as MceAddAddrColumnTask).
        injectFailPointBeforeInternalizeChangeWriteMode(executionContext);
    }

    private static void injectFailPointBeforeInternalizeChangeWriteMode(ExecutionContext executionContext) {
        MceTaskFailPoint.pauseWhileEnabled(MceTaskFailPoint.FP_MCE_INTERNALIZE_BEFORE_CHANGE_WRITE_MODE,
            executionContext);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        long t0 = System.currentTimeMillis();
        MCE_LOGGER.info(String.format("[MCE] MceInternalizeChangeWriteModeTask START table=%s.%s contentCol=%s",
            schemaName, tableName, columnName));

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        ColumnsAccessor columnsAccessor = new ColumnsAccessor();
        columnsAccessor.setConnection(metaDbConnection);

        // Strict preflight on the current terminal layout inside the transaction.
        List<ColumnsRecord> addrRecords = tableInfoManager.queryOneColumn(schemaName, tableName, addrColumnName);
        if (addrRecords.size() != 1 || !addrRecords.get(0).isExternalizedColumn()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                "Internalize expects exactly one externalized addr record '%s' for %s.%s",
                addrColumnName, schemaName, tableName));
        }
        boolean contentRegistered = !tableInfoManager.queryOneColumn(schemaName, tableName, columnName).isEmpty();

        if (!contentRegistered) {
            // 1) Register the physical content column added by MceAddContentColumnTask, pulling the
            //    authoritative type from the DN information_schema.
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
            McePhysicalTableResolver.PhysicalTableTarget sampleTarget =
                McePhysicalTableResolver.resolve(schemaName, tableName, tableMeta, executionContext).get(0);
            TableInfoManager.PhyInfoSchemaContext context =
                CommonMetaChanger.getPhyInfoSchemaContext(schemaName, tableName, sampleTarget.groupName,
                    sampleTarget.physicalTable);
            LogicMeta.LogicalTableMetaDetail detail = tableInfoManager.fetchLogicalTableMetaFromInfoSchema(context);
            tableInfoManager.addColumns(context, detail.getColumnsJdbcExtInfo(),
                Collections.singletonList(columnName), detail);

            // addColumns follows the generic ADD-at-tail contract, while MceAddContentColumnTask physically adds the
            // restored content column immediately AFTER the addr carrier. Mirror that physical order in MetaDB before
            // publishing the content column as PUBLIC; otherwise a following user column precedes content in
            // TableMeta and INSERT without an explicit column list binds their values in the wrong order.
            columnsAccessor.adjustColumnPosition(schemaName, tableName, columnName, addrColumnName);
        }

        // 2) Content record becomes the READ_ADDR content side: PUBLIC + externalized flag +
        //    mapping → addr.
        tableInfoManager.updateColumnStatus(schemaName, tableName,
            Collections.singletonList(columnName), ColumnStatus.PUBLIC.getValue());
        tableInfoManager.setExternalizedColumnFlag(schemaName, tableName, columnName);
        tableInfoManager.updateColumnMappingName(schemaName, tableName, columnName, addrColumnName);

        // 3) Addr record demotes to the READ_ADDR addr side: WRITE_ONLY + no flag + mapping → content.
        columnsAccessor.resetColumnFlag(schemaName, tableName, addrColumnName,
            ColumnsRecord.FLAG_EXTERNALIZED_COLUMN);
        tableInfoManager.updateColumnStatus(schemaName, tableName,
            Collections.singletonList(addrColumnName), ColumnStatus.WRITE_ONLY.getValue());
        tableInfoManager.updateColumnMappingName(schemaName, tableName, addrColumnName, columnName);

        // 4) Control row is born READ_ADDR in the same transaction as the two-record layout.
        MceColumnStateAccessor mceAccessor = new MceColumnStateAccessor();
        mceAccessor.setConnection(metaDbConnection);
        MceColumnStateRecord record = new MceColumnStateRecord();
        record.setJobId(getJobId() == null ? 0L : getJobId());
        record.setTaskId(getTaskId() == null ? 0L : getTaskId());
        record.setTableSchema(schemaName);
        record.setTableName(tableName);
        record.setColumnName(columnName);
        record.setAddrColumnName(addrColumnName);
        record.setState(MceColumnStateRecord.STATE_READ_ADDR);
        record.setStatus(MceColumnStateRecord.STATUS_RUNNING);
        record.setPhysicalDb("");
        record.setPhysicalTable("");
        record.setPartitionName("");
        MceControlStateHelper.insertOrValidateReadAddr(mceAccessor, record);

        ExternalColumnTableIdResolver.getInstance().invalidateForTable(schemaName, tableName);

        MCE_LOGGER.info(
            String.format("[MCE] MceInternalizeChangeWriteModeTask DONE table=%s.%s → READ_ADDR (cost %sms)",
                schemaName, tableName, System.currentTimeMillis() - t0));
    }

    /**
     * Flip the two-record layout back to the terminal single-record layout in one transaction.
     * Physical columns are untouched here; MceAddContentColumnTask's rollback drops them.
     */
    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        ColumnsAccessor columnsAccessor = new ColumnsAccessor();
        columnsAccessor.setConnection(metaDbConnection);

        tableInfoManager.removeColumns(schemaName, tableName, Collections.singletonList(columnName));
        // The forward metadata flip shifted columns following addr to make room for content. Compact them after
        // removing content so rollback restores the exact terminal externalized column order.
        tableInfoManager.resetColumnOrder(schemaName, tableName);

        tableInfoManager.setExternalizedColumnFlag(schemaName, tableName, addrColumnName);
        tableInfoManager.updateColumnStatus(schemaName, tableName,
            Collections.singletonList(addrColumnName), ColumnStatus.PUBLIC.getValue());
        tableInfoManager.updateColumnMappingName(schemaName, tableName, addrColumnName, null);

        MceColumnStateAccessor mceAccessor = new MceColumnStateAccessor();
        mceAccessor.setConnection(metaDbConnection);
        MceControlStateHelper.deleteIfPresent(mceAccessor, getJobId() == null ? 0L : getJobId(),
            schemaName, tableName, columnName, addrColumnName, MceColumnStateRecord.STATE_READ_ADDR);

        ExternalColumnTableIdResolver.getInstance().invalidateForTable(schemaName, tableName);

        LOG.info(String.format("[MCE] MceInternalizeChangeWriteModeTask rolled back %s.%s.%s to terminal layout",
            schemaName, tableName, columnName));
    }

    @Override
    protected String remark() {
        return String.format("|MCE internalize EXTERNALIZED→READ_ADDR (atomic layout flip), table=%s, column=%s",
            tableName, columnName);
    }
}
