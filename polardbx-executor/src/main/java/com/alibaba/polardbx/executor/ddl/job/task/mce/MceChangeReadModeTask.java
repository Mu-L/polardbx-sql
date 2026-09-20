package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * MCE Task: transition column-level MCE state from DUAL_WRITE → READ_ADDR in MetaDB.
 *
 * <p>In this state the content column is marked as externalized and its
 * {@code column_mapping_name} points to the addr column. After the following
 * TableSyncTask, {@code ToDrdsRelVisitor.resolveExternalizedColumns()} auto-injects
 * FETCH_BLOB above every LogicalView reading the content column.
 */
@Getter
@TaskName(name = "MceChangeReadModeTask")
public class MceChangeReadModeTask extends BaseGmsTask {

    /**
     * MCE state-transition timeline goes to tddl.log (SLS-collected) instead of the
     * ddl-engine log, so migration troubleshooting survives log collection.
     */
    private static final Logger MCE_LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private final String tableName;
    private final String columnName;
    private final String addrColumnName;

    public MceChangeReadModeTask(String schemaName, String tableName, String columnName) {
        super(schemaName, tableName);
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = columnName + "_addr_";
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        long t0 = System.currentTimeMillis();
        MCE_LOGGER.info(String.format("[MCE] MceChangeReadModeTask START table=%s.%s",
            schemaName, tableName));

        // READ_ADDR is the point of no return. Disable CANCEL in the same MetaDB transaction as
        // the flag, mapping and control-state cutover so no cancelable READ_ADDR window exists.
        updateSupportedCommands(true, false, metaDbConnection);

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        // Mark content column as externalized; point its mapping to the addr column
        // so the read path auto-injects FETCH_BLOB(addr) and rewrites column names.
        tableInfoManager.setExternalizedColumnFlag(schemaName, tableName, columnName);
        tableInfoManager.updateColumnMappingName(schemaName, tableName, columnName, addrColumnName);

        // Advance column-level MCE state to READ_ADDR (sole authoritative signal).
        com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor mceAccessor =
            new com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor();
        mceAccessor.setConnection(metaDbConnection);
        MceControlStateHelper.transition(mceAccessor, getJobId() == null ? 0L : getJobId(),
            schemaName, tableName, columnName, addrColumnName,
            com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord.STATE_DUAL_WRITE,
            com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord.STATE_READ_ADDR);

        MCE_LOGGER.info(String.format("[MCE] MceChangeReadModeTask DONE table=%s.%s → READ_ADDR (cost %sms)",
            schemaName, tableName, System.currentTimeMillis() - t0));
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // Not rollbackable after cutover (Phase 7+); see plan doc.
    }

    @Override
    protected String remark() {
        return String.format("|MCE DUAL_WRITE→READ_ADDR, table=%s, column=%s→%s",
            tableName, columnName, addrColumnName);
    }
}
