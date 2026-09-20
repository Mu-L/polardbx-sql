package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.ExtColumnMappingManager;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.ExtColumnMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

@Getter
@TaskName(name = "MceChangeWriteModeTask")
public class MceChangeWriteModeTask extends BaseGmsTask {

    /**
     * MCE state-transition timeline goes to tddl.log (SLS-collected) instead of the
     * ddl-engine log, so migration troubleshooting survives log collection.
     */
    private static final Logger MCE_LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private final String tableName;
    private final String columnName;
    private final String addrColumnName;

    public MceChangeWriteModeTask(String schemaName, String tableName, String columnName) {
        super(schemaName, tableName);
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = columnName + "_addr_";
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        super.beforeTransaction(executionContext);
        injectFailPointBeforeChangeWriteMode(executionContext);
    }

    private static void injectFailPointBeforeChangeWriteMode(ExecutionContext executionContext) {
        MceTaskFailPoint.pauseWhileEnabled(MceTaskFailPoint.FP_MCE_BEFORE_CHANGE_WRITE_MODE, executionContext);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        long t0 = System.currentTimeMillis();
        MCE_LOGGER.info(String.format("[MCE] MceChangeWriteModeTask START table=%s.%s addrCol=%s",
            schemaName, tableName, addrColumnName));

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        validateColumnMapping(metaDbConnection);

        tableInfoManager.updateColumnMappingName(schemaName, tableName, addrColumnName, columnName);
        tableInfoManager.updateColumnStatus(schemaName, tableName,
            Collections.singletonList(addrColumnName), ColumnStatus.WRITE_ONLY.getValue());

        com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor mceAccessor =
            new com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor();
        mceAccessor.setConnection(metaDbConnection);
        MceControlStateHelper.transition(mceAccessor, getJobId() == null ? 0L : getJobId(),
            schemaName, tableName, columnName, addrColumnName,
            com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord.STATE_NONE,
            com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord.STATE_DUAL_WRITE);

        MCE_LOGGER.info(String.format("[MCE] MceChangeWriteModeTask DONE table=%s.%s → DUAL_WRITE (cost %sms)",
            schemaName, tableName, System.currentTimeMillis() - t0));
    }

    private void validateColumnMapping(Connection metaDbConnection) {
        List<ExtColumnMappingRecord> publicMappings =
            ExtColumnMappingManager.getInstance().queryPublic(metaDbConnection, schemaName, tableName, columnName);
        if (publicMappings.size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("MCE requires exactly one PUBLIC ext-column mapping before entering DUAL_WRITE for "
                        + "%s.%s.%s, found %d",
                    schemaName, tableName, columnName, publicMappings.size()));
        }
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        tableInfoManager.updateColumnStatus(schemaName, tableName,
            Collections.singletonList(addrColumnName), ColumnStatus.ABSENT.getValue());
        com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor mceAccessor =
            new com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor();
        mceAccessor.setConnection(metaDbConnection);
        MceControlStateHelper.transition(mceAccessor, getJobId() == null ? 0L : getJobId(),
            schemaName, tableName, columnName, addrColumnName,
            com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord.STATE_DUAL_WRITE,
            com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord.STATE_NONE);
    }

    @Override
    protected String remark() {
        return String.format("|MCE INIT→DUAL_WRITE, table=%s, column=%s→%s",
            tableName, columnName, addrColumnName);
    }
}
