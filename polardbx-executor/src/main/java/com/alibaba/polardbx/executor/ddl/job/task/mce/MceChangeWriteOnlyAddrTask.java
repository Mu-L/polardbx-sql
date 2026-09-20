package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * MCE Task: the column enters the terminal EXTERNALIZED state.
 *
 * <p>Concretely: persist the column's control row as EXTERNALIZED. The control row is retained
 * until MceDropContentColumnTask commits the final MetaDB cutover, so a missing READ_ADDR row can
 * no longer be mistaken for a legal terminal state.
 *
 * <p>The subsequent MceDropContentColumnTask physically drops the content column and removes the
 * control/checkpoint rows in its final MetaDB transaction.
 */
@Getter
@TaskName(name = "MceChangeWriteOnlyAddrTask")
public class MceChangeWriteOnlyAddrTask extends BaseGmsTask {

    /**
     * MCE state-transition timeline goes to tddl.log (SLS-collected) instead of the
     * ddl-engine log, so migration troubleshooting survives log collection.
     */
    private static final Logger MCE_LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private final String tableName;
    private final String columnName;
    private final String addrColumnName;

    public MceChangeWriteOnlyAddrTask(String schemaName, String tableName, String columnName) {
        super(schemaName, tableName);
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = columnName + "_addr_";
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        super.beforeTransaction(executionContext);
        injectFailPointBeforeWriteOnlyAddr(executionContext);
    }

    private static void injectFailPointBeforeWriteOnlyAddr(ExecutionContext executionContext) {
        MceTaskFailPoint.pauseWhileEnabled(MceTaskFailPoint.FP_MCE_BEFORE_WRITE_ONLY_ADDR, executionContext);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        MCE_LOGGER.info(String.format("[MCE] MceChangeWriteOnlyAddrTask START table=%s.%s (enter EXTERNALIZED)",
            schemaName, tableName));

        com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor mceAccessor =
            new com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor();
        mceAccessor.setConnection(metaDbConnection);
        MceControlStateHelper.transition(mceAccessor, getJobId() == null ? 0L : getJobId(),
            schemaName, tableName, columnName, addrColumnName,
            com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord.STATE_READ_ADDR,
            com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord.STATE_EXTERNALIZED);

        MCE_LOGGER.info(String.format("[MCE] MceChangeWriteOnlyAddrTask DONE table=%s.%s → EXTERNALIZED",
            schemaName, tableName));
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // Not rollbackable after cutover
    }

    @Override
    protected String remark() {
        return String.format("|MCE READ_ADDR→ADDR_ONLY, table=%s, column=%s", tableName, columnName);
    }
}
