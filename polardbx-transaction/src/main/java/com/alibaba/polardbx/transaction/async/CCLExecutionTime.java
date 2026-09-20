package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.gms.metadb.ccl.CclBlockerAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclBlockerRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.ccl.common.CclBlockerInfo;
import com.alibaba.polardbx.optimizer.ccl.common.CclSqlMetric;
import com.alibaba.polardbx.optimizer.ccl.common.CclSqlMetricChecker;
import com.alibaba.polardbx.optimizer.ccl.service.impl.CclBlockerService;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.transaction.TransactionLogger;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;

public class CCLExecutionTime implements Runnable {

    private static Class killSyncActionClass;

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            killSyncActionClass =
                Class.forName("com.alibaba.polardbx.server.response.KillSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    @Override
    public void run() {
        for (String schema : OptimizerHelper.getServerConfigManager().getLoadedSchemas()) {
            final ITransactionManager tm = ExecutorContext.getContext(schema).getTransactionManager();
            Collection<ITransaction> transactions = tm.getTransactions().values();
            List<CclBlockerRecord> executionTimeBlockers = null;
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                metaDbConn.setAutoCommit(false);
                CclBlockerAccessor cclBlockerAccessor = new CclBlockerAccessor();
                cclBlockerAccessor.setConnection(metaDbConn);
                executionTimeBlockers = cclBlockerAccessor.queryConditionLike("EXECUTION_TIME");
            } catch (Throwable throwable) {
                throw new TddlRuntimeException(ErrorCode.ERR_CCL, throwable.getMessage(), throwable);
            }
            for (ITransaction tran : transactions) {
                if (tran.getUser() == null) {
                    continue;
                }
                for (CclBlockerRecord blocker : executionTimeBlockers) {
                    String[] splits = tran.getUser().split("@");
                    String userName = splits[0];
                    String host = splits.length > 1 ? splits[1] : "%";
                    String schemaName = tran.getExecutionContext().getSchemaName();

                    if (!CclBlockerService.matchSchemaOrUser(blocker.schema, schemaName, blocker.user, blocker.host,
                        userName, host)) {
                        continue;
                    }

                    CclBlockerInfo cclBlockerInfo = CclBlockerInfo.create(blocker);

                    for (CclSqlMetricChecker checker : cclBlockerInfo.getCclSqlMetricCheckers()) {
                        long currentTime = System.currentTimeMillis();
                        long lastSqlStarTime = tran.getStartTimeInMs();
                        CclSqlMetric sqlMetric = new CclSqlMetric();
                        sqlMetric.setExecutionTime(currentTime - lastSqlStarTime);
                        if (!checker.check(sqlMetric)) {
                            continue;
                        }
                        long connId = tran.getExecutionContext().getConnId();
                        ISyncAction killSyncAction;
                        try {
                            TransactionLogger.warn(
                                "kill sql exceed CCL_EXECUTION_TIME " + Long.toHexString(tran.getId())
                                    + ", conn id "
                                    + connId);
                            killSyncAction =
                                (ISyncAction) killSyncActionClass
                                    .getConstructor(String.class, Long.TYPE, Boolean.TYPE, Boolean.TYPE,
                                        ErrorCode.class)
                                    // KillSyncAction(String user, long id, boolean killQuery, boolean skipValidation, ErrorCode cause)
                                    .newInstance("", connId, true, true,
                                        ErrorCode.ERR_SQL_EXCEED_CCL_EXECUTION_TIME);
                        } catch (Exception e) {
                            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
                        }
                        killSyncAction.sync();
                    }
                }
            }
        }
    }
}
