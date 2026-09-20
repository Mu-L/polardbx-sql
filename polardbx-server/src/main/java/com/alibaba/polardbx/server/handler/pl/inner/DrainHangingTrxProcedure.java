package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

public class DrainHangingTrxProcedure extends BaseInnerProcedure {
    private static final Logger logger = LoggerFactory.getLogger(DrainHangingTrxProcedure.class);

    @Override
    void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        cursor.addColumn("RESULT", DataTypes.StringType);
        cursor.addColumn("MESSAGE", DataTypes.StringType);
        List<SQLExpr> params = statement.getParameters();
        long timeoutMilli;
        if (params.isEmpty()) {
            timeoutMilli = Long.MAX_VALUE;
        } else {
            timeoutMilli = Long.parseLong(params.get(0).toString());
        }

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMilli) {
            ITopologyExecutor executor = ExecutorContext.getContext(DEFAULT_DB_NAME).getTopologyExecutor();
            ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
            Set<String> dnIds = StorageHaManager.getAllDnId(true);
            boolean exists = ExecUtils.existsHangingTrx(dnIds, executor, exceptions);
            if (!exceptions.isEmpty()) {
                exceptions.forEach(logger::warn);
                cursor.addRow(new Object[] {"FAIL", exceptions.peek().getMessage()});
                return;
            }
            if (exists) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    cursor.addRow(new Object[] {"FAIL", "Query interrupted."});
                    return;
                }
            } else {
                cursor.addRow(new Object[] {"OK", ""});
                return;
            }
        }

        cursor.addRow(new Object[] {"FAIL", "Timeout."});
    }
}
