package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

/**
 * @author fangwu
 */
public class JdbcUrlRecordSyncAction implements ISyncAction {

    public static final String JDBC_CONNECTION = "JDBC_CONNECTION";

    public JdbcUrlRecordSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = buildResultCursor();

        for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc instanceof ServerConnection) {
                    ServerConnection c= (ServerConnection) fc;
                    if (c.getJdbcUrl() != null) {
                        long time = (System.nanoTime() - c.getLastActiveTime()) / 1000000000;
                        resultCursor.addRow(new Object[] {
                            c.getId(),
                            c.getUser(),
                            c.getHost() + ":" + c.getPort(),
                            c.getSchema(),
                            time,
                            c.getJdbcUrl()
                        });
                    }
                }
            }
        }
        return resultCursor;
    }

    public static ArrayResultCursor buildResultCursor() {

        ArrayResultCursor resultCursor = new ArrayResultCursor("JDBC_URL_SYNC_RESULT");
        resultCursor.addColumn("ID", DataTypes.LongType);
        resultCursor.addColumn("USER", DataTypes.StringType);
        resultCursor.addColumn("HOST", DataTypes.StringType);
        resultCursor.addColumn("DB", DataTypes.StringType);
        resultCursor.addColumn("TIME", DataTypes.LongType);
        resultCursor.addColumn("JDBC_CONNECTION", DataTypes.StringType);
        resultCursor.initMeta();
        return resultCursor;
    }
}
