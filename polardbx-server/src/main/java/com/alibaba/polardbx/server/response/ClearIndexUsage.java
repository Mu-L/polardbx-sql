package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.server.ServerConnection;

/**
 * Clear index usage statistics from all DN nodes
 */
public final class ClearIndexUsage {

    public static boolean response(ServerConnection c, boolean hasMore) {
        // Get schema
        String db = c.getSchema();
        if (db == null) {
            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return false;
        }

        SchemaConfig schema = c.getSchemaConfig();
        if (schema == null) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return false;
        }

        TDataSource ds = schema.getDataSource();
        if (!ds.isInited()) {
            try {
                ds.init();
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_HANDLE_DATA, e);
                return false;
            }
        }

        OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());

        // Execute sync action directly on current node only (not broadcasting to other CN nodes)
        // This is similar to how SyncHandler.java works - directly calling action.sync()
        ClearIndexUsageSyncAction clearAction = new ClearIndexUsageSyncAction(db);
        clearAction.sync();

        PacketOutputProxyFactory.getInstance().createProxy(c)
            .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
        return true;
    }
}
