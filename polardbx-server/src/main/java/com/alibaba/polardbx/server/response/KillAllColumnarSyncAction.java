package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

/**
 * Kill all frontend connections on a columnar read-only instance.
 * <p>
 * Since columnar instances have no DN physical connections, the standard killall()
 * logic (which iterates DN groups and sends KILL SQL) does not work. This SyncAction
 * directly closes CN frontend connections instead.
 * <p>
 * Invoked from LogicalKillHandler via Class.forName + SyncManagerHelper
 * (executor module cannot directly depend on server module).
 *
 * @see KillSyncAction
 */
public class KillAllColumnarSyncAction implements ISyncAction {

    private static final Logger logger = LoggerFactory.getLogger(KillAllColumnarSyncAction.class);

    private long currentConnId;

    public KillAllColumnarSyncAction() {
    }

    public KillAllColumnarSyncAction(long currentConnId) {
        this.currentConnId = currentConnId;
    }

    public long getCurrentConnId() {
        return currentConnId;
    }

    public void setCurrentConnId(long currentConnId) {
        this.currentConnId = currentConnId;
    }

    @Override
    public ResultCursor sync() {
        int count = 0;
        NIOProcessor[] processors = CobarServer.getInstance().getProcessors();

        for (NIOProcessor p : processors) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                // Skip the current connection that is executing this kill 'all' command
                if (fc.getId() == currentConnId) {
                    continue;
                }
                try {
                    fc.close();
                    count++;
                } catch (Exception e) {
                    logger.warn("Failed to close connection " + fc.getId(), e);
                }
            }
        }

        ArrayResultCursor result = new ArrayResultCursor("KILL_ALL");
        result.addColumn(ResultCursor.AFFECT_ROW, DataTypes.IntegerType);
        result.initMeta();
        result.addRow(new Object[] {count});
        return result;
    }
}
