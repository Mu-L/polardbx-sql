package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.external.ConnectorRuntimeManager;

/**
 * Broadcasts {@code RELOAD CONNECTORS} to all CN nodes.
 */
public class ConnectorSyncAction implements ISyncAction {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorSyncAction.class);

    public ConnectorSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        ConnectorRuntimeManager manager = ConnectorRuntimeManager.getInstance();
        if (manager == null) {
            // mock mode or node not fully initialized; nothing to reload
            LOGGER.warn("ConnectorRuntimeManager is not initialized, skip reloading connectors");
            return null;
        }
        LOGGER.warn("RELOAD CONNECTORS: reloading connector plugins on this node; "
            + "in-flight queries on external catalogs may fail");
        manager.reload();
        return null;
    }
}
