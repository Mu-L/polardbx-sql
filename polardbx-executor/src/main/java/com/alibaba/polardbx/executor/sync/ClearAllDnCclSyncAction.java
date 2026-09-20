package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.node.CCLDetectDnActor;
import com.alibaba.polardbx.gms.node.CCLDetectManager;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.Iterator;

/**
 * Sync action to clear all DN CCL rules from leader node
 *
 * @author liugaoji
 */
public class ClearAllDnCclSyncAction implements IGmsSyncAction {

    protected static final Logger logger = LoggerFactory.getLogger(ClearAllDnCclSyncAction.class);

    public ClearAllDnCclSyncAction() {
    }

    @Override
    public Object sync() {
        ArrayResultCursor result = new ArrayResultCursor("CLEAR_ALL_DN_CCL");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("STATUS", DataTypes.StringType);
        result.addColumn("MESSAGE", DataTypes.StringType);

        // Check if current node is leader
        if (!ConfigDataMode.isMasterMode() || !LeaderStatusBridge.getInstance().hasLeadership()) {
            // Non-leader nodes return empty result
            return result;
        }

        boolean success = true;
        StringBuilder messageBuilder = new StringBuilder();

        try {
            // Step 1: Clear CCLDetectManager GlobalGenKey
            CCLDetectManager.getInstance().clearGlobalGenKey();
            messageBuilder.append("Cleared CCLDetectManager GlobalGenKey; ");
        } catch (Exception e) {
            success = false;
            messageBuilder.append("Failed to clear CCLDetectManager GlobalGenKey: ").append(e.getMessage())
                .append("; ");
            logger.error("Clear CCLDetectManager Key Error", e);
        }

        // Step 2: Clear all DN CCL rules
        Iterator<StorageInstHaContext> iterator = CCLDetectManager.getDnMasterIterator();
        while (iterator.hasNext()) {
            StorageInstHaContext instHaContext = iterator.next();
            try {
                boolean dnResult =
                    CCLDetectDnActor.deleteAllDnCCL(() -> DbTopologyManager.getConnectionForStorage(instHaContext));
                if (dnResult) {
                    messageBuilder.append("Cleared DN CCL rules for ").append(instHaContext.getStorageInstId())
                        .append("; ");
                } else {
                    success = false;
                    messageBuilder.append("Failed to clear DN CCL rules for ").append(instHaContext.getStorageInstId())
                        .append("; ");
                }
            } catch (Exception e) {
                success = false;
                messageBuilder.append("Error clearing DN CCL rules for ").append(instHaContext.getStorageInstId())
                    .append(": ").append(e.getMessage()).append("; ");
                logger.error("Clear DN CCL rules error for " + instHaContext.getStorageInstId(), e);
            }
        }

        // Add result row
        result.addRow(new Object[] {
            TddlNode.getHost() + ":" + TddlNode.getPort(),
            success ? "SUCCESS" : "FAILED",
            messageBuilder.toString()
        });

        return result;
    }
}