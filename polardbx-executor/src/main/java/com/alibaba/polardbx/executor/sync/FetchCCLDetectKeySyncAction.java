package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.node.CCLDetectManager;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Sync action to fetch CCL detect keys from leader node
 *
 * @author liugaoji
 */
public class FetchCCLDetectKeySyncAction implements IGmsSyncAction {

    public FetchCCLDetectKeySyncAction() {
    }

    @Override
    public Object sync() {
        ArrayResultCursor result = new ArrayResultCursor("CCL_DETECT_KEYS");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("INST_ID", DataTypes.StringType);
        result.addColumn("TEMPLATE_ID", DataTypes.StringType);
        result.addColumn("CONCURRENCY_COUNT", DataTypes.LongType);

        // Check if current node is leader
        if (!ConfigDataMode.isMasterMode() || !LeaderStatusBridge.getInstance().hasLeadership()) {
            return result;
        }

        // Get GlobalGenKeyDict from CCLDetectManager
        ConcurrentHashMap<String, Pair<Long, Long>> globalGenKeyDict = CCLDetectManager.getInstance()
            .getGlobalGenKeyDict();

        // Process and return data needed by InformationSchemaDnCclDryRunHandler
        for (Map.Entry<String, Pair<Long, Long>> entry : globalGenKeyDict.entrySet()) {
            String key = entry.getKey();
            String[] parts = key.split(";");
            if (parts.length >= 3) {
                String instId = parts[0];
                String templateId = parts[2];
                Long concurrencyCount = entry.getValue().getKey();

                result.addRow(new Object[] {
                    TddlNode.getHost() + ":" + TddlNode.getPort(),
                    instId,
                    templateId,
                    concurrencyCount
                });
            }
        }

        return result;
    }
}