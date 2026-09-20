package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.node.GmsNodeManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Authoritative eligibility checks for the staging write/flush lifecycle.
 */
public final class StagingLifecycleEligibility {

    private StagingLifecycleEligibility() {
    }

    /**
     * Only writable master CNs may create, write, or flush staging tables.
     * Read-only learner modes must remain excluded even when they allow DML.
     */
    public static boolean isEligible() {
        return !ConfigDataMode.isReadOnlyMode() && ConfigDataMode.isMasterMode();
    }

    /**
     * Return alive staging owners in writable master instances only.
     */
    public static List<String> getEligibleMasterCnIds() {
        return getCnIds(GmsNodeManager.getInstance().getMasterNodes());
    }

    /**
     * Return every READY CN owner, including read-only instances.
     * Read-only CNs cannot use staging, but their historical seqs must not be
     * treated as orphaned while the CN is still alive.
     */
    public static List<String> getAliveCnIds() {
        return getCnIds(GmsNodeManager.getInstance().getAllNodes());
    }

    private static List<String> getCnIds(List<GmsNodeManager.GmsNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> cnIds = new ArrayList<>(nodes.size());
        for (GmsNodeManager.GmsNode node : nodes) {
            if (node == null) {
                continue;
            }
            String serverKey = node.getServerKey();
            if (serverKey != null && !serverKey.isEmpty()) {
                cnIds.add(serverKey);
            }
        }
        return cnIds;
    }
}
