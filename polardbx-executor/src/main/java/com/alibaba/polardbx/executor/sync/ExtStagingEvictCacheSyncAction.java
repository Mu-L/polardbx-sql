package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.ExtStagingDnRouter;
import com.alibaba.polardbx.executor.columnar.StagingTableManager;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Broadcast to every CN after drain completes: evict seqDnCache entries for
 * the removed DNs and clear the draining set.
 *
 * <p>This prevents stale cache hits where a CN still maps a seqId to a
 * now-removed DN, which would cause FETCH_BLOB to fail instead of falling
 * back to OSS.
 */
public class ExtStagingEvictCacheSyncAction implements IGmsSyncAction {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private List<String> dnIds;

    public ExtStagingEvictCacheSyncAction() {
    }

    public ExtStagingEvictCacheSyncAction(List<String> dnIds) {
        this.dnIds = dnIds;
    }

    public List<String> getDnIds() {
        return dnIds;
    }

    public void setDnIds(List<String> dnIds) {
        this.dnIds = dnIds;
    }

    @Override
    public Object sync() {
        if (dnIds == null || dnIds.isEmpty()) {
            return null;
        }
        Set<String> set = new HashSet<>(dnIds);
        StagingTableManager manager = StagingTableManager.getInstance();
        // Keep the routing barrier until all local cache state is known fresh.
        manager.evictCacheForDns(set);
        manager.refreshWatermarkStrict();
        ExtStagingDnRouter.getInstance().clearDraining(set);
        LOGGER.warn("EXT_STAGING_EVICT_CACHE sync applied: dnIds=" + set);
        return null;
    }
}
