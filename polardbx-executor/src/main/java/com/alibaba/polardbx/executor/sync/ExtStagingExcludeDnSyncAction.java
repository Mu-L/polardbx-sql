package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.ExtStagingDnConnector;
import com.alibaba.polardbx.executor.columnar.ExtStagingDnRouter;
import com.alibaba.polardbx.executor.columnar.StagingFlushTaskScheduler;
import com.alibaba.polardbx.executor.columnar.StagingTableManager;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Broadcast to every CN: a set of DNs is being drained, stop allocating new
 * staging seqs there and rotate off if currently active.
 *
 * <p>Each receiving CN does the following on {@link #sync()}:
 * <ol>
 *   <li>{@link ExtStagingDnRouter#markDraining(Set)} — new seqs skip these DNs</li>
 *   <li>{@link StagingTableManager#forceRotateOff(Set)} — if local active seq is
 *       on a draining DN, rotate to another DN immediately</li>
 *   <li>refresh the cdc topology DN map so subsequent picks observe membership</li>
 *   <li>kick the flush scheduler once to accelerate cleanup</li>
 * </ol>
 */
public class ExtStagingExcludeDnSyncAction implements IGmsSyncAction {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private List<String> dnIds;

    public ExtStagingExcludeDnSyncAction() {
    }

    public ExtStagingExcludeDnSyncAction(List<String> dnIds) {
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
        ExtStagingDnRouter.getInstance().markDraining(set);
        StagingTableManager.getInstance().forceRotateOff(set);
        ExtStagingDnConnector.getInstance().refreshDnGroupMap();
        // Kick the flush scheduler so SEALED seqs on draining DNs flush ASAP.
        StagingFlushTaskScheduler.getInstance().triggerOnce();
        LOGGER.warn("EXT_STAGING_EXCLUDE_DN sync applied: dnIds=" + set);
        return null;
    }
}
