package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.columnar.ExtStagingDnRouter;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;

import java.util.HashSet;
import java.util.List;

/**
 * Remove only the specified DNs from the local staging drain barrier.
 *
 * <p>This is the rollback counterpart of {@link ExtStagingExcludeDnSyncAction}.
 * It intentionally leaves cache and watermark state untouched because it is
 * used when the diagnostic drain pipeline fails before physical drain completes.
 */
public class ExtStagingClearDrainingSyncAction implements IGmsSyncAction {

    private List<String> dnIds;

    public ExtStagingClearDrainingSyncAction() {
    }

    public ExtStagingClearDrainingSyncAction(List<String> dnIds) {
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
        ExtStagingDnRouter.getInstance().clearDraining(new HashSet<>(dnIds));
        return null;
    }
}
