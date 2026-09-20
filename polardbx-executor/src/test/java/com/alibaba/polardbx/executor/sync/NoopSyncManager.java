package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.ISyncResultHandler;
import com.alibaba.polardbx.gms.sync.SyncScope;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test-only ISyncManager provider so that SyncManagerHelper can be initialized in unit tests.
 */
public class NoopSyncManager implements ISyncManager {

    private volatile boolean inited = false;

    @Override
    public void init() {
        inited = true;
    }

    @Override
    public void destroy() {
        inited = false;
    }

    @Override
    public boolean isInited() {
        return inited;
    }

    @Override
    public List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName, SyncScope scope,
                                                boolean throwExceptions) {
        return Collections.emptyList();
    }

    @Override
    public void sync(IGmsSyncAction action, String schemaName, SyncScope scope, ISyncResultHandler handler,
                     boolean throwExceptions) {
    }

    @Override
    public List<Map<String, Object>> sync(IGmsSyncAction action, String schemaName, String serverKey) {
        return Collections.emptyList();
    }
}
