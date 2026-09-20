package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.ttl.query.TtlQueryStatManager;

public class ClearTtlQueryStatSyncAction implements ISyncAction {

    private String db;

    public ClearTtlQueryStatSyncAction() {
    }

    public ClearTtlQueryStatSyncAction(String db) {
        this.db = db;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    @Override
    public ResultCursor sync() {
        TtlQueryStatManager.getInstance().getTtlQueryStatMap().clear();
        return null;
    }
}