package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

public class BaselineDeleteUnfixedSyncAction implements ISyncAction {

    String schema;

    public BaselineDeleteUnfixedSyncAction(String schema) {
        this.schema = schema;
    }

    @Override
    public ResultCursor sync() {
        PlanManager.getInstance().deleteBaselineUnfixed(schema);
        return null;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

}

