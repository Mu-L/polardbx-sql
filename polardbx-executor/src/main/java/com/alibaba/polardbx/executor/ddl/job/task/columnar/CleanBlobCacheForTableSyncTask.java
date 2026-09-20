package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.CleanBlobCacheForTableSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "CleanBlobCacheForTableSyncTask")
public class CleanBlobCacheForTableSyncTask extends BaseSyncTask {

    private final String logicalTableName;
    private final boolean cleanOnRollback;

    public CleanBlobCacheForTableSyncTask(String schemaName, String logicalTableName) {
        this(schemaName, logicalTableName, false);
    }

    @JSONCreator
    public CleanBlobCacheForTableSyncTask(String schemaName, String logicalTableName, boolean cleanOnRollback) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.cleanOnRollback = cleanOnRollback;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        cleanCacheOnAllCns();
    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        if (cleanOnRollback) {
            cleanCacheOnAllCns();
        }
    }

    private void cleanCacheOnAllCns() {
        SyncManagerHelper.syncThrowExceptions(
            new CleanBlobCacheForTableSyncAction(schemaName, logicalTableName, null), schemaName, SyncScope.ALL);
    }

    @Override
    protected String remark() {
        return "|tableName: " + logicalTableName;
    }
}
