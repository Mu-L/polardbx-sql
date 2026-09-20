package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.ExternalCatalogSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "ExternalCatalogSyncTask")
public class ExternalCatalogSyncTask extends BaseSyncTask {

    public enum SyncAction {
        ADD, UPDATE, REMOVE, REFRESH
    }

    private final String catalogName;
    private final SyncAction action;

    public ExternalCatalogSyncTask(String catalogName, SyncAction action) {
        super(SystemDbHelper.DEFAULT_DB_NAME);
        this.catalogName = catalogName;
        this.action = action;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        try {
            SyncManagerHelper.syncThrowExceptions(
                new ExternalCatalogSyncAction(catalogName, action.name()), SyncScope.ALL);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync external catalog, catalogName:%s, action:%s", catalogName, action));
            throw GeneralUtil.nestedException(t);
        }
    }
}
