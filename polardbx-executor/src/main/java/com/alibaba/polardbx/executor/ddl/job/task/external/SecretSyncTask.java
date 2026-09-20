package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SecretSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "SecretSyncTask")
public class SecretSyncTask extends BaseSyncTask {

    public enum SecretOpType {
        ADD, UPDATE, REMOVE
    }

    private final String secretName;
    private final SecretOpType action;

    public SecretSyncTask(String secretName, SecretOpType action) {
        super(SystemDbHelper.DEFAULT_DB_NAME);
        this.secretName = secretName;
        this.action = action;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        try {
            SyncManagerHelper.syncThrowExceptions(
                new SecretSyncAction(secretName, action.name()),
                SyncScope.ALL);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync secret, secretName:%s, action:%s", secretName, action));
            throw GeneralUtil.nestedException(t);
        }
    }
}
