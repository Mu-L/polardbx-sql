package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "DropSecretRemoveMetaTask")
public class DropSecretRemoveMetaTask extends BaseGmsTask {

    private final String secretName;

    public DropSecretRemoveMetaTask(String secretName) {
        super(SystemDbHelper.DEFAULT_DB_NAME, null);
        this.secretName = secretName;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        new ExternalSecretAccessor(metaDbConnection).deleteByName(secretName);
        // The deletion is already visible to other CNs once committed, and the following
        // sync task has no rollback, so forbid ROLLBACK DDL to avoid cache/MetaDB splits.
        // Takes effect when this task's transaction commits.
        updateSupportedCommands(true, false, metaDbConnection);
    }
}
