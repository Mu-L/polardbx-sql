package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "AlterSecretUpdateMetaTask")
public class AlterSecretUpdateMetaTask extends BaseGmsTask {

    private final String secretName;
    private final byte[] encryptedKv;

    public AlterSecretUpdateMetaTask(String secretName, byte[] encryptedKv) {
        super(SystemDbHelper.DEFAULT_DB_NAME, null);
        this.secretName = secretName;
        this.encryptedKv = encryptedKv;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        new ExternalSecretAccessor(metaDbConnection).updateEncryptedKv(secretName, encryptedKv);
        // The updated secret is already visible to other CNs once written, and the following
        // sync task has no rollback, so forbid ROLLBACK DDL to avoid cache/MetaDB splits.
        // Takes effect when this task's transaction commits.
        updateSupportedCommands(true, false, metaDbConnection);
    }
}
