package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "CreateSecretAddMetaTask")
public class CreateSecretAddMetaTask extends BaseGmsTask {

    private final String secretName;
    private final String type;
    private final byte[] encryptedKv;

    public CreateSecretAddMetaTask(String secretName, String type,
                                   byte[] encryptedKv) {
        super(SystemDbHelper.DEFAULT_DB_NAME, null);
        this.secretName = secretName;
        this.type = type;
        this.encryptedKv = encryptedKv;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        ExternalSecretAccessor accessor = new ExternalSecretAccessor();
        accessor.setConnection(metaDbConnection);
        accessor.insert(secretName, type, encryptedKv);
        // The secret row is already visible to other CNs once written, and the following
        // sync task has no rollback, so forbid ROLLBACK DDL to avoid cache/MetaDB splits.
        // Takes effect when this task's transaction commits.
        updateSupportedCommands(true, false, metaDbConnection);
    }
}
