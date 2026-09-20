package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoAccessor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "CreateExternalCatalogAddMetaTask")
public class CreateExternalCatalogAddMetaTask extends BaseGmsTask {

    private final String catalogName;
    private final String connector;
    private final byte[] encryptedProperties;
    private final String secretName;
    private final String comment;

    public CreateExternalCatalogAddMetaTask(String catalogName, String connector,
                                            byte[] encryptedProperties, String secretName,
                                            String comment) {
        super(SystemDbHelper.DEFAULT_DB_NAME, null);
        this.catalogName = catalogName;
        this.connector = connector;
        this.encryptedProperties = encryptedProperties;
        this.secretName = secretName;
        this.comment = comment;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor(metaDbConnection);
        accessor.insert(catalogName, connector, encryptedProperties, secretName, comment);
        // The catalog row is already visible to other CNs once written, and the following
        // sync task has no rollback, so forbid ROLLBACK DDL to avoid cache/MetaDB splits.
        // Takes effect when this task's transaction commits.
        updateSupportedCommands(true, false, metaDbConnection);
    }
}
