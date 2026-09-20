package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoAccessor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "DropExternalCatalogRemoveMetaTask")
public class DropExternalCatalogRemoveMetaTask extends BaseGmsTask {

    private final String catalogName;

    public DropExternalCatalogRemoveMetaTask(String catalogName) {
        super(SystemDbHelper.DEFAULT_DB_NAME, null);
        this.catalogName = catalogName;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        new ExternalCatalogInfoAccessor(metaDbConnection).deleteByName(catalogName);
        // The deletion is already visible to other CNs once committed, and the following
        // sync task has no rollback, so forbid ROLLBACK DDL to avoid cache/MetaDB splits.
        // Takes effect when this task's transaction commits.
        updateSupportedCommands(true, false, metaDbConnection);
    }
}
