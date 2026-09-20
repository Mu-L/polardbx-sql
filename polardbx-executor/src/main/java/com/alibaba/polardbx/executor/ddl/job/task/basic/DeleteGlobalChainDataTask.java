package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.chain.GlobalChainAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "DeleteGlobalChainDataTask")
public class DeleteGlobalChainDataTask extends BaseDdlTask {
    private final String tableName;

    public DeleteGlobalChainDataTask(String schemaName, String tableName) {
        super(schemaName);
        this.tableName = tableName;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        GlobalChainAccessor globalChainAccessor = new GlobalChainAccessor();
        globalChainAccessor.setConnection(metaDbConnection);
        globalChainAccessor.updateDeleteBySchemaAndTable(schemaName, tableName);
    }
}
