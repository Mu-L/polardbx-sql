package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.columnar.ExtColumnMappingManager;
import com.alibaba.polardbx.executor.columnar.ExternalColumnTableIdResolver;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "DropBlobColumnMappingByColumnTask")
public class DropBlobColumnMappingByColumnTask extends BaseDdlTask {

    private final String logicalTableName;
    private final List<String> droppedLogicalColumnNames;

    @JSONCreator
    public DropBlobColumnMappingByColumnTask(String schemaName, String logicalTableName,
                                             List<String> droppedLogicalColumnNames) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.droppedLogicalColumnNames = droppedLogicalColumnNames;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        ExtColumnMappingManager manager = ExtColumnMappingManager.getInstance();
        for (String columnName : droppedLogicalColumnNames) {
            manager.markDropByColumn(metaDbConnection, schemaName, logicalTableName, columnName);
        }
        ExternalColumnTableIdResolver.getInstance().invalidateForTable(schemaName, logicalTableName);
    }

}
