package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.executor.columnar.ExtColumnMappingManager;
import com.alibaba.polardbx.executor.columnar.ExternalColumnTableIdResolver;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.ExtColumnMappingRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Getter
@TaskName(name = "DropBlobColumnMappingTask")
public class DropBlobColumnMappingTask extends BaseDdlTask {

    private final String logicalTableName;
    private List<Long> changedMappingTableIds;

    public DropBlobColumnMappingTask(String schemaName, String logicalTableName) {
        this(schemaName, logicalTableName, Collections.emptyList());
    }

    @JSONCreator
    public DropBlobColumnMappingTask(String schemaName, String logicalTableName, List<Long> changedMappingTableIds) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.changedMappingTableIds = changedMappingTableIds == null ? null : new ArrayList<>(changedMappingTableIds);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        ExtColumnMappingManager manager = ExtColumnMappingManager.getInstance();

        List<ExtColumnMappingRecord> publicMappings = manager.queryBySchemaTableAndStatus(metaDbConnection,
            schemaName, logicalTableName, ExtColumnMappingRecord.STATUS_PUBLIC);
        changedMappingTableIds = new ArrayList<>(publicMappings.size());
        for (ExtColumnMappingRecord mapping : publicMappings) {
            changedMappingTableIds.add(mapping.tableId);
            int changed = manager.updateStatusByTableIdIfStatus(metaDbConnection,
                mapping.tableId, ExtColumnMappingRecord.STATUS_DROP, ExtColumnMappingRecord.STATUS_PUBLIC);
            if (changed != 1) {
                throw mappingStateChanged("mark DROP", mapping.tableId);
            }
        }
        ExternalColumnTableIdResolver.getInstance().invalidateForTable(schemaName, logicalTableName);

        EventLogger.log(EventType.EXT_COL_DROP, String.format(
            "schema=%s, table=%s", schemaName, logicalTableName));
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        if (changedMappingTableIds == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "Cannot safely roll back ext-column mappings without the exact changed table IDs for "
                    + schemaName + "." + logicalTableName);
        }

        ExtColumnMappingManager manager = ExtColumnMappingManager.getInstance();
        for (Long tableId : changedMappingTableIds) {
            int restored = manager.updateStatusByTableIdIfStatus(metaDbConnection,
                tableId, ExtColumnMappingRecord.STATUS_PUBLIC, ExtColumnMappingRecord.STATUS_DROP);
            if (restored == 1) {
                continue;
            }

            List<ExtColumnMappingRecord> current = manager.queryTableId(metaDbConnection, tableId);
            if (current.size() != 1 || !ExtColumnMappingRecord.STATUS_PUBLIC.equals(current.get(0).status)) {
                throw mappingStateChanged("restore PUBLIC", tableId);
            }
        }
        ExternalColumnTableIdResolver.getInstance().invalidateForTable(schemaName, logicalTableName);
    }

    private TddlRuntimeException mappingStateChanged(String operation, long tableId) {
        return new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
            String.format("Cannot %s for ext-column mapping table_id=%d of %s.%s because its state changed",
                operation, tableId, schemaName, logicalTableName));
    }
}
