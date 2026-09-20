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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Getter
@TaskName(name = "RegisterBlobColumnMappingTask")
public class RegisterBlobColumnMappingTask extends BaseDdlTask {

    private final String logicalTableName;
    private final List<String> externalizedColumnNames;
    private List<Long> registeredMappingTableIds;

    public RegisterBlobColumnMappingTask(String schemaName, String logicalTableName,
                                         List<String> externalizedColumnNames) {
        this(schemaName, logicalTableName, externalizedColumnNames, Collections.emptyList());
    }

    @JSONCreator
    public RegisterBlobColumnMappingTask(String schemaName, String logicalTableName,
                                         List<String> externalizedColumnNames,
                                         List<Long> registeredMappingTableIds) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.externalizedColumnNames = externalizedColumnNames;
        this.registeredMappingTableIds = registeredMappingTableIds == null ? null
            : new ArrayList<>(registeredMappingTableIds);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        ExtColumnMappingManager manager = ExtColumnMappingManager.getInstance();
        Set<Long> previouslyRegisteredIds = validatePreviouslyRegisteredIds(manager, metaDbConnection);
        registeredMappingTableIds = new ArrayList<>(externalizedColumnNames.size());

        // A PUBLIC row can predate this task. Reuse it without claiming ownership;
        // rollback may only DROP rows whose exact IDs were inserted below. On an
        // uncertain commit retry, retain ownership only when the same exact ID is
        // visible as PUBLIC; a truly rolled-back insert has no matching row.
        for (String columnName : externalizedColumnNames) {
            List<ExtColumnMappingRecord> publicMappings =
                manager.queryPublic(metaDbConnection, schemaName, logicalTableName, columnName);
            if (!publicMappings.isEmpty()) {
                for (ExtColumnMappingRecord mapping : publicMappings) {
                    if (previouslyRegisteredIds.contains(mapping.tableId)) {
                        registeredMappingTableIds.add(mapping.tableId);
                    }
                }
                continue;
            }
            registeredMappingTableIds.add(
                manager.insertPublic(metaDbConnection, schemaName, logicalTableName, columnName));
        }
        ExternalColumnTableIdResolver.getInstance().invalidateForTable(schemaName, logicalTableName);

        EventLogger.log(EventType.EXT_COL_CREATE, String.format(
            "schema=%s, table=%s, columns=%s",
            schemaName, logicalTableName, externalizedColumnNames));
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        if (registeredMappingTableIds == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "Cannot safely roll back ext-column mappings without the exact registered table IDs for "
                    + schemaName + "." + logicalTableName);
        }

        ExtColumnMappingManager manager = ExtColumnMappingManager.getInstance();
        for (Long tableId : registeredMappingTableIds) {
            int dropped = manager.updateStatusByTableIdIfStatus(metaDbConnection,
                tableId, ExtColumnMappingRecord.STATUS_DROP, ExtColumnMappingRecord.STATUS_PUBLIC);
            if (dropped == 1) {
                continue;
            }

            List<ExtColumnMappingRecord> current = manager.queryTableId(metaDbConnection, tableId);
            if (current.size() != 1 || !ExtColumnMappingRecord.STATUS_DROP.equals(current.get(0).status)) {
                throw mappingStateChanged("mark DROP", tableId);
            }
        }
        ExternalColumnTableIdResolver.getInstance().invalidateForTable(schemaName, logicalTableName);
    }

    private TddlRuntimeException mappingStateChanged(String operation, long tableId) {
        return new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
            String.format("Cannot %s for ext-column mapping table_id=%d of %s.%s because its state changed",
                operation, tableId, schemaName, logicalTableName));
    }

    private Set<Long> validatePreviouslyRegisteredIds(ExtColumnMappingManager manager, Connection connection) {
        if (registeredMappingTableIds == null || registeredMappingTableIds.isEmpty()) {
            return Collections.emptySet();
        }

        Set<Long> validIds = new HashSet<>(registeredMappingTableIds.size());
        int missingCount = 0;
        for (Long tableId : registeredMappingTableIds) {
            List<ExtColumnMappingRecord> current = manager.queryTableId(connection, tableId);
            if (current.isEmpty()) {
                missingCount++;
                continue;
            }
            if (current.size() != 1 || !isOwnedPublicMapping(current.get(0))) {
                throw mappingStateChanged("retain ownership", tableId);
            }
            validIds.add(tableId);
        }
        if (missingCount > 0 && missingCount < registeredMappingTableIds.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "Cannot safely retry ext-column mapping registration because only part of the registered table IDs "
                    + "still exist for " + schemaName + "." + logicalTableName);
        }
        return validIds;
    }

    private boolean isOwnedPublicMapping(ExtColumnMappingRecord mapping) {
        if (!ExtColumnMappingRecord.STATUS_PUBLIC.equals(mapping.status)
            || !schemaName.equalsIgnoreCase(mapping.tableSchema)
            || !logicalTableName.equalsIgnoreCase(mapping.tableName)) {
            return false;
        }
        for (String columnName : externalizedColumnNames) {
            if (columnName.equalsIgnoreCase(mapping.columnName)) {
                return true;
            }
        }
        return false;
    }
}
