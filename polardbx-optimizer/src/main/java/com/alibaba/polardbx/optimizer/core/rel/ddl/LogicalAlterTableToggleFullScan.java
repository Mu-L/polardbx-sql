package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableToggleFullScanPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableToggleFullScan;

public class LogicalAlterTableToggleFullScan extends BaseDdlOperation {

    private AlterTableToggleFullScanPreparedData preparedData;

    public LogicalAlterTableToggleFullScan(DDL ddl) {
        super(ddl, ((AlterTableToggleFullScan) ddl).getObjectNames());
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
    }

    public void preparedData(ExecutionContext ec) {
        AlterTableToggleFullScan alterTableToggleFullScan = (AlterTableToggleFullScan) relDdl;

        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);

        preparedData = new AlterTableToggleFullScanPreparedData();
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        if (tableMeta.isGsi()) {
            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                "not support toggle full scan for gsi table");
        }
        preparedData.setTableVersion(tableMeta.getVersion());
        preparedData.setEnable(alterTableToggleFullScan.isEnable());
    }

    public AlterTableToggleFullScanPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableToggleFullScan create(DDL ddl) {
        return new LogicalAlterTableToggleFullScan(ddl);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        if (Engine.isFileStore(tableMeta.getEngine())) {
            return true;
        }
        return false;
    }
}
