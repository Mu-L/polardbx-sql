package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableToggleFullScanJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableToggleFullScan;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableToggleFullScanPreparedData;

import java.util.Map;
import java.util.Set;

public class LogicalAlterTableToggleFullScanHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableToggleFullScanHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public void prepareFixedResources(BaseDdlOperation logicalDdlPlan,
                                      ExecutionContext executionContext, Set<String> sharedResources,
                                      Set<String> exclusiveResources, Map<String, Long> tableVersions) {
        String tableName = logicalDdlPlan.getTableName();
        exclusiveResources.add(concatWithDot(logicalDdlPlan.getSchemaName(), tableName));
        TableMeta tableMeta =
            executionContext.getSchemaManager(logicalDdlPlan.getSchemaName()).getTableWithNull(tableName);
        if (tableMeta != null) {
            tableVersions.put(tableName, tableMeta.getVersion());
        }
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableToggleFullScan logicalAlterTableToggleFullScan =
            (LogicalAlterTableToggleFullScan) logicalDdlPlan;
        if (logicalAlterTableToggleFullScan.getPreparedData() == null) {
            logicalAlterTableToggleFullScan.preparedData(executionContext);
        }
        AlterTableToggleFullScanPreparedData preparedData = logicalAlterTableToggleFullScan.getPreparedData();

        return new AlterTableToggleFullScanJobFactory(logicalAlterTableToggleFullScan.relDdl,
            preparedData,
            executionContext).create();
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableToggleFullScan logicalAlterTableToggleFullScan =
            (LogicalAlterTableToggleFullScan) logicalDdlPlan;
        if (logicalAlterTableToggleFullScan.getPreparedData() == null) {
            logicalAlterTableToggleFullScan.preparedData(executionContext);
        }
        AlterTableToggleFullScanPreparedData preparedData = logicalAlterTableToggleFullScan.getPreparedData();

        TableMeta tableMeta =
            executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(preparedData.getTableName());

        if (tableMeta.isGsi()) {
            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                "not support toggle full scan for gsi table");
        }
        return false;
    }
}
