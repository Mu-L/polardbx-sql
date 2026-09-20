package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableRemoveAutoPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRemoveAutoPartition;
import org.apache.calcite.sql.SqlAlterTableRemoveAutoPartition;

import java.util.Map;
import java.util.Set;

/**
 * @author wumu
 */
public class LogicalAlterTableRemoveAutoPartitionHandler extends LogicalCommonDdlHandler {
    public LogicalAlterTableRemoveAutoPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public void prepareFixedResources(BaseDdlOperation logicalDdlPlan,
                                      ExecutionContext executionContext, Set<String> sharedResources,
                                      Set<String> exclusiveResources, Map<String, Long> tableVersions) {
        exclusiveResources.add(concatWithDot(logicalDdlPlan.getSchemaName(), logicalDdlPlan.getTableName()));
        TableMeta tableMeta = executionContext.getSchemaManager(logicalDdlPlan.getSchemaName())
            .getTableWithNull(logicalDdlPlan.getTableName());
        if (tableMeta != null) {
            tableVersions.put(logicalDdlPlan.getTableName(), tableMeta.getVersion());
        }
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final LogicalAlterTableRemoveAutoPartition logicalAlterTableRemoveAutoPartition =
            (LogicalAlterTableRemoveAutoPartition) logicalDdlPlan;

        SqlAlterTableRemoveAutoPartition ast =
            logicalAlterTableRemoveAutoPartition.getSqlAlterTableRemoveAutoPartition();

        String schemaName = ast.getSchemaName();
        String logicalTableName = ast.getPrimaryTableName();

        return new AlterTableRemoveAutoPartitionJobFactory(schemaName, logicalTableName, executionContext).create();
    }
}
