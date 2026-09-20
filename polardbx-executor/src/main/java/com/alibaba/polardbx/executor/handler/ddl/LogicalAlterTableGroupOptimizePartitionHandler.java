package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupOptimizePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupTruncatePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupOptimizePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupTruncatePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.Map;
import java.util.Set;

public class LogicalAlterTableGroupOptimizePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableGroupOptimizePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public void prepareFixedResources(BaseDdlOperation logicalDdlPlan,
                                      ExecutionContext executionContext, Set<String> sharedResources,
                                      Set<String> exclusiveResources, Map<String, Long> tableVersions) {
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) logicalDdlPlan.getNativeSqlNode();
        String tableGroupName = ((SqlIdentifier) sqlAlterTableGroup.getTableGroupName()).getLastName();
        exclusiveResources.add(concatWithDot(logicalDdlPlan.getSchemaName(), tableGroupName));
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableGroupOptimizePartition logicalAlterTableGroupOptimizePartition =
            (LogicalAlterTableGroupOptimizePartition) logicalDdlPlan;

        logicalAlterTableGroupOptimizePartition.prepareData(executionContext);

        String dbName = logicalAlterTableGroupOptimizePartition.getPreparedData().getSchemaName();
        String tgName = logicalAlterTableGroupOptimizePartition.getPreparedData().getTableGroupName();

        CheckOSSArchiveUtil.checkTableGroupWithoutOSS(dbName, tgName);

        return new AlterTableGroupOptimizePartitionJobFactory(logicalAlterTableGroupOptimizePartition.relDdl,
            logicalAlterTableGroupOptimizePartition.getPreparedData(), executionContext).create();
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTableGroupUtils.alterTableGroupPreCheck(
            (SqlAlterTableGroup) logicalDdlPlan.relDdl.getSqlNode(),
            logicalDdlPlan.getSchemaName(),
            executionContext);
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}
