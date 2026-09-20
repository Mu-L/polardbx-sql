package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableExchangePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableMovePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableExchangePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableMovePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableMovePartitionPreparedData;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableExchangePartition;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

import java.util.Map;
import java.util.Set;

public class LogicalAlterTableExchangePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableExchangePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public void prepareFixedResources(BaseDdlOperation logicalDdlPlan,
                                      ExecutionContext executionContext, Set<String> sharedResources,
                                      Set<String> exclusiveResources, Map<String, Long> tableVersions) {
        String schemaName = logicalDdlPlan.getSchemaName();

        SqlAlterTable sqlAlterTable = (SqlAlterTable) logicalDdlPlan.getNativeSqlNode();
        String sourceTableName = logicalDdlPlan.getTableName();

        SqlAlterTableExchangePartition sqlExchangePartition =
            (SqlAlterTableExchangePartition) sqlAlterTable.getAlters().get(0);
        String targetTableName = Util.last(((SqlIdentifier) sqlExchangePartition.getTableName()).names);

        exclusiveResources.add(concatWithDot(schemaName, sourceTableName));
        exclusiveResources.add(concatWithDot(schemaName, targetTableName));

        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTableWithNull(sourceTableName);
        if (tableMeta != null) {
            tableVersions.put(sourceTableName, tableMeta.getVersion());
        }
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableExchangePartition logicalAlterTableExchangePartition =
            (LogicalAlterTableExchangePartition) logicalDdlPlan;

        logicalAlterTableExchangePartition.preparedData(executionContext);

        return AlterTableExchangePartitionJobFactory.create(
            logicalAlterTableExchangePartition.getPreparedData(executionContext), executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableExchangePartition logicalAlterTableMovePartition =
            (LogicalAlterTableExchangePartition) logicalDdlPlan;
        AlterTable alterTable = (AlterTable) logicalAlterTableMovePartition.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();

        assert sqlAlterTable.getAlters().size() == 1;
        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableExchangePartition;

        String schemaName = logicalAlterTableMovePartition.getSchemaName();
        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);

        TableValidator.validateTableExistence(schemaName, logicalTableName, executionContext);

        AlterTableGroupUtils.alterTableExchangePartitionCheck(alterTable,
            logicalAlterTableMovePartition.getPreparedData(executionContext), executionContext);
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}
