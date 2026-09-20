package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.PhysicalRoute;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedInsertInput;
import com.alibaba.polardbx.optimizer.core.rel.dml.WritePlanHook;
import org.apache.calcite.rel.RelNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * INSERT-family hook that materializes external values after primary routing and builds staging plans after primary
 * business plans exist. One instance belongs to one statement execution. For example, a WRITE_ONLY split-partition
 * INSERT materializes one canonical BlobRef on the source primary route; the original replication Writer then copies
 * that parameter into its target plan, rather than this hook uploading a second GSI/replica-owned object.
 */
public final class ExternalizedInsertWritePlanHook implements WritePlanHook<RoutedInsertInput> {
    private final LogicalInsert logicalInsert;
    private final TableMeta tableMeta;
    private TransactionalStagingWriteBatch stagingBatch = new TransactionalStagingWriteBatch();

    public ExternalizedInsertWritePlanHook(LogicalInsert logicalInsert, TableMeta tableMeta) {
        this.logicalInsert = logicalInsert;
        this.tableMeta = tableMeta;
    }

    @Override
    public void materialize(RoutedInsertInput routedInput, ExecutionContext executionContext) {
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> ownerRoutes = new HashMap<>();
        for (Map.Entry<Integer, PhysicalRoute> entry : routedInput.getRouteByRowIndex().entrySet()) {
            PhysicalRoute route = entry.getValue();
            ownerRoutes.put(entry.getKey(), new TransactionalStagingWriteBatch.OwnerRoute(
                route.getSchemaName(), route.getGroupName(), route.getPhysicalTableName()));
        }

        TransactionalStagingWriteBatch materialized = new TransactionalStagingWriteBatch();
        materialized.addAll(ExternalizedColumnDmlHelper.materializeUpsertPushdownParameters(
            logicalInsert, tableMeta, executionContext, ownerRoutes));
        if (tableMeta.hasExternalizedColumn()) {
            materialized.addAll(ExternalizedColumnDmlHelper.transformInsertForExternalizedColumns(
                logicalInsert, tableMeta, executionContext, ownerRoutes));
        }
        if (tableMeta.isMceDualWriteEnabled()) {
            materialized.addAll(ExternalizedColumnDmlHelper.transformInsertForMceAddrColumns(
                logicalInsert, tableMeta, executionContext, ownerRoutes));
        }
        stagingBatch = materialized;
    }

    @Override
    public List<RelNode> buildStagingPlans(List<RelNode> primaryPlans, ExecutionContext executionContext) {
        return stagingBatch.buildPhysicalPlans(primaryPlans, executionContext);
    }

    @Override
    public void afterExecutionSuccess() {
        stagingBatch.recordExecutionSuccess();
    }
}
