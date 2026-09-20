package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedModifyInput;
import com.alibaba.polardbx.optimizer.core.rel.dml.WritePlanHook;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * UPDATE-family hook that delegates existing row-binding semantics to an execution-scoped materializer while owning
 * the common staging-plan lifecycle. For {@code UPDATE t SET body=CONCAT(body, '-tail')}, the ordinary Writer first
 * classifies and routes the selected row, the materializer turns the evaluated logical body into a route-owned
 * BlobRef, and the same Writer builds its primary/replica plans from that value.
 */
public final class ExternalizedModifyWritePlanHook implements WritePlanHook<RoutedModifyInput> {
    private final BiFunction<RoutedModifyInput, ExecutionContext, TransactionalStagingWriteBatch> materializer;
    private TransactionalStagingWriteBatch stagingBatch = new TransactionalStagingWriteBatch();

    public ExternalizedModifyWritePlanHook(
        BiFunction<RoutedModifyInput, ExecutionContext, TransactionalStagingWriteBatch> materializer) {
        this.materializer = Objects.requireNonNull(materializer, "modify materializer is null");
    }

    @Override
    public void materialize(RoutedModifyInput routedInput, ExecutionContext executionContext) {
        stagingBatch = Objects.requireNonNull(materializer.apply(routedInput, executionContext),
            "modify materializer returned null staging batch");
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
