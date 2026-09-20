package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedInsertInput;
import com.alibaba.polardbx.optimizer.core.rel.dml.WritePlanHook;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * INSERT hook for exact rows produced by an INSERT-family composite Writer.
 *
 * <p>Unlike {@link ExternalizedInsertWritePlanHook}, which transforms caller JDBC parameters, this hook delegates
 * materialization of a classified after-image such as an UPSERT relocate row. The leaf {@code InsertWriter} still
 * computes the route first and builds its original primary/replica plans after the materializer has copied the
 * resulting BlobRefs into that leaf's parameter rows.
 */
public final class ExternalizedInsertRowsWritePlanHook implements WritePlanHook<RoutedInsertInput> {
    private final BiFunction<RoutedInsertInput, ExecutionContext, TransactionalStagingWriteBatch> materializer;
    private TransactionalStagingWriteBatch stagingBatch = new TransactionalStagingWriteBatch();

    public ExternalizedInsertRowsWritePlanHook(
        BiFunction<RoutedInsertInput, ExecutionContext, TransactionalStagingWriteBatch> materializer) {
        this.materializer = Objects.requireNonNull(materializer, "insert-row materializer is null");
    }

    @Override
    public void materialize(RoutedInsertInput routedInput, ExecutionContext executionContext) {
        stagingBatch = Objects.requireNonNull(materializer.apply(routedInput, executionContext),
            "insert-row materializer returned null staging batch");
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
