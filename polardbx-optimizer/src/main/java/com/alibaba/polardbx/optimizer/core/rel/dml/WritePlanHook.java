package com.alibaba.polardbx.optimizer.core.rel.dml;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

import java.util.Collections;
import java.util.List;

/**
 * Execution-scoped extension point between primary routing and physical-plan construction.
 *
 * <p>The optimizer owns the write pipeline and invokes this hook at deterministic boundaries. Implementations may
 * materialize values after primary routing and add staging plans after primary business plans exist. Implementations
 * must not retain plan-cache writers or be reused by concurrent statement executions.
 *
 * <p>Example: {@code INSERT INTO t(id, body) VALUES (7, 'payload')} first routes row 0 to
 * {@code db1/g0/t_0003}. {@link #materialize(RoutedWriteInput, ExecutionContext)} uploads {@code body} for that owner
 * and replaces it with BlobRef B; the Writer then builds its normal primary and replica plans from B;
 * {@link #buildStagingPlans(List, ExecutionContext)} finally creates the staging INSERT paired with the primary
 * plan. The hook changes only value representation and staging—not routing, replica expansion or GSI lifecycle.
 */
public interface WritePlanHook<R extends RoutedWriteInput> {

    void materialize(R routedInput, ExecutionContext executionContext);

    List<RelNode> buildStagingPlans(List<RelNode> primaryPlans, ExecutionContext executionContext);

    default void afterExecutionSuccess() {
    }

    default boolean isNoop() {
        return false;
    }

    @SuppressWarnings("unchecked")
    static <T extends RoutedWriteInput> WritePlanHook<T> noop() {
        return (WritePlanHook<T>) NoopHolder.INSTANCE;
    }

    final class NoopHolder {
        private static final WritePlanHook<RoutedWriteInput> INSTANCE = new WritePlanHook<RoutedWriteInput>() {
            @Override
            public void materialize(RoutedWriteInput routedInput, ExecutionContext executionContext) {
            }

            @Override
            public List<RelNode> buildStagingPlans(List<RelNode> primaryPlans, ExecutionContext executionContext) {
                return Collections.emptyList();
            }

            @Override
            public boolean isNoop() {
                return true;
            }
        };

        private NoopHolder() {
        }
    }
}
