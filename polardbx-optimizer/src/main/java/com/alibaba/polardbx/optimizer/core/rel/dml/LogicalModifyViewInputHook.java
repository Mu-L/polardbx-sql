package com.alibaba.polardbx.optimizer.core.rel.dml;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Map;

/**
 * Execution-scoped extension point inside the original LogicalModifyView route/build pipeline.
 *
 * <p>The hook runs only after pruning has selected one concrete primary physical branch and before
 * {@code PlannerUtils.buildParam} converts logical JDBC parameters into physical SQL parameters. It may replace
 * values for that branch and add companion plans, but it must not calculate another route or retain a cached
 * LogicalModifyView.
 *
 * <p>For example, {@code UPDATE t SET body=? WHERE id IN (1, 100001)} may route to
 * {@code g0/t_0000} and {@code g1/t_0003}. An external-column hook copies each branch's parameter map, materializes
 * two route-owned BlobRefs, lets the normal builder create both UPDATE plans, and then prepends the matching staging
 * INSERT plans. Ordinary UPDATEs do not create a hook and continue through the original {@code getInput} entry.
 */
public interface LogicalModifyViewInputHook {

    Map<Integer, ParameterContext> materializeParameters(PhysicalRoute route,
                                                         Map<Integer, ParameterContext> parameters,
                                                         ExecutionContext executionContext);

    List<RelNode> buildStagingPlans(List<RelNode> unmergedPrimaryPlans, ExecutionContext executionContext);

    default void afterExecutionSuccess() {
    }
}
