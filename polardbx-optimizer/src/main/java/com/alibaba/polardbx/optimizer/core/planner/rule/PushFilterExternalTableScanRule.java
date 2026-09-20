package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import com.alibaba.polardbx.optimizer.PlannerContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * Planner rule that pushes a {@link Filter} predicate into an
 * {@link ExternalTableScan} by calling its {@code pushFilter} interface.
 * <p>
 * The rule passes the <em>entire</em> condition to
 * {@link ExternalTableScan#pushFilter(RexNode)} and interprets the returned
 * {@link Pair}:
 * <ul>
 *   <li>{@code key == false} (nothing pushed) → rule does nothing to avoid an
 *       infinite loop.</li>
 *   <li>{@code key == true} and {@code value} is empty → source accepted
 *       everything; the {@link Filter} node is removed.</li>
 *   <li>{@code key == true} and {@code value} is non-empty → source accepted
 *       part of the condition; the residual conjuncts in {@code value} are
 *       composed back into an AND and kept as a new {@link Filter} above the
 *       updated scan.</li>
 * </ul>
 *
 * @see PushFilterRule
 */
public class PushFilterExternalTableScanRule extends RelOptRule {

    public static final PushFilterExternalTableScanRule INSTANCE =
        new PushFilterExternalTableScanRule();

    public PushFilterExternalTableScanRule() {
        super(operand(Filter.class, operand(ExternalTableScan.class, none())),
            "PushFilterExternalTableScanRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final ExternalTableScan scan = call.rel(1);

        if (!PlannerContext.getPlannerContext(filter).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_EXTERNAL_PUSH_FILTER)) {
            return;
        }

        // Copy the scan so pushed-down state is isolated per planning attempt.
        final ExternalTableScan newScan =
            (ExternalTableScan) scan.copy(scan.getTraitSet(), scan.getInputs());

        final Pair<Boolean, List<RexNode>> result = newScan.pushFilter(filter.getCondition());

        // key == false means nothing was pushed – avoid an infinite loop.
        if (!result.getKey()) {
            return;
        }

        final List<RexNode> residualConjuncts = result.getValue();
        final RelNode newRel;
        if (CollectionUtils.isEmpty(residualConjuncts)) {
            // Entire condition pushed: drop the Filter node.
            newRel = newScan;
        } else {
            // Partial push: compose residual conjuncts into a single AND
            // expression and keep it as a new Filter above the scan.
            final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
            final RexNode residual =
                RexUtil.composeConjunction(rexBuilder, residualConjuncts, false);
            newRel = filter.copy(filter.getTraitSet(), newScan, residual);
        }

        call.transformTo(newRel);
    }
}
