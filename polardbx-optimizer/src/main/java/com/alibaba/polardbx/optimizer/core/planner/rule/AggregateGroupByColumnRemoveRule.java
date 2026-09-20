package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.math.NumberUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * SELECT 1, URL FROM hits GROUP BY 1, URL => SELECT 1, URL FROM hits GROUP BY URL
 */
public class AggregateGroupByColumnRemoveRule extends RelOptRule {

    public static final AggregateGroupByColumnRemoveRule INSTANCE =
        new AggregateGroupByColumnRemoveRule(LogicalAggregate.class);

    /**
     * Creates an AggregateGroupByColumnRemoveRule.
     */
    public AggregateGroupByColumnRemoveRule(Class<? extends Aggregate> aggregateClass) {
        super(
            operand(aggregateClass,
                operand(LogicalProject.class, any())),
            "AggregateGroupByColumnRemoveRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_SIMPLIFY_GROUP_BY_RULE);
    }

    public void onMatch(RelOptRuleCall call) {
        final LogicalAggregate aggregate = call.rel(0);
        final LogicalProject input = call.rel(1);
        // input: [group key1 column, group key2 column, ..., agg call1 column, agg call2 column]
        // e.g. SELECT URL, min(Referer), max(Referer), 1111, 44444 FROM hits GROUP BY 4, URL, 5, UserID;
        //      input:[4444, URL, $f2, UserId, Refer]
        List<RexNode> groupByList = new ArrayList<>();
        List<Integer> constantList = new ArrayList<>();
        // to avoid group by single constant column, e.g. group by 79, etc.
        if (aggregate.getGroupSet().cardinality() <= 1) {
            return ;
        }
        for (int i : aggregate.getGroupSet()) {
            // (groupNode, group by key). e.g. ($0, 1), ($1, URL)
            Pair<RexNode, String> pair = RexInputRef.of2(i, input.getRowType().getFieldList());
            // whether group by key is constant
            if (NumberUtils.isDigits(pair.getValue())) {
                // where the aggregate index is constant
                constantList.add(i);
                continue;
            }
            groupByList.add(pair.getKey());
        }

        // to avoid extra impact, we ensure there must be 1 constant
        if (constantList.size() != 1 || constantList.get(0) != 0) {
            return;
        }

        // build the new aggregate operator
        final RelBuilder relBuilder = call.builder();
        relBuilder.push(input);
        relBuilder.aggregate(relBuilder.groupKey(groupByList), aggregate.getAggCallList());
        RelNode newAgg = relBuilder.build();
        if (!(newAgg instanceof LogicalAggregate)) {
            return;
        }
        relBuilder.push(newAgg);
        //add the top project operator
        List<Pair<RexNode, String>> projects = Lists.newArrayList();
        // new project: [group by key 1(constant) group by key 2, ..., agg call 1, agg call 2]
        //   agg output: [group by key 2, ..., agg call 1, agg call 2]
        //     input project: [group by key 1(constant), group by key 2, ..., agg call 1, agg call 2]
        for (int i : constantList) {
            if (!(input.getProjects().get(i) instanceof RexDynamicParam)) {
                return;
            }
            projects.add(Pair.of(input.getProjects().get(i), input.getRowType().getFieldList().get(i).getName()));
        }
        for (int i = 0; i < newAgg.getRowType().getFieldList().size(); i++) {
            projects.add(Pair.of(relBuilder.getRexBuilder().makeInputRef(newAgg, i),
                newAgg.getRowType().getFieldList().get(i).getName()));
        }
        relBuilder.project(Pair.left(projects), Pair.right(projects));
        call.transformTo(relBuilder.build());
    }
}