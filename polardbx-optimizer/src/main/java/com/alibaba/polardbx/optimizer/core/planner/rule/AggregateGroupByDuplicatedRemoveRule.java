package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.List;

/**
 * GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 => GROUP BY ClientIP
 */
public class AggregateGroupByDuplicatedRemoveRule extends RelOptRule {
    public static final AggregateGroupByDuplicatedRemoveRule INSTANCE =
        new AggregateGroupByDuplicatedRemoveRule(LogicalAggregate.class);

    public AggregateGroupByDuplicatedRemoveRule(Class<? extends Aggregate> aggregateClass) {
        super(
            operand(aggregateClass,
                operand(LogicalProject.class, any())),
            "AggregateGroupByDuplicatedRemoveRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_SIMPLIFY_GROUP_BY_RULE);
    }

    public void onMatch(RelOptRuleCall call) {
        final LogicalAggregate aggregate = call.rel(0);
        final LogicalProject input = call.rel(1);
        String pattern = null;
        boolean isMatch = true;
        for (Object i : input.getProjects()) {
            if (pattern == null) {
                if (!(i instanceof RexInputRef) || (RexUtil.getInputRefCount((RexInputRef) i) != 1)) {
                    return;
                }
                //IDEA may report error, but this regex is correct
                pattern = "-(" + ((RexInputRef) i).getName() + ", ?";
            } else {
                if (!(i instanceof RexCall) || (RexUtil.getInputRefCount((RexCall) i) != 1)) {
                    return;
                }
                //project need match: -($x, ?y)
                String project = i.toString();
                if (!project.startsWith(pattern)
                    || !NumberUtils.isDigits(project.substring(pattern.length(), project.length() - 1))
                    || !project.endsWith(")")) {
                    isMatch = false;
                    break;
                }
            }
        }
        if (!isMatch) {
            return;
        }

        if (aggregate.getGroupCount() <= 1) {
            return;
        }

        RexNode firstGroupByColumn = null;
        for (int i : aggregate.getGroupSet()) {
            //groupNode -> select index
            Pair<RexNode, String> pair = RexInputRef.of2(i, input.getRowType().getFieldList());
            firstGroupByColumn = pair.getKey();
            break;
        }

        final RelBuilder relBuilder = call.builder();

        //rebuild new group by key
        relBuilder.push(input);
        relBuilder.aggregate(relBuilder.groupKey(firstGroupByColumn), aggregate.getAggCallList());
        RelNode newAgg = relBuilder.build();
        if (!(newAgg instanceof LogicalAggregate)) {
            return;
        }
        relBuilder.push(newAgg);

        //original project output is: ClientIP, ClientIP-1, ClientIP-2, ..., therefore we need to build a new project to match it
        //operator tree like:
        // project_new($0, $0-1,$0-2,$0-3)
        //  aggregate($0)
        //   project_old($7,$7-1,$7-2,$7-3)
        //notice: the reference of project_new is not equal to project_old, we need to shift the offset by TargetMapping

        List<Pair<RexNode, String>> projects = Lists.newArrayList();
        RexInputRef firstRef = (RexInputRef) (input.getProjects().get(((RexInputRef) firstGroupByColumn).getIndex()));

        for (int i : aggregate.getGroupSet()) {
            RexNode node = input.getProjects().get(i);
            RexNode newNode = RexUtil.shift(node, -firstRef.getIndex());
            projects.add(Pair.of(newNode, input.getRowType().getFieldList().get(i).getName()));
        }

        for (int i = 1; i < newAgg.getRowType().getFieldList().size(); i++) {
            projects.add(Pair.of(relBuilder.getRexBuilder().makeInputRef(newAgg, i),
                newAgg.getRowType().getFieldList().get(i).getName()));
        }
        relBuilder.project(Pair.left(projects), Pair.right(projects));
        call.transformTo(relBuilder.build());
    }

}
