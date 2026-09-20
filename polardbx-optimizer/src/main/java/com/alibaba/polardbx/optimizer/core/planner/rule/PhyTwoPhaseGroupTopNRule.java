package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.alibaba.polardbx.optimizer.core.rel.mpp.ColumnarExchange;
import com.alibaba.polardbx.optimizer.core.rel.mpp.MppExchange;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.Map;

public class PhyTwoPhaseGroupTopNRule extends RelOptRule {
    public PhyTwoPhaseGroupTopNRule(RelOptRuleOperand operand, String description) {
        super(operand, "PhyTwoPhaseGroupTopNRule:" + description);
    }

    private static final Predicate<AbstractRelNode> PREDICATE =
        new PredicateImpl<AbstractRelNode>() {
            public boolean test(AbstractRelNode node) {
                if (node instanceof GroupTopN) {
                    // don't use two phase groupTopN if two phase groupTopN is used
                    return !(((GroupTopN) node).isPartial());
                }
                return true;
            }
        };

    public static final PhyTwoPhaseGroupTopNRule INSTANCE = new PhyTwoPhaseGroupTopNRule(
        operand(GroupTopN.class,
            operand(Exchange.class, operand(AbstractRelNode.class, null, PREDICATE, any()))), "COLUMNAR") {
        @Override
        public boolean matches(RelOptRuleCall call) {
            if (!super.matches(call)) {
                return false;
            }
            final GroupTopN groupTopN = (GroupTopN) call.rels[0];
            final Exchange exchange = (Exchange) call.rels[1];
            return ((!groupTopN.isPartial()) && (exchange.getTraitSet().getCollation().isTop()));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            onMatchInstance(call);
        }
    };

    public static final PhyTwoPhaseGroupTopNRule PROJECT = new PhyTwoPhaseGroupTopNRule(
        operand(GroupTopN.class,
            operand(PhysicalProject.class, operand(Exchange.class, any()))), "COLUMNAR_PROJECT") {
        @Override
        public boolean matches(RelOptRuleCall call) {
            if (!super.matches(call)) {
                return false;
            }
            final GroupTopN groupTopN = (GroupTopN) call.rels[0];
            final Exchange exchange = (Exchange) call.rels[2];
            return ((!groupTopN.isPartial()) && (exchange.getTraitSet().getCollation().isTop()));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            onMatchProject(call);
        }
    };

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_PARTIAL_GROUP_TOPN)) {
            return false;
        }
        if (!PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.PREFER_PARTIAL_GROUP_TOPN)) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
    }

    public void onMatchInstance(RelOptRuleCall call) {
        final GroupTopN groupTopN = (GroupTopN) call.rels[0];
        final Exchange exchange = (Exchange) call.rels[1];
        final RelNode relNode = call.rels[2];

        split(call, groupTopN, exchange, relNode);
    }

    public void onMatchProject(RelOptRuleCall call) {
        final GroupTopN groupTopN = (GroupTopN) call.rels[0];
        final PhysicalProject project = (PhysicalProject) call.rels[1];
        final Exchange exchange = (Exchange) call.rels[2];

        // make sure distribution columns are preserved in project
        Map<Integer, Integer> map = Maps.newHashMap();
        for (int i = 0; i < project.getProjects().size(); i++) {
            RexNode node = project.getProjects().get(i);
            if (node instanceof RexInputRef) {
                map.put(((RexInputRef) node).getIndex(), i);
            }
        }
        final Mapping mapping = (Mapping) Mappings.target(
            map::get,
            exchange.getRowType().getFieldCount(),
            project.getRowType().getFieldCount());
        RelDistribution distribution = exchange.getDistribution().apply(mapping);
        if (exchange.getDistribution().getType() != distribution.getType()) {
            return;
        }
        PhysicalProject newProject = project.copy(exchange.getInput().getTraitSet(), exchange.getInput(),
            project.getProjects(), project.getRowType());
        Exchange newExchange = createExchange(exchange, newProject, RelCollations.EMPTY, distribution);
        if (newExchange == null) {
            return;
        }

        split(call, groupTopN, newExchange, newProject);
    }

    private void split(RelOptRuleCall call,
                       GroupTopN groupTopN,
                       Exchange exchange,
                       RelNode bottom) {
        // partial group topN has no collation trait
        GroupTopN partialGroupTopN = GroupTopN.create(
            bottom.getTraitSet().replace(RelCollations.EMPTY),
            bottom,
            groupTopN.getInnerCollation(),
            groupTopN.getOffset(),
            groupTopN.getFetch(),
            groupTopN.getGroupSet(),
            true); // partial = true

        Exchange newExchange =
            createExchange(exchange, partialGroupTopN, RelCollations.EMPTY, exchange.getDistribution());
        if (newExchange == null) {
            return;
        }

        // Create global GroupTopN
        GroupTopN globalGroupTopN = GroupTopN.create(
            groupTopN.getTraitSet().replace(newExchange.getDistribution()),
            newExchange,
            groupTopN.getInnerCollation(),
            groupTopN.getOffset(),
            groupTopN.getFetch(),
            groupTopN.getGroupSet(),
            false); // partial = false

        call.transformTo(globalGroupTopN);
    }

    Exchange createExchange(Exchange oldExchange, RelNode input, RelCollation collation, RelDistribution distribution) {
        if (oldExchange instanceof MppExchange) {
            return MppExchange.create(input, collation, distribution);
        }
        if (oldExchange instanceof ColumnarExchange) {
            return ColumnarExchange.create(input, collation, distribution);
        }
        return null;
    }
}