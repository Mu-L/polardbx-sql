package com.alibaba.polardbx.planner.subqueryplan;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.rule.cte.SubqueryAwareRelShuttle;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.junit.Assert;
import org.junit.runners.Parameterized;

import java.util.List;

public class HolDecorrelatorExtraTest extends ParameterizedTestCommon {
    public HolDecorrelatorExtraTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        //setExplainCost(true);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(HolDecorrelatorExtraTest.class);
    }

    protected Planner getPlanner() {
        return new Planner() {
            public RelNode optimizeByHolisticSubQueryToCorrelate(RelNode input,
                                                                 PlannerContext plannerContext) {
                RelNode relNode = super.optimizeByHolisticSubQueryToCorrelate(input, plannerContext);
                relNode.accept(new SubqueryAwareRelShuttle() {
                    @Override
                    public RelNode visit(LogicalJoin join) {
                        Assert.assertTrue(
                            "LogicalJoin should carry no correlation variables after holistic decorrelation, but got: "
                                + join.getVariablesSet(),
                            join.getVariablesSet().isEmpty());
                        return super.visit(join);
                    }
                });
                return relNode;
            }
        };
    }
}
