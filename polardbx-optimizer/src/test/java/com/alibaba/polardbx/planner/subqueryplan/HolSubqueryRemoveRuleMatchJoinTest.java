package com.alibaba.polardbx.planner.subqueryplan;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.apache.calcite.rel.RelNode;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Tests for HolSubqueryRemoveRule.matchJoin with multiple subqueries in join condition.
 * Verifies that variablesSet is correctly propagated when a join condition contains
 * multiple correlated subqueries (left-only+left-only, left-only+right-only, correlated+uncorrelated).
 * <p>
 * Uses optimizeByHolisticSubQueryToCorrelate to test just the SubQuery-to-Correlate step
 * (HolSubqueryRemoveRule), without running the full decorrelation pipeline.
 */
public class HolSubqueryRemoveRuleMatchJoinTest extends ParameterizedTestCommon {
    public HolSubqueryRemoveRuleMatchJoinTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                              String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(HolSubqueryRemoveRuleMatchJoinTest.class);
    }

    protected Planner getPlanner() {
        return new Planner() {
            @Override
            public RelNode optimize(RelNode input, PlannerContext plannerContext) {
                return optimizeByHolisticSubQueryUnnest(input, plannerContext, true);
            }

            @Override
            protected void initPlanShardInfo(ExecutionPlan executionPlan, ExecutionContext ec) {
            }
        };
    }
}
