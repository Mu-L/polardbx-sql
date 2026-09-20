package com.alibaba.polardbx.planner.subqueryplan;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.apache.calcite.rel.RelNode;
import org.junit.runners.Parameterized;

import java.util.List;

public class HolSubqueryToCorTest extends ParameterizedTestCommon {
    public HolSubqueryToCorTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(HolSubqueryToCorTest.class);
    }

    protected Planner getPlanner() {
        return new Planner() {
            @Override
            public RelNode optimize(RelNode input, PlannerContext plannerContext) {
                return optimizeByHolisticSubQueryToCorrelate(input, plannerContext);
            }

            @Override
            protected void initPlanShardInfo(ExecutionPlan executionPlan, ExecutionContext ec) {
            }
        };
    }
}
