package com.alibaba.polardbx.planner.cte;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.planner.common.CTEReuseTestCommon;
import org.apache.calcite.rel.RelNode;
import org.junit.runners.Parameterized;

import java.util.List;

public class ExplainCteConsumerTest extends CTEReuseTestCommon {
    public ExplainCteConsumerTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(ExplainCteConsumerTest.class);
    }

    protected Planner getPlanner() {
        return new Planner() {

            @Override
            protected void initPlanShardInfo(ExecutionPlan executionPlan, ExecutionContext ec) {
            }

            @Override
            public RelNode optimizeByPlanEnumerator(RelNode originInput, RelNode input, PlannerContext plannerContext) {
                return input;
            }
        };
    }
}
