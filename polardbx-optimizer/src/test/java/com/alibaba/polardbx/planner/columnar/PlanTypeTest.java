package com.alibaba.polardbx.planner.columnar;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;

public class PlanTypeTest extends ParameterizedTestCommon {
    public PlanTypeTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(PlanTypeTest.class);
    }

    @Override
    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param,
                                         SqlExplainLevel sqlExplainLevel) {
        return PlannerContext.getPlannerContext(plan).getPlanType().toString() + "\n"
            + "columnar_optimizer:" + PlannerContext.getPlannerContext(plan).isColumnarOptimizer() + "\n"
            + super.removeSubqueryHashCode(planStr, plan, param, sqlExplainLevel);
    }
}
