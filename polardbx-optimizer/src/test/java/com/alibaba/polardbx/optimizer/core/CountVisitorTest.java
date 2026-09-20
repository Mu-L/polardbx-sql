package com.alibaba.polardbx.optimizer.core;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.core.rel.CountVisitor;
import com.alibaba.polardbx.planner.common.EclipseParameterized;
import com.alibaba.polardbx.planner.rewriter.BasePlanRewriterTest;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;

@RunWith(EclipseParameterized.class)
public class CountVisitorTest extends BasePlanRewriterTest {

    public CountVisitorTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(CountVisitorTest.class);
    }

    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param) {

        CountVisitor countVisitor = new CountVisitor();
        plan.accept(countVisitor);
        countVisitor.build();
        return String.valueOf(countVisitor.getMaxContinuousJoinCount());
        //+ "\n" + planStr;
    }
}
