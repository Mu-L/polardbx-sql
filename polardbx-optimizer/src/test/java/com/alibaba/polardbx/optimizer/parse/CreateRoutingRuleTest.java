package com.alibaba.polardbx.optimizer.parse;

import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class CreateRoutingRuleTest extends ParameterizedTestCommon {
    public CreateRoutingRuleTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(CreateRoutingRuleTest.class);
    }
}
