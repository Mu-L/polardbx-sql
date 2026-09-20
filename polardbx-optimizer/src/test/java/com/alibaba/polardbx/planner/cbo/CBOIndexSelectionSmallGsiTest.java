package com.alibaba.polardbx.planner.cbo;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class CBOIndexSelectionSmallGsiTest extends PlanTestCommon {

    public CBOIndexSelectionSmallGsiTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(CBOIndexSelectionSmallGsiTest.class);
    }

}