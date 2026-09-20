package com.alibaba.polardbx.planner.cte;

import com.alibaba.polardbx.planner.common.CTEReuseTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class CTEReuseTest extends CTEReuseTestCommon {

    public CTEReuseTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(CTEReuseTest.class);
    }
}