package com.alibaba.polardbx.planner.columnar;

import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.Ignore;
import org.junit.runners.Parameterized;

import java.util.List;

@Ignore("reopen when support cte")
public class TpcdsColumnarTest extends ParameterizedTestCommon {

    public TpcdsColumnarTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(TpcdsColumnarTest.class);
    }
}