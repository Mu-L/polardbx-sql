package com.alibaba.polardbx.planner.oss.ttl;

import org.junit.runners.Parameterized;

import java.util.List;

public class TtlQueryPlanSubPartitionTest extends TtlTransparentQueryPlanTestBase {
    public TtlQueryPlanSubPartitionTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                        String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(TtlQueryPlanSubPartitionTest.class);
    }
}
