package com.alibaba.polardbx.planner.forceindex;

import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class PaginationForceTest extends ParameterizedTestCommon {
    public PaginationForceTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(PaginationForceTest.class);
    }

    @Override
    protected String getPlan(String testSql) {
        boolean is80 = InstanceVersion.isMYSQL80();
        try {
            InstanceVersion.setMYSQL80(false);
            return super.getPlan(testSql);
        } finally {
            InstanceVersion.setMYSQL80(is80);
        }
    }
}