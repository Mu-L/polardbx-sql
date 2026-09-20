package com.alibaba.polardbx.planner.forceindex;

import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class MaterializedSemiJoinCoexistenceTest extends ParameterizedTestCommon {
    public MaterializedSemiJoinCoexistenceTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                               String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(MaterializedSemiJoinCoexistenceTest.class);
    }

    @Override
    protected String getPlan(String testSql) {
        boolean is80 = InstanceVersion.isMYSQL80();
        try {
            InstanceVersion.setMYSQL80(true);
            return super.getPlan(testSql);
        } finally {
            InstanceVersion.setMYSQL80(is80);
        }
    }
}
