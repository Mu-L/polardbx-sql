package com.alibaba.polardbx.dump.operator;

import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * can't be opensource
 */
public class GroupTopNSwitchTest extends ParameterizedTestCommon {
    public GroupTopNSwitchTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected String getPlan(String testSql) {
        try {
            ConfigDataMode.setInstanceRole(InstanceRole.COLUMNAR_SLAVE);
            return super.getPlan(testSql);
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(GroupTopNSwitchTest.class);
    }
}
