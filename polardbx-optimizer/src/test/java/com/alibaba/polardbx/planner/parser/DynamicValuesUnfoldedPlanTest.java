package com.alibaba.polardbx.planner.parser;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.Before;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Baseline plans with ENABLE_DYNAMIC_VALUES_OPTIMIZATION left at its default (disabled): typed
 * table-source VALUES keep one DynamicValues tuple per row.
 */
public class DynamicValuesUnfoldedPlanTest extends ParameterizedTestCommon {
    public DynamicValuesUnfoldedPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                         String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(DynamicValuesUnfoldedPlanTest.class);
    }

    @Before
    public void disableDynamicValuesOptimization() {
        DynamicConfig.getInstance().setEnableDynamicValuesOptimization(false);
    }
}
