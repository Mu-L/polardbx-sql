package com.alibaba.polardbx.planner.parser;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.After;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * End-to-end plans with ENABLE_DYNAMIC_VALUES_OPTIMIZATION enabled: typed table-source VALUES are
 * folded into a single-tuple DynamicValues whose parameters carry the per-column value lists.
 * Folded plans carry the optimizer-maintained raw-string mode marker.
 */
public class DynamicValuesFoldPlanTest extends ParameterizedTestCommon {
    public DynamicValuesFoldPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(DynamicValuesFoldPlanTest.class);
    }

    @Override
    public void beforeOptimize(ExecutionContext ec) {
        DynamicConfig.getInstance().setEnableDynamicValuesOptimization(true);
    }

    @After
    public void resetDynamicValuesOptimization() {
        DynamicConfig.getInstance().setEnableDynamicValuesOptimization(false);
    }
}
