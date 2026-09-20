package com.alibaba.polardbx.planner.external;

import com.alibaba.polardbx.optimizer.external.connector.PushdownCapability;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.EnumSet;
import java.util.List;

/**
 * Plan-level unit tests for external table pushdown switches.
 * Verifies that ENABLE_EXTERNAL_PUSH_PROJECT/FILTER/SORT/AGG switches
 * correctly disable the corresponding pushdown rules when set to false via hint.
 */
public class ExternalTablePushdownSwitchTest extends ParameterizedTestCommon {

    public ExternalTablePushdownSwitchTest(String caseName, int sqlIndex, String sql,
                                           String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected EnumSet<PushdownCapability> getCapabilities() {
        return EnumSet.of(
            PushdownCapability.PROJECT,
            PushdownCapability.FILTER,
            PushdownCapability.SORT,
            PushdownCapability.AGG);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(ExternalTablePushdownSwitchTest.class);
    }
}
