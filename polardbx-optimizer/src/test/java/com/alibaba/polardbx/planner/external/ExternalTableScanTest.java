package com.alibaba.polardbx.planner.external;

import com.alibaba.polardbx.optimizer.external.connector.PushdownCapability;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.EnumSet;
import java.util.List;

/**
 * Plan-level unit tests for {@link com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan}.
 * <p>
 * Each SQL / expected-plan pair is driven by {@code ExternalTableScanTest.yml}.
 */
public class ExternalTableScanTest extends ParameterizedTestCommon {

    public ExternalTableScanTest(String caseName, int sqlIndex, String sql,
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
        return loadSqls(ExternalTableScanTest.class);
    }
}
