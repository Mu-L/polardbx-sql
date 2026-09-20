package com.alibaba.polardbx.planner.cbo;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Test for AONE-81039659: GSI Lookup optimization with MPP execution.
 * <p>
 * This test verifies that queries on partition tables with multiple GSI indexes
 * work correctly when MPP mode is enabled. The bug was that GsiColsReplaceRule
 * produced a PhysicalProject with Convention.NONE trait, which the MPP planner
 * couldn't handle, causing "MPP Sql could not be implemented" error.
 * <p>
 * After the fix, this test should generate a valid MPP execution plan.
 */
public class MppGsiLookupPlanTest extends PlanTestCommon {

    public MppGsiLookupPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        enableMpp = true;
        forceWorkloadTypeAP = true;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(MppGsiLookupPlanTest.class);
    }
}
