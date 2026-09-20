package com.alibaba.polardbx.planner.planmanagement;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class PlanManagementCteTest extends PlanTestCommon {

    public PlanManagementCteTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        enablePlanManagementTest = true;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(PlanManagementCteTest.class);
    }

    @Override
    protected String getPlan(String testSql) {
        String oldValue = String.valueOf(DynamicConfig.getInstance().isEnableCTEReuse());
        String oldValue1 = String.valueOf(DynamicConfig.getInstance().getCteParserThreshold());
        try {
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.ENABLE_CTE_REUSE, "true");
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.CTE_PARSER_THRESHOLD, String.valueOf(0));
            return super.getPlan(testSql);
        } finally {
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_CTE_REUSE, oldValue);
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.CTE_PARSER_THRESHOLD, oldValue1);
        }
    }
}

