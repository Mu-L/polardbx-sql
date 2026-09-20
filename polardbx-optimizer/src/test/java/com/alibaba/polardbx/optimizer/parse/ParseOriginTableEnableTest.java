package com.alibaba.polardbx.optimizer.parse;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class ParseOriginTableEnableTest extends ParameterizedTestCommon {
    public ParseOriginTableEnableTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(ParseOriginTableEnableTest.class);
    }

    @Override
    protected String getPlan(String testSql) {
        try {
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_PARSE_ORIGINAL_TABLE, "true");
            return super.getPlan(testSql);
        } finally {
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_PARSE_ORIGINAL_TABLE, "false");
        }
    }
}