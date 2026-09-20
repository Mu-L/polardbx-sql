package com.alibaba.polardbx.planner.htap;

import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleRecord;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingRuleClassifier;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingRuleManager;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingType;
import com.alibaba.polardbx.planner.common.RoutingRuleTestCommon;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

public class RoutingHtapTest extends RoutingRuleTestCommon {

    public RoutingHtapTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(RoutingHtapTest.class);
    }

    @Override
    protected RoutingRuleClassifier buildRoutingClassifier() {
        RoutingRuleRecord record = new RoutingRuleRecord();
        record.userName = RoutingRuleManager.ALL_USER;
        record.keywords = Arrays.asList("count", "lineitem");
        record.routingType = RoutingType.HTAP.name();
        return RoutingRuleClassifier.build(Arrays.asList(record));
    }
}
