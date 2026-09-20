package com.alibaba.polardbx.planner.subqueryplan;

import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;

/**
 * @author chenghui.lch 2018年1月4日 下午1:44:54
 * @since 5.0.0
 */
public class FilterSubQueryPlanV1Test extends PlanTestCommon {

    public FilterSubQueryPlanV1Test(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum,
                                    String expect, String nodetree, String struct) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum, expect, nodetree, struct);
    }

    @Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(FilterSubQueryPlanV1Test.class);
    }

    @Override
    protected String getPlan(String testSql) {
        try {
            CostModelWeight.setVersion("v1");
            return super.getPlan(testSql);
        } finally {
            CostModelWeight.setVersion("");
        }
    }
}
