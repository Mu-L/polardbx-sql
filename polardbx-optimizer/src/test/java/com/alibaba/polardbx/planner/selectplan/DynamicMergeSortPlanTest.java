package com.alibaba.polardbx.planner.selectplan;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;

/**
 * @author chenghui.lch 2018年1月4日 上午11:19:54
 * @since 5.0.0
 */
public class DynamicMergeSortPlanTest extends PlanTestCommon {

    public DynamicMergeSortPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(DynamicMergeSortPlanTest.class);
    }

}
