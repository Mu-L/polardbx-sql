package com.alibaba.polardbx.planner.rewriter;

import com.alibaba.polardbx.planner.common.EclipseParameterized;
import com.alibaba.polardbx.planner.hintplan.index.ParameterizedHintTestCommon;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(EclipseParameterized.class)
public class PruningTest extends ParameterizedHintTestCommon {

    public PruningTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(PruningTest.class);
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }
}
