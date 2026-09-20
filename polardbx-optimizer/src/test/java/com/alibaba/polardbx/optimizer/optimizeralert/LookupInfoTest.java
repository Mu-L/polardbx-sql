package com.alibaba.polardbx.optimizer.optimizeralert;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.planmanager.LogicalViewFinder;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;

public class LookupInfoTest extends ParameterizedTestCommon {
    public LookupInfoTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(LookupInfoTest.class);
    }

    @Override
    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param,
                                         SqlExplainLevel sqlExplainLevel) {
        LogicalViewFinder logicalViewFinder = new LogicalViewFinder();
        plan.accept(logicalViewFinder);
        boolean found = false;
        for (LogicalView lv : logicalViewFinder.getResult()) {
            if (lv.isLookupTable() && lv.getLookupInfo().isGsiLookup()) {
                found = true;
            }
        }
        return found + "\n" + super.removeSubqueryHashCode(planStr, plan, param, sqlExplainLevel);
    }
}
