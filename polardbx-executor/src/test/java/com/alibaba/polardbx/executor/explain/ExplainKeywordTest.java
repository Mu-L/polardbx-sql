package com.alibaba.polardbx.executor.explain;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.utils.ExplainExecutorUtil;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class ExplainKeywordTest extends ParameterizedTestCommon {

    public ExplainKeywordTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(ExplainKeywordTest.class);
    }

    protected void processParameter(SqlParameterized sqlParameterized, ExecutionContext executionContext) {
        super.processParameter(sqlParameterized, executionContext);
        if (sqlParameterized != null) {
            executionContext.setSqlParameterized(sqlParameterized);
        }
    }

    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param,
                                         SqlExplainLevel sqlExplainLevel) {
        ExecutionContext executionContext = PlannerContext.getPlannerContext(plan).getExecutionContext();
        try {
            StringBuilder sb = new StringBuilder();
            Method method = ExplainExecutorUtil.class.getDeclaredMethod("handleExplainKeyword", ExecutionContext.class);
            method.setAccessible(true);
            ResultCursor cursor = (ResultCursor) method.invoke(null, executionContext);
            Row row = cursor.next();
            while (row != null) {
                sb.append(row.getString(0));
                row = cursor.next();
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke handleExplainKeyword", e);
        }
    }
}
