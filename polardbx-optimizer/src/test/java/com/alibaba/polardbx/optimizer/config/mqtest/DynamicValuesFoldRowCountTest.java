package com.alibaba.polardbx.optimizer.config.mqtest;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.After;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Verifies that DrdsRelMdRowCount reports the RawString array length for folded VALUES
 * (single tuple of CAST(?) column arrays) instead of falling back to the default of one row.
 */
public class DynamicValuesFoldRowCountTest extends ParameterizedTestCommon {
    public DynamicValuesFoldRowCountTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                         String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(DynamicValuesFoldRowCountTest.class);
    }

    @Override
    public void beforeOptimize(ExecutionContext ec) {
        DynamicConfig.getInstance().setEnableDynamicValuesOptimization(true);
    }

    @After
    public void resetDynamicValuesOptimization() {
        DynamicConfig.getInstance().setEnableDynamicValuesOptimization(false);
    }

    @Override
    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param,
                                         SqlExplainLevel sqlExplainLevel) {
        RelMetadataQuery mq = plan.getCluster().getMetadataQuery();
        AtomicReference<Double> rowCount = new AtomicReference<>(-1D);
        plan.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(RelNode other) {
                if (other instanceof DynamicValues) {
                    rowCount.set(mq.getRowCount(other));
                }
                return super.visit(other);
            }
        });
        return String.valueOf(rowCount.get().longValue());
    }
}
