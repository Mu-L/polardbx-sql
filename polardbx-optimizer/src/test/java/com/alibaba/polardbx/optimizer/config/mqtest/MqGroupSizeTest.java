package com.alibaba.polardbx.optimizer.config.mqtest;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;

public class MqGroupSizeTest extends ParameterizedTestCommon {
    public MqGroupSizeTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(MqGroupSizeTest.class);
    }

    @Override
    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param,
                                         SqlExplainLevel sqlExplainLevel) {
        RelMetadataQuery mq = plan.getCluster().getMetadataQuery();
        return String.valueOf(mq.getColumnsGroupSize(plan, ImmutableBitSet.range(plan.getRowType().getFieldCount())));
    }
}
