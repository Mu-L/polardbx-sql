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
import java.util.Set;
import java.util.stream.Collectors;

public class MqUniqueKeysTest extends ParameterizedTestCommon {
    public MqUniqueKeysTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(MqUniqueKeysTest.class);
    }

    @Override
    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param,
                                         SqlExplainLevel sqlExplainLevel) {
        RelMetadataQuery mq = plan.getCluster().getMetadataQuery();
        Set<ImmutableBitSet> uks = mq.getUniqueKeys(plan);
        if (uks == null) {
            return "null";
        }
        if (uks.isEmpty()) {
            return "empty";
        }
        return uks.stream().sorted().map(ImmutableBitSet::toString).collect(Collectors.joining("\n"));
    }
}
