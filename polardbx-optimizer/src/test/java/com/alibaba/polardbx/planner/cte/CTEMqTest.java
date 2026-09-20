package com.alibaba.polardbx.planner.cte;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.cte.CTEContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.cte.SubqueryAwareRelShuttle;
import com.alibaba.polardbx.planner.common.CTEReuseTestCommon;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CTEMqTest extends CTEReuseTestCommon {
    public CTEMqTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        //setExplainCost(true);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(CTEMqTest.class);
    }

    @Override
    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param,
                                         SqlExplainLevel sqlExplainLevel) {
        RelMetadataQuery mq = plan.getCluster().getMetadataQuery();
        CTEContext cteContext = PlannerContext.getPlannerContext(plan).getCteContext();
        cteContext.clear();
        cteContext.reCollect(plan);
        StringBuilder sb = new StringBuilder();

        List<Set<RelColumnOrigin>> columns = mq.getColumnOriginNames(plan);
        sb.append("ColumnOriginNames:")
            .append(columns.stream().map(column -> column.stream()
                .map(RelColumnOrigin::getColumnName).sorted(String::compareToIgnoreCase)
                .collect(Collectors.joining(" "))
            ).collect(Collectors.joining(",")))
            .append("\n");

        Set<RelColumnOrigin> columnOrigin = mq.getColumnOrigins(plan, 0);
        sb.append("ColumnOrigins:")
            .append(columnOrigin.stream()
                .map(RelColumnOrigin::getColumnName)
                .sorted(String::compareToIgnoreCase)
                .collect(Collectors.joining(" "))
            )
            .append("\n");

        Set<RexTableInputRef.RelTableRef> tableReferences = mq.getTableReferences(plan);
        sb.append("TableReferences:")
            .append(tableReferences.stream().map(x -> Util.last(x.getTable().getQualifiedName()))
                .sorted(String::compareToIgnoreCase)
                .collect(Collectors.joining(",")))
            .append("\n");

        Double maxRowCount = mq.getMaxRowCount(plan);
        sb.append("MaxRowCount:").append(maxRowCount).append("\n");

        Set<ImmutableBitSet> uniqueKeys = mq.getUniqueKeys(plan);
        sb.append("UniqueKeys:").append(uniqueKeys).append("\n");
        return sb.toString();
    }
}
