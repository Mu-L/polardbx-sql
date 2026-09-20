package com.alibaba.polardbx.planner.forceindex;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.planmanager.LogicalViewFinder;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ComplexIndexTest extends ParameterizedTestCommon {
    public ComplexIndexTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(ComplexIndexTest.class);
    }

    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param,
                                         SqlExplainLevel sqlExplainLevel) {
        ExecutionContext executionContext = PlannerContext.getPlannerContext(plan).getExecutionContext();
        try {
            StringBuilder sb = new StringBuilder();
            LogicalViewFinder logicalViewFinder = new LogicalViewFinder();
            plan.accept(logicalViewFinder);
            Set<String> tables =
                logicalViewFinder.getResult().stream().map(LogicalView::getTableNames).flatMap(Collection::stream)
                    .map(x -> StatisticManager.getSourceTableName(executionContext.getSchemaName(), x))
                    .collect(Collectors.toSet());
            for (String tableName : tables) {
                List<String> realTables = Lists.newArrayList();
                realTables.add(tableName);
                realTables.addAll(GlobalIndexMeta.getPublishedIndexNames(tableName, executionContext.getSchemaName(),
                    executionContext));
                for (String realTable : realTables) {
                    TableMeta tm = executionContext.getSchemaManager().getTable(realTable);
                    sb.append(tm.getTableName()).append("\n");
                    for (int i = 0; i < tm.getIndexes().size(); i++) {
                        IndexMeta indexMeta = tm.getIndexes().get(i);
                        sb.append(i).append(":").append(indexMeta.getPhysicalIndexName());
                        sb.append("(").append(indexMeta.getUserDefinedKeyParts()).append(":")
                            .append(indexMeta.getActualKeyParts()).append("):");
                        indexMeta.getKeyColumnsExt().forEach(x -> sb.append(x.toString()).append(", "));
                        sb.append("\n");
                    }
                    for (ColumnMeta columnMeta : tm.getAllColumns()) {
                        sb.append(columnMeta.getName()).append(":");
                        sb.append("part_of_key").append(columnMeta.getPartOfKey());
                        sb.append(" part_of_sort_key").append(columnMeta.getPartOfSortKey()).append("\n");
                    }
                }
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke handleExplainKeyword", e);
        }
    }
}
