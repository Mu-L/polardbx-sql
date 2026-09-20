package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.TableSource;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test-only {@link TableSource} that wraps a {@link LogicalView} (and its embedded
 * {@link com.alibaba.polardbx.optimizer.core.rel.PushDownOpt}) to support push-down
 * of project / filter / sort / aggregate and physical-SQL generation.
 * <p>
 * When {@link #initPushDown(RelOptCluster, String)} is called with a cluster that has
 * a valid {@link com.alibaba.polardbx.optimizer.PlannerContext}, a {@link LogicalView}
 * is created from the underlying {@link RelOptTable}.  All push-down operations are then
 * delegated to the {@code LogicalView}'s {@code PushDownOpt}, and {@link #display()}
 * returns the generated physical SQL.
 * <p>
 * If the cluster lacks a {@code PlannerContext} (pure unit test without full context
 * setup), the source falls back to a simple recording mode: pushed expressions are
 * stored in lists and {@link #display()} returns a compact summary instead of SQL.
 */
public class InMemoryTableSource extends TableSource {

    private final String type;
    private final Map<String, String> options;
    private final EnumSet<PushdownCapability> capabilities;
    private final DbType dbType;

    // Configurable statistics
    private final long rowCount;
    private final Map<String, ConnectorColumnStatistics> columnStats;

    // -----------------------------------------------------------------
    // PushDownOpt-backed state (created in initPushDown)
    // -----------------------------------------------------------------

    private LogicalView logicalView;

    // -----------------------------------------------------------------
    // Fallback recording state (used when logicalView == null)
    // -----------------------------------------------------------------

    private final List<RexNode> recProjects = new ArrayList<>();
    private final List<RexNode> recFilters = new ArrayList<>();
    private Sort recSort;
    private LogicalAggregate recAgg;

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    public InMemoryTableSource(String type, Map<String, String> options, RelOptTable table,
                               EnumSet<PushdownCapability> capabilities,
                               long rowCount,
                               Map<String, ConnectorColumnStatistics> columnStats) {
        this(type, options, table, capabilities, rowCount, columnStats, DbType.MYSQL);
    }

    public InMemoryTableSource(String type, Map<String, String> options, RelOptTable table,
                               EnumSet<PushdownCapability> capabilities,
                               long rowCount,
                               Map<String, ConnectorColumnStatistics> columnStats,
                               DbType dbType) {
        super(table);
        this.type = type;
        this.options = options != null ? new HashMap<>(options) : new HashMap<>();
        this.capabilities = capabilities != null
            ? EnumSet.copyOf(capabilities)
            : EnumSet.noneOf(PushdownCapability.class);
        this.rowCount = rowCount;
        this.columnStats = columnStats != null ? new HashMap<>(columnStats) : Collections.emptyMap();
        this.dbType = dbType;
    }

    // -----------------------------------------------------------------
    // TableSource basics
    // -----------------------------------------------------------------

    @Override
    public String connectorType() {
        return type;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    /**
     * Creates a {@link LogicalView} from the underlying {@link RelOptTable} so that
     * the enclosed {@link com.alibaba.polardbx.optimizer.core.rel.PushDownOpt} can be
     * used for push-down and SQL generation.
     * <p>
     * If the cluster does not carry a valid {@link com.alibaba.polardbx.optimizer.PlannerContext}
     * the creation is skipped silently and the source operates in recording mode.
     */
    @Override
    public void initPushDown(RelOptCluster cluster, String schemaName) {
        try {
            LogicalTableScan scan = LogicalTableScan.create(cluster, table);
            logicalView = new LogicalView(scan, SqlSelect.LockMode.UNDEF);
        } catch (Throwable t) {
            // Fallback: operate in recording mode
            logicalView = null;
        }
    }

    // -----------------------------------------------------------------
    // Push-down: project
    // -----------------------------------------------------------------

    @Override
    public Pair<List<RexNode>, List<RexNode>> pushProject(List<RexNode> projects) {
        if (!capabilities.contains(PushdownCapability.PROJECT)) {
            return Pair.of(Collections.emptyList(), projects);
        }

        if (logicalView != null) {
            try {
                RelNode input = logicalView.getPushedRelNode();
                RelOptCluster cluster = input.getCluster();

                List<String> fieldNames = new ArrayList<>();
                for (int i = 0; i < projects.size(); i++) {
                    fieldNames.add("$f" + i);
                }
                RelDataType rowType = cluster.getTypeFactory().createStructType(
                    projects.stream().map(RexNode::getType).collect(Collectors.toList()),
                    fieldNames);

                LogicalProject project = LogicalProject.create(input, projects, rowType);
                logicalView.push(project);
                return Pair.of(new ArrayList<>(projects), Collections.emptyList());
            } catch (Throwable t) {
                // fall through to recording
            }
        }

        recProjects.addAll(projects);
        return Pair.of(new ArrayList<>(projects), Collections.emptyList());
    }

    // -----------------------------------------------------------------
    // Push-down: filter
    // -----------------------------------------------------------------

    @Override
    public Pair<Boolean, List<RexNode>> pushFilter(RexNode condition) {
        if (!capabilities.contains(PushdownCapability.FILTER)) {
            return Pair.of(false, Collections.singletonList(condition));
        }

        if (logicalView != null) {
            try {
                RelNode input = logicalView.getPushedRelNode();
                LogicalFilter filter = LogicalFilter.create(input, condition);
                logicalView.push(filter);
                return Pair.of(true, Collections.emptyList());
            } catch (Throwable t) {
                // fall through to recording
            }
        }

        recFilters.add(condition);
        return Pair.of(true, Collections.emptyList());
    }

    // -----------------------------------------------------------------
    // Push-down: sort
    // -----------------------------------------------------------------

    @Override
    public boolean pushSort(Sort sort) {
        if (!capabilities.contains(PushdownCapability.SORT)) {
            return false;
        }

        if (logicalView != null) {
            try {
                if (sort instanceof LogicalSort) {
                    logicalView.push(sort);
                    return true;
                }
                // Convert non-LogicalSort to LogicalSort
                RelNode input = logicalView.getPushedRelNode();
                LogicalSort logicalSort =
                    LogicalSort.create(input, sort.getCollation(),
                        sort.offset, sort.fetch);
                logicalView.push(logicalSort);
                return true;
            } catch (Throwable t) {
                // fall through to recording
            }
        }

        recSort = sort;
        return true;
    }

    // -----------------------------------------------------------------
    // Push-down: aggregate
    // -----------------------------------------------------------------

    @Override
    public boolean pushAgg(LogicalAggregate agg) {
        if (!capabilities.contains(PushdownCapability.AGG)) {
            return false;
        }

        if (logicalView != null) {
            try {
                logicalView.push(agg);
                return true;
            } catch (Throwable t) {
                // fall through to recording
            }
        }

        recAgg = agg;
        return true;
    }

    // -----------------------------------------------------------------
    // Display / SQL generation
    // -----------------------------------------------------------------

    /**
     * Returns the physical SQL generated by the embedded {@code PushDownOpt}, prefixed
     * with {@code "sql:"}.  When operating in recording mode, returns a compact summary
     * of pushed expressions.
     */
    @Override
    public String display() {
        if (logicalView != null) {
            try {
                SqlNode sqlNode = logicalView.getNativeSqlNode();
                if (sqlNode != null) {
                    String rawSql = sqlNode.toString();
                    // Normalise whitespace: collapse runs of whitespace (including newlines)
                    // into a single space so the plan output is deterministic and compact.
                    String normalised = rawSql.replaceAll("\\s+", " ").trim();
                    return "sql:" + normalised;
                }
            } catch (Throwable t) {
                // fall through to recording display
            }
        }

        StringBuilder sb = new StringBuilder();
        if (!recProjects.isEmpty()) {
            sb.append("projects:").append(recProjects.size());
        }
        if (!recFilters.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append("filters:").append(recFilters.size());
        }
        if (recSort != null) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append("sort:true");
        }
        if (recAgg != null) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append("agg:true");
        }
        return sb.toString();
    }

    // -----------------------------------------------------------------
    // Row type (reflects pushed-down projection)
    // -----------------------------------------------------------------

    @Override
    public RelDataType getRowType() {
        if (logicalView != null) {
            try {
                return logicalView.getPushedRelNode().getRowType();
            } catch (Throwable t) {
                // fall through
            }
        }
        return table.getRowType();
    }

    // -----------------------------------------------------------------
    // Copy
    // -----------------------------------------------------------------

    @Override
    public TableSource copy() {
        InMemoryTableSource copy = new InMemoryTableSource(
            type, options, table, capabilities, rowCount, columnStats, dbType);

        if (logicalView != null) {
            try {
                copy.logicalView = logicalView.copy(logicalView.getTraitSet());
            } catch (Throwable t) {
                copy.logicalView = null;
            }
        }

        copy.recProjects.addAll(this.recProjects);
        copy.recFilters.addAll(this.recFilters);
        copy.recSort = this.recSort;
        copy.recAgg = this.recAgg;
        return copy;
    }

    // -----------------------------------------------------------------
    // mq overrides — configurable statistics
    // -----------------------------------------------------------------

    @Override
    public double getRowCount(RelMetadataQuery mq) {
        if (logicalView != null) {
            try {
                Double estimated = logicalView.getPushDownOpt().estimateRowCount(mq);
                if (estimated != null) {
                    return estimated;
                }
            } catch (Throwable t) {
                // fall through
            }
        }
        return rowCount > 0 ? rowCount : table.getRowCount();
    }

    @Override
    public Double getDistinctRowCount(RelMetadataQuery mq,
                                      ImmutableBitSet groupKey,
                                      RexNode predicate) {
        if (groupKey.cardinality() == 1) {
            int colIdx = groupKey.nth(0);
            List<RelDataTypeField> fields = table.getRowType().getFieldList();
            if (colIdx < fields.size()) {
                String colName = fields.get(colIdx).getName();
                ConnectorColumnStatistics cs = columnStats.get(colName);
                if (cs != null && cs.getNdv() > 0) {
                    return (double) cs.getNdv();
                }
            }
        }
        return super.getDistinctRowCount(mq, groupKey, predicate);
    }

    @Override
    public RelOptCost getNonCumulativeCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = getRowCount(mq);
        double cpu = rows + 1;
        if (logicalView != null || !recFilters.isEmpty()) {
            cpu *= 0.5;
        }
        if (logicalView != null || !recProjects.isEmpty()) {
            cpu *= 0.8;
        }
        return planner.getCostFactory().makeCost(rows, cpu, 0, 0, 0);
    }

    // -----------------------------------------------------------------
    // Serialization (minimal — UT only)
    // -----------------------------------------------------------------

    @Override
    public Map<String, Object> toJson() {
        Map<String, Object> json = new HashMap<>();
        json.put("options", new HashMap<>(options));
        json.put("rowCount", rowCount);
        json.put("type", type);
        return json;
    }

    @Override
    public void fromJsonState(Map<String, Object> json, RelOptTable table) {
        // UT only — no-op
    }

}
