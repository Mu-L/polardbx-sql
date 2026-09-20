package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.utils.Pair;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A leaf relational node that represents an external-table data source.
 * <p>
 * ExternalTable inherits from {@link TableScan} and delegates all push-down
 * and metadata-query (mq) calls to its embedded {@link TableSource}, following
 * the same proxy pattern used by {@code LogicalView → MysqlNode}.
 */
public class ExternalTableScan extends TableScan {

    /**
     * The connector-specific source that backs this scan.
     * All push-down and mq calls are forwarded here.
     */
    private final TableSource tableSource;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Primary constructor.
     */
    protected ExternalTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                                RelOptTable table, TableSource tableSource) {
        super(cluster, traitSet, table, new SqlNodeList(SqlParserPos.ZERO));
        this.tableSource = tableSource;
    }

    /**
     * For JSON de-serialization ({@code RelInput}).
     */
    public ExternalTableScan(RelInput relInput) {
        super(relInput);
        RelOptTable relOptTable = relInput.getTable("table");
        @SuppressWarnings("unchecked")
        Map<String, Object> tsMap = (Map<String, Object>) relInput.get("tableSource");
        TableSource deserialized = TableSource.fromJson(tsMap, relOptTable);
        if (deserialized == null) {
            throw new IllegalStateException("Failed to deserialize TableSource for ExternalTableScan");
        }
        this.tableSource = deserialized;
    }

    // -------------------------------------------------------------------------
    // Factory method
    // -------------------------------------------------------------------------

    /**
     * Creates an {@code ExternalTableScan} with the given {@link TableSource} implementation.
     *
     * @param cluster the planner cluster
     * @param relOptTable table meta-info
     * @param tableSource connector-specific source; must not be null
     */
    public static ExternalTableScan create(RelOptCluster cluster, RelOptTable relOptTable, TableSource tableSource) {
        if (tableSource == null) {
            throw new IllegalArgumentException("TableSource must not be null for ExternalTableScan");
        }
        RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
        return new ExternalTableScan(cluster, traitSet, relOptTable, tableSource);
    }

    // -------------------------------------------------------------------------
    // Accessor
    // -------------------------------------------------------------------------

    public TableSource getTableSource() {
        return tableSource;
    }

    // -------------------------------------------------------------------------
    // RelNode overrides
    // -------------------------------------------------------------------------

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new ExternalTableScan(getCluster(), traitSet, table, tableSource.copy());
    }

    public ExternalTableScan copy(RelTraitSet traitSet) {
        return new ExternalTableScan(getCluster(), traitSet, table, tableSource.copy());
    }

    @Override
    public RelDataType deriveRowType() {
        return tableSource.getRowType();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return tableSource.getNonCumulativeCost(planner, mq);
    }

    // -------------------------------------------------------------------------
    // Explain / serialization
    // -------------------------------------------------------------------------

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw)
            .item("table", table.getQualifiedName())
            .item("tableSource", tableSource);
        return pw;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "ExternalTable");
        pw.item("name", table.getQualifiedName());
        pw.item("source", tableSource.getClass().getSimpleName());
        String pushDown = tableSource.display();
        if (!pushDown.isEmpty()) {
            pw.item("pushDown", pushDown);
        }
        return pw;
    }

    // -------------------------------------------------------------------------
    // Push-down interfaces — delegate to tableSource
    // -------------------------------------------------------------------------

    /**
     * Delegates projection push-down to the embedded {@link TableSource}.
     * <p>
     * After delegation, if the source accepted all projections (residual is empty),
     * the scan's rowType is updated to reflect the pushed projection schema so that
     * the caller can remove the Project node entirely.
     * If a non-empty residual exists, the scan's rowType is narrowed to the pushed
     * schema and the residual (re-written in terms of the new scan output) is returned
     * for the caller to keep as a Project above the scan.
     *
     * @param projects list of RexNode projections to push down
     * @return residual projections that must remain above the scan;
     * empty list means the Project node can be dropped
     */
    public List<RexNode> pushProject(List<RexNode> projects) {
        final Pair<List<RexNode>, List<RexNode>> result = tableSource.pushProject(projects);
        final List<RexNode> pushed = result.getKey();
        final List<RexNode> residual = result.getValue();

        // If nothing was pushed, return the original list object so the Rule
        // can detect a no-op via reference equality and avoid an infinite loop.
        if (pushed.isEmpty()) {
            return projects;
        }

        // rowType is now derived lazily via deriveRowType() → tableSource.getRowType(),
        // so we must clear the cached rowType to force re-derivation on next access.
        rowType = null;
        return residual;
    }

    /**
     * Delegates filter push-down to the embedded {@link TableSource}.
     * <p>
     * The source internally splits {@code condition} into pushable and non-pushable parts.
     *
     * @param condition the full filter condition to push down
     * @return a {@link Pair} where {@code key} is {@code true} if at least part was pushed,
     * and {@code value} is the list of residual conjuncts (empty if fully accepted)
     */
    public Pair<Boolean, List<RexNode>> pushFilter(RexNode condition) {
        return tableSource.pushFilter(condition);
    }

    /**
     * Delegates sort push-down to the embedded {@link TableSource}.
     *
     * @param sort the Sort relational expression to push down
     * @return true if the sort was accepted by the source
     */
    public boolean pushSort(Sort sort) {
        return tableSource.pushSort(sort);
    }

    /**
     * Delegates aggregate push-down to the embedded {@link TableSource}.
     *
     * @param agg the LogicalAggregate relational expression to push down
     * @return true if the aggregate was accepted by the source
     */
    public boolean pushAgg(LogicalAggregate agg) {
        if (tableSource.pushAgg(agg)) {
            rowType = null;
            return true;
        }
        return false;
    }

    // -------------------------------------------------------------------------
    // Metadata-query delegation — mirrors DrdsRelMd*.SOURCE handlers
    // -------------------------------------------------------------------------

    /**
     * DrdsRelMdRowCount
     */
    public double getRowCount(RelMetadataQuery mq) {
        return tableSource.getRowCount(mq);
    }

    /**
     * DrdsRelMdMaxRowCount
     */
    public double getMaxRowCount(RelMetadataQuery mq) {
        return tableSource.getMaxRowCount(mq);
    }

    /**
     * DrdsRelMdColumnOrigins
     */
    public Set<RelColumnOrigin> getColumnOrigins(RelMetadataQuery mq, int iOutputColumn) {
        return tableSource.getColumnOrigins(mq, iOutputColumn);
    }

    /**
     * DrdsRelMdColumnOriginNames
     */
    public List<Set<RelColumnOrigin>> getColumnOriginNames(RelMetadataQuery mq) {
        return tableSource.getColumnOriginNames(mq);
    }

    /**
     * DrdsRelMdDmlColumnNames
     */
    public List<Set<RelColumnOrigin>> getDmlColumnNames(RelMetadataQuery mq) {
        return tableSource.getDmlColumnNames(mq);
    }

    /**
     * DrdsRelMdOriginalRowType
     */
    public RelDataType getOriginalRowType(RelMetadataQuery mq) {
        return tableSource.getOriginalRowType(mq);
    }

    /**
     * DrdsRelMdCoveringIndex
     */
    public List<Set<RelColumnOrigin>> isCoveringIndex(RelMetadataQuery mq, RelOptTable indexTable, String index) {
        return tableSource.isCoveringIndex(mq, indexTable, index);
    }

    /**
     * DrdsRelMdTableReferences
     */
    public Set<RexTableInputRef.RelTableRef> getTableReferences(RelMetadataQuery mq) {
        return tableSource.getTableReferences(mq);
    }

    /**
     * DrdsRelMdUniqueKeys
     */
    public Set<ImmutableBitSet> getUniqueKeys(RelMetadataQuery mq, boolean ignoreNulls) {
        return tableSource.getUniqueKeys(mq, ignoreNulls);
    }

    /**
     * DrdsRelMdColumnGroupSize
     */
    public Integer getColumnsGroupSize(RelMetadataQuery mq, ImmutableBitSet columns) {
        return tableSource.getColumnsGroupSize(mq, columns);
    }

    /**
     * DrdsRelMdColumnUniqueness
     */
    public Boolean areColumnsUnique(RelMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls) {
        return tableSource.areColumnsUnique(mq, columns, ignoreNulls);
    }

    /**
     * DrdsRelMdCompositePk
     */
    public ImmutableBitSet getPrimaryKey(RelMetadataQuery mq) {
        return tableSource.getPrimaryKey(mq);
    }

    /**
     * DrdsRelMdPopulationSize
     */
    public Double getPopulationSize(RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return tableSource.getPopulationSize(mq, groupKey);
    }

    /**
     * DrdsRelMdDistribution
     */
    public RelDistribution distribution(RelMetadataQuery mq) {
        return tableSource.distribution(mq);
    }

    /**
     * DrdsRelMdDistinctRowCount
     */
    public Double getDistinctRowCount(RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate) {
        return tableSource.getDistinctRowCount(mq, groupKey, predicate);
    }

    /**
     * DrdsRelMdSelectivity
     */
    public Double getSelectivity(RelMetadataQuery mq, RexNode predicate) {
        return tableSource.getSelectivity(mq, predicate);
    }

    /**
     * DrdsRelMdPredicates
     */
    public RelOptPredicateList getPredicates(RelMetadataQuery mq) {
        return tableSource.getPredicates(mq);
    }

    /**
     * DrdsRelMdAllPredicates
     */
    public RelOptPredicateList getAllPredicates(RelMetadataQuery mq) {
        return tableSource.getAllPredicates(mq);
    }

    /**
     * DrdsRelMdFunctionalDependency
     */
    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        return tableSource.getFunctionalDependency(mq, iOutputColumns);
    }

    /**
     * DrdsRelMdCost — non-cumulative cost
     */
    public RelOptCost getNonCumulativeCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return computeSelfCost(planner, mq);
    }

    /**
     * DrdsRelMdCost — start-up cost
     */
    public RelOptCost getStartUpCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return tableSource.getStartUpCost(planner, mq);
    }
}
