package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract base for external data sources.
 * <p>
 * Provides the table meta-info holder, push-down interfaces for project/filter/sort,
 * and mq (metadata-query) delegation interfaces that mirror the methods registered in
 * each {@code DrdsRelMd*.SOURCE} handler for {@link ExternalTableScan}.
 * <p>
 * Concrete implementations supply connector-specific statistics and push-down logic.
 * The default implementations in this class match the original ExternalTable behaviour
 * (conservative / unknown) so that sub-classes only need to override what they support.
 */
public abstract class TableSource {

    /**
     * The RelOptTable that carries the meta-info (row type, statistics, etc.)
     * for the external table.
     */
    protected final RelOptTable table;

    protected TableSource(RelOptTable table) {
        this.table = table;
    }

    public RelOptTable getTable() {
        return table;
    }

    public abstract String connectorType();

    /**
     * Returns the external table options (e.g., location, format, connector properties).
     * <p>
     * Default implementation returns an empty map. Concrete sub-classes should override
     * this to return the options that were passed during creation via
     * {@link #create(Map, RelOptTable)}.
     *
     * @return the table options; never {@code null}
     */
    public Map<String, String> getOptions() {
        return Collections.emptyMap();
    }

    public void initPushDown(RelOptCluster cluster, String schemaName) {
    }

    // -------------------------------------------------------------------------
    // Push-down interfaces
    // -------------------------------------------------------------------------

    /**
     * Try to push down a projection (column pruning) into this source.
     * <p>
     * The implementation is responsible for merging {@code projects} with any
     * previously pushed projections and saving the result internally.
     * <p>
     * The return value is a {@link Pair} where:
     * <ul>
     *   <li>{@code key} ({@code List<RexNode>}) – the expressions actually pushed
     *       into the source (i.e., the new {@code pushedProjects} after merging).
     *       Empty list means nothing was pushed.</li>
     *   <li>{@code value} ({@code List<RexNode>}) – the residual projections
     *       re-written in terms of the pushed expressions that must remain as a
     *       {@link org.apache.calcite.rel.core.Project} above the scan.
     *       <em>Empty list</em> means the push was complete and the Project node
     *       above can be removed entirely.</li>
     * </ul>
     *
     * @param projects list of RexNode projections to push down
     * @return a {@link Pair} of (pushedProjects, residualProjects)
     */
    public abstract Pair<List<RexNode>, List<RexNode>> pushProject(List<RexNode> projects);

    /**
     * Try to push down a filter predicate into this source.
     * <p>
     * The implementation is responsible for internally splitting {@code condition}
     * into pushable and non-pushable parts. The pushable part is absorbed into the
     * source; the non-pushable remainder is returned as the second element of the
     * returned pair so the caller can keep it as a residual
     * {@link org.apache.calcite.rel.core.Filter} above this scan.
     *
     * @param condition the full filter condition to push down (may be a conjunction)
     * @return a {@link Pair} where:
     * <ul>
     *   <li>{@code key} ({@code Boolean}) – {@code true} if at least part of the
     *       condition was accepted; {@code false} if nothing was pushed down.</li>
     *   <li>{@code value} ({@code List<RexNode>}) – the list of residual conjuncts
     *       that could <em>not</em> be pushed down (may be empty if the entire
     *       condition was accepted). The caller is responsible for composing these
     *       conjuncts back into a single AND expression when needed.</li>
     * </ul>
     */
    public abstract Pair<Boolean, List<RexNode>> pushFilter(RexNode condition);

    /**
     * Try to push down a sort (ORDER BY / LIMIT) into this source.
     *
     * @param sort the Sort relational expression to push down
     * @return true if the sort was successfully pushed down
     */
    public abstract boolean pushSort(Sort sort);

    /**
     * Try to push down an aggregate into this source.
     * <p>
     * Default implementation returns {@code false} (aggregation push-down not supported).
     * Sub-classes that support agg push-down should override this method.
     *
     * @param agg the LogicalAggregate relational expression to push down
     * @return true if the aggregate was successfully pushed down
     */
    public abstract boolean pushAgg(LogicalAggregate agg);

    /**
     * Creates a shallow copy of this {@code TableSource}, duplicating any pushed-down state
     * so that the owning {@link com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan}
     * can be safely cloned during planning.
     *
     * @return a new {@code TableSource} instance with the same configuration and push-down state
     */
    public abstract TableSource copy();

    /**
     * Returns the effective output row type of this source, reflecting any
     * projection push-down that has been applied.
     * <p>
     * The default implementation returns the original table row type.
     * Sub-classes that support project push-down should override this method
     * to return a row type derived from the pushed projections.
     *
     * @return the current output {@link RelDataType}; never {@code null}
     */
    public RelDataType getRowType() {
        return table.getRowType();
    }

    // -------------------------------------------------------------------------
    // Display / explain helpers
    // -------------------------------------------------------------------------

    /**
     * Produces a human-readable summary of the push-down state held by this source.
     * <p>
     * The result contains two sections, each of which is omitted when empty:
     * <ul>
     *   <li>{@code projects:[expr, ...]} – the composed projections that have been
     *       pushed into the source.</li>
     *   <li>{@code filters:[cond, ...]} – the filter conjuncts that have been pushed
     *       into the source.</li>
     * </ul>
     * Example output:
     * <pre>
     *   projects:[$0, $2], filters:[$0 > 10, $2 = 'foo']
     * </pre>
     * Returns an empty string when neither projects nor filters have been pushed.
     *
     * @return display string; never {@code null}
     */
    public abstract String display();

    // -------------------------------------------------------------------------
    // Metadata-query (mq) delegation interfaces
    // Each method corresponds to a handler in the matching DrdsRelMd*.SOURCE.
    // The default implementations are deliberately conservative.
    // -------------------------------------------------------------------------

    // --- DrdsRelMdRowCount ---

    /**
     * Returns the estimated row count for this source.
     * Default: delegates to {@link RelOptTable#getRowCount()}.
     */
    public double getRowCount(RelMetadataQuery mq) {
        return table.getRowCount();
    }

    // --- DrdsRelMdMaxRowCount ---

    /**
     * Returns the maximum possible row count.
     * Default: {@link Double#POSITIVE_INFINITY} (unknown upper bound).
     */
    public double getMaxRowCount(RelMetadataQuery mq) {
        return Double.POSITIVE_INFINITY;
    }

    // --- DrdsRelMdColumnOrigins ---

    /**
     * Returns the column origins for the given output column index.
     * Default: returns a singleton origin pointing back to the source table.
     */
    public Set<RelColumnOrigin> getColumnOrigins(RelMetadataQuery mq, int iOutputColumn) {
        return Collections.singleton(new RelColumnOrigin(table, iOutputColumn, false));
    }

    // --- DrdsRelMdColumnOriginNames ---

    /**
     * Returns the full list of column origins (one entry per output field).
     * Default: builds a simple 1-to-1 mapping from source table columns.
     */
    public List<Set<RelColumnOrigin>> getColumnOriginNames(RelMetadataQuery mq) {
        return table.getRowType().getFieldList().stream()
            .map(field -> (Set<RelColumnOrigin>) ImmutableSet.<RelColumnOrigin>of(
                new RelColumnOrigin(table, field.getIndex(), false)))
            .collect(Collectors.toList());
    }

    // --- DrdsRelMdDmlColumnNames ---

    /**
     * Returns the DML column origins (one entry per output field).
     * Default: same as {@link #getColumnOriginNames}.
     */
    public List<Set<RelColumnOrigin>> getDmlColumnNames(RelMetadataQuery mq) {
        return getColumnOriginNames(mq);
    }

    // --- DrdsRelMdOriginalRowType ---

    /**
     * Returns the original row type of this source.
     * Default: {@link RelOptTable#getRowType()}.
     */
    public RelDataType getOriginalRowType(RelMetadataQuery mq) {
        return table.getRowType();
    }

    // --- DrdsRelMdCoveringIndex ---

    /**
     * Returns the covering-index information, or {@code null} if unknown.
     * Default: {@code null} (not supported).
     */
    public List<Set<RelColumnOrigin>> isCoveringIndex(RelMetadataQuery mq, RelOptTable indexTable, String index) {
        return null;
    }

    // --- DrdsRelMdTableReferences ---

    /**
     * Returns the set of table references visible from this source.
     * Default: singleton containing this table at ordinal 0.
     */
    public Set<RexTableInputRef.RelTableRef> getTableReferences(RelMetadataQuery mq) {
        return Sets.newHashSet(RexTableInputRef.RelTableRef.of(table, 0));
    }

    // --- DrdsRelMdUniqueKeys ---

    /**
     * Returns the set of unique-key bit-sets, or {@code null} if unknown.
     * Default: {@code null} (unknown).
     */
    public Set<ImmutableBitSet> getUniqueKeys(RelMetadataQuery mq, boolean ignoreNulls) {
        return null;
    }

    // --- DrdsRelMdColumnGroupSize ---

    /**
     * Returns the column group size for the given column set, or {@code null} if no statistics are available.
     * Default: {@code null}.
     */
    public Integer getColumnsGroupSize(RelMetadataQuery mq, ImmutableBitSet columns) {
        return null;
    }

    // --- DrdsRelMdColumnUniqueness ---

    /**
     * Returns whether the specified columns are unique, or {@code null} if unknown.
     * Default: {@code null} (unknown).
     */
    public Boolean areColumnsUnique(RelMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls) {
        return null;
    }

    // --- DrdsRelMdCompositePk ---

    /**
     * Returns the primary-key bit-set, or {@code null} if unknown.
     * Default: {@code null}.
     */
    public ImmutableBitSet getPrimaryKey(RelMetadataQuery mq) {
        return null;
    }

    // --- DrdsRelMdPopulationSize ---

    /**
     * Returns the population size for the given group key.
     * Default: delegates to {@link #getDistinctRowCount} for the same group key.
     */
    public Double getPopulationSize(RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return getDistinctRowCount(mq, groupKey, null);
    }

    // --- DrdsRelMdDistribution ---

    /**
     * Returns the data distribution of this source.
     * Default: {@link org.apache.calcite.rel.RelDistributions#ANY}.
     */
    public RelDistribution distribution(RelMetadataQuery mq) {
        return RelDistributions.ANY;
    }

    // --- DrdsRelMdDistinctRowCount ---

    /**
     * Returns the estimated distinct row count for the given group key and predicate.
     * Default: falls back to total row count as a rough estimate.
     */
    public Double getDistinctRowCount(RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate) {
        if ((predicate == null || predicate.isAlwaysTrue()) && groupKey.isEmpty()) {
            return 1D;
        }
        return getRowCount(mq);
    }

    // --- DrdsRelMdSelectivity ---

    /**
     * Returns the selectivity of the given predicate on this source.
     * Default: 1.0 for null/always-true, otherwise a guess via RelMdUtil.
     */
    public Double getSelectivity(RelMetadataQuery mq, RexNode predicate) {
        if (predicate == null || predicate.isAlwaysTrue()) {
            return 1.0;
        }
        return RelMdUtil.guessSelectivity(predicate);
    }

    // --- DrdsRelMdPredicates ---

    /**
     * Returns the pulled-up predicates for this source.
     * Default: {@link RelOptPredicateList#EMPTY}.
     */
    public RelOptPredicateList getPredicates(RelMetadataQuery mq) {
        return RelOptPredicateList.EMPTY;
    }

    // --- DrdsRelMdAllPredicates ---

    /**
     * Returns all predicates (including those pushed down) for this source.
     * Default: {@link RelOptPredicateList#EMPTY}.
     */
    public RelOptPredicateList getAllPredicates(RelMetadataQuery mq) {
        return RelOptPredicateList.EMPTY;
    }

    // --- DrdsRelMdFunctionalDependency ---

    /**
     * Returns the functional-dependency map for the given output columns.
     * Default: empty map (no functional dependencies known).
     */
    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        return new LinkedHashMap<>();
    }

    // --- DrdsRelMdCost ---

    /**
     * Returns the non-cumulative (self) cost for this source.
     * Default: rowCount rows, rowCount+1 cpu, zero memory/io/net.
     */
    public RelOptCost getNonCumulativeCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = table.getRowCount();
        double cpu = rowCount + 1;
        return planner.getCostFactory().makeCost(rowCount, cpu, 0, 0, 0);
    }

    /**
     * Returns the start-up cost for this source.
     * Default: tiny cost.
     */
    public RelOptCost getStartUpCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    // -------------------------------------------------------------------------
    // Serialization / deserialization
    // -------------------------------------------------------------------------

    public abstract Map<String, Object> toJson();

    /**
     * Restores the state of this {@code TableSource} from a map previously produced by {@link #toJson()}.
     * <p>
     * Implementations must restore all state from {@code json}.
     *
     * @param json the serialized map (including the {@code "class"} key written by the framework)
     * @param table the {@link RelOptTable} for the external table
     */
    public abstract void fromJsonState(Map<String, Object> json, RelOptTable table);

    /**
     * Creates a {@link TableSource} instance from the given options map, routing
     * to the correct sub-class based on the {@code engine_type} key.
     *
     * <p>The {@code engine_type} value is written into the options JSON by
     * {@code RegisterExternalTableJobFactory} when the external table is registered,
     * and is loaded (with all keys lower-cased) into {@code TableMeta.externalTableOptions}
     * by {@code GmsTableMetaManager}.
     *
     * <p>Delegates to the {@link ConnectorDescriptor} registered for the connector type.
     *
     * @param options the lower-cased options map from the external table metadata;
     * must not be {@code null}
     * @param table the {@link RelOptTable} for the external table
     * @return a concrete {@link TableSource} for the engine sub-type, or {@code null}
     * if {@code engine_type} is absent or unrecognised
     */
    public static TableSource create(Map<String, String> options, RelOptTable table) {
        if (options == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "External table options must not be null");
        }
        String connectorType = options.get(ExternalCatalogConstants.OPTION_CONNECTOR);
        if (connectorType == null) {
            connectorType = options.get(ExternalCatalogConstants.OPTION_ENGINE_TYPE);
        }
        if (connectorType == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "External table options missing 'connector' or 'engine_type'");
        }
        ConnectorDescriptor factory = ConnectorRegistry.getInstance().getOrNull(connectorType);
        if (factory == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Unknown connector: " + connectorType
                    + ". Available: " + ConnectorRegistry.getInstance().visibleTypes());
        }
        TableSource source = factory.createTableSource(options, table);
        if (source == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Connector '" + connectorType + "' does not implement createTableSource");
        }
        return source;
    }

    /**
     * Reconstructs a {@link TableSource} from the map previously produced during serialization.
     * <p>
     * The map must contain a {@code "class"} entry with the fully-qualified sub-class name.
     * That class must expose a public {@link RelOptTable}-arg constructor or a no-arg constructor;
     * {@link #fromJsonState(Map, RelOptTable)} is then invoked to restore the remaining state.
     *
     * @param json the serialized map (as read back from JSON); may be {@code null}
     * @param table the {@link RelOptTable} for the external table
     * @return the restored {@link TableSource}, or {@code null} if {@code json} is {@code null} / has no class
     */
    @SuppressWarnings("unchecked")
    public static TableSource fromJson(Map<String, Object> json, RelOptTable table) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        String className = (String) json.get("class");
        if (className == null) {
            return null;
        }
        try {
            Class<?> clazz = Class.forName(className);
            // Try (RelOptTable) constructor first, then no-arg constructor
            TableSource instance;
            try {
                instance = (TableSource) clazz.getDeclaredConstructor(RelOptTable.class).newInstance(table);
            } catch (NoSuchMethodException e) {
                instance = (TableSource) clazz.getDeclaredConstructor().newInstance();
            }
            instance.fromJsonState(json, table);
            return instance;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, e,
                "Failed to deserialize TableSource of class '" + className + "': " + e.getMessage());
        }
    }
}
