package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ForceIndexUtil;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * Auto Pagination Union Rule - A query optimization rule that transforms queries with OR conditions
 * into UNION queries to leverage index-based sorting and improve performance.
 * <p>
 * This rule is particularly effective for queries with:
 * - ORDER BY clauses with LIMIT
 * - OR conditions in WHERE clause
 * - Available indexes that can skip sorting for individual OR branches
 * <p>
 * The optimization works by:
 * 1. Identifying OR conditions that can benefit from index-based sorting
 * 2. Splitting OR conditions into separate UNION branches
 * 3. Applying sorting and limiting to each branch individually
 * 4. Combining results with UNION to eliminate duplicates
 */
public class AutoPaginationUnionRule extends RelOptRule {

    private static final int INVALID = Integer.MIN_VALUE;
    /**
     * Rule instance for pattern: Sort -> Filter -> TableScan
     * Handles queries without projection.
     */
    public static final AutoPaginationUnionRule INSTANCE = new AutoPaginationUnionRule(
        operand(LogicalSort.class, null, ForceIndexUtil.ORDER_BY_LIMIT,
            operand(LogicalFilter.class, null, ForceIndexUtil.FILTER_NO_SUBQUERY,
                operand(TableScan.class, null, none()))),
        RelFactories.LOGICAL_BUILDER, "INSTANCE"
    );

    /**
     * Rule instance for pattern: Sort -> Project -> Filter -> TableScan
     * Handles queries with projection.
     */
    public static final AutoPaginationUnionRule PROJECT = new AutoPaginationUnionRule(
        operand(LogicalSort.class, null, ForceIndexUtil.ORDER_BY_LIMIT,
            operand(LogicalProject.class, null, ForceIndexUtil.PROJECT_NO_SUBQUERY,
                operand(LogicalFilter.class, null, ForceIndexUtil.FILTER_NO_SUBQUERY,
                    operand(TableScan.class, null, none())))),
        RelFactories.LOGICAL_BUILDER, "PROJECT"
    );

    public AutoPaginationUnionRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
                                   String description) {
        super(operand, relBuilderFactory, "AutoPaginationUnionRule:" + description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call.rels[0]).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_AUTO_PAGINATION_UNION);
    }

    /**
     * Main entry point for rule application. Handles both patterns:
     * 1. Sort -> Project -> Filter -> TableScan (PROJECT rule)
     * 2. Sort -> Filter -> TableScan (INSTANCE rule)
     */
    public void onMatch(RelOptRuleCall call) {
        if (call.getRule() == PROJECT) {
            // Handle pattern with projection
            LogicalSort sort = call.rel(0);
            LogicalProject project = call.rel(1);
            LogicalFilter filter = call.rel(2);
            TableScan scan = call.rel(3);

            RelNode unionNode = tryCreateUnionForRel(sort, project, filter, scan);
            if (unionNode != null) {
                PlannerContext.getPlannerContext(sort).setHasAutoPagination(true);
                call.transformTo(unionNode);
            }
        } else {
            // Handle pattern without projection
            LogicalSort sort = call.rel(0);
            LogicalFilter filter = call.rel(1);
            TableScan scan = call.rel(2);

            RelNode unionNode = tryCreateUnionForRel(sort, filter, scan);
            if (unionNode != null) {
                PlannerContext.getPlannerContext(sort).setHasAutoPagination(true);
                call.transformTo(unionNode);
            }
        }
    }

    /**
     * Attempts to create a union node for queries with projection.
     * Validates that the sort collation is compatible with the projection mapping.
     */
    private RelNode tryCreateUnionForRel(LogicalSort sort, LogicalProject project,
                                         LogicalFilter filter, TableScan scan) {
        // Create mapping from project outputs to filter inputs
        final Mappings.TargetMapping map = RelOptUtil.permutation(project.getProjects(), filter.getRowType());

        // Verify that all sort fields are available after projection
        for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
            if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
                return null;
            }
        }

        // Apply the mapping to create new collation
        RelCollation newCollation = sort.getCluster().traitSet().canonize(RexUtil.apply(map, sort.getCollation()));
        return createUnionNode(newCollation, filter, scan, sort, project);
    }

    /**
     * Attempts to create a union node for queries without projection.
     */
    private RelNode tryCreateUnionForRel(LogicalSort sort, LogicalFilter filter, TableScan scan) {
        return createUnionNode(sort.getCollation(), filter, scan, sort, null);
    }

    /**
     * Creates a union node by splitting OR conditions into separate branches.
     * This is the core optimization logic that:
     * 1. Analyzes OR conditions to find splittable groups
     * 2. Evaluates which splits can benefit from index-based sorting
     * 3. Creates UNION branches for optimal OR conditions
     * 4. Combines results with proper sorting and limiting
     *
     * @param sortCollation Sorting information from the original query
     * @param filter Filter condition containing OR clauses
     * @param scan Table scan information
     * @param sort Original sort node for copying traits
     * @param project Project node (can be null for queries without projection)
     * @return Union node if OR conditions can be optimized, null otherwise
     */
    private RelNode createUnionNode(RelCollation sortCollation, LogicalFilter filter,
                                    TableScan scan, LogicalSort sort, LogicalProject project) {
        // Validate prerequisites for optimization
        if (sortCollation == null || CollectionUtils.isEmpty(sortCollation.getFieldCollations())) {
            return null;
        }

        // Get table metadata and ensure it has a primary key
        TableMeta tm = ForceIndexUtil.getIndexableTableMeta(scan.getTable());
        if (tm == null || !tm.isHasPrimaryKey()) {
            return null;
        }

        // Extract OR conditions from the filter
        Pair<List<List<RexNode>>, List<RexNode>> pair = extractOrConditions(filter);
        List<List<RexNode>> orConditionGroups = pair.getKey();
        if (CollectionUtils.isEmpty(orConditionGroups)) {
            return null;
        }
        List<RexNode> restConditions = pair.getValue();

        final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        builder.set(0, tm.getIndexes().size());
        sortCollation.getFieldCollations().forEach(x ->
            builder.intersect(tm.getAllColumns().get(x.getFieldIndex()).getPartOfSortKey()));
        ImmutableBitSet usableIndexes = IndexUtil.buildAvailableIndex(scan.getIndexNode(), tm.getIndexes(), builder);
        if (usableIndexes.isEmpty()) {
            return null;
        }

        // Build column mapping and analyze equal conditions
        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        Map<String, Integer> columnOrd = ForceIndexUtil.buildColumnarOrdinalMap(tm);
        RexNode restCondition = RexUtil.composeConjunction(rexBuilder, restConditions, false);
        List<RexNode> splitConditions =
            bestSplitConditions(sortCollation, scan, columnOrd, restCondition, orConditionGroups, tm, usableIndexes);
        if (splitConditions == null) {
            return null;
        }

        // Prepare conditions for union branches
        for (List<RexNode> orGroup : orConditionGroups) {
            if (orGroup == splitConditions) {
                continue;
            }
            restConditions.add(RexUtil.composeDisjunction(rexBuilder, orGroup, false));
        }

        // Handle missing primary key columns for projection
        List<Integer> missedPk = getMissedPk(project, tm, columnOrd);

        // Create union branches for split conditions
        List<RelNode> unionInputs = Lists.newArrayList();
        for (RexNode condition : splitConditions) {
            // Combine current condition with remaining conditions
            List<RexNode> branchConditions = Lists.newArrayList();
            branchConditions.addAll(restConditions);
            branchConditions.add(condition);
            RexNode branchCondition = RexUtil.composeConjunction(rexBuilder, branchConditions, false);

            // Create individual branch with sorting and limiting
            unionInputs.add(createUnionBranch(scan, branchCondition, sort, project, missedPk));
        }

        // Create union (use UNION instead of UNION ALL for automatic deduplication)
        LogicalUnion union = LogicalUnion.create(unionInputs, false);

        if (missedPk.isEmpty()) {
            // Apply final sort and limit directly
            return sort.copy(sort.getTraitSet(), union, sort.getCollation(), sort.offset, sort.fetch);
        }

        // Remove temporary primary key columns added for deduplication
        List<RexNode> projects = Lists.newArrayList();
        for (int i = 0; i < union.getRowType().getFieldCount() - missedPk.size(); i++) {
            projects.add(rexBuilder.makeInputRef(union, i));
        }
        LogicalProject pruneColumn = LogicalProject.create(union, projects, project.getRowType().getFieldNames());
        return sort.copy(sort.getTraitSet(), pruneColumn, sort.getCollation(), sort.offset, sort.fetch);
    }

    /**
     * Extracts OR conditions from the filter condition by analyzing conjunctions and disjunctions.
     * Separates conditions into OR groups (disjunctions with multiple terms) and
     * regular conditions (single terms or conjunctions).
     *
     * @param filter The filter to analyze
     * @return Pair containing OR groups and remaining conditions
     */
    private Pair<List<List<RexNode>>, List<RexNode>> extractOrConditions(LogicalFilter filter) {
        List<List<RexNode>> orGroups = Lists.newArrayList();
        List<RexNode> restConditions = Lists.newArrayList();

        int threshold = PlannerContext.getPlannerContext(filter).getParamManager().
            getInt(ConnectionParams.AUTO_PAGINATION_OR_THRESHOLD);
        // Analyze each conjunct in the condition
        for (RexNode conjunct : RelOptUtil.conjunctions(filter.getCondition())) {
            List<RexNode> disjuncts = RelOptUtil.disjunctions(conjunct);
            if (disjuncts.size() > 1 && disjuncts.size() <= threshold) {
                // This is an OR condition with multiple terms
                orGroups.add(disjuncts);
            } else {
                // This is a regular condition
                restConditions.add(conjunct);
            }
        }
        return Pair.of(orGroups, restConditions);
    }

    public ImmutableBitSet conditionCoveringIndexes(TableMeta tm, RexNode condition) {
        final ImmutableBitSet.Builder coveringIndexesBuilder = ImmutableBitSet.builder();
        coveringIndexesBuilder.set(0, tm.getIndexes().size());
        for (int col : RelOptUtil.InputFinder.bits(condition)) {
            ColumnMeta columnMeta = tm.getAllColumns().get(col);
            coveringIndexesBuilder.intersect(columnMeta.getPartOfKey().union(columnMeta.getPartOfPrefixKey()));
        }
        return coveringIndexesBuilder.build();
    }

    /**
     * Determines the best OR condition group to split the query for optimizing sort skip ability
     *
     * @param sortCollation The collation of sort columns
     * @param scan The table scan node
     * @param columnOrd Mapping from column names to column ordinals
     * @param restCondition Remaining WHERE conditions
     * @param orConditionGroups List of OR condition groups
     * @param tm Table metadata information
     * @param usableIndexes Bitmap set of usable indexes
     * @return Best split condition list, or null if no benefit
     */
    List<RexNode> bestSplitConditions(RelCollation sortCollation,
                                      TableScan scan, Map<String, Integer> columnOrd,
                                      RexNode restCondition, List<List<RexNode>> orConditionGroups,
                                      TableMeta tm, ImmutableBitSet usableIndexes) {

        // Calculate baseline sort skip capability
        int startLoc = INVALID;
        ImmutableBitSet coveringIndexes = conditionCoveringIndexes(tm, restCondition);
        for (int skIndex : usableIndexes) {
            IndexMeta indexMeta = tm.getIndexes().get(skIndex);
            Pair<Boolean, Integer> pair =
                ForceIndexUtil.testIfSkipSortOrder(sortCollation, scan, columnOrd, indexMeta, skIndex, restCondition,
                    coveringIndexes.get(skIndex));
            if (pair.getKey()) {
                startLoc = Math.max(startLoc, pair.getValue());
            }
        }

        // Find the best OR condition group to split
        List<RexNode> splitConditions = null;
        int bestStartLoc = INVALID;
        RexBuilder builder = scan.getCluster().getRexBuilder();
        List<ImmutableBitSet> orCoveringIndexes = Lists.newArrayList();
        for (List<RexNode> orGroup : orConditionGroups) {
            orCoveringIndexes.add(conditionCoveringIndexes(tm, builder.makeCall(SqlStdOperatorTable.OR, orGroup)));
        }
        for (int i = 0; i < orConditionGroups.size(); i++) {
            List<RexNode> orGroup = orConditionGroups.get(i);
            int curr = canBenefitFromSplit(sortCollation, scan, columnOrd, restCondition, orGroup, tm, usableIndexes,
                orCoveringIndexes, i);
            if (bestStartLoc < curr) {
                bestStartLoc = curr;
                splitConditions = orGroup;
            }
        }

        // Only proceed if splitting provides benefit
        if (bestStartLoc <= startLoc) {
            return null;
        }
        return splitConditions;
    }

    private int canBenefitFromSplit(RelCollation sortCollation,
                                    TableScan scan, Map<String, Integer> columnOrd,
                                    RexNode restCondition, List<RexNode> orConditions,
                                    TableMeta tm, ImmutableBitSet usableIndexes,
                                    List<ImmutableBitSet> orCoveringIndexes, int idx) {
        int ans = Integer.MAX_VALUE;
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
        // Check each OR branch
        for (RexNode orCondition : orConditions) {
            int singleStartLoc = INVALID;
            RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.AND, orCondition, restCondition);
            ImmutableBitSet coveringIndexes = conditionCoveringIndexes(tm, condition);
            for (int skIndex : usableIndexes) {
                boolean covering = coveringIndexes.get(skIndex);
                for (int i = 0; i < orCoveringIndexes.size(); i++) {
                    if (i != idx) {
                        covering &= orCoveringIndexes.get(i).get(skIndex);
                    }
                }
                IndexMeta indexMeta = tm.getIndexes().get(skIndex);
                Pair<Boolean, Integer> pair =
                    ForceIndexUtil.testIfSkipSortOrder(sortCollation, scan, columnOrd, indexMeta, skIndex, condition,
                        covering);
                if (pair.getKey()) {
                    singleStartLoc = Math.max(singleStartLoc, pair.getValue());
                }
            }

            // If any branch cannot skip sort, the split is not beneficial
            if (singleStartLoc < 0) {
                return INVALID;
            }

            // Track minimum capability across all branches
            ans = Math.min(ans, singleStartLoc);
        }
        return ans;
    }

    /**
     * Identifies primary key columns that are missing from the projection.
     * These columns need to be temporarily added for proper deduplication in UNION.
     *
     * @param project Project node (null if no projection)
     * @param tm Table metadata
     * @param columnOrd Column name to ordinal mapping
     * @return List of missing primary key column ordinals
     */
    private List<Integer> getMissedPk(Project project, TableMeta tm, Map<String, Integer> columnOrd) {
        if (project == null) {
            return Lists.newArrayList();
        }

        List<Integer> missedPk = Lists.newArrayList();

        // Build set of projected columns
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        project.getProjects().forEach(x -> {
            if (x instanceof RexInputRef) {
                builder.set(((RexInputRef) x).getIndex());
            }
        });
        ImmutableBitSet columnProject = builder.build();

        // Find missing primary key columns
        for (IndexColumnMeta col : tm.getPrimaryIndex().getKeyColumnsExt()) {
            if (!col.hasColumn()) {
                continue;
            }
            Integer ord = columnOrd.get(col.getColumnMeta().getName());
            if (ord != null && ord >= 0 && !columnProject.get(ord)) {
                missedPk.add(ord);
            }
        }
        return missedPk;
    }

    /**
     * Creates a single branch of the union with the specified condition.
     * Each branch includes the table scan, filter, optional projection, and sorting with limit.
     *
     * @param scan Original table scan
     * @param condition Filter condition for this branch
     * @param sort Original sort node for copying traits
     * @param project Original project node (can be null)
     * @param missedPk Missing primary key columns to add temporarily
     * @return Constructed branch node
     */
    private RelNode createUnionBranch(TableScan scan, RexNode condition,
                                      LogicalSort sort, LogicalProject project, List<Integer> missedPk) {
        // Create new table scan with same properties
        LogicalTableScan newTableScan = LogicalTableScan.create(scan.getCluster(), scan.getTable(),
            scan.getHints(), scan.getIndexNode(), scan.getFlashback(),
            scan.getFlashbackOperator(), scan.getPartitions());

        // Apply filter condition
        LogicalFilter newFilter = LogicalFilter.create(newTableScan, condition);

        if (project != null) {
            // Create projection with additional primary key columns if needed
            List<RexNode> projects = Lists.newArrayList();
            projects.addAll(project.getProjects());
            List<String> newFieldNames = Lists.newArrayList();
            newFieldNames.addAll(project.getRowType().getFieldNames());

            // Add missing primary key columns for deduplication
            for (int ref : missedPk) {
                projects.add(RexInputRef.of(ref, newFilter.getRowType()));
                newFieldNames.add(scan.getRowType().getFieldList().get(ref).getName());
            }

            LogicalProject newProject = LogicalProject.create(newFilter, projects, newFieldNames);
            return sort.copy(sort.getTraitSet(), newProject, sort.getCollation(), sort.offset, sort.fetch);
        } else {
            // Apply sort and limit directly to filter
            return sort.copy(sort.getTraitSet(), newFilter, sort.getCollation(), sort.offset, sort.fetch);
        }
    }
}