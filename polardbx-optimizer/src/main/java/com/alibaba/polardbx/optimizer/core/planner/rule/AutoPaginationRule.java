package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ForceIndexUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ForceIndexUtil.SargAbleHandler;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class AutoPaginationRule extends RelOptRule {

    public static final AutoPaginationRule INSTANCE = new AutoPaginationRule(
        operand(LogicalSort.class, null, ForceIndexUtil.ORDER_BY_LIMIT,
            operand(LogicalFilter.class, null, ForceIndexUtil.FILTER_NO_SUBQUERY,
                operand(TableScan.class, null, none()))),
        RelFactories.LOGICAL_BUILDER, "INSTANCE"
    );

    public static final AutoPaginationRule PROJECT = new AutoPaginationRule(
        operand(LogicalSort.class, null, ForceIndexUtil.ORDER_BY_LIMIT,
            operand(LogicalProject.class, null, ForceIndexUtil.PROJECT_NO_SUBQUERY,
                operand(LogicalFilter.class, null, ForceIndexUtil.FILTER_NO_SUBQUERY,
                    operand(TableScan.class, null, none())))),
        RelFactories.LOGICAL_BUILDER, "PROJECT"
    );

    public AutoPaginationRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
                              String description) {
        super(operand, relBuilderFactory, "AutoPaginationRule:" + description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call.rels[0]).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_AUTO_PAGINATION_INDEX);
    }

    public void onMatch(RelOptRuleCall call) {
        if (call.getRule() == PROJECT) {
            LogicalSort sort = call.rel(0);
            LogicalProject project = call.rel(1);
            LogicalFilter filter = call.rel(2);
            TableScan scan = call.rel(3);
            SqlNode indexNode = genIndexNodeForRel(sort, project, filter, scan);
            if (indexNode != null) {
                PlannerContext.getPlannerContext(sort).setHasAutoPagination(true);
                LogicalTableScan newTableScan =
                    LogicalTableScan.create(scan.getCluster(), scan.getTable(), scan.getHints(),
                        indexNode, scan.getFlashback(), scan.getFlashbackOperator(), scan.getPartitions());
                LogicalFilter newFilter = filter.copy(filter.getTraitSet(), newTableScan, filter.getCondition());
                LogicalProject newProject = project.copy(project.getTraitSet(), newFilter, project.getProjects(),
                    project.getRowType());
                LogicalSort newSort =
                    sort.copy(sort.getTraitSet(), newProject, sort.getCollation(), sort.offset, sort.fetch);
                call.transformTo(newSort);
            }
        } else {
            LogicalSort sort = call.rel(0);
            LogicalFilter filter = call.rel(1);
            TableScan scan = call.rel(2);
            SqlNode indexNode = genIndexNodeForRel(sort, filter, scan);
            if (indexNode != null) {
                PlannerContext.getPlannerContext(sort).setHasAutoPagination(true);
                LogicalTableScan newTableScan =
                    LogicalTableScan.create(scan.getCluster(), scan.getTable(), scan.getHints(),
                        indexNode, scan.getFlashback(), scan.getFlashbackOperator(), scan.getPartitions());
                LogicalFilter newFilter = filter.copy(filter.getTraitSet(), newTableScan, filter.getCondition());
                LogicalSort newSort =
                    sort.copy(sort.getTraitSet(), newFilter, sort.getCollation(), sort.offset, sort.fetch);
                call.transformTo(newSort);
            }
        }
    }

    private SqlNode genIndexNodeForRel(LogicalSort sort, LogicalProject project,
                                       LogicalFilter filter, TableScan scan) {
        ImmutableBitSet.Builder usedColBuilder = ImmutableBitSet.builder();
        usedColBuilder.addAll(RelOptUtil.InputFinder.bits(filter.getCondition()));
        final Mappings.TargetMapping map = RelOptUtil.permutation(project.getProjects(), filter.getRowType());
        for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
            if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
                return null;
            }
            usedColBuilder.set(map.getTargetOpt(fc.getFieldIndex()));
        }
        RelCollation newCollation = sort.getCluster().traitSet().canonize(RexUtil.apply(map, sort.getCollation()));

        ImmutableBitSet.Builder outputColBuilder = ImmutableBitSet.builder();
        for (RexNode rex : project.getProjects()) {
            outputColBuilder.addAll(RelOptUtil.InputFinder.bits(rex));
        }
        return genIndexNode(newCollation, filter, usedColBuilder.build(), outputColBuilder.build(), scan);
    }

    private SqlNode genIndexNodeForRel(LogicalSort sort, LogicalFilter filter, TableScan scan) {
        ImmutableBitSet.Builder usedColBuilder = ImmutableBitSet.builder();
        usedColBuilder.addAll(RelOptUtil.InputFinder.bits(filter.getCondition()));
        for (int key : sort.getCollation().getKeys()) {
            usedColBuilder.set(key);
        }
        return genIndexNode(sort.getCollation(), filter, usedColBuilder.build(),
            ImmutableBitSet.range(sort.getRowType().getFieldCount()), scan);
    }

    /**
     * Generates an index node based on the given sorting information, filter condition, used columns, and table scan.
     * This method aims to select the most appropriate index for query optimization.
     *
     * @param sortCollation Sorting information, used to determine if the index matches the sorting.
     * @param filter Filter condition, used to determine if the index can be utilized.
     * @param usedCols Columns used in the query for filter and sort, used to evaluate index usability.
     * @param outputCols Columns output in the query, used to evaluate index usability.
     * @param scan Table scan information, containing details about the table and possible indexes.
     * @return Returns the generated index node, which could be a force index, ignore index, or paging force index.
     */
    private SqlNode genIndexNode(RelCollation sortCollation, LogicalFilter filter,
                                 ImmutableBitSet usedCols, ImmutableBitSet outputCols, TableScan scan) {
        if (CollectionUtils.isEmpty(sortCollation.getFieldCollations())) {
            return null;
        }

        TableMeta tm = ForceIndexUtil.getIndexableTableMeta(scan.getTable());
        if (tm == null) {
            return null;
        }
        final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        builder.set(0, tm.getIndexes().size());
        usedCols.forEach(x -> {
            ColumnMeta columnMeta = tm.getAllColumns().get(x);
            builder.intersect(columnMeta.getPartOfKey().union(columnMeta.getPartOfPrefixKey()));
        });
        ImmutableBitSet usableIndexes = IndexUtil.buildAvailableIndex(scan.getIndexNode(), tm.getIndexes(), builder);

        // unusable indexes for paging force
        final ImmutableBitSet.Builder unusableBuilder = ImmutableBitSet.builder();
        unusableBuilder.set(0, tm.getIndexes().size());
        outputCols.forEach(x -> unusableBuilder.intersect(tm.getAllColumns().get(x).getPartOfKey()));
        ImmutableBitSet unusableIndexes = unusableBuilder.build();

        // Attempt to use the force index specified by the user, if any.
        SqlNode node = scan.getIndexNode();
        // if force index is used, try to convert it to paging force
        if (node != null) {
            return genPagingForceIndexNode(node, sortCollation, filter, usableIndexes, unusableIndexes, scan);
        }

        // Auto select force index when user did not specify a force index.
        node = genForceIndexNode(sortCollation, filter, usedCols, scan);
        // try to auto ignore index if auto force index failed
        if (node == null) {
            return genIgnoreIndexNode(sortCollation, filter, usableIndexes, scan);
        }
        // try to auto paging_force index if auto force index succeed
        SqlNode pagingNode = genPagingForceIndexNode(node, sortCollation, filter, usableIndexes, unusableIndexes, scan);
        return pagingNode != null ? pagingNode : node;
    }

    /**
     * Generates a SqlNode object for a force index hint based on the given conditions.
     * This method decides whether to force the use of a specific index for a query based on the sort order,
     * filter conditions, and indexes covering all columns used in the query.
     *
     * @param sortCollation The sort information of the query.
     * @param filter The filter conditions of the query.
     * @param usedCols Columns used in the query for filter and sort, used to evaluate index usability.
     * @param scan The table scan node.
     * @return Returns the SqlNode object for the force index hint, or null if no suitable index is found.
     */
    SqlNode genForceIndexNode(RelCollation sortCollation, LogicalFilter filter,
                              ImmutableBitSet usedCols, TableScan scan) {
        // If the table scan already has an index hint, no further processing is needed.
        if (ForceIndexUtil.hasIndexHint(scan)) {
            return null;
        }
        TableMeta tm = ForceIndexUtil.getIndexableTableMeta(scan.getTable());
        if (tm == null) {
            return null;
        }

        ImmutableBitSet.Builder sortColBuilder = ImmutableBitSet.builder();
        for (int key : sortCollation.getKeys()) {
            sortColBuilder.set(key);
        }
        ImmutableBitSet sortCols = sortColBuilder.build();

        ParamManager paramManager = PlannerContext.getPlannerContext(scan).getParamManager();
        List<ColumnMeta> columns = tm.getAllColumns();
        // Build a map of column ordinals.
        Map<String, Integer> columnOrd = ForceIndexUtil.buildColumnarOrdinalMap(tm);
        // Get the shard key column ordinals.
        List<Integer> skList = ForceIndexUtil.getSkList(tm, columnOrd);
        ImmutableBitSet.Builder shardKeyBuilder = ImmutableBitSet.builder();
        shardKeyBuilder.set(0, tm.getIndexes().size());
        skList.forEach(x -> shardKeyBuilder.intersect(columns.get(x).getPartOfKey()));
        ImmutableBitSet skBitSet = shardKeyBuilder.build();

        // Initialize the best index for skipping sort.
        ForceIndexUtil.BestIndex bestSkipSortIndex = new ForceIndexUtil.BestIndex(null, -1, -1D, -1, false);
        // Initialize the best index for sort.
        ForceIndexUtil.BestIndex bestSortIndex = new ForceIndexUtil.BestIndex(null, -1, -1D, -1, false);
        int usedLen = usedCols.cardinality();
        int sortLen = sortCols.cardinality();
        int uncoverColLimit = paramManager.getInt(ConnectionParams.PAGINATION_UNCOVER_COL);
        // Iterate through all indexes of the table to find the best index.
        for (int skIndex = 0; skIndex < tm.getIndexes().size(); skIndex++) {
            IndexMeta indexMeta = tm.getIndexes().get(skIndex);
            int coverUsedLen = calcCoverColLen(tm, skIndex, usedCols);
            int coverSortLen = calcCoverColLen(tm, skIndex, sortCols);
            if (coverSortLen != sortLen) {
                continue;
            }
            if (uncoverColLimit <= 0) {
                if (usedLen > coverUsedLen) {
                    continue;
                }
            } else {
                if (usedLen - coverUsedLen > uncoverColLimit) {
                    continue;
                }
            }
            // Check if the current index covers the shard keys.
            boolean coverSk = skBitSet.get(skIndex);
            // If the current index can skip the sort, update the best index for skipping sort.
            Pair<Boolean, Integer> pair =
                ForceIndexUtil.testIfSkipSortOrder(sortCollation, scan, columnOrd, indexMeta, skIndex, filter, true);
            if (pair.getKey()) {
                int constKeyPartLen = pair.getValue();
                bestSkipSortIndex = bestSkipSortIndex.findBetterIndex(indexMeta, constKeyPartLen,
                    calcCardinality(constKeyPartLen, indexMeta, tm), coverUsedLen, coverSk);
            } else {
                // If the current index cannot skip the sort, update the best index for sort.
                Set<String> equalColumnsSet = ForceIndexUtil.equalOrInColumnsSetForFilter(filter, scan);
                int loc;
                for (loc = 0; loc < indexMeta.getUserDefinedKeyParts(); loc++) {
                    ColumnMeta columnMeta = indexMeta.getKeyColumnsExt().get(loc).getColumnMeta();
                    if (columnMeta == null) {
                        continue;
                    }
                    if (!equalColumnsSet.contains(columnMeta.getName())) {
                        break;
                    }
                }
                bestSortIndex = bestSortIndex.findBetterIndex(indexMeta, loc,
                    calcCardinality(loc, indexMeta, tm), coverUsedLen, coverSk);
            }
        }

        // If a suitable index for skipping sort is found, generate and return the corresponding SqlNode object.
        // prefer skip sort index
        if (bestSkipSortIndex.getIndexMeta() != null
            && bestSkipSortIndex.getEqPreLen() >= paramManager.getInt(ConnectionParams.SKIP_SORT_EQ_PRE_COL)) {
            return ForceIndexUtil.genForceSqlNode(bestSkipSortIndex.getIndexMeta().getPhysicalIndexName());
        }

        // If a suitable index for sort is found, generate and return the corresponding SqlNode object.
        if (bestSortIndex.getIndexMeta() != null
            && bestSortIndex.getEqPreLen() >= paramManager.getInt(ConnectionParams.SORT_EQ_PRE_COL)) {
            return ForceIndexUtil.genForceSqlNode(bestSortIndex.getIndexMeta().getPhysicalIndexName());
        }
        // If no suitable index is found, return null.
        return null;
    }

    int calcCoverColLen(TableMeta tm, int skIndex, ImmutableBitSet colBit) {
        int len = 0;
        for (int x = colBit.nextSetBit(0); x >= 0; x = colBit.nextSetBit(x + 1)) {
            ColumnMeta columnMeta = tm.getAllColumns().get(x);
            if (columnMeta.getPartOfKey().get(skIndex) || columnMeta.getPartOfPrefixKey().get(skIndex)) {
                len++;
            }
        }
        return len;
    }

    SqlNode genPagingForceIndexNode(SqlNode node, RelCollation sortCollation, LogicalFilter filter,
                                    ImmutableBitSet coveringIndexes, ImmutableBitSet unusableIndexes, TableScan scan) {
        TableMeta tm = ForceIndexUtil.getIndexableTableMeta(scan.getTable());
        if (tm == null) {
            return null;
        }
        if (!PlannerContext.getPlannerContext(scan).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_AUTO_PAGINATION_PAGING_FORCE)) {
            return null;
        }
        Optional<String> indexNames = IndexUtil.getForceIndex(node).stream().findFirst();
        String indexName = indexNames.map(s -> SQLUtils.normalizeNoTrim(s.toLowerCase())).orElse(null);
        if (StringUtils.isEmpty(indexName)) {
            return null;
        }
        Map<String, Integer> columnOrd = ForceIndexUtil.buildColumnarOrdinalMap(tm);

        IndexMeta indexMeta = null;
        int keyIndex = -1;
        for (IndexMeta meta : tm.getIndexes()) {
            keyIndex++;
            if (meta.getPhysicalIndexName().equalsIgnoreCase(indexName)) {
                indexMeta = meta;
                break;
            }
        }
        if (indexMeta == null) {
            return null;
        }

        // index must cover all columns used in filter and sort
        if (!coveringIndexes.get(keyIndex)) {
            return null;
        }

        // index can't cover all output columns
        if (unusableIndexes.get(keyIndex)) {
            return null;
        }

        // index can't skip sort
        if (!ForceIndexUtil.testIfSkipSortOrder(sortCollation, scan, columnOrd, indexMeta, keyIndex, filter, true)
            .getKey()) {
            return ForceIndexUtil.genPagingForceSqlNode(indexName);
        }
        return null;
    }

    SqlNode genIgnoreIndexNode(RelCollation sortCollation, LogicalFilter filter, ImmutableBitSet coveringIndexes,
                               TableScan scan) {
        TableMeta tm = ForceIndexUtil.getIndexableTableMeta(scan.getTable());
        if (tm == null) {
            return null;
        }
        PlannerContext plannerContext = PlannerContext.getPlannerContext(scan);
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_AUTO_PAGINATION_IGNORE_INDEX)) {
            return null;
        }

        List<RexNode> sargs = Lists.newArrayList();
        SargAbleHandler sargAbleHandler = new SargAbleHandler(plannerContext) {
            @Override
            protected void handleUnaryRef(RexInputRef ref, RexCall call) {
                sargs.add(call);
            }

            @Override
            protected void handleBinaryRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
                sargs.add(call);
            }

            @Override
            protected void handleTernaryRef(RexInputRef firstRex, RexNode leftRex, RexNode rightRex, RexCall call) {
                sargs.add(call);
            }

            @Override
            protected void handleInVec(RexCall leftRex, RexNode rightRex, RexCall call) {
                sargs.add(call);
            }

            protected void handleEqualVec(RexCall leftRex, RexNode rightRex, RexCall call) {
                sargs.add(call);
            }

            protected void handleNotEqualRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
                // do nothing
            }
        };

        for (RexNode cond : RelOptUtil.conjunctions(filter.getCondition())) {
            sargAbleHandler.handleCondition(cond);
        }
        ImmutableBitSet.Builder sargBitsBuilder = ImmutableBitSet.builder();
        for (RexNode sarg : sargs) {
            sargBitsBuilder.addAll(RelOptUtil.InputFinder.bits(sarg));
        }
        ImmutableBitSet sargBits = sargBitsBuilder.build();
        ImmutableBitSet filterColumnBits = RelOptUtil.InputFinder.bits(filter.getCondition());
        Set<String> ignoreIndexSet = Sets.newHashSet();
        Map<String, Integer> columnOrd = ForceIndexUtil.buildColumnarOrdinalMap(tm);
        boolean useAbleIndex = false;
        // ignore index starting with sort column if there are any other index usable
        for (int skIndex = 0; skIndex < tm.getIndexes().size(); skIndex++) {
            IndexMeta indexMeta = tm.getIndexes().get(skIndex);
            if (indexMeta.getUserDefinedKeyParts() <= 0) {
                continue;
            }
            int columnIndex;
            String firstColumn = indexMeta.getKeyColumnsExt().get(0).getName();
            if (firstColumn == null || !columnOrd.containsKey(firstColumn)) {
                continue;
            }
            columnIndex = columnOrd.get(firstColumn);
            if (sargBits.get(columnIndex)) {
                useAbleIndex = true;
            }
            if (filterColumnBits.get(columnIndex)) {
                continue;
            }
            Pair<Boolean, Integer> pair =
                ForceIndexUtil.testIfSkipSortOrder(sortCollation, scan, columnOrd, indexMeta, skIndex, filter,
                    coveringIndexes.get(skIndex));
            if (pair.getKey() && pair.getValue() == 0) {
                ignoreIndexSet.add(indexMeta.getPhysicalIndexName());
            }
        }
        if (useAbleIndex && !CollectionUtils.isEmpty(ignoreIndexSet)) {
            return ForceIndexUtil.genIgnoreSqlNode(ignoreIndexSet);
        }
        return null;
    }

    private double calcCardinality(int startLoc, IndexMeta indexMeta, TableMeta tm) {
        double cardinality = 1D;
        List<IndexColumnMeta> columns = indexMeta.getKeyColumnsExt();
        for (int i = 0; i < Math.min(startLoc, columns.size()); i++) {
            if (columns.get(i).hasColumn()) {
                StatisticResult statisticResult1 =
                    StatisticManager.getInstance().getCardinality(tm.getSchemaName(), tm.getTableName(),
                        columns.get(i).getName(), true, true);
                cardinality *= statisticResult1.getLongValue();
            }
        }
        return cardinality;
    }
}
