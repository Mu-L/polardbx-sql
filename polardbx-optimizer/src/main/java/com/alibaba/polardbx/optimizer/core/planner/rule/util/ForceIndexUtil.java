package com.alibaba.polardbx.optimizer.core.planner.rule.util;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.LookupInfo;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.index.TableScanFinder;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.DrdsRexFolder;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.stats.metric.FeatureStats;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.stats.metric.FeatureStatsItem.EXPAND_IN;

public class ForceIndexUtil {
    public static final int INDEX_MAX_LEN = 16;

    /**
     * Predicate to match LogicalSort nodes that have both ORDER BY and LIMIT clauses.
     * This is the target pattern for pagination query
     */
    public static final Predicate<LogicalSort> ORDER_BY_LIMIT = new PredicateImpl<LogicalSort>() {
        @Override
        public boolean test(LogicalSort logicalSort) {
            return logicalSort != null && logicalSort.fetch != null && logicalSort.withOrderBy();
        }
    };

    public static final Predicate<LogicalFilter> FILTER_NO_SUBQUERY = new PredicateImpl<LogicalFilter>() {
        @Override
        public boolean test(LogicalFilter logicalFilter) {
            return logicalFilter != null && !RexUtil.hasSubQuery(logicalFilter.getCondition());
        }
    };

    public static final Predicate<LogicalProject> PROJECT_NO_SUBQUERY = new PredicateImpl<LogicalProject>() {
        @Override
        public boolean test(LogicalProject logicalProject) {
            if (logicalProject == null) {
                return false;
            }
            for (RexNode rexNode : logicalProject.getProjects()) {
                if (RexUtil.hasSubQuery(rexNode)) {
                    return false;
                }
            }
            return true;
        }
    };

    public static String genAutoForceIndex(LogicalView logicalView) {
        // get index to be used in force index
        LookupInfo lookupInfo = logicalView.getLookupInfo();
        if (lookupInfo != null && lookupInfo.isGsiLookup() && lookupInfo.isPrimaryHasGsiLocalIndex()) {
            return null;
        }

        TableScan tableScan = getTableScan(logicalView);
        if (tableScan == null) {
            return null;
        }
        TableMeta tm = CBOUtil.getTableMeta(tableScan.getTable());

        // consider bka join column
        ImmutableList<String> bkaColumns = null;
        if (logicalView.isLookupTable()) {
            LookupPredicate predicate = logicalView.getLookupInfo().getPredicates();
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (int i = 0; i < predicate.size(); i++) {
                builder.add(predicate.getColumn(i).getLastName());
            }
            bkaColumns = builder.build();
        }
        // collect required columns
        EqualColumnsCollector equalColumnsCollector = new EqualColumnsCollector(
            tableScan, logicalView.getCluster().getMetadataQuery(), bkaColumns);
        RequiredColumnsCollector requiredColumnsCollector = new RequiredColumnsCollector(
            tableScan, logicalView.getCluster().getMetadataQuery(), bkaColumns);
        logicalView.getPushedRelNode().accept(equalColumnsCollector);
        logicalView.getPushedRelNode().accept(requiredColumnsCollector);

        // get index to be used in force index
        return genAutoForceIndex(equalColumnsCollector.build(), requiredColumnsCollector.build(), tm);
    }

    public static SqlNode genForceSqlNode(String indexName) {
        return new SqlNodeList(ImmutableList.of(
            new SqlIndexHint(SqlLiteral.createCharString("FORCE INDEX", SqlParserPos.ZERO), null,
                new SqlNodeList(ImmutableList.of(SqlLiteral.createCharString(
                    SqlIdentifier.surroundWithBacktick(indexName), SqlParserPos.ZERO)),
                    SqlParserPos.ZERO), SqlParserPos.ZERO)), SqlParserPos.ZERO);
    }

    public static SqlNode genPagingForceSqlNode(String indexName) {
        return new SqlNodeList(ImmutableList.of(
            new SqlIndexHint(SqlLiteral.createCharString("PAGING_FORCE INDEX", SqlParserPos.ZERO), null,
                new SqlNodeList(ImmutableList.of(SqlLiteral.createCharString(
                    SqlIdentifier.surroundWithBacktick(indexName), SqlParserPos.ZERO)),
                    SqlParserPos.ZERO), SqlParserPos.ZERO)), SqlParserPos.ZERO);
    }

    public static SqlNode genIgnoreSqlNode(Collection<String> indexName) {
        return new SqlNodeList(ImmutableList.of(
            new SqlIndexHint(SqlLiteral.createCharString("IGNORE INDEX", SqlParserPos.ZERO), null,
                new SqlNodeList(
                    indexName.stream().map(x -> SqlLiteral.createCharString(
                        SqlIdentifier.surroundWithBacktick(x), SqlParserPos.ZERO)).collect(Collectors.toList()),
                    SqlParserPos.ZERO), SqlParserPos.ZERO)), SqlParserPos.ZERO);
    }

    private static TableScan getTableScan(LogicalView logicalView) {
        final ParamManager paramManager = PlannerContext.getPlannerContext(logicalView).getParamManager();
        if (!paramManager.getBoolean(ConnectionParams.ENABLE_AUTO_FORCE_INDEX)) {
            return null;
        }

        if (logicalView instanceof OSSTableScan) {
            return null;
        }

        // should contain only one table
        if (logicalView.getTableNames().size() != 1) {
            return null;
        }
        // logicalView should have no index hint
        RelNode rel = logicalView.getPushedRelNode();
        TableScanFinder tableScanFinder = new TableScanFinder();
        rel.accept(tableScanFinder);
        List<Pair<String, TableScan>> tableScans = tableScanFinder.getResult();
        if (tableScans.size() != 1) {
            return null;
        }
        TableScan tableScan = tableScans.get(0).getValue();
        if (hasIndexHint(tableScan)) {
            return null;
        }
        return tableScan;
    }

    public static boolean hasIndexHint(TableScan tableScan) {
        if (tableScan == null || tableScan.getIndexNode() == null) {
            return false;
        }
        if (tableScan.getIndexNode() instanceof SqlNodeList) {
            SqlNodeList sqlNodeList = (SqlNodeList) tableScan.getIndexNode();
            if (sqlNodeList.size() == 0) {
                return false;
            }
        }
        return true;
    }

    private static String genAutoForceIndex(EqualColumns requiredEqualColumns,
                                            RequiredColumns requiredColumns,
                                            TableMeta tm) {
        if (requiredEqualColumns != null) {
            // check pk first
            if (requiredEqualColumns.checkCoverage(tm.getPrimaryIndex())) {
                return "PRIMARY";
            }
            // check uk next
            for (IndexMeta index : tm.getUniqueIndexes(false)) {
                if (requiredEqualColumns.checkCoverage(index)) {
                    return index.getPhysicalIndexName();
                }
            }
        }

        if (requiredColumns == null) {
            return null;
        }
        // check common key last
        IndexMeta bestIndex = null;
        for (int skIndex = 0; skIndex < tm.getIndexes().size(); skIndex++) {
            IndexMeta index = tm.getIndexes().get(skIndex);
            if (requiredColumns.checkIndexCoverage(index, skIndex)) {
                // get shortest index
                if (bestIndex == null || index.getKeyColumnsExt().size() < bestIndex.getKeyColumnsExt().size()) {
                    bestIndex = index;
                }
            }
        }
        return bestIndex == null ? null : bestIndex.getPhysicalIndexName();
    }

    public static class EqualColumnsCollector extends RelShuttleImpl {
        // columns list used in 'equal' or 'in'
        private final List<List<String>> equalColumnsList;
        private final boolean supportIn;
        private final RelMetadataQuery mq;
        private final PlannerContext plannerContext;
        private final boolean collectSuccess;

        public EqualColumnsCollector(TableScan tableScan) {
            this.mq = tableScan.getCluster().getMetadataQuery();
            this.plannerContext = PlannerContext.getPlannerContext(tableScan);
            this.equalColumnsList = Lists.newArrayList();
            this.collectSuccess = ForceIndexUtil.getIndexableTableMeta(tableScan.getTable()) != null;
            this.supportIn = false;
        }

        public EqualColumnsCollector(TableScan tableScan, RelMetadataQuery mq, List<String> bkaColumns) {
            this.mq = mq;
            this.plannerContext = PlannerContext.getPlannerContext(tableScan);
            this.equalColumnsList = Lists.newArrayList();
            if (CollectionUtils.isNotEmpty(bkaColumns)) {
                equalColumnsList.add(bkaColumns);
            }
            this.collectSuccess = ForceIndexUtil.getIndexableTableMeta(tableScan.getTable()) != null;
            this.supportIn = true;
        }

        public SargAbleHandler getSargAbleHandler(LogicalFilter filter, PlannerContext plannerContext) {
            return new SargAbleHandler(plannerContext) {
                @Override
                protected void handleUnaryRef(RexInputRef ref, RexCall call) {
                }

                @Override
                protected void handleBinaryRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
                }

                @Override
                protected void handleTernaryRef(RexInputRef firstRex, RexNode leftRex, RexNode rightRex,
                                                RexCall call) {
                }

                @Override
                protected void handleInRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
                    boolean enable = supportIn || checkInDynamicApply(plannerContext, rightRex);
                    if (enable) {
                        addEqualColumnList(filter.getInput(), ImmutableList.of(leftRef));
                    }
                }

                @Override
                protected void handleInVec(RexCall leftRex, RexNode rightRex, RexCall call) {
                    boolean enable = supportIn || checkInDynamicApply(plannerContext, rightRex);
                    if (enable) {
                        addEqualColumnList(filter.getInput(), leftRex.getOperands());
                    }
                }

                protected void handleEqualRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
                    addEqualColumnList(filter.getInput(), ImmutableList.of(leftRef));
                }

                protected void handleEqualVec(RexCall leftRex, RexNode rightRex, RexCall call) {
                    addEqualColumnList(filter.getInput(), leftRex.getOperands());
                }

                protected void handleIsNullRef(RexInputRef leftRef, RexCall call) {
                    addEqualColumnList(filter.getInput(), ImmutableList.of(leftRef));
                }
            };
        }

        public static boolean checkInDynamicApply(PlannerContext pc, RexNode rex) {
            if (rex instanceof RexCall && rex.isA(SqlKind.ROW)) {
                List<RexNode> rexNodeList = ((RexCall) rex).getOperands();
                if (rexNodeList.size() != 1) {
                    return false;
                }
                RexNode value = rexNodeList.get(0);
                if (!(value instanceof RexDynamicParam)) {
                    return false;
                }
                if (((RexDynamicParam) value).getIndex() == PlannerUtils.APPLY_IN_VALUES_PARAM_INDEX) {
                    return true;
                }
                if (pc.getParamManager().getBoolean(ConnectionParams.ENABLE_AUTO_PAGINATION_IN_SCALAR)) {
                    Object inValue = DrdsRexFolder.fold(value, pc);
                    if (inValue instanceof List) {
                        return ((List) inValue).size() == 1;
                    }
                }
            }
            return false;
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            super.visit(filter);
            SargAbleHandler sargAbleHandler = getSargAbleHandler(filter, plannerContext);
            for (RexNode node : RelOptUtil.conjunctions(filter.getCondition())) {
                sargAbleHandler.handleCondition(node);
            }
            return filter;
        }

        protected void addEqualColumnList(RelNode input, List<RexNode> references) {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (RexNode rexNode : references) {
                if (!(rexNode instanceof RexInputRef)) {
                    return;
                }
                RelColumnOrigin relColumnOrigin = mq.getColumnOrigin(input, ((RexInputRef) rexNode).getIndex());
                if (relColumnOrigin == null) {
                    return;
                }
                builder.add(relColumnOrigin.getColumnName());
            }
            equalColumnsList.add(builder.build());
        }

        public List<String> getEqualColumnsList() {
            return equalColumnsList.stream().flatMap(List::stream).collect(Collectors.toList());
        }

        EqualColumns build() {
            return !collectSuccess ? null : new EqualColumns(equalColumnsList);
        }
    }

    private static class EqualColumns {
        // columns list used in 'equal' or 'in'
        private final Map<String, List<String>> equalColumnsMap;
        private int equalColumnSize;

        public EqualColumns(List<List<String>> equalColumnsList) {
            this.equalColumnSize = 0;
            this.equalColumnsMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (List<String> columns : equalColumnsList) {
                this.equalColumnsMap.put(columns.get(0), columns);
                this.equalColumnSize += columns.size();
            }
        }

        /**
         * check whether required equal columns can cover current index
         *
         * @param index index to be checked, must be unique
         * @return true if required columns can cover index
         */
        private boolean checkCoverage(IndexMeta index) {
            if (index == null) {
                return false;
            }
            if (!index.isUniqueIndex() && !index.isPrimaryKeyIndex()) {
                return false;
            }
            List<IndexColumnMeta> indexColumns = index.getKeyColumnsExt();
            // all columns of unique key must be covered using equal condition
            if (indexColumns.size() > equalColumnSize) {
                return false;
            }

            int loc = 0;
            while (loc < indexColumns.size()) {
                ColumnMeta columnMeta = indexColumns.get(loc).getColumnMeta();
                if (columnMeta == null) {
                    return false;
                }
                List<String> equalColumns = equalColumnsMap.getOrDefault(columnMeta.getName(), null);
                if (equalColumns == null) {
                    // can't find equal condition on current index column
                    return false;
                }
                loc++;
                for (int i = 1; i < equalColumns.size(); i++) {
                    if (loc == indexColumns.size()) {
                        return true;
                    }
                    if (!equalColumns.get(i).equals(indexColumns.get(loc).getName())) {
                        return false;
                    }
                    loc++;
                }
            }
            return loc == indexColumns.size();
        }
    }

    private static class RequiredColumnsCollector extends RelShuttleImpl {
        // columns list used in 'equal' or 'in'
        private final List<List<String>> equalColumnsList;
        // columns used in order by
        private List<String> orderByColumns;
        private List<Boolean> orderByAsc;
        private boolean collectSuccess;
        private final RelMetadataQuery mq;
        private final PlannerContext plannerContext;

        public RequiredColumnsCollector(TableScan tableScan, RelMetadataQuery mq, List<String> bkaColumns) {
            this.mq = mq;
            this.plannerContext = PlannerContext.getPlannerContext(tableScan);
            equalColumnsList = Lists.newArrayList();
            orderByColumns = null;
            collectSuccess = ForceIndexUtil.getIndexableTableMeta(tableScan.getTable()) != null;
            if (CollectionUtils.isNotEmpty(bkaColumns)) {
                equalColumnsList.add(bkaColumns);
            }
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            super.visit(filter);
            // failed already
            if (!collectSuccess) {
                return filter;
            }
            // consider filters before sort
            if (orderByColumns != null) {
                return filter;
            }
            try {
                List<RexNode> conjunctions = RelOptUtil.conjunctions(filter.getCondition());
                for (RexNode node : conjunctions) {
                    if (!RexUtil.containsInputRef(node)) {
                        continue;
                    }
                    // only consider searchable filters
                    if (!node.getKind().belongsTo(SqlKind.SARGABLE)) {
                        throw Util.FoundOne.NULL;
                    }
                    if (!(node instanceof RexCall)) {
                        throw Util.FoundOne.NULL;
                    }
                    RexCall call = (RexCall) node;
                    switch (node.getKind()) {
                    case IN:
                        if (call.getOperands().size() == 2) {
                            RexNode leftRexNode = call.getOperands().get(0);
                            RexNode rightRexNode = call.getOperands().get(1);
                            if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexCall && rightRexNode
                                .isA(SqlKind.ROW)) {
                                if (DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                                    addEqualColumnList(filter.getInput(), ImmutableList.of(leftRexNode));
                                    break;
                                }
                            }
                            if (leftRexNode.isA(SqlKind.ROW) && leftRexNode instanceof RexCall && rightRexNode.isA(
                                SqlKind.ROW)) {
                                boolean leftRef = ((RexCall) leftRexNode).getOperands().stream()
                                    .allMatch(x -> x instanceof RexInputRef);
                                if (leftRef && DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                                    addEqualColumnList(filter.getInput(), ((RexCall) leftRexNode).getOperands());
                                    break;
                                }
                            }
                        }
                        throw Util.FoundOne.NULL;
                    case EQUALS:
                        if (call.getOperands().size() == 2) {
                            RexNode leftRexNode = call.getOperands().get(0);
                            RexNode rightRexNode = call.getOperands().get(1);
                            // xx = xx
                            if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexInputRef) {
                                break;
                            }
                            // xx = ?
                            if (leftRexNode instanceof RexInputRef &&
                                DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                                addEqualColumnList(filter.getInput(), ImmutableList.of(leftRexNode));
                                break;
                            }
                            // ? = xx
                            if (rightRexNode instanceof RexInputRef &&
                                DrdsRexFolder.fold(leftRexNode, plannerContext) != null) {
                                addEqualColumnList(filter.getInput(), ImmutableList.of(rightRexNode));
                                break;
                            }
                            //(xx,xx)=(?,?)
                            if (leftRexNode.isA(SqlKind.ROW) && rightRexNode.isA(SqlKind.ROW)
                                && leftRexNode instanceof RexCall && rightRexNode instanceof RexCall) {
                                boolean leftRef = ((RexCall) leftRexNode).getOperands().stream()
                                    .allMatch(x -> x instanceof RexInputRef);
                                boolean rightRef = ((RexCall) rightRexNode).getOperands().stream()
                                    .allMatch(x -> x instanceof RexInputRef);
                                // (xx,xx)=(xx,xx)
                                if (leftRef && rightRef) {
                                    break;
                                }
                                if (leftRef && DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                                    addEqualColumnList(filter.getInput(), ((RexCall) leftRexNode).getOperands());
                                    break;
                                }
                                if (rightRef && DrdsRexFolder.fold(leftRexNode, plannerContext) != null) {
                                    addEqualColumnList(filter.getInput(), ((RexCall) rightRexNode).getOperands());
                                    break;
                                }
                            }
                        }
                        throw Util.FoundOne.NULL;
                    case IS_NULL:
                        RexNode rexNode = call.getOperands().get(0);
                        if (rexNode instanceof RexInputRef) {
                            RelColumnOrigin relColumnOrigin =
                                mq.getColumnOrigin(filter.getInput(), ((RexInputRef) rexNode).getIndex());
                            if (relColumnOrigin != null) {
                                equalColumnsList.add(ImmutableList.of(relColumnOrigin.getColumnName()));
                                break;
                            }
                        }
                        throw Util.FoundOne.NULL;
                    case NOT_EQUALS:
                    case LESS_THAN:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN_OR_EQUAL:
                        // reference input columns
                        if (call.getOperands().size() == 2) {
                            RexNode leftRexNode = call.getOperands().get(0);
                            RexNode rightRexNode = call.getOperands().get(1);
                            if (RexUtil.containsInputRef(leftRexNode) && RexUtil.containsInputRef(rightRexNode)) {
                                break;
                            }
                        }
                        throw Util.FoundOne.NULL;
                    case BETWEEN:
                    default:
                        throw Util.FoundOne.NULL;
                    }
                }
                return filter;
            } catch (Util.FoundOne find) {
                collectSuccess = false;
                return filter;
            }
        }

        @Override
        public RelNode visit(LogicalSort sort) {
            super.visit(sort);
            try {
                if (orderByColumns != null) {
                    throw Util.FoundOne.NULL;
                }
                ImmutableList.Builder<String> builder = ImmutableList.builder();
                ImmutableList.Builder<Boolean> builder1 = ImmutableList.builder();
                for (RelFieldCollation relFieldCollation : sort.getCollation().getFieldCollations()) {
                    RelColumnOrigin relColumnOrigin =
                        mq.getColumnOrigin(sort.getInput(), relFieldCollation.getFieldIndex());
                    if (relColumnOrigin == null) {
                        throw Util.FoundOne.NULL;
                    }
                    builder.add(relColumnOrigin.getColumnName());
                    builder1.add(!relFieldCollation.getDirection().isDescending());
                }
                orderByColumns = builder.build();
                orderByAsc = builder1.build();
            } catch (Util.FoundOne find) {
                collectSuccess = false;
                return sort;
            }
            return sort;
        }

        private void addEqualColumnList(RelNode input, List<RexNode> references) {
            // todo: check type equal
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (RexNode rexNode : references) {
                if (!(rexNode instanceof RexInputRef)) {
                    throw Util.FoundOne.NULL;
                }
                RelColumnOrigin relColumnOrigin = mq.getColumnOrigin(input, ((RexInputRef) rexNode).getIndex());
                if (relColumnOrigin == null) {
                    throw Util.FoundOne.NULL;
                }
                builder.add(relColumnOrigin.getColumnName());
            }
            equalColumnsList.add(builder.build());
        }

        RequiredColumns build() {
            if (!collectSuccess) {
                return null;
            }
            RequiredColumns requiredColumns = new RequiredColumns(equalColumnsList, orderByColumns, orderByAsc);
            if (!requiredColumns.build()) {
                return null;
            }
            if (requiredColumns.size() == 0) {
                return null;
            }
            return requiredColumns;
        }
    }

    private static class RequiredColumns {
        // columns list used in 'equal' or 'in'
        private final List<List<String>> equalColumnsList;
        // columns used in order by
        private final List<String> orderByColumns;
        private final List<Boolean> orderByAsc;
        private Map<String, List<String>> equalColumnsMap;
        private int equalColumnSize;
        private int totalColumnSize;

        public RequiredColumns(List<List<String>> equalColumnsList, List<String> orderByColumns,
                               List<Boolean> orderByAsc) {
            this.equalColumnsList = equalColumnsList;
            this.orderByColumns = orderByColumns;
            this.orderByAsc = orderByAsc;
            equalColumnSize = 0;
            totalColumnSize = 0;
        }

        private boolean build() {
            equalColumnsMap = Maps.newHashMap();
            for (List<String> columns : equalColumnsList) {
                if (equalColumnsMap.put(columns.get(0), columns) != null) {
                    return false;
                }
                equalColumnSize += columns.size();
            }
            if (CollectionUtils.isNotEmpty(orderByColumns)) {
                if (equalColumnsMap.containsKey(orderByColumns.get(0))) {
                    return false;
                }
                totalColumnSize = orderByColumns.size();
            }
            totalColumnSize += equalColumnSize;
            return true;
        }

        /**
         * get equal-condition-column list containing given column.
         *
         * @param column given column
         * @return column set contains given column
         */
        List<String> getEqualColumns(String column) {
            if (column == null) {
                return null;
            }
            return equalColumnsMap.getOrDefault(column, null);
        }

        public List<String> getOrderByColumns() {
            return orderByColumns;
        }

        public List<Boolean> getOrderByAsc() {
            return orderByAsc;
        }

        int size() {
            return totalColumnSize;
        }

        int equalColumnSize() {
            return equalColumnSize;
        }

        /**
         * check whether current index can cover all required columns
         *
         * @param index index to be checked
         * @return true if index can cover all required columns
         */
        private boolean checkIndexCoverage(IndexMeta index, int skIndex) {
            List<IndexColumnMeta> indexColumns = index.getKeyColumnsExt();
            // all required columns must be covered
            if (indexColumns.size() < size()) {
                return false;
            }

            int loc = 0;
            // cover equal columns
            while (loc < equalColumnSize()) {
                List<String> equalColumns = getEqualColumns(indexColumns.get(loc).getName());
                if (equalColumns == null) {
                    // can't find equal condition on current index column
                    return false;
                }
                loc++;
                for (int i = 1; i < equalColumns.size(); i++) {
                    if (!equalColumns.get(i).equals(indexColumns.get(loc).getName())) {
                        return false;
                    }
                    loc++;
                }
            }

            List<String> orderByColumns = getOrderByColumns();
            if (CollectionUtils.isEmpty(orderByColumns)) {
                return true;
            }
            int forward = 0;
            for (int i = 0; i < orderByColumns.size(); i++) {
                String orderByColumn = orderByColumns.get(i);
                Boolean orderByAsc = getOrderByAsc().get(i);
                IndexColumnMeta meta = indexColumns.get(loc);
                if (!meta.hasColumn()) {
                    return false;
                }
                if (!meta.getColumnMeta().getPartOfSortKey().get(skIndex)) {
                    return false;
                }
                if (!orderByColumn.equals(indexColumns.get(loc).getName())) {
                    return false;
                }
                int currentForward = meta.getOrderByOption().isAsc() && orderByAsc ? 1 : -1;
                if (forward == 0) {
                    forward = currentForward;
                }
                if (forward != currentForward) {
                    return false;
                }
                loc++;
            }
            return true;
        }
    }

    public static Map<String, Integer> buildColumnarOrdinalMap(TableMeta tm) {
        Map<String, Integer> columnOrd = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < tm.getAllColumns().size(); i++) {
            columnOrd.put(tm.getAllColumns().get(i).getName(), i);
        }
        return columnOrd;
    }

    public static List<String> getSkNameList(TableMeta tm) {
        String schemaName = tm.getSchemaName();
        boolean isPartedTb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        List<String> sk = Lists.newArrayList();
        if (isPartedTb) {
            sk.addAll(tm.getPartitionInfo().getPartitionColumns());
        } else {
            TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
            TableRule rule = tddlRuleManager.getTableRule(tm.getTableName());
            if (rule == null) {
                return null;
            }
            if (CollectionUtils.isNotEmpty(rule.getDbPartitionKeys())) {
                sk.addAll(rule.getDbPartitionKeys());
            }
            if (CollectionUtils.isNotEmpty(rule.getTbPartitionKeys())) {
                sk.addAll(rule.getTbPartitionKeys());
            }
        }

        return sk;
    }

    public static List<Integer> getSkList(TableMeta tm, Map<String, Integer> columnOrd) {
        // build sk list
        List<String> sk = getSkNameList(tm);
        if (sk == null) {
            return Lists.newArrayList();
        }
        List<Integer> skList = Lists.newArrayList();
        for (String column : sk) {
            int loc = columnOrd.getOrDefault(column, -1);
            if (loc >= 0) {
                if (!skList.contains(loc)) {
                    skList.add(loc);
                }
            }
        }
        return skList;
    }

    public static TableMeta getIndexableTableMeta(RelOptTable table) {
        TableMeta tm = CBOUtil.getTableMeta(table);
        if (tm == null) {
            return null;
        }
        // ignore table with Functional indexes
        for (IndexMeta indexMeta : tm.getIndexes()) {
            if (indexMeta.isSpecialIndex()) {
                return null;
            }
        }
        return tm;
    }

    public static Pair<Boolean, Integer> testIfSkipSortOrder(RelCollation sortCollation,
                                                             TableScan scan,
                                                             Map<String, Integer> columnOrd,
                                                             IndexMeta indexMeta,
                                                             int skIndex,
                                                             RexNode condition,
                                                             boolean covering) {
        return testIfSkipSortOrder(sortCollation, scan, columnOrd, indexMeta, skIndex,
            LogicalFilter.create(scan, condition), covering);
    }

    /**
     * Tests whether the sort order can be skipped based on the provided collation, scan, index metadata,
     * and filter conditions. This function evaluates different scenarios such as exact match, reverse order,
     * and range-based conditions to determine if sorting can be avoided.
     *
     * @param sortCollation The collation specifying the sort order.
     * @param scan The table scan node being evaluated.
     * @param columnOrd A mapping of column names to their ordinal positions.
     * @param indexMeta Metadata about the index being used.
     * @param skIndex The index of the sort key in the collation.
     * @param filter The logical filter applied to the scan.
     * @return A pair where the first element indicates whether the sort can be skipped,
     * and the second element indicates the length of the constant key part used for skipping.
     */
    public static Pair<Boolean, Integer> testIfSkipSortOrder(RelCollation sortCollation,
                                                             TableScan scan,
                                                             Map<String, Integer> columnOrd,
                                                             IndexMeta indexMeta,
                                                             int skIndex,
                                                             LogicalFilter filter,
                                                             boolean covering) {
        Set<String> constColumnSet = ForceIndexUtil.equalColumnsSetForFilter(filter, scan);

        // Test if the order by key allows skipping sort under various conditions
        SkipSortInfo skipSortInfo =
            testIfOrderByKey(sortCollation, scan, columnOrd, indexMeta, skIndex, constColumnSet);

        // If direction is ascending (1), sort can be skipped directly
        if (skipSortInfo.getDirection() == 1) {
            return Pair.of(true, skipSortInfo.getConstKeyPartLen());
        }

        // If direction is descending (-1), additional checks are needed
        if (skipSortInfo.getDirection() == -1) {
            // If not quick skip or first sort column index is invalid, still skip sort
            if (!(skipSortInfo.isSkipQuick() && skipSortInfo.getFirstSortColumnIdx() >= 0)) {
                return Pair.of(true, skipSortInfo.getConstKeyPartLen());
            }

            boolean mightBeRange = false;

            // Check if any filter condition involves a range operation on the first sort column
            for (RexNode condition : RelOptUtil.conjunctions(filter.getCondition())) {
                if (condition instanceof RexCall) {
                    RexCall rexCall = (RexCall) condition;
                    if (rexCall.getKind().belongsTo(SqlKind.SARGABLE)
                        && !rexCall.getKind().belongsTo(SqlKind.BINARY_EQUALITY)
                        && !rexCall.getKind().equals(SqlKind.IS_NULL)) {
                        final ImmutableBitSet projRefs = RelOptUtil.InputFinder.bits(rexCall);
                        if (projRefs.get(skipSortInfo.getFirstSortColumnIdx())) {
                            mightBeRange = true;
                        }
                    }
                }
            }

            // If no range condition is found, sort can still be skipped
            if (!mightBeRange) {
                return Pair.of(true, skipSortInfo.getConstKeyPartLen());
            }
        }

        // If direction is neutral (0), attempt to push down range limit for further optimization
        if (skipSortInfo.getDirection() == 0 && covering) {
            skipSortInfo = tryPushDownRangeLimit(sortCollation, scan, columnOrd, indexMeta, skIndex, filter);
            if (skipSortInfo.getDirection() != 0) {
                return Pair.of(true, skipSortInfo.getConstKeyPartLen());
            }
        }

        // Default case: sort cannot be skipped
        return Pair.of(false, -1);
    }

    private static class SkipSortInfo {
        private final int direction;
        private final int firstSortColumnIdx;
        private final boolean skipQuick;
        private final int constKeyPartLen;

        public SkipSortInfo(int direction, int firstSortColumn, boolean skipQuick, int constKeyPartLen) {
            this.direction = direction;
            this.firstSortColumnIdx = firstSortColumn;
            this.skipQuick = skipQuick;
            this.constKeyPartLen = constKeyPartLen;
        }

        public int getDirection() {
            return direction;
        }

        public int getFirstSortColumnIdx() {
            return firstSortColumnIdx;
        }

        public boolean isSkipQuick() {
            return skipQuick;
        }

        public int getConstKeyPartLen() {
            return constKeyPartLen;
        }

        static SkipSortInfo noSkip() {
            return new SkipSortInfo(0, -1, false, -1);
        }
    }

    /**
     * Tests whether the given sort collation can be satisfied by an index scan, potentially skipping
     * explicit sorting.
     * <p>
     * This method checks if the ordering specified in {@code sortCollation} aligns with the column
     * order defined in {@code indexMeta}, considering constant columns from {@code constColumnSet}.
     * It also handles cases where the primary key may act as a suffix to the index for ordering purposes.
     *
     * @param sortCollation The collation representing the desired sort order.
     * @param scan The table scan node being evaluated.
     * @param columnOrd A mapping from column names to their ordinal positions in the table schema.
     * @param indexMeta Metadata about the index being considered for scanning.
     * @param skIndex The index of the current shard key (used for checking sort key participation).
     * @param constColumnSet A set of column names that are constrained to constant values in the query.
     * @return A {@link SkipSortInfo} object indicating whether sorting can be skipped and related metadata.
     */
    public static SkipSortInfo testIfOrderByKey(RelCollation sortCollation,
                                                TableScan scan,
                                                Map<String, Integer> columnOrd,
                                                IndexMeta indexMeta,
                                                int skIndex,
                                                Set<String> constColumnSet) {
        int reverse = 0;
        int keyPartIdx = 0;
        boolean onPkSuffix = false;
        boolean mixedOrder = false;

        int firstSortColumnIdx = -1;
        boolean skipQuick = false;
        int constKeyPartLen = 0;
        TableMeta table = CBOUtil.getTableMeta(scan.getTable());
        List<IndexColumnMeta> columns = indexMeta.getKeyColumnsExt();
        boolean reverseSorted = !columns.get(0).getOrderByOption().isAsc();
        IndexColumnMeta keyPart;

        // Iterate through each field in the required sort order
        for (RelFieldCollation order : sortCollation.getFieldCollations()) {
            int field = order.getFieldIndex();

            // Skip key parts that are constants but do not match the current sort field
            for (; keyPartIdx < columns.size() &&
                constColumnSet.contains(columns.get(keyPartIdx).getName()) &&
                columnOrd.get(columns.get(keyPartIdx).getName()) != field; keyPartIdx++) {
            }

            // If all key parts have been processed, try using the primary key as a suffix
            if (keyPartIdx == columns.size()) {
                if (!onPkSuffix && table.isHasPrimaryKey() && table.getPrimaryIndex() != indexMeta) {
                    onPkSuffix = true;
                    columns = table.getPrimaryIndex().getKeyColumnsExt();
                    keyPartIdx = 0;

                    // Again, skip constant key parts that don't match the current sort field
                    for (; keyPartIdx < columns.size() &&
                        constColumnSet.contains(columns.get(keyPartIdx).getName()) &&
                        columnOrd.get(columns.get(keyPartIdx).getName()) != field; keyPartIdx++) {
                    }
                } else {
                    return SkipSortInfo.noSkip();
                }
            }

            // If we've gone beyond available key parts, return default skip info
            if (keyPartIdx >= columns.size()) {
                return SkipSortInfo.noSkip();
            }

            keyPart = columns.get(keyPartIdx);
            int keyPartField = columnOrd.get(keyPart.getName());

            // Check if the current key part matches the expected field and participates in sort
            if (keyPartField != field || !keyPart.getColumnMeta().getPartOfSortKey().get(skIndex)) {
                return SkipSortInfo.noSkip();
            }

            // Record the position of the first non-constant sort column
            if (!constColumnSet.contains(keyPart.getName()) && firstSortColumnIdx < 0) {
                constKeyPartLen = keyPartIdx + (onPkSuffix ? indexMeta.getKeyColumnsExt().size() : 0);
                firstSortColumnIdx = keyPartField;
            }

            boolean keyPartOrder = !keyPart.getOrderByOption().isAsc();
            // Determine scan direction compatibility: 1 for compatible, -1 otherwise
            int curScanDir = (order.direction.isDescending() == keyPartOrder) ? 1 : -1;

            // Return early if inconsistent scan directions are detected
            if (reverse != 0 && curScanDir != reverse) {
                return SkipSortInfo.noSkip();
            }

            // Update scan direction and check for mixed ordering
            reverse = curScanDir;
            mixedOrder |= (reverseSorted != keyPartOrder);
            keyPartIdx++;
        }

        // Enable quick skip optimization under specific conditions
        if (mixedOrder && reverse < 0) {
            skipQuick = true;
        }

        // Finalize length of constant prefix if no variable sort column was found
        if (firstSortColumnIdx < 0) {
            constKeyPartLen = keyPartIdx + (onPkSuffix ? indexMeta.getKeyColumnsExt().size() : 0);
        }

        // Return final decision with collected metadata
        return new SkipSortInfo(reverse, firstSortColumnIdx, skipQuick, constKeyPartLen);
    }

    public static SkipSortInfo tryPushDownRangeLimit(RelCollation sortCollation,
                                                     TableScan scan,
                                                     Map<String, Integer> columnOrd,
                                                     IndexMeta indexMeta,
                                                     int skIndex,
                                                     LogicalFilter filter) {
        Set<String> equalOrInColumnSet = ForceIndexUtil.equalOrInColumnsSetForFilter(filter, scan);
        SkipSortInfo skipSortInfo =
            testIfOrderByKey(sortCollation, scan, columnOrd, indexMeta, skIndex, equalOrInColumnSet);
        if (skipSortInfo.getDirection() != 0) {
            FeatureStats.getInstance().increment(EXPAND_IN);
        }
        ExecutionContext ec = PlannerContext.getPlannerContext(scan).getExecutionContext();
        if (ec != null && !ec.supportPushdownRangeLimit()) {
            return SkipSortInfo.noSkip();
        } else {
            return skipSortInfo;
        }
    }

    public static Set<String> equalColumnsSetForFilter(LogicalFilter filter, TableScan scan) {
        EqualColumnsCollector equalColumnsCollector = new EqualColumnsCollector(scan);
        // Collect columns that are constant
        filter.accept(equalColumnsCollector);
        Set<String> constColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (String equalColumn : equalColumnsCollector.getEqualColumnsList()) {
            constColumnSet.add(equalColumn.toLowerCase());
        }
        return constColumnSet;
    }

    public static Set<String> equalOrInColumnsSetForFilter(LogicalFilter filter, TableScan scan) {
        EqualColumnsCollector equalColumnsCollector =
            new EqualColumnsCollector(scan, scan.getCluster().getMetadataQuery(), null);
        filter.accept(equalColumnsCollector);
        List<String> equalOrInColumnList = equalColumnsCollector.getEqualColumnsList();
        Set<String> equalOrInColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (String constColumn : equalOrInColumnList) {
            equalOrInColumnSet.add(constColumn.toLowerCase());
        }
        return equalOrInColumnSet;
    }

    public static class BestIndex {
        IndexMeta indexMeta;
        int eqPreLen;
        double cardinality;
        int coverUsedLen;
        boolean coverSk;

        public BestIndex(IndexMeta indexMeta, int eqPreLen, double cardinality, int coverUsedLen, boolean coverSk) {
            this.indexMeta = indexMeta;
            this.eqPreLen = eqPreLen;
            this.cardinality = cardinality;
            this.coverUsedLen = coverUsedLen;
            this.coverSk = coverSk;
        }

        /**
         * Finds a better index based on the given index information and returns the best index.
         * This method aims to decide whether to use the current index or the new index based on the comparison of index performance.
         *
         * @param newIndexMeta The metadata of the new index, containing information such as index columns.
         * @param newEqPreLen The length of the new index's prefix matching columns, used to evaluate index efficiency.
         * @param newCoverUsedLen The length of the new index's covering columns in filer, used to evaluate index efficiency.
         * @param newCoverSk Whether the new index covers the required columns, affecting query performance.
         * @return Returns the BestIndex object representing the best index choice.
         */
        public BestIndex findBetterIndex(IndexMeta newIndexMeta, int newEqPreLen, double newCardinality,
                                         int newCoverUsedLen, boolean newCoverSk) {
            if (indexMeta == null) {
                return new BestIndex(newIndexMeta, newEqPreLen, newCardinality, newCoverUsedLen, newCoverSk);
            }
            //Index covering more columns used in filter and sort is better.
            if (newCoverUsedLen < coverUsedLen) {
                return this;
            }
            if (newCoverUsedLen > coverUsedLen) {
                return new BestIndex(newIndexMeta, newEqPreLen, newCardinality, newCoverUsedLen, newCoverSk);
            }
            // If the prefix length of the new index is greater than the current index, the new index is better
            if (newEqPreLen > eqPreLen) {
                return new BestIndex(newIndexMeta, newEqPreLen, newCardinality, newCoverUsedLen, newCoverSk);
            }
            // If the prefix lengths are equal, further comparison is needed
            if (newEqPreLen == eqPreLen) {
                if (cardinality < 0 || newCardinality < 0) {
                    if (indexMeta.getKeyColumnsExt().size() > newIndexMeta.getKeyColumnsExt().size()) {
                        return new BestIndex(newIndexMeta, newEqPreLen, newCardinality, newCoverUsedLen, newCoverSk);
                    }
                    if (indexMeta.getKeyColumnsExt().size() == newIndexMeta.getKeyColumnsExt().size()
                        && newCoverSk && !coverSk) {
                        return new BestIndex(newIndexMeta, newEqPreLen, newCardinality, newCoverUsedLen, newCoverSk);
                    }
                    return this;
                }
                if (newCardinality > 1.1 * cardinality) {
                    return new BestIndex(newIndexMeta, newEqPreLen, newCardinality, newCoverUsedLen, newCoverSk);
                }
                if (newCardinality > 0.9 * cardinality) {
                    if (indexMeta.getKeyColumnsExt().size() > newIndexMeta.getKeyColumnsExt().size()) {
                        return new BestIndex(newIndexMeta, newEqPreLen, newCardinality, newCoverUsedLen, newCoverSk);
                    }
                    if (indexMeta.getKeyColumnsExt().size() == newIndexMeta.getKeyColumnsExt().size()
                        && newCoverSk && !coverSk) {
                        return new BestIndex(newIndexMeta, newEqPreLen, newCardinality, newCoverUsedLen, newCoverSk);
                    }
                }
            }
            // If the above conditions are not met, the current index remains the best choice
            return this;
        }

        public IndexMeta getIndexMeta() {
            return indexMeta;
        }

        public int getEqPreLen() {
            return eqPreLen;
        }
    }

    public abstract static class SargAbleHandler {

        PlannerContext plannerContext;

        public SargAbleHandler(PlannerContext plannerContext) {
            this.plannerContext = plannerContext;
        }

        public void handleCondition(RexNode condition) {
            if (!(condition instanceof RexCall)) {
                return;
            }
            RexCall call = (RexCall) condition;
            switch (call.getOperator().getKind()) {
            case IN:
                handleIn(call);
                return;
            case EQUALS:
                handleEquals(call);
                return;
            case NOT_EQUALS:
                handleNotEquals(call);
                return;
            case LESS_THAN:
                handleLessThan(call);
                return;
            case LESS_THAN_OR_EQUAL:
                handleLessThanOrEqual(call);
                return;
            case GREATER_THAN:
                handleGreaterThan(call);
                return;
            case GREATER_THAN_OR_EQUAL:
                handleGreaterThanOrEqual(call);
                return;
            case BETWEEN:
                handleBetween(call);
                return;
            case IS_NULL:
                handleIsNull(call);
                return;
            default:
                handleDefault(call);
            }
        }

        protected void handleIn(RexCall call) {
            if (call.getOperands().size() != 2) {
                return;
            }
            RexNode firstRex = call.getOperands().get(0);
            RexNode secondRex = call.getOperands().get(1);
            if (firstRex instanceof RexInputRef && secondRex.isA(SqlKind.ROW)) {
                if (DrdsRexFolder.fold(secondRex, plannerContext) != null) {
                    checkRefValid((RexInputRef) firstRex);
                    handleInRef((RexInputRef) firstRex, secondRex, call);
                    return;
                }
            }
            if (firstRex.isA(SqlKind.ROW) && firstRex instanceof RexCall
                && secondRex.isA(SqlKind.ROW)) {
                boolean leftRef = ((RexCall) firstRex).getOperands().stream()
                    .allMatch(x -> x instanceof RexInputRef && checkRefValid((RexInputRef) x));
                if (leftRef && DrdsRexFolder.fold(secondRex, plannerContext) != null) {
                    handleInVec((RexCall) firstRex, secondRex, call);
                }
            }
        }

        protected void handleEquals(RexCall call) {
            if (call.getOperands().size() != 2) {
                return;
            }
            RexNode firstRex = call.getOperands().get(0);
            RexNode secondRex = call.getOperands().get(1);
            // xx = ?
            if (firstRex instanceof RexInputRef && DrdsRexFolder.fold(secondRex, plannerContext) != null) {
                checkRefValid((RexInputRef) firstRex);
                handleEqualRef((RexInputRef) firstRex, secondRex, call);
                return;
            }
            // ? = xx
            if (secondRex instanceof RexInputRef && DrdsRexFolder.fold(firstRex, plannerContext) != null) {
                checkRefValid((RexInputRef) secondRex);
                handleEqualRef((RexInputRef) secondRex, firstRex, call);
                return;
            }
            //(xx,xx)=(?,?)
            if (firstRex.isA(SqlKind.ROW) && secondRex.isA(SqlKind.ROW)
                && firstRex instanceof RexCall && secondRex instanceof RexCall) {
                boolean leftVecRef = ((RexCall) firstRex).getOperands().stream()
                    .allMatch(x -> x instanceof RexInputRef && checkRefValid((RexInputRef) x));
                boolean rightVecRef = ((RexCall) secondRex).getOperands().stream()
                    .allMatch(x -> x instanceof RexInputRef && checkRefValid((RexInputRef) x));
                // (xx,xx)=(xx,xx)
                if (leftVecRef && rightVecRef) {
                    return;
                }
                if (leftVecRef && DrdsRexFolder.fold(secondRex, plannerContext) != null) {
                    handleEqualVec((RexCall) firstRex, secondRex, call);
                    return;
                }
                if (rightVecRef && DrdsRexFolder.fold(firstRex, plannerContext) != null) {
                    handleEqualVec((RexCall) secondRex, firstRex, call);
                }
            }
        }

        protected void handleNotEquals(RexCall call) {
            if (call.getOperands().size() != 2) {
                return;
            }
            RexNode firstRex = call.getOperands().get(0);
            RexNode secondRex = call.getOperands().get(1);
            // xx != ?
            if (firstRex instanceof RexInputRef && DrdsRexFolder.fold(secondRex, plannerContext) != null) {
                checkRefValid((RexInputRef) firstRex);
                handleNotEqualRef((RexInputRef) firstRex, secondRex, call);
                return;
            }
            // ? != xx
            if (secondRex instanceof RexInputRef && DrdsRexFolder.fold(firstRex, plannerContext) != null) {
                checkRefValid((RexInputRef) secondRex);
                handleNotEqualRef((RexInputRef) secondRex, firstRex, call);
            }
        }

        protected void handleLessThan(RexCall call) {
            if (call.getOperands().size() != 2) {
                return;
            }
            RexNode firstRex = call.getOperands().get(0);
            RexNode secondRex = call.getOperands().get(1);
            if (firstRex instanceof RexInputRef && DrdsRexFolder.fold(secondRex, plannerContext) != null) {
                checkRefValid((RexInputRef) firstRex);
                handleLessThanRef((RexInputRef) firstRex, secondRex, call);
                return;
            }
            if (secondRex instanceof RexInputRef && DrdsRexFolder.fold(firstRex, plannerContext) != null) {
                checkRefValid((RexInputRef) secondRex);
                handleGreaterThanRef((RexInputRef) secondRex, firstRex, call);
            }
        }

        protected void handleGreaterThan(RexCall call) {
            if (call.getOperands().size() != 2) {
                return;
            }
            RexNode firstRex = call.getOperands().get(0);
            RexNode secondRex = call.getOperands().get(1);
            if (firstRex instanceof RexInputRef && DrdsRexFolder.fold(secondRex, plannerContext) != null) {
                checkRefValid((RexInputRef) firstRex);
                handleGreaterThanRef((RexInputRef) firstRex, secondRex, call);
                return;
            }
            if (secondRex instanceof RexInputRef && DrdsRexFolder.fold(firstRex, plannerContext) != null) {
                checkRefValid((RexInputRef) secondRex);
                handleLessThanRef((RexInputRef) secondRex, firstRex, call);
            }
        }

        protected void handleLessThanOrEqual(RexCall call) {
            if (call.getOperands().size() != 2) {
                return;
            }
            RexNode firstRex = call.getOperands().get(0);
            RexNode secondRex = call.getOperands().get(1);
            if (firstRex instanceof RexInputRef && DrdsRexFolder.fold(secondRex, plannerContext) != null) {
                checkRefValid((RexInputRef) firstRex);
                handleLessThanOrEqualRef((RexInputRef) firstRex, secondRex, call);
                return;
            }
            if (secondRex instanceof RexInputRef && DrdsRexFolder.fold(firstRex, plannerContext) != null) {
                checkRefValid((RexInputRef) secondRex);
                handleGreaterThanOrEqualRef((RexInputRef) secondRex, firstRex, call);
            }
        }

        protected void handleGreaterThanOrEqual(RexCall call) {
            if (call.getOperands().size() != 2) {
                return;
            }
            RexNode firstRex = call.getOperands().get(0);
            RexNode secondRex = call.getOperands().get(1);
            if (firstRex instanceof RexInputRef && DrdsRexFolder.fold(secondRex, plannerContext) != null) {
                checkRefValid((RexInputRef) firstRex);
                handleGreaterThanOrEqualRef((RexInputRef) firstRex, secondRex, call);
                return;
            }
            if (secondRex instanceof RexInputRef && DrdsRexFolder.fold(firstRex, plannerContext) != null) {
                checkRefValid((RexInputRef) secondRex);
                handleGreaterThanOrEqualRef((RexInputRef) secondRex, firstRex, call);
            }
        }

        protected void handleBetween(RexCall call) {
            if (call.getOperands().size() != 3) {
                return;
            }
            RexNode firstRex = call.getOperands().get(0);
            RexNode secondRex = call.getOperands().get(1);
            RexNode thirdRex = call.getOperands().get(2);
            if (firstRex instanceof RexInputRef
                && DrdsRexFolder.fold(secondRex, plannerContext) != null
                && DrdsRexFolder.fold(thirdRex, plannerContext) != null) {
                checkRefValid((RexInputRef) firstRex);
                handleBetweenRef((RexInputRef) firstRex, secondRex, thirdRex, call);
            }
        }

        protected void handleIsNull(RexCall call) {
            if (call.getOperands().size() != 1) {
                return;
            }
            RexNode firstRex = call.getOperands().get(0);
            if (firstRex instanceof RexInputRef) {
                checkRefValid((RexInputRef) firstRex);
                handleIsNullRef((RexInputRef) firstRex, call);
            }
        }

        protected void handleDefault(RexCall call) {
            // do nothing by default
        }

        protected boolean checkRefValid(RexInputRef rexInputRef) {
            return true;
        }

        protected abstract void handleUnaryRef(RexInputRef ref, RexCall call);

        protected abstract void handleBinaryRef(RexInputRef leftRef, RexNode rightRex, RexCall call);

        protected abstract void handleTernaryRef(RexInputRef firstRex, RexNode leftRex, RexNode rightRex, RexCall call);

        protected void handleInRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
            handleBinaryRef(leftRef, rightRex, call);
        }

        protected void handleInVec(RexCall leftRex, RexNode rightRex, RexCall call) {
            // do nothing by default
        }

        protected void handleEqualRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
            handleBinaryRef(leftRef, rightRex, call);
        }

        protected void handleEqualVec(RexCall leftRex, RexNode rightRex, RexCall call) {
            // do nothing by default
        }

        protected void handleNotEqualRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
            handleBinaryRef(leftRef, rightRex, call);
        }

        protected void handleLessThanRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
            handleBinaryRef(leftRef, rightRex, call);
        }

        protected void handleLessThanOrEqualRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
            handleBinaryRef(leftRef, rightRex, call);
        }

        protected void handleGreaterThanRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
            handleBinaryRef(leftRef, rightRex, call);
        }

        protected void handleGreaterThanOrEqualRef(RexInputRef leftRef, RexNode rightRex, RexCall call) {
            handleBinaryRef(leftRef, rightRex, call);
        }

        protected void handleBetweenRef(RexInputRef ref, RexNode lowerRex, RexNode upperRex, RexCall call) {
            handleTernaryRef(ref, lowerRex, upperRex, call);
        }

        protected void handleIsNullRef(RexInputRef leftRef, RexCall call) {
            handleUnaryRef(leftRef, call);
        }
    }
}