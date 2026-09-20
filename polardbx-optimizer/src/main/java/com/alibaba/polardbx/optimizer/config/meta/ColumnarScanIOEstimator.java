package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.alibaba.polardbx.optimizer.index.Index;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * ColumnarScan代价估算的作用：
 * 1. OssTableScan vs OssTableScan，列存索引推荐和多CCI索引选择，主要关注排序键和分区键的对IO的影响，
 * 2. LogicalView vs OssTableScan，行列混合查询，需要统一行存和列存Scan的IO cost的单位，需要考虑更多细节，比如压缩率、元信息加载的影响,IO page的大小
 * <p>
 * 返回的是读取的RowGroup的字节数
 */
public class ColumnarScanIOEstimator extends TableScanIOEstimator {

    //TODO : orc文件压缩比
    private static final double cciCompressRatio = 0.2;

    private final OrcTableScan orcTableScan;

    private List<RelDataTypeField> filterColumnFields;

    private List<RelDataTypeField> projectColumnFields;

    //经过分区裁剪后的总行数
    private final Double totalRowCount;

    private final Double estimatedRowCount;

    /**
     * 过滤列需要check的rowgroup数量
     */
    private double filterRowGroupCount;

    private double maxFilterRowGroupCount;

    public ColumnarScanIOEstimator(OrcTableScan orcTableScan, TableScan tableScan, RelMetadataQuery metadataQuery,
                                   Double totalRowCount, Double estimatedRowCount) {
        super(tableScan, metadataQuery, -1);
        this.orcTableScan = orcTableScan;
        this.totalRowCount = totalRowCount;
        this.estimatedRowCount = estimatedRowCount;
        this.maxFilterRowGroupCount = getMaxFilterRowGroupCount();
        this.filterRowGroupCount = maxFilterRowGroupCount;
        init();
    }

    public void init() {
        Set<Integer> filterColumnIndexes = orcTableScan.getOriFilters().stream().map(rexNode -> {
            AtomicInteger index = new AtomicInteger(-1);
            rexNode.accept(new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef input) {
                    index.set(input.getIndex());
                    return input;
                }
            });
            return index.get();
        }).filter(index -> index >= 0).collect(Collectors.toSet());

        Set<Integer> projectColumnIndexes = new HashSet<>(orcTableScan.getInProjects());
        projectColumnIndexes.removeAll(filterColumnIndexes);

        this.filterColumnFields =
            filterColumnIndexes.stream().map(index -> orcTableScan.getTable().getRowType().getFieldList().get(index))
                .collect(Collectors.toList());
        this.projectColumnFields =
            projectColumnIndexes.stream().map(index -> orcTableScan.getTable().getRowType().getFieldList().get(index))
                .collect(Collectors.toList());
        //只需要io project列和filter列
        this.maxIO = (totalRowCount * TableScanIOEstimator.estimateRowSize(filterColumnFields)
            + totalRowCount * TableScanIOEstimator.estimateRowSize(projectColumnFields)) * cciCompressRatio;
    }

    @Override
    public Double visitCall(RexCall call) {
        if (call.getOperator() == SqlStdOperatorTable.OR) {
            AtomicDouble currentFilterRowGroupCount = new AtomicDouble(0d);
            Double orIO = call.getOperands().stream().map(
                rexNode -> {
                    Double result = this.evaluate(rexNode);
                    currentFilterRowGroupCount.addAndGet(this.filterRowGroupCount);
                    return result;
                }).reduce(0.0, (a, b) -> normalize(a) + normalize(b));
            this.filterRowGroupCount = normalizeFilterRowGroupCount(currentFilterRowGroupCount.get());
            return normalize(orIO);
        } else if (call.getOperator() == SqlStdOperatorTable.NOT) {
            //目前ColumnarScan没有针对Not做裁剪，默认全扫
            return normalize(noIndexIO(Collections.singletonList(call)).getValue());
        } else {
            return normalize(getColumnarTableScanConjunctionsIO(call));
        }
    }

    private double getColumnarTableScanConjunctionsIO(RexNode predicate) {
        if ((predicate == null) || predicate.isAlwaysTrue() || tableMeta == null) {
            return getMaxIO();
        }

        if (predicate.isAlwaysFalse()) {
            return 0;
        }

        List<RexNode> conjunctions = RelOptUtil.conjunctions(predicate);

        Pair<Double, Double> pair;
        double currentIo;
        Double currentFilterRowGroupCount;
        //可以基于SortKey查询
        if ((pair = sortKeyIndexIO(tableMeta, conjunctions, plannerContext)) != null) {
            currentFilterRowGroupCount = pair.getKey();
            currentIo = pair.getValue();
        } else {
            currentIo = maxIO;
            currentFilterRowGroupCount = maxFilterRowGroupCount;
        }

        /*
         * deal with other predicate
         */
        for (RexNode otherPredicate : conjunctions) {
            if (otherPredicate instanceof RexCall) {
                RexCall otherCall = (RexCall) otherPredicate;
                if (otherCall.getOperator() == SqlStdOperatorTable.OR) {
                    double orIo = this.evaluate(otherCall);
                    if (orIo < currentIo) {
                        currentIo = orIo;
                        currentFilterRowGroupCount = this.filterRowGroupCount;
                    }
                }
            }
        }

        //maxIO说明所有谓词都无法走sortKey索引
        if (currentIo == maxIO) {
            pair = noIndexIO(conjunctions);
            currentIo = normalize(pair.getValue());
            currentFilterRowGroupCount = normalizeFilterRowGroupCount(pair.getKey());
        }
        this.filterRowGroupCount = currentFilterRowGroupCount;
        return normalize(currentIo);
    }

    private Pair<Double, Double> sortKeyIndexIO(TableMeta tableMeta, List<RexNode> conjunctions,
                                                PlannerContext plannerContext) {
        if (tableMeta.getGsiTableMetaBean().tableType != GsiMetaManager.TableType.COLUMNAR) {
            return null;
        }

        List<ColumnMeta> sortKeyColumns = new ArrayList<>();
        for (GsiMetaManager.GsiIndexColumnMetaBean gsiIndexColumnMetaBean : tableMeta.getGsiTableMetaBean().gsiMetaBean.indexColumns) {
            sortKeyColumns.add(tableMeta.getColumn(gsiIndexColumnMetaBean.columnName));
        }

        List<RexNode> newConjunctions = new ArrayList<>();
        newConjunctions.addAll(conjunctions);
        List<IndexContext> indexContextList = new ArrayList<>();

        for (ColumnMeta indexColumnMeta : sortKeyColumns) {
            IndexContext indexContext = oneIndexColumnContext(indexColumnMeta, newConjunctions, plannerContext);
            if (indexContext == null) {
                break;
            }
            indexContextList.add(indexContext);
            if (indexContext.predicateType == Index.PredicateType.RANGE) {
                break;
            }
        }

        if (indexContextList.isEmpty()) {
            return null;
        }

        /**
         * filter 列的IO 的rowgroup数量和project列是不一样的
         *
         * filter列是基于排序列进行裁剪， 然后读取裁剪后的rowgroup，再基于所有的列进行裁剪，得到project的rowgroup。
         * 如果排序列不对应filter列，那么filter 列基于minmax裁剪的概率就很低，几乎会获取所有的rowgroup，但是project列会按照filter具体执行的结果，获取到具体的rowgroup数量
         */

        //排序键裁剪，得到filter列需要扫描的行数
        double filterColumnRowCount = totalRowCount;
        int fanOut = 1;
        for (IndexContext indexContext : indexContextList) {
            filterColumnRowCount = Math.ceil(filterColumnRowCount * indexContext.selectivity);
            fanOut *= indexContext.fanOut;
        }

        //对in查询进行RowGroup数量惩罚
        double ossRowGroupSize;
        if (indexContextList.get(indexContextList.size() - 1).predicateType == Index.PredicateType.RANGE
            || indexContextList.get(indexContextList.size() - 1).predicateType == Index.PredicateType.EQUAL) {
            ossRowGroupSize = CostModelWeight.OSS_ROW_GROUP_SIZE;
        } else {
            ossRowGroupSize = CostModelWeight.OSS_ROW_GROUP_SIZE / 8;
        }

        //计算filter列的rowgroup
        double currentFilterRowGroupCount = Math.ceil(filterColumnRowCount / ossRowGroupSize);

        //计算project列的rowgroup
        double currentProjectRowGroupCount =
            Math.ceil(Math.min(estimateRowCount(conjunctions), filterColumnRowCount) / ossRowGroupSize);
        currentProjectRowGroupCount = Math.min(currentProjectRowGroupCount, currentFilterRowGroupCount);

        double compressRatio = cciCompressRatio;//orc文件压缩比
        double rowGroupByteSize =
            currentFilterRowGroupCount * CostModelWeight.OSS_ROW_GROUP_SIZE * TableScanIOEstimator.estimateRowSize(
                filterColumnFields)
                + currentProjectRowGroupCount * CostModelWeight.OSS_ROW_GROUP_SIZE
                * TableScanIOEstimator.estimateRowSize(projectColumnFields);
        rowGroupByteSize *= compressRatio;

        double io = rowGroupByteSize * fanOut;
        return Pair.of(currentFilterRowGroupCount, io);

    }

    private Pair<Double, Double> noIndexIO(List<RexNode> rexNodes) {

        double projectRowGroupCount =
            OrcTableScan.getGroupNumberForRandomDistribution(estimateRowCount(rexNodes), totalRowCount,
                (int) CostModelWeight.OSS_ROW_GROUP_SIZE);
        double filterRowGroupCount = maxFilterRowGroupCount;

        double compressRatio = cciCompressRatio;//orc文件压缩比
        double rowGroupByteSize =
            filterRowGroupCount * CostModelWeight.OSS_ROW_GROUP_SIZE * TableScanIOEstimator.estimateRowSize(
                filterColumnFields)
                + projectRowGroupCount * CostModelWeight.OSS_ROW_GROUP_SIZE * TableScanIOEstimator.estimateRowSize(
                projectColumnFields);
        rowGroupByteSize *= compressRatio;

        double io = rowGroupByteSize;
        return Pair.of(filterRowGroupCount, io);

    }

    public Double normalizeFilterRowGroupCount(Double filterRowGroupCount) {
        if (filterRowGroupCount == null) {
            return maxFilterRowGroupCount;
        } else if (filterRowGroupCount <= 0) {
            return 0.0;
        } else if (filterRowGroupCount >= maxFilterRowGroupCount) {
            return maxFilterRowGroupCount;
        } else {
            return Math.ceil(filterRowGroupCount);
        }
    }

    private double getMaxFilterRowGroupCount() {
        return Math.ceil(totalRowCount / CostModelWeight.OSS_ROW_GROUP_SIZE);
    }

    public double getFilterRowGroupCount() {
        return filterRowGroupCount;
    }

    public List<RelDataTypeField> getFilterColumnFields() {
        return filterColumnFields;
    }

    public List<RelDataTypeField> getProjectColumnFields() {
        return projectColumnFields;
    }

    public double estimateRowCount(List<RexNode> rexNodes) {
        double selectivity = metadataQuery.getSelectivity(tableScan,
            RexUtil.composeConjunction(tableScan.getCluster().getRexBuilder(), rexNodes, false));
        return Math.min(tableRowCount * selectivity, totalRowCount);
    }
}
