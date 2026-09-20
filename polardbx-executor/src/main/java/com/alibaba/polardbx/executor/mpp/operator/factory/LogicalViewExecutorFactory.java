/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.operator.AbstractOSSTableScanExec;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner;
import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.mpp.planner.PipelineFragment;
import com.alibaba.polardbx.executor.mpp.planner.RangeScanUtils;
import com.alibaba.polardbx.executor.operator.AbstractOSSTableScanExec;
import com.alibaba.polardbx.executor.operator.AdaptiveRangeScanClient;
import com.alibaba.polardbx.executor.operator.ColumnarScanExec;
import com.alibaba.polardbx.executor.operator.ColumnarSpecifiedScanExec;
import com.alibaba.polardbx.executor.operator.DrivingStreamTableScanExec;
import com.alibaba.polardbx.executor.operator.DrivingStreamTableScanSortExec;
import com.alibaba.polardbx.executor.operator.DynamicMergeSortTableScanClient;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.LookupTableScanExec;
import com.alibaba.polardbx.executor.operator.LookupTableSortRangeScanExec;
import com.alibaba.polardbx.executor.operator.LookupTableSortScanExec;
import com.alibaba.polardbx.executor.operator.MergeSortTableScanClient;
import com.alibaba.polardbx.executor.operator.MergeSortWithBufferTableScanClient;
import com.alibaba.polardbx.executor.operator.NormalRangeScanClient;
import com.alibaba.polardbx.executor.operator.RangeScanClientBase;
import com.alibaba.polardbx.executor.operator.RangeScanSortExec;
import com.alibaba.polardbx.executor.operator.ResumeTableScanExec;
import com.alibaba.polardbx.executor.operator.ResumeTableScanSortExec;
import com.alibaba.polardbx.executor.operator.TableScanClient;
import com.alibaba.polardbx.executor.operator.TableScanExec;
import com.alibaba.polardbx.executor.operator.TableScanSortExec;
import com.alibaba.polardbx.executor.operator.lookup.LookupConditionBuilder;
import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermitManager;
import com.alibaba.polardbx.executor.operator.scan.impl.ColumnarMemoryPermitManagerImpl;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.bloomfilter.BloomFilterConsume;
import com.alibaba.polardbx.executor.operator.util.bloomfilter.BloomFilterExpression;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.build.InExpressionParamRewriter;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.join.LookupEquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class LogicalViewExecutorFactory extends ExecutorFactory {
    private static final Logger MPP_LOGGER = LoggerFactory.getLogger(LocalExecutionPlanner.class);
    private final PipelineFragment fragment;

    private final int totalPrefetch;
    private final CursorMeta meta;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final int parallelism;
    private final long maxRowCount;
    private final LogicalView logicalView;
    private TableScanClient scanClient;
    private final SpillerFactory spillerFactory;

    private final boolean bSort;
    private final Sort sort;
    private final long fetch;
    private final long skip;

    private BloomFilterExpression filterExpression;

    private boolean enablePassiveResume;

    private boolean enableDrivingResume;

    private List<LookupEquiJoinKey> allJoinKeys; // including null-safe equal (`<=>`)
    private LookupPredicate predicates;
    private List<DataType> dataTypeList;

    private RangeScanMode rangeScanMode;

    private Map<Integer, Map<String, List>> rewriterParams;
    private ExecutorService scanExecutor;
    private ColumnarMemoryPermitManager columnarMemoryPermitManager;

    public LogicalViewExecutorFactory(
        PipelineFragment fragment, LogicalView logicalView,
        int totalPrefetch, int parallelism, long maxRowCount, boolean bSort, Sort sort,
        long fetch, long skip, SpillerFactory spillerFactory, Map<Integer, BloomFilterExpression> bloomFilters,
        boolean enableRuntimeFilter, RangeScanMode rangeScanMode) {
        this.fragment = fragment;
        this.logicalView = logicalView;
        this.totalPrefetch = totalPrefetch;
        this.meta = CursorMeta.build(CalciteUtils.buildColumnMeta(logicalView, "TableScanColumns"));
        this.parallelism = parallelism;
        this.maxRowCount = maxRowCount;
        this.bSort = bSort;
        this.sort = sort;
        this.fetch = fetch;
        this.skip = skip;
        this.spillerFactory = spillerFactory;
        this.rangeScanMode = rangeScanMode;

        if (logicalView.isLookupTable()) {
            this.allJoinKeys = ImmutableList.copyOf(logicalView.getLookupInfo().getAllJoinKeys());
            this.predicates = logicalView.getLookupInfo().getPredicates();
        }

        if (enableRuntimeFilter) {
            List<Integer> bloomFilterIds = logicalView.getBloomFilters();
            if (bloomFilterIds.size() > 0) {
                List<BloomFilterConsume> consumes = new ArrayList<>();
                for (Integer bloomId : bloomFilterIds) {
                    consumes.add(new BloomFilterConsume(null, bloomId));
                }
                this.filterExpression = new BloomFilterExpression(consumes, true);
                bloomFilters.put(logicalView.getRelatedId(), filterExpression);
            }
        }
        this.dataTypeList = CalciteUtils.getTypes(logicalView.getRowType());
    }

    /**
     * Pre-build the row-store SQL template for tables with externalized columns so logical-to-physical
     * column name rewriting happens before execution. OSSTableScan exposes logical values directly.
     */
    public void adjustDataTypesForExternalizedColumns(ExecutionContext context) {
        if (logicalView instanceof OSSTableScan) {
            return;
        }

        String schemaName = logicalView.getSchemaName();
        if (schemaName == null || schemaName.isEmpty()) {
            schemaName = context.getSchemaName();
        }
        SchemaManager sm = context.getSchemaManager(schemaName);
        if (sm == null) {
            return;
        }
        TableMeta tableMeta = sm.getTableWithNull(logicalView.getLogicalTableName());
        if (tableMeta == null) {
            return;
        }

        if (!tableMeta.hasExternalizedColumn()) {
            return;
        }

        // Ensure buildSqlTemplate has run so FETCH_BLOB descriptors are available.
        logicalView.getSqlTemplate(context);

        // NOTE: We intentionally do NOT change dataTypeList to LongType here.
        // The columnar file meta (via buildColumnMeta forColumnar=true) already stores
        // the physical VARCHAR(64) type, and projectCsvChunk() passes it through
        // for pipeline compatibility. The pipeline operators (Sort, Project) use
        // VarcharType from the Calcite plan, so dataTypeList must stay VarcharType.
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        if (logicalView instanceof OSSTableScan) {
            return buildOSSTableScanExec(context);
        } else {
            return buildTableScanExec(context);
        }
    }

    @NotNull
    private Executor buildTableScanExec(ExecutionContext context) {
        TableScanExec scanExec;
        if (logicalView.isLookupTable()) {
            boolean canShard = false;
            if (context.getParamManager().getBoolean(ConnectionParams.ENABLE_BKA_PRUNING)) {
                LogicalView lv = this.getLogicalView();
                if (lv.getTableNames().size() == 1) {
                    canShard = new LookupConditionBuilder(allJoinKeys, predicates, lv, context).canShard();
                }
            }
            scanExec = createLookupScanExec(context, canShard, predicates, allJoinKeys);
        } else {
            boolean useTransactionConnection = ExecUtils.useExplicitTransaction(context);

            if (bSort) {
                if (rangeScanMode != null) {
                    this.scanClient = getRangeScanClient(context, useTransactionConnection);
                } else {
                    boolean dynamicSort = RangeScanUtils.isDynamicMergeSort(logicalView);
                    long limit = context.getParamManager().getLong(ConnectionParams.MERGE_SORT_BUFFER_SIZE);
                    if (dynamicSort) {
                        Sort sort = (Sort) logicalView.getOptimizedPushedRelNodeForMetaQuery();
                        RelCollation collation = sort.getCollation();
                        List<RelFieldCollation> sortList = collation.getFieldCollations();
                        List<OrderByOption> orderByOptions = ExecUtils.convertFrom(sortList);
                        this.scanClient = new DynamicMergeSortTableScanClient(
                            context, meta, useTransactionConnection, totalPrefetch, orderByOptions, logicalView,
                            skip + fetch);
                    } else if (limit > 0 && logicalView.pushedRelNodeIsSort()) {
                        this.scanClient = new MergeSortWithBufferTableScanClient(
                            context, meta, useTransactionConnection, totalPrefetch);
                    } else {
                        this.scanClient = new MergeSortTableScanClient(
                            context, meta, useTransactionConnection, totalPrefetch);
                    }
                }
            } else if (useTransactionConnection || enablePassiveResume || enableDrivingResume) {
                int prefetch = calculatePrefetchNum(counter.incrementAndGet(), parallelism);
                this.scanClient = new TableScanClient(context, meta, useTransactionConnection, prefetch);
            } else {
                synchronized (this) {
                    if (scanClient == null) {
                        this.scanClient =
                            new TableScanClient(context, meta, false, Math.max(totalPrefetch, parallelism));
                    }
                }
            }

            if (filterExpression != null) {
                scanClient.initWaitFuture(filterExpression.getWaitBloomFuture());
            }

            scanExec = buildTableScanExec(scanClient, context);
        }
        registerRuntimeStat(scanExec, logicalView, context);

        return scanExec;
    }

    @Override
    protected void registerRuntimeStat(Executor scanExec, RelNode relNode, ExecutionContext context) {
        super.registerRuntimeStat(scanExec, relNode, context);
        if (context.getRuntimeStatistics() != null) {
            if (bSort && scanExec instanceof TableScanSortExec) {
                RuntimeStatHelper.registerStatForExec(sort, scanExec, context);
            }
        }
    }

    private Executor buildOSSTableScanExec(ExecutionContext context) {
        OSSTableScan ossTableScan = (OSSTableScan) logicalView;

        if (context.isEnableOrcDeletedScan()
            || context.isCciIncrementalCheck()
            || context.isReadCsvOnly()
            || context.isReadOrcOnly()
            || context.isReadSpecifiedColumnarFiles()) {
            // Special path for check cci consistency.
            // Normal oss read should not get here.
            Executor exec = new ColumnarSpecifiedScanExec(ossTableScan, context, dataTypeList);
            registerRuntimeStat(exec, logicalView, context);
            return exec;
        }

        // Use columnar table scan exec.
        if (context.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_SCAN_EXEC)) {
            ColumnarScanExec exec;

            String groupName = logicalView.getDigest();

            if (scanExecutor == null) {
                scanExecutor = ColumnarScanExec.SCAN_EXECUTOR.acquireGroup(groupName, parallelism);
            }

            if (columnarMemoryPermitManager == null) {
                float columnarMemoryPermitRatio =
                    context.getParamManager().getFloat(ConnectionParams.COLUMNAR_SCAN_MAXIMUM_MEMORY_PERMITS_RATIO);

                long columnarMemoryPermit = (long) (Runtime.getRuntime().maxMemory() * columnarMemoryPermitRatio);

                columnarMemoryPermitManager = new ColumnarMemoryPermitManagerImpl(columnarMemoryPermit);
            }

            // handle early stop manager.
            if (fragment.getEarlyStopManager() != null && fragment.getEarlyStopManager().isEnabled()) {
                exec = new ColumnarScanExec(ossTableScan, context, dataTypeList, fragment.getEarlyStopManager(),
                    scanExecutor, columnarMemoryPermitManager);
            } else {
                exec = new ColumnarScanExec(ossTableScan, context, dataTypeList,
                    scanExecutor, columnarMemoryPermitManager);
            }

            registerRuntimeStat(exec, logicalView, context);

            if (context.getParamManager().getBoolean(ConnectionParams.ENABLE_IN_VALUE_LIST_REWRITE)
                && rewriterParams == null) {
                rewriterParams = InExpressionParamRewriter.rewriterParams(ossTableScan, context);
            }
            exec.setRewriterParams(rewriterParams);

            if (fragment.getFragmentRFManager() != null) {
                FragmentRFManager fragmentRFManager = fragment.getFragmentRFManager();

                Map<FragmentRFItemKey, FragmentRFItem> allItems = fragmentRFManager.getAllItems();

                for (FragmentRFItemKey itemKey : allItems.keySet()) {
                    FragmentRFItem item = allItems.get(itemKey);

                    String probeColumnName = item.getProbeColumnName();

                    // inspect the filter channel according to registered RF columns.
                    List<String> fieldNames = ossTableScan.getOutputColumnOriginalNames();
                    int indexOfRowType = fieldNames.indexOf(probeColumnName);
                    Integer outProjectIndex = null;
                    if (indexOfRowType >= 0 && indexOfRowType < ossTableScan.getOrcNode().getOutProjects().size()) {
                        outProjectIndex = ossTableScan.getOrcNode().getOutProjects().get(indexOfRowType);
                    }

                    if (outProjectIndex == null) {
                        if (MPP_LOGGER.isDebugEnabled()) {
                            MPP_LOGGER.debug(
                                "Cannot find the filter channel according to registered RF columns "
                                    + ", fragmentRFItemKey = " + itemKey
                                    + ", for scan: " + logicalView);
                        }

                    } else {
                        // Mapping to input index in file.
                        final int inProjectIndex = ossTableScan.getOrcNode().getInProjects().get(outProjectIndex);
                        item.setSourceRefInFile(inProjectIndex);
                        item.setSourceFilterChannel(outProjectIndex);

                        // register column scan exec in all threads into fragment RF manager.
                        item.registerSource(exec);
                        itemKey.setValid(true);
                    }
                }

            }

            return exec;
        }

        AbstractOSSTableScanExec exec = AbstractOSSTableScanExec.create(ossTableScan, context, dataTypeList);

        OrcTableScan orcTableScan = ossTableScan.getOrcNode();
        if (!orcTableScan.getFilters().isEmpty()) {
            RexNode filterCondition = orcTableScan.getFilters().get(0);

            List<DataType<?>> inputTypes = orcTableScan.getInProjectsDataType();

            // binding vec expression
            RexNode root = VectorizedExpressionBuilder.rewriteRoot(filterCondition, true);
            InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
            root = root.accept(inputRefTypeChecker);
            Rex2VectorizedExpressionVisitor converter =
                new Rex2VectorizedExpressionVisitor(context, inputTypes.size());
            VectorizedExpression vectorizedExpression = root.accept(converter);
            List<DataType<?>> filterOutputTypes = converter.getOutputDataTypes();
            MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
                .addEmptySlots(inputTypes)
                .addEmptySlots(filterOutputTypes)
                .build();

            // prepare filter bitmap
            List<Integer> inputIndex = VectorizedExpressionUtils.getInputIndex(vectorizedExpression);
            int[] filterBitmap = new int[inputTypes.size() + filterOutputTypes.size()];
            for (int i : inputIndex) {
                filterBitmap[i] = 1;
            }

            exec.setPreAllocatedChunk(preAllocatedChunk);
            exec.setFilterInputTypes(inputTypes);
            exec.setFilterOutputTypes(filterOutputTypes);
            exec.setCondition(vectorizedExpression);
            exec.setFilterBitmap(filterBitmap);
            int[] outProject = new int[orcTableScan.getOutProjects().size()];
            for (int i = 0; i < orcTableScan.getOutProjects().size(); i++) {
                outProject[i] = orcTableScan.getOutProjects().get(i);
            }
            exec.setOutProject(outProject);
        }
        registerRuntimeStat(exec, logicalView, context);

        if (filterExpression != null) {
            exec.initWaitFuture(filterExpression.getWaitBloomFuture());
        }

        return exec;
    }

    private TableScanExec buildTableScanExec(TableScanClient scanClient, ExecutionContext context) {
        int stepSize = context.getParamManager().getInt(ConnectionParams.RESUME_SCAN_STEP_SIZE);
        if (enablePassiveResume && !context.isShareReadView()) {
            if (bSort) {
                return new ResumeTableScanSortExec(
                    logicalView, context, scanClient.incrementSourceExec(), maxRowCount, skip, fetch, spillerFactory,
                    stepSize, dataTypeList);
            } else {
                return new ResumeTableScanExec(logicalView, context, scanClient.incrementSourceExec(),
                    spillerFactory, stepSize, dataTypeList);

            }
        } else if (enableDrivingResume) {
            if (bSort) {
                return new DrivingStreamTableScanSortExec(
                    logicalView, context, scanClient.incrementSourceExec(), maxRowCount, skip, fetch, spillerFactory,
                    stepSize, dataTypeList);
            } else {
                return new DrivingStreamTableScanExec(logicalView, context, scanClient.incrementSourceExec(),
                    spillerFactory, stepSize, dataTypeList);

            }
        } else {
            if (bSort) {
                if (rangeScanMode != null) {
                    return new RangeScanSortExec(
                        logicalView, context, scanClient.incrementSourceExec(), maxRowCount, skip, fetch,
                        spillerFactory, dataTypeList);
                } else {
                    return new TableScanSortExec(
                        logicalView, context, scanClient.incrementSourceExec(), maxRowCount, skip, fetch,
                        spillerFactory, dataTypeList);
                }
            } else {
                return new TableScanExec(logicalView, context, scanClient.incrementSourceExec(),
                    maxRowCount,
                    spillerFactory, dataTypeList);
            }
        }
    }

    public void enablePassiveResumeSource() {
        this.enablePassiveResume = true;
        Preconditions.checkArgument(
            !(enablePassiveResume && enableDrivingResume), "Don't support stream scan in different mode");
    }

    public void enableDrivingResumeSource() {
        this.enableDrivingResume = true;
        Preconditions.checkArgument(
            !(enablePassiveResume && enableDrivingResume), "Don't support stream scan in different mode");
    }

    public TableScanExec createLookupScanExec(ExecutionContext context, boolean canShard,
                                              LookupPredicate predicate, List<LookupEquiJoinKey> allJoinKeys) {
        boolean useTransaction = ExecUtils.useExplicitTransaction(context);
        if (bSort) {
            TableScanExec scanExec = null;
            if (rangeScanMode != null) {
                this.scanClient = getRangeScanClient(context, useTransaction);
                scanExec = new LookupTableSortRangeScanExec(
                    logicalView, context, scanClient.incrementSourceExec(), maxRowCount, skip, fetch,
                    spillerFactory, dataTypeList);
            } else {
                boolean dynamicSort = RangeScanUtils.isDynamicMergeSort(logicalView);
                if (dynamicSort) {
                    Sort sort = (Sort) logicalView.getOptimizedPushedRelNodeForMetaQuery();
                    RelCollation collation = sort.getCollation();
                    List<RelFieldCollation> sortList = collation.getFieldCollations();
                    List<OrderByOption> orderByOptions = ExecUtils.convertFrom(sortList);
                    this.scanClient = new DynamicMergeSortTableScanClient(
                        context, meta, useTransaction, totalPrefetch, orderByOptions, logicalView, skip + fetch);
                } else {
                    this.scanClient = new MergeSortTableScanClient(context, meta, useTransaction, totalPrefetch);
                }
                scanExec = new LookupTableSortScanExec(
                    logicalView, context, scanClient.incrementSourceExec(), maxRowCount, skip, fetch, spillerFactory,
                    dataTypeList);
            }

            registerRuntimeStat(scanExec, logicalView, context);
            return scanExec;
        } else {
            boolean allowMultipleReadConn = ExecUtils.allowMultipleReadConns(context, logicalView);
            int prefetch = 1;
            if (allowMultipleReadConn) {
                prefetch = calculatePrefetchNum(counter.incrementAndGet(), parallelism);
                if (parallelism > 1 && totalPrefetch > 1) {
                    //由于bkaJoin有动态裁剪能力，会导致部分scan的分配split被裁剪为0，浪费prefetch的分配名额
                    prefetch = prefetch == 1 ? 2 : prefetch;
                }
            }
            TableScanClient scanClient = new TableScanClient(context, meta, useTransaction, prefetch);
            TableScanExec scanExec =
                new LookupTableScanExec(logicalView, context, scanClient.incrementSourceExec(), canShard,
                    spillerFactory,
                    predicate, allJoinKeys, dataTypeList);
            registerRuntimeStat(scanExec, logicalView, context);
            return scanExec;
        }
    }

    private int calculatePrefetchNum(int index, int parallelism) {
        Preconditions.checkArgument(index <= parallelism, "index must less than " + parallelism);
        if (parallelism >= totalPrefetch) {
            return 1;
        } else {
            if (index <= totalPrefetch % parallelism) {
                return totalPrefetch / parallelism + 1;
            } else {
                return totalPrefetch / parallelism;
            }
        }
    }

    public boolean isPushDownSort() {
        return bSort;
    }

    public LogicalView getLogicalView() {
        return logicalView;
    }

    public int getParallelism() {
        return this.parallelism;
    }

    private TableScanClient getRangeScanClient(ExecutionContext context, boolean useTransactionConnection) {
        if (rangeScanMode == RangeScanMode.SERIALIZE) {
            if (totalPrefetch != 1) {
                MPP_LOGGER.warn(String.format(
                    "prefetch under serialize mode should be 1, but was %s, and trace id is %s",
                    totalPrefetch, context.getTraceId()));
            }
            this.scanClient = new RangeScanClientBase(
                context, meta, useTransactionConnection, 1, rangeScanMode, logicalView);
        } else if (rangeScanMode == RangeScanMode.NORMAL) {
            this.scanClient =
                new NormalRangeScanClient(context, meta, useTransactionConnection, totalPrefetch,
                    RangeScanMode.NORMAL, logicalView);
        } else {
            this.scanClient =
                new AdaptiveRangeScanClient(context, meta, useTransactionConnection, totalPrefetch, logicalView);
        }
        return scanClient;
    }
}
