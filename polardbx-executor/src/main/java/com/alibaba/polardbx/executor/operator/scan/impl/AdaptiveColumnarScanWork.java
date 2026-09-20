package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.columnar.ColumnarScanMetrics;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.columnar.LazyBlock;
import com.alibaba.polardbx.executor.mpp.planner.EarlyStopManager;
import com.alibaba.polardbx.executor.mpp.planner.EarlyStopManagerImpl;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermit;
import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermitManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnarScanMonitor;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.executor.operator.scan.RowGroupIterator;
import com.alibaba.polardbx.executor.operator.scan.RowGroupReader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.roaringbitmap.RoaringBitmap;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Example of scan work
 */
public class AdaptiveColumnarScanWork extends AbstractMultiSubTaskScanWork {
    private static final Logger LOGGER = LoggerFactory.getLogger(EarlyStopManagerImpl.class);

    public static final int INITIAL_LIST_CAPACITY = 16;

    /**
     * Enum representing different types of fallback scenarios during execution.
     */
    enum FallBackType {
        /**
         * Full task filter IO fallback scenario.
         */
        FULL_TASK_FILTER_IO_FALLBACK,

        /**
         * Full task filter fallback scenario.
         */
        FULL_TASK_FILTER_FALLBACK,

        /**
         * Full task project IO fallback scenario.
         */
        FULL_TASK_PROJECT_IO_FALLBACK,

        /**
         * Full task project fallback scenario.
         */
        FULL_TASK_PROJECT_FALLBACK,

        /**
         * Sub-task filter IO fallback scenario.
         */
        SUB_TASK_FILTER_IO_FALLBACK,

        /**
         * Sub-task filter fallback scenario.
         */
        SUB_TASK_FILTER_FALLBACK,

        /**
         * Sub-task project IO fallback scenario.
         */
        SUB_TASK_PROJECT_IO_FALLBACK,

        /**
         * Sub-task project fallback scenario.
         */
        SUB_TASK_PROJECT_FALLBACK
    }

    public static final String FAILURE_IN_MIN_GRANULARITY =
        "fallback subtask failed too many times in minimum granularity";
    private final boolean activeLoading;
    private final int chunkLimit;
    private final boolean useInFlightBlockCache;
    private final boolean isWarmup;

    protected RowGroupIteratorBuilder rowGroupIteratorBuilder;
    protected RowGroupIterator<Block, ColumnStatistics> rgIterator;

    /**
     * The Fragment-level runtime filter manager.
     */
    private final FragmentRFManager fragmentRFManager;

    /**
     * Record the actual file column channel for a given item key.
     */
    private final Map<FragmentRFItemKey, Integer> rfFilterRefInFileMap;

    /**
     * Record the existence of bloom filter for each item keys.
     */
    private Map<FragmentRFItemKey, RFBloomFilter[]> rfBloomFilters;

    private final OperatorStatistics operatorStatistics;

    private volatile RFLazyEvaluator rfEvaluator;

    // for row-group iterator
    private Path filePath;
    private int stripeId;

    private int rowGroupCount;
    private BlockCacheManager<Block> blockCacheManager;

    // columnar memory permits
    private ColumnarMemoryPermitManager columnarMemoryPermitManager;

    // task granularity
    private int maxRowGroupCountPerTask;
    private boolean[] currentSubTaskRowGroupBitmap;
    private AtomicInteger taskSequenceNumber;

    /**
     * Granularity reduction scale(>1).
     */
    private int granularityReductionScale;

    /**
     * Thread limit reduction factor (<1.0).
     */
    private double threadLimitReductionFactor;

    // intermediate results
    private Map<Integer, List<Chunk>> chunksWithGroup;
    private boolean[] rowGroupIncluded;
    private AtomicReference<FallBackType> taskFallBack;
    private AtomicBoolean subTaskFailed;
    private int failureAtMinGranularity;
    private AtomicInteger lastProjectedRowGroupId;

    /**
     * no push-down filter, skip evaluation.
     * if evaluator is a constant expression, we should not skip the evaluation.
     */
    private boolean skipEvaluation;

    private final EarlyStopManager earlyStopManager;
    private List<OrderByOption> scanOrderByOptions;
    private List<Integer> orderByInProjects;

    // metrics
    private ColumnarScanMetrics columnarScanMetrics;
    private long totalScanRows;
    private long totalScanBytes;
    private long totalFilteredRows;

    private ResizableThreadGroup threadGroup;

    protected OperatorMemoryOwnerId memoryOwnerId;

    public AdaptiveColumnarScanWork(String workId,

                                    // for memory permit
                                    ColumnarMemoryPermitManager columnarMemoryPermitManager,

                                    // for task granularity control
                                    int granularityReductionScale,
                                    double threadLimitReductionFactor,

                                    RuntimeMetrics metrics,
                                    boolean enableMetrics,
                                    LazyEvaluator<Chunk, BitSet> lazyEvaluator,
                                    RowGroupIteratorBuilder rgIteratorBuilder,
                                    RoaringBitmap deletionBitmap,
                                    MorselColumnarSplit.ScanRange scanRange,
                                    List<Integer> inputRefsForFilter,
                                    List<Integer> inputRefsForProject,
                                    int partNum,
                                    int nodePartCount, boolean activeLoading, int chunkLimit,
                                    boolean useInFlightBlockCache,
                                    boolean isWarmup,
                                    FragmentRFManager fragmentRFManager,
                                    Map<FragmentRFItemKey, Integer> rfFilterRefInFileMap,

                                    // for top-k early stop
                                    EarlyStopManager earlyStopManager,
                                    List<OrderByOption> scanOrderByOptions,
                                    List<Integer> orderByInProjects,

                                    OperatorStatistics operatorStatistics,
                                    OSSColumnTransformer columnTransformer,
                                    ColumnarScanMetrics columnarScanMetrics,
                                    String logicalTable, String logicalSchema) {
        super(workId, metrics, enableMetrics, lazyEvaluator, deletionBitmap, scanRange, inputRefsForFilter,
            inputRefsForProject, partNum, nodePartCount, columnTransformer, logicalTable, logicalSchema);

        this.rowGroupIteratorBuilder = rgIteratorBuilder;

        this.activeLoading = activeLoading;
        this.chunkLimit = chunkLimit;
        this.useInFlightBlockCache = useInFlightBlockCache;
        this.isWarmup = isWarmup;
        this.fragmentRFManager = fragmentRFManager;
        this.rfFilterRefInFileMap = rfFilterRefInFileMap;
        this.rfBloomFilters = new HashMap<>();
        this.operatorStatistics = operatorStatistics;
        this.columnarScanMetrics = columnarScanMetrics;

        this.earlyStopManager = earlyStopManager;
        this.scanOrderByOptions = scanOrderByOptions;
        this.orderByInProjects = orderByInProjects;

        // columnar memory permits shared by all scan works
        this.columnarMemoryPermitManager = columnarMemoryPermitManager;
        this.granularityReductionScale = granularityReductionScale;
        this.threadLimitReductionFactor = threadLimitReductionFactor;

        // task granularity control
        this.taskFallBack = new AtomicReference<>(null);
        this.subTaskFailed = new AtomicBoolean(false);
        this.taskSequenceNumber = new AtomicInteger(0);
        this.failureAtMinGranularity = 0;
        this.lastProjectedRowGroupId = new AtomicInteger(-1);

        // Get and lazily evaluate chunks until row group count exceeds the threshold.
        // NOTE: the row-group and chunk must be in order.
        this.chunksWithGroup = new TreeMap<>();

        // Check should we use skip-eval mode.
        int filterColumns = inputRefsForFilter.size();
        this.skipEvaluation = lazyEvaluator == null && filterColumns == 0;

        if (this.fragmentRFManager != null && skipEvaluation) {
            // create a light-weight evaluator for runtime filter.
            this.rfEvaluator = new RFLazyEvaluator(fragmentRFManager, operatorStatistics, rfBloomFilters);
        }

        if (this.fragmentRFManager != null && lazyEvaluator != null) {
            // register RF to predicate.
            ((DefaultLazyEvaluator) lazyEvaluator).registerRF(fragmentRFManager, operatorStatistics, rfBloomFilters);
        }
    }

    @Override
    public long getMemoryUsage() {
        return columnarMemoryPermitManager.getCurrentUsage();
    }

    @Override
    public void invoke(ExecutorService executor, OperatorMemoryOwnerId memoryOwnerId) {
        this.memoryOwnerId = memoryOwnerId;
        threadGroup = (ResizableThreadGroup) executor;

        try {
            handleNextWork();
        } catch (Throwable e) {
            // has been handled in handleNextWork()
        }

    }

    @Override
    public void close(boolean force) {
        // The close method must be idempotent.
        if (rgIterator != null) {
            rgIterator.close(force);
        }

        if (ioStatus != null) {
            ioStatus.close();
        }
    }

    @Override
    protected void handleNextWork() throws Throwable {
        AdaptiveColumnarScanStatus fullTaskStatus =
            ColumnarScanMonitor.getInstance().create(internalWorkId, taskSequenceNumber, false);
        fullTaskStatus.setStartTime(System.currentTimeMillis());
        fullTaskStatus.setStatus(AdaptiveColumnarScanStatus.Status.QUEUE);

        FullTask fullTask = new FullTask(fullTaskStatus);
        threadGroup.submit(fullTask);
    }

    private void decreaseGranularity() {
        maxRowGroupCountPerTask = Math.max(1, maxRowGroupCountPerTask / granularityReductionScale);
    }

    private void decreaseThreadLimit() {
        threadGroup.decreaseConcurrency(threadLimitReductionFactor);
    }

    protected void doFallbackSubTask() {
        decreaseGranularity();

        // build sub row group iterators
        List<RowGroupIteratorImpl> subRowGroupIteratorList =
            rowGroupIteratorBuilder.buildSubRowGroupIterators(lastProjectedRowGroupId.get(), maxRowGroupCountPerTask);

        // submit first subtask asynchronously
        AdaptiveColumnarScanStatus subTaskStatus =
            ColumnarScanMonitor.getInstance().create(internalWorkId, taskSequenceNumber, true);
        subTaskStatus.setStartTime(System.currentTimeMillis());
        subTaskStatus.setStatus(AdaptiveColumnarScanStatus.Status.QUEUE);
        SubTask subTask = new SubTask(subRowGroupIteratorList, 0, subTaskStatus);

        // After this subtask completes, the next subtask will be executed automatically.
        threadGroup.submit(subTask);
    }

    private class FullTask implements Runnable {

        AdaptiveColumnarScanStatus fullTaskStatus;

        FullTask(AdaptiveColumnarScanStatus fullTaskStatus) {
            this.fullTaskStatus = fullTaskStatus;
        }

        @Override
        public void run() {
            try {
                fullTaskStatus.setStatus(AdaptiveColumnarScanStatus.Status.RUNNING);
                fullTaskStatus.setScheduleTime(System.currentTimeMillis());

                doFullTask(fullTaskStatus);

                fullTaskStatus.setEndTime(System.currentTimeMillis());
                fullTaskStatus.setStatus((taskFallBack.get() == null && fullTaskStatus.getErrorMsg() == null)
                    ? AdaptiveColumnarScanStatus.Status.SUCCESS
                    : AdaptiveColumnarScanStatus.Status.FAILED);
                if (taskFallBack.get() != null && fullTaskStatus.getErrorMsg() == null) {
                    fullTaskStatus.setErrorMsg(taskFallBack.get().name());
                }

                try {
                    if (taskFallBack.get() != null) {
                        taskFallBack.set(null);
                        doFallbackSubTask();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    // notify the exception to downstream
                    ioStatus.addException(e);
                }

            } catch (Throwable e) {
                e.printStackTrace();
                ioStatus.addException(e);
            }
        }
    }

    private class SubTask implements Runnable {
        List<RowGroupIteratorImpl> subRowGroupIteratorList;
        int subIteratorIndex;
        AdaptiveColumnarScanStatus subTaskStatus;

        SubTask(List<RowGroupIteratorImpl> subRowGroupIteratorList, int subIteratorIndex,
                AdaptiveColumnarScanStatus subTaskStatus) {
            this.subRowGroupIteratorList = subRowGroupIteratorList;
            this.subIteratorIndex = subIteratorIndex;
            this.subTaskStatus = subTaskStatus;
        }

        @Override
        public void run() {
            try {
                subTaskStatus.setStatus(AdaptiveColumnarScanStatus.Status.RUNNING);
                subTaskStatus.setScheduleTime(System.currentTimeMillis());

                doSubTask(subRowGroupIteratorList, subIteratorIndex, subTaskStatus);

                subTaskStatus.setEndTime(System.currentTimeMillis());
                subTaskStatus.setStatus(
                    (!subTaskFailed.get() && taskFallBack.get() == null && subTaskStatus.getErrorMsg() == null)
                        ? AdaptiveColumnarScanStatus.Status.SUCCESS
                        : AdaptiveColumnarScanStatus.Status.FAILED);
                if (taskFallBack.get() != null && subTaskStatus.getErrorMsg() == null) {
                    subTaskStatus.setErrorMsg(taskFallBack.get().name());
                }

                // if last subtask failed, don't invoke the next.
                if (subTaskFailed.get()) {
                    return;
                }

                if (taskFallBack.get() != null) {
                    if (threadGroup.limit() == 1 && maxRowGroupCountPerTask == 1) {
                        failureAtMinGranularity++;
                        if (failureAtMinGranularity > 3) {
                            subTaskStatus.setErrorMsg(FAILURE_IN_MIN_GRANULARITY);
                            throw GeneralUtil.nestedException(FAILURE_IN_MIN_GRANULARITY);
                        }
                    }

                    // shrink thread group limit.
                    taskFallBack.set(null);

                    if (maxRowGroupCountPerTask <= 1) {
                        // reach the minimum granularity, start to decrease thread limit.
                        decreaseThreadLimit();
                    } else {
                        decreaseGranularity();
                    }

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("workId = " + workId
                            + "reschedule subtask " + subIteratorIndex
                            + ", current thread limit = " + threadGroup.limit()
                            + ", current granularity = " + maxRowGroupCountPerTask
                        );
                    }

                    // split to fine-grained subtasks.
                    rowGroupIteratorBuilder.splitSubRowGroupIterators(
                        subRowGroupIteratorList, subIteratorIndex,
                        lastProjectedRowGroupId.get(), maxRowGroupCountPerTask
                    );

                    // reschedule the current subtask
                    AdaptiveColumnarScanStatus newSubTaskStatus =
                        ColumnarScanMonitor.getInstance().create(internalWorkId, taskSequenceNumber, true);
                    newSubTaskStatus.setStartTime(System.currentTimeMillis());
                    newSubTaskStatus.setStatus(AdaptiveColumnarScanStatus.Status.QUEUE);
                    threadGroup.submit(new SubTask(subRowGroupIteratorList, subIteratorIndex, newSubTaskStatus));
                    return;
                }

                // automatically submit next subtask.
                if (subIteratorIndex < subRowGroupIteratorList.size() - 1) {
                    AdaptiveColumnarScanStatus nextSubTaskStatus =
                        ColumnarScanMonitor.getInstance().create(internalWorkId, taskSequenceNumber, true);
                    nextSubTaskStatus.setStartTime(System.currentTimeMillis());
                    nextSubTaskStatus.setStatus(AdaptiveColumnarScanStatus.Status.QUEUE);
                    threadGroup.submit(new SubTask(subRowGroupIteratorList, subIteratorIndex + 1, nextSubTaskStatus));
                }
            } catch (Throwable e) {
                // notify the exception to downstream
                e.printStackTrace();
                ioStatus.addException(e);
            }

        }
    }

    private void doSubTask(List<RowGroupIteratorImpl> subRowGroupIteratorList, int subIteratorIndex,
                           AdaptiveColumnarScanStatus status) {
        final RowGroupIteratorImpl subRowGroupIterator = subRowGroupIteratorList.get(subIteratorIndex);

        this.rgIterator = subRowGroupIterator;
        this.filePath = rgIterator.filePath();
        this.stripeId = rgIterator.stripeId();
        this.currentSubTaskRowGroupBitmap = rgIterator.rgIncluded();
        this.rowGroupCount = currentSubTaskRowGroupBitmap.length;
        this.blockCacheManager = rgIterator.getCacheManager();
        status.setStartRowGroupId(rgIterator.getStartRowGroupId());
        status.setThreadLimit(threadGroup.limit());
        status.setGranularity(rgIterator.getEffectiveGroupCount());

        List<Optional<ColumnarMemoryPermit>> permits = new ArrayList<>();
        Map<Integer, Optional<ColumnarMemoryPermit>> permitOfFilterBlocksList = new HashMap<>();
        Map<Integer, Optional<ColumnarMemoryPermit>> permitOfProjectBlocksList = new HashMap<>();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(MessageFormat.format(
                "start sub rgIterator, filePath = {0}, stripeId = {1}, "
                    + "startRowGroupId = {2}, effectiveGroupCount = {3}",
                filePath, stripeId, rgIterator.getStartRowGroupId(), rgIterator.getEffectiveGroupCount()
            ));
        }
        try {
            // must open it here.
            this.rgIterator.open(memoryOwnerId);

            if (checkEarlyStop()) {
                return;
            }

            long ioMemoryUsage = getIOMemoryUsage(
                rgIterator, filePath, stripeId, inputRefsForFilter, blockCacheManager,
                currentSubTaskRowGroupBitmap, rgIterator.enableBlockCache()
            );

            // (buffer chunk mem + io buffer mem) / 2 * (maximum LZ4 compression ratio)
            long estimatedUncompressedBytes = 0;
            long totalColdStartMemoryUsage = ioMemoryUsage + estimatedUncompressedBytes;

            // Try to acquire permits of filter process.
            Optional<ColumnarMemoryPermit> permitOfFilter =
                columnarMemoryPermitManager.tryAcquire(totalColdStartMemoryUsage, false);
            permits.add(permitOfFilter);

            status.setAcquiredFilterIOPermits(totalColdStartMemoryUsage);
            status.setRemainingPermits(
                columnarMemoryPermitManager.getMaxPermits() - columnarMemoryPermitManager.getCurrentUsage());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "workId = " + workId.substring(workId.length() - 16)
                        + ", task granularity = " + rgIterator.getEffectiveGroupCount()
                        + ", totalColdStartMemoryUsage = " + totalColdStartMemoryUsage
                        + ", SUB TASK permitOfFilter = " + permitOfFilter);
            }

            // check fall back before filter task.
            taskFallBack.set(permitOfFilter.isPresent() ? null : FallBackType.SUB_TASK_FILTER_IO_FALLBACK);
            if (taskFallBack.get() != null) {
                return;
            }

            permitOfFilterBlocksList = doFilterTask(status);

            if (taskFallBack.get() != null) {
                return;
            }

            // no group is selected.
            if (rowGroupIncluded == null || allFalse(rowGroupIncluded)) {
                return;
            }

            long ioMemoryUsageInProject = getIOMemoryUsage(
                rgIterator, filePath, stripeId, inputRefsForProject,
                blockCacheManager, rowGroupIncluded, rgIterator.enableBlockCache()
            );
            // (buffer chunk mem + io buffer mem) / 2 * (maximum LZ4 compression ratio)
            long estimatedUncompressedBytesInProject = 0;
            long totalColdStartMemoryUsageInProject = ioMemoryUsageInProject + estimatedUncompressedBytesInProject;

            // Try to acquire permits of project process.
            Optional<ColumnarMemoryPermit> permitOfProject =
                columnarMemoryPermitManager.tryAcquire(totalColdStartMemoryUsageInProject, false);
            permits.add(permitOfProject);

            status.setAcquiredProjectIOPermits(totalColdStartMemoryUsageInProject);
            status.setRemainingPermits(
                columnarMemoryPermitManager.getMaxPermits() - columnarMemoryPermitManager.getCurrentUsage());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "workId = " + workId.substring(workId.length() - 16)
                        + ", task granularity = " + rgIterator.getEffectiveGroupCount()
                        + ", totalColdStartMemoryUsageInProject = " + totalColdStartMemoryUsageInProject
                        + ", SUB TASK permitOfProject = " + permitOfProject);
            }

            // check fall back before project task.
            taskFallBack.set(permitOfProject.isPresent() ? null : FallBackType.SUB_TASK_PROJECT_IO_FALLBACK);
            if (taskFallBack.get() != null) {
                return;
            }

            // it will partially fall back and the row group is not entirely projected.
            permitOfProjectBlocksList = doProjectTask(status, permitOfFilterBlocksList);

        } catch (Throwable e) {
            // notify the exception to downstream
            ioStatus.addException(e);
            subTaskFailed.set(true);

            status.setErrorMsg(e.getMessage());

            // log the error
            LOGGER.error("fail to execute sequential scan work: " + e);
        } finally {

            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(MessageFormat.format(
                        "finish sub rgIterator, filePath = {0}, stripeId = {1}, "
                            + "startRowGroupId = {2}, effectiveGroupCount = {3}",
                        filePath, stripeId, rgIterator.getStartRowGroupId(), rgIterator.getEffectiveGroupCount()
                    ));
                }

                // release memory and reference in row group iterator
                releaseMemory();

                // release permits anyway.
                for (Optional<ColumnarMemoryPermit> permit : permits) {
                    if (permit.isPresent()) {
                        permit.get().release();
                    }
                }

                if (taskFallBack.get() != null) {
                    for (Map.Entry<Integer, Optional<ColumnarMemoryPermit>> entry : permitOfFilterBlocksList.entrySet()) {
                        Integer rowGroupId = entry.getKey();
                        if (rowGroupId > lastProjectedRowGroupId.get()) {
                            // only if the row group is not projected, we need to release the filter permit.
                            if (entry.getValue().isPresent()) {
                                entry.getValue().get().release();
                            }
                        }
                    }
                }

                // for last subtask, notify finished
                if (taskFallBack.get() == null && subIteratorIndex == subRowGroupIteratorList.size() - 1) {
                    ioStatus.finish();
                }

                // clear intermediate results.
                this.chunksWithGroup.clear();
                if (rowGroupIncluded != null) {
                    Arrays.fill(rowGroupIncluded, false);
                }

                // update IO metrics
                if (operatorStatistics != null) {
                    totalScanBytes += operatorStatistics.getIOReadBytes();
                }

                // update columnar scan metrics
                if (columnarScanMetrics != null) {
                    columnarScanMetrics.updateTotalScanRows(totalScanRows, totalScanBytes, totalFilteredRows);
                }

                status.setTotalScanRows(totalScanRows);
                status.setTotalScanBytes(totalScanBytes);
                status.setTotalFilteredRows(totalFilteredRows);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                ioStatus.addException(throwable);
            }
        }
    }

    protected void doFullTask(AdaptiveColumnarScanStatus status) {
        this.rgIterator = rowGroupIteratorBuilder.build();
        this.filePath = rgIterator.filePath();
        this.stripeId = rgIterator.stripeId();
        this.currentSubTaskRowGroupBitmap = rgIterator.rgIncluded();
        this.rowGroupCount = currentSubTaskRowGroupBitmap.length;
        this.blockCacheManager = rgIterator.getCacheManager();
        this.maxRowGroupCountPerTask = rgIterator.getEffectiveGroupCount();

        status.setStartRowGroupId(rgIterator.getStartRowGroupId());
        status.setGranularity(rgIterator.getEffectiveGroupCount());
        status.setThreadLimit(threadGroup.limit());

        List<Optional<ColumnarMemoryPermit>> permits = new ArrayList<>();
        Map<Integer, Optional<ColumnarMemoryPermit>> permitOfFilterBlocksList = new HashMap<>();
        Map<Integer, Optional<ColumnarMemoryPermit>> permitOfProjectBlocksList = new HashMap<>();

        try {
            // must open it here.
            this.rgIterator.open(memoryOwnerId);

            if (checkEarlyStop()) {
                return;
            }

            long ioMemoryUsage = getIOMemoryUsage(
                rgIterator, filePath, stripeId, inputRefsForFilter, blockCacheManager,
                currentSubTaskRowGroupBitmap, rgIterator.enableBlockCache()
            );

            // (buffer chunk mem + io buffer mem) / 2 * (maximum LZ4 compression ratio)
            long estimatedUncompressedBytes = 0;
            long totalColdStartMemoryUsage = ioMemoryUsage + estimatedUncompressedBytes;

            // Try to acquire permits of filter process.
            Optional<ColumnarMemoryPermit> permitOfFilter =
                columnarMemoryPermitManager.tryAcquire(totalColdStartMemoryUsage, false);
            permits.add(permitOfFilter);

            status.setAcquiredFilterIOPermits(totalColdStartMemoryUsage);
            status.setRemainingPermits(
                columnarMemoryPermitManager.getMaxPermits() - columnarMemoryPermitManager.getCurrentUsage());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "workId = " + workId.substring(workId.length() - 16)
                        + ", task granularity = " + rgIterator.getEffectiveGroupCount()
                        + ", totalColdStartMemoryUsage = " + totalColdStartMemoryUsage
                        + ", FULL TASK permitOfFilter = " + permitOfFilter);
            }

            // check fall back before filter task.
            taskFallBack.set(permitOfFilter.isPresent() ? null : FallBackType.FULL_TASK_FILTER_IO_FALLBACK);
            if (taskFallBack.get() != null) {
                return;
            }

            permitOfFilterBlocksList = doFilterTask(status);

            if (taskFallBack.get() != null) {
                return;
            }

            // no group is selected.
            if (rowGroupIncluded == null || allFalse(rowGroupIncluded)) {
                return;
            }

            long ioMemoryUsageInProject = getIOMemoryUsage(
                rgIterator, filePath, stripeId, inputRefsForProject,
                blockCacheManager, rowGroupIncluded, rgIterator.enableBlockCache()
            );
            // (buffer chunk mem + io buffer mem) / 2 * (maximum LZ4 compression ratio)
            long estimatedUncompressedBytesInProject = 0;
            long totalColdStartMemoryUsageInProject = ioMemoryUsageInProject + estimatedUncompressedBytesInProject;

            // Try to acquire permits of project process.
            Optional<ColumnarMemoryPermit> permitOfProject =
                columnarMemoryPermitManager.tryAcquire(totalColdStartMemoryUsageInProject, false);
            permits.add(permitOfProject);

            status.setAcquiredProjectIOPermits(totalColdStartMemoryUsageInProject);
            status.setRemainingPermits(
                columnarMemoryPermitManager.getMaxPermits() - columnarMemoryPermitManager.getCurrentUsage());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "workId = " + workId.substring(workId.length() - 16)
                        + ", task granularity = " + rgIterator.getEffectiveGroupCount()
                        + ", totalColdStartMemoryUsageInProject = " + totalColdStartMemoryUsageInProject
                        + ", FULL TASK permitOfProject = " + permitOfProject);
            }

            // check fall back before project task.
            taskFallBack.set(permitOfProject.isPresent() ? null : FallBackType.FULL_TASK_PROJECT_IO_FALLBACK);
            if (taskFallBack.get() != null) {
                return;
            }

            // it will partially fall back and the row group is not entirely projected.
            permitOfProjectBlocksList = doProjectTask(status, permitOfFilterBlocksList);

        } catch (Throwable e) {
            // notify the exception to downstream
            ioStatus.addException(e);

            // add exception to status
            status.setErrorMsg(e.getMessage());

            // log the error
            LOGGER.error("fail to execute sequential scan work: " + e);
        } finally {
            try {
                // release memory and reference in row group iterator
                releaseMemory();

                // manage io status
                if (taskFallBack.get() == null) {
                    ioStatus.finish();
                }

                // release permits anyway.
                for (Optional<ColumnarMemoryPermit> permit : permits) {
                    if (permit.isPresent()) {
                        permit.get().release();
                    }
                }

                if (taskFallBack.get() != null) {
                    for (Map.Entry<Integer, Optional<ColumnarMemoryPermit>> entry : permitOfFilterBlocksList.entrySet()) {
                        Integer rowGroupId = entry.getKey();
                        if (rowGroupId > lastProjectedRowGroupId.get()) {
                            // only if the row group is not projected, we need to release the filter permit.
                            if (entry.getValue().isPresent()) {
                                entry.getValue().get().release();
                            }
                        }
                    }
                }

                // clear intermediate results.
                this.chunksWithGroup.clear();
                if (rowGroupIncluded != null) {
                    Arrays.fill(rowGroupIncluded, false);
                }

                // update IO metrics
                if (operatorStatistics != null) {
                    totalScanBytes += operatorStatistics.getIOReadBytes();
                }

                // update columnar scan metrics
                if (columnarScanMetrics != null) {
                    columnarScanMetrics.updateTotalScanRows(totalScanRows, totalScanBytes, totalFilteredRows);
                }

                status.setTotalScanRows(totalScanRows);
                status.setTotalScanBytes(totalScanBytes);
                status.setTotalFilteredRows(totalFilteredRows);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                ioStatus.addException(throwable);
            }

        }

    }

    private boolean checkEarlyStop() {
        if (earlyStopManager != null) {
            // Check early stop by preceding scan works.
            if (earlyStopManager.isEarlyStopRegistered(workId)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("early stop scan work: " + workId);
                }
                return true;
            }
        }
        return false;
    }

    private Map<Integer, Optional<ColumnarMemoryPermit>> doFilterTask(AdaptiveColumnarScanStatus status) {
        final List<Integer> selectedRowGroups = new ArrayList<>();

        // for filter column, initialize or open the related modules.
        int filterColumns = inputRefsForFilter.size();
        if (filterColumns == 1) {
            // use single IO if filter columns = 1
            final Integer filterColId = columnTransformer.getLocInOrc(chunkRefMap[inputRefsForFilter.get(0)]);
            if (filterColId != null) {
                singleIO(rgIterator, filterColId, filePath, stripeId,
                    currentSubTaskRowGroupBitmap, blockCacheManager, useInFlightBlockCache);
            }
        } else if (filterColumns > 1) {
            // use merging IO if filter columns > 1.
            mergeIO(rgIterator,
                filePath, stripeId,
                inputRefsForFilter,
                blockCacheManager,
                currentSubTaskRowGroupBitmap,
                useInFlightBlockCache);
        }

        if (earlyStopManager != null && scanOrderByOptions != null) {

            // Get file column ref from in project index.
            List<Integer> earlyStopChannels = new ArrayList<>();
            for (int i = 0; i < orderByInProjects.size(); i++) {
                int inProjectIndex = orderByInProjects.get(i);

                // avoid repeatedly reading the same column.
                if (inputRefsForFilter.indexOf(inProjectIndex) == -1) {
                    earlyStopChannels.add(inProjectIndex);
                }
            }
            mergeIO(rgIterator,
                filePath, stripeId,
                earlyStopChannels,
                blockCacheManager,
                currentSubTaskRowGroupBitmap,
                useInFlightBlockCache);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("earlyStopChannels = " + earlyStopChannels);
            }
        }

        long cachedFilterBlockMemory = 0L;
        Map<Integer, Optional<ColumnarMemoryPermit>> permitOfFilterBlocksList = new HashMap<>();
        boolean[] bitmap = new boolean[chunkLimit];
        boolean needEarlyStop = false;
        if (earlyStopManager != null && earlyStopManager.isDesc()) {
            rgIterator.reverse();
        }
        while (!isCanceled && rgIterator.hasNext()) {

            if (needEarlyStop) {
                // early stop
                break;
            }

            rgIterator.next();
            LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup = rgIterator.current();
            final int rowGroupId = logicalRowGroup.groupId();

            // The row group id in iterator must be valid.
            Preconditions.checkArgument(currentSubTaskRowGroupBitmap[rowGroupId]);

            // A flag for each row group to indicate that at least one block selected in row group.
            boolean rgSelected = false;

            Chunk chunk;
            RowGroupReader<Chunk> rowGroupReader = earlyStopManager != null && earlyStopManager.isDesc()
                ? logicalRowGroup.getReversedReader()
                : logicalRowGroup.getReader();
            while (!isCanceled && ((chunk = rowGroupReader.nextBatch()) != null)) {
                final int[] batchRange = rowGroupReader.batchRange();

                totalScanRows += batchRange[1];

                // Try top-k early stop.
                if (earlyStopManager != null && scanOrderByOptions != null) {

                    final int firstReadablePosition = earlyStopManager.isDesc()
                        ? lastReadablePosition(batchRange, deletionBitmap)
                        : firstReadablePosition(batchRange, deletionBitmap);
                    if (firstReadablePosition != -1) {
                        // Check the first readable position in chunk.
                        needEarlyStop = earlyStopManager.needEarlyStop(
                            chunk, scanOrderByOptions, firstReadablePosition, workId);

                        if (needEarlyStop) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("early stop register: " + workId);
                            }

                            earlyStopManager.registerEarlyStop(workId);
                            break;
                        }
                    }
                }

                // Check the runtime filter and invoke single IO before evaluation.
                if (fragmentRFManager != null) {
                    for (FragmentRFItemKey itemKey : rfFilterRefInFileMap.keySet()) {

                        if (rfBloomFilters.get(itemKey) == null) {

                            // try to fetch the runtime filter of given item key,
                            FragmentRFItem item = fragmentRFManager.getAllItems().get(itemKey);
                            RFBloomFilter[] bfArray = item.getRF();

                            if (bfArray != null) {
                                // Success to get the generated runtime filter.
                                rfBloomFilters.put(itemKey, bfArray);

                                // use single IO to open the filter column.
                                final int filterRefInFile = rfFilterRefInFileMap.get(itemKey);
                                final int filterColId = filterRefInFile + 1;
                                singleIO(rgIterator, filterColId, filePath, stripeId,
                                    currentSubTaskRowGroupBitmap, blockCacheManager, useInFlightBlockCache);
                            }
                        }
                    }
                }

                if (skipEvaluation) {

                    int[] preSelection = null;
                    if (rfEvaluator != null) {
                        // Filter the chunk use fragment RF.
                        int selectCount =
                            rfEvaluator.eval(chunk, batchRange[0], batchRange[1], deletionBitmap, bitmap);

                        preSelection = selectionOf(bitmap, selectCount);
                    } else {
                        preSelection = selectionOf(batchRange, deletionBitmap);
                    }

                    if (preSelection != null) {
                        // rebuild chunk according to project refs.
                        chunk = rebuildProject(chunk, preSelection, preSelection.length);

                        cachedFilterBlockMemory += chunk.getLoadedBlockMemoryUsage();
                    }

                    // no evaluation, just buffer the unloaded chunks.
                    List<Chunk> chunksInGroup =
                        chunksWithGroup.computeIfAbsent(rowGroupId, any -> new ArrayList<>(INITIAL_LIST_CAPACITY));
                    chunksInGroup.add(chunk);
                    rgSelected = true;
                    continue;
                }

                // Proactively load the filter-blocks
                for (int filterRef : inputRefsForFilter) {
                    int chunkIndex = chunkRefMap[filterRef];
                    Preconditions.checkArgument(chunkIndex >= 0);

                    // all blocks in chunk is lazy
                    // NOTE: explicit type cast?
                    LazyBlock filterBlock = (LazyBlock) chunk.getBlock(chunkIndex);

                    // Proactively invoke loading, or we can load it during evaluation.
                    filterBlock.load();
                }

                long start = System.nanoTime();

                // Get selection array of this range [n * 1000, (n+1) * 1000] in row group,
                // and then evaluate the filter.
                int selectCount =
                    lazyEvaluator.eval(chunk, batchRange[0], batchRange[1], deletionBitmap, bitmap);

                // check zeros in selection array,
                // and mark whether this row group is selected or not
                boolean hasSelectedPositions = selectCount > 0;

                rgSelected |= hasSelectedPositions;
                if (!hasSelectedPositions) {
                    // if all positions are filtered, skip to the next chunk.
                    if (enableMetrics) {
                        evaluationTimer.inc(System.nanoTime() - start);
                    }

                    // The created chunk and block-loader will be abandoned here.
                    releaseRef(chunk);
                    continue;
                }

                Chunk projectChunk;
                if (selectCount == chunk.getPositionCount()) {
                    projectChunk = rebuildProject(chunk);
                } else {
                    // hold this chunk util all row groups in scan work are handled.
                    int[] selection = selectionOf(bitmap, selectCount);
                    if (enableMetrics) {
                        evaluationTimer.inc(System.nanoTime() - start);
                    }

                    // rebuild chunk according to project refs.
                    projectChunk = rebuildProject(chunk, selection, selection.length);
                }

                cachedFilterBlockMemory += projectChunk.getLoadedBlockMemoryUsage();

                List<Chunk> chunksInGroup = chunksWithGroup.computeIfAbsent(rowGroupId, any -> new ArrayList<>(
                    INITIAL_LIST_CAPACITY));
                chunksInGroup.add(projectChunk);
            }
            // the chunk in this row group is run out, change to the next.
            if (rgSelected) {
                selectedRowGroups.add(rowGroupId);
            } else {
                // if row-group is not selected, remove all chunks of this row-group from buffer.
                List<Chunk> chunksInGroup;
                if ((chunksInGroup = chunksWithGroup.remove(rowGroupId)) != null) {
                    chunksInGroup.clear();
                }
            }

            // for each row-group, try to acquire permits.
            if (cachedFilterBlockMemory > 0) {
                Optional<ColumnarMemoryPermit> permitOfFilterBlocks =
                    columnarMemoryPermitManager.tryAcquire(cachedFilterBlockMemory, false);
                permitOfFilterBlocksList.put(rowGroupId, permitOfFilterBlocks);

                status.setAcquiredFilterPermits(status.getAcquiredFilterPermits() + cachedFilterBlockMemory);
                status.setRemainingPermits(
                    columnarMemoryPermitManager.getMaxPermits() - columnarMemoryPermitManager.getCurrentUsage()
                );

                if (!permitOfFilterBlocks.isPresent()) {
                    // fall back
                    FallBackType fallBackType = status.isSplitTask()
                        ? FallBackType.SUB_TASK_FILTER_FALLBACK
                        : FallBackType.FULL_TASK_FILTER_FALLBACK;
                    taskFallBack.set(fallBackType);
                    return permitOfFilterBlocksList;
                }
            }
        }

        if (isCanceled) {
            throw GeneralUtil.nestedException(MessageFormat.format("scan work: {0} is canceled", workId));
        }

        // There is no more chunk produced by this row group iterator.
        rgIterator.noMoreChunks();

        // no group is selected.
        if (selectedRowGroups.isEmpty()) {
            return permitOfFilterBlocksList;
        }

        // We collect all chunks in several row groups here,
        // so we can merge the IO range of different row group to improve I/O efficiency.
        rowGroupIncluded = toRowGroupBitmap(rowGroupCount, selectedRowGroups);
        return permitOfFilterBlocksList;
    }

    private Map<Integer, Optional<ColumnarMemoryPermit>> doProjectTask(
        AdaptiveColumnarScanStatus status,
        Map<Integer, Optional<ColumnarMemoryPermit>> permitOfFilterBlocksMap) {
        // collect all row-groups for mering IO tasks.
        mergeIO(rgIterator, filePath, stripeId, inputRefsForProject, blockCacheManager, rowGroupIncluded);

        final int blockIndexSize = inputRefsForProject.size();
        List<Chunk> chunkResults = new ArrayList();
        Map<Integer, Optional<ColumnarMemoryPermit>> permitOfProjectBlocksList = new HashMap<>();

        // load project columns
        if (isWarmup) {

            for (Map.Entry<Integer, List<Chunk>> entry : chunksWithGroup.entrySet()) {
                List<Chunk> chunksInGroup = entry.getValue();

                for (int blockIndex = 0; blockIndex < blockIndexSize; blockIndex++) {
                    for (Chunk bufferedChunk : chunksInGroup) {

                        // Just warmup with data loading.
                        if (activeLoading) {
                            Block[] blocks = bufferedChunk.getBlocks();

                            LazyBlock lazyBlock = (LazyBlock) blocks[blockIndex];
                            lazyBlock.warmup();
                        }

                    }
                }
            }

        } else {
            for (Map.Entry<Integer, List<Chunk>> entry : chunksWithGroup.entrySet()) {
                for (Chunk chunk : entry.getValue()) {
                    totalFilteredRows += chunk.getPositionCount();
                }
            }

            for (Map.Entry<Integer, List<Chunk>> entry : chunksWithGroup.entrySet()) {
                final Integer rowGroupId = entry.getKey();
                List<Chunk> chunksInGroup = entry.getValue();

                // Load Blocks by Column and Acquire Memory Permit.
                List<Optional<ColumnarMemoryPermit>> columnLevelPermitList = new ArrayList<>();
                chunkResults.clear();
                try {
                    for (int blockIndex = 0; blockIndex < blockIndexSize; blockIndex++) {
                        long projectColumnLevelMemory = 0L;

                        for (Chunk bufferedChunk : chunksInGroup) {

                            // The target chunk may be in lazy mode or changed to be in normal mode.
                            Chunk targetChunk = bufferedChunk;
                            if (activeLoading) {
                                Block[] blocks = bufferedChunk.getBlocks();

                                LazyBlock lazyBlock = (LazyBlock) blocks[blockIndex];
                                lazyBlock.load();

                                blocks[blockIndex] = lazyBlock.getLoaded();
                            }

                            if (blockIndex == 0) {
                                targetChunk.setPartIndex(partNum);
                                targetChunk.setPartCount(nodePartCount);
                                chunkResults.add(targetChunk);
                            }

                        }

                        // Acquire memory permit of column for all chunks in this row-group
                        if (projectBlockIndexBitmap[blockIndex]) {
                            for (Chunk bufferedChunk : chunksInGroup) {
                                projectColumnLevelMemory += bufferedChunk.getBlock(blockIndex).getMemoryUsage();
                            }

                            Optional<ColumnarMemoryPermit> columnLevelPermit =
                                columnarMemoryPermitManager.tryAcquire(projectColumnLevelMemory, false);
                            columnLevelPermitList.add(columnLevelPermit);

                            status.setAcquiredProjectPermits(
                                status.getAcquiredProjectPermits() + projectColumnLevelMemory);
                            status.setRemainingPermits(
                                columnarMemoryPermitManager.getMaxPermits()
                                    - columnarMemoryPermitManager.getCurrentUsage()
                            );

                            if (!columnLevelPermit.isPresent()) {
                                // fall back, and outer method will rebuild subtask from last project row-group
                                // by parameter lastProjectedRowGroupId
                                FallBackType fallBackType = status.isSplitTask()
                                    ? FallBackType.SUB_TASK_PROJECT_FALLBACK
                                    : FallBackType.FULL_TASK_PROJECT_FALLBACK;
                                taskFallBack.set(fallBackType);
                                return permitOfProjectBlocksList;
                            }
                        }
                    }
                } finally {
                    // Release column level permits here
                    if (taskFallBack.get() != null) {
                        chunkResults.clear();
                        for (Optional<ColumnarMemoryPermit> columnLevelPermit : columnLevelPermitList) {
                            if (columnLevelPermit.isPresent()) {
                                columnLevelPermit.get().release();
                            }
                        }
                    }
                }

                Optional<ColumnarMemoryPermit> permitOfProjectBlocks =
                    ColumnarMemoryPermitImpl.merge(columnLevelPermitList);

                // get permit of filter blocks
                Optional<ColumnarMemoryPermit> permitOfFilterBlocks = permitOfFilterBlocksMap.get(rowGroupId);
                if (permitOfFilterBlocks == null) {
                    permitOfFilterBlocks = Optional.empty();
                }

                // build evictable for row-group
                final Optional<ColumnarMemoryPermit> evictableProjectPermit = permitOfProjectBlocks;
                final Optional<ColumnarMemoryPermit> evictableFilterPermit = permitOfFilterBlocks;
                Runnable evictable = () -> {
                    if (evictableProjectPermit.isPresent()) {
                        evictableProjectPermit.get().release();
                    }

                    if (evictableFilterPermit.isPresent()) {
                        evictableFilterPermit.get().release();
                    }
                };

                // update last project row-group
                lastProjectedRowGroupId.set(rowGroupId);

                // add result to io status
                ioStatus.addResult(rowGroupId, evictable, chunkResults);
            }
        }

        return permitOfProjectBlocksList;
    }

    private void releaseMemory() {
        if (activeLoading) {
            // force columnar reader to close.
            forceClose(inputRefsForFilter);
            forceClose(inputRefsForProject);

            // Clear the path to GC root.
            // when using active loading, the row group iterator will not be accessed anymore.
            rgIterator = null;
        }
    }

    private void forceClose(List<Integer> inputRefs) {
        for (int i = 0; i < inputRefs.size(); i++) {
            Integer columnId = columnTransformer.getLocInOrc(chunkRefMap[inputRefs.get(i)]);
            if (columnId == null) {
                continue;
            }
            ColumnReader columnReader = rgIterator.getColumnReader(columnId);
            if (columnReader != null) {
                columnReader.close();
            }
        }
    }

    private static boolean allFalse(boolean[] rowGroupIncluded) {
        if (rowGroupIncluded != null) {
            for (boolean b : rowGroupIncluded) {
                if (b) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

}
