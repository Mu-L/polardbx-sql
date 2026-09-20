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

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.BlockingFuture;
import com.alibaba.polardbx.common.BlockingReason;
import com.alibaba.polardbx.common.columnar.VersionStorageStatistics;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.orc.PreheatFileMeta;
import com.alibaba.polardbx.common.orc.PreheatMetaManager;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.pruning.ColumnarPruneManager;
import com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruner;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.operator.scan.ScanPreProcessor;
import com.alibaba.polardbx.gms.engine.OssGeneralCacheOverrideFileSystem;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.OrcTail;
import org.roaringbitmap.RoaringBitmap;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils.transformRexToIndexMergeTree;

/**
 * A mocked implementation of ScanPreProcessor that can generate
 * file preheat meta, deletion bitmap and pruning result (all selected).
 */
public class DefaultScanPreProcessor implements ScanPreProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultScanPreProcessor.class);

    /**
     * File path participated in preprocessor.
     */
    protected final List<Path> filePaths;

    /**
     * A shared configuration object to avoid initialization of large parameter list.
     */
    private final Configuration configuration;

    /**
     * The filesystem storing the files in file path list.
     */
    protected final FileSystem fileSystem;

    /**
     * Per-statement GeneralCache override extracted from {@link #fileSystem} when it is a
     * {@link OssGeneralCacheOverrideFileSystem}. Null means the current statement did not
     * set the HINT and dynamic config should be used.
     */
    protected final Boolean generalCacheOverride;

    /**
     * To enable index pruning.
     */
    protected final boolean enableIndexPruning;

    /**
     * To enable oss compatible.
     */
    protected final boolean enableOssCompatible;

    protected final String schemaName;
    protected final String logicalTableName;

    /**
     * The column meta list from query plan.
     */
    protected final List<ColumnMeta> columns;

    /**
     * The pushed-down predicate.
     */
    protected final List<RexNode> rexList;

    /**
     * The ratio that row-groups will be selected in a stripe.
     */
    private final double groupsRatio;

    /**
     * The ratio that row positions in file will be marked.
     */
    private final double deletionRatio;

    /**
     * The checkpoint tso in columnar mode.
     * It will be null if in archive mode.
     */
    protected final Long tso;

    /**
     * The columnar manager will be null if in archive mode.
     */
    protected final ColumnarManager columnarManager;

    /**
     * Field id of each column for columnar index
     */
    private final List<Long> columnFieldIdList;

    protected final boolean useParallelPreheatFileMeta;

    /**
     * The future will be null if preparation has not been invoked.
     */
    protected ListenableFuture<?> future;

    /**
     * The mocked pruning results that mapping from file path to stride + group info.
     */
    protected Map<Path, SortedMap<Integer, boolean[]>> rowGroupMatrix;

    /**
     * Mapping from file path to deletion bitmap.
     */
    protected Map<Path, RoaringBitmap> deletions;

    /**
     * Store the throwable info generated during preparation.
     */
    protected Throwable throwable;

    protected IndexPruneContext indexPruneContext;

    protected List<OrderByOption> sortKeys;

    protected VersionStorageStatistics versionStorageStatistics;

    protected ListenableFuture<?> preheatCloseFuture;

    public DefaultScanPreProcessor(Configuration configuration,
                                   FileSystem fileSystem,

                                   // for pruning
                                   String schemaName,
                                   String logicalTableName,
                                   boolean enableIndexPruning,
                                   boolean enableOssCompatible,
                                   List<ColumnMeta> columns,
                                   List<RexNode> rexList,
                                   Map<Integer, ParameterContext> params,

                                   // for mock
                                   double groupsRatio,
                                   double deletionRatio,

                                   // for columnar mode.
                                   ColumnarManager columnarManager,
                                   Long tso,
                                   List<Long> columnFieldIdList,
                                   List<OrderByOption> sortKeys,
                                   boolean useParallelPreheatFileMeta,
                                   VersionStorageStatistics versionStorageStatistics,
                                   ListenableFuture<?> preheatCloseFuture,
                                   ZoneId zoneId) {
        this.useParallelPreheatFileMeta = useParallelPreheatFileMeta;

        this.filePaths = new ArrayList<>();
        this.configuration = configuration;
        this.fileSystem = fileSystem;
        this.generalCacheOverride = (fileSystem instanceof OssGeneralCacheOverrideFileSystem)
            ? ((OssGeneralCacheOverrideFileSystem) fileSystem).isGeneralCacheEnabled()
            : null;

        // for pruning.
        this.enableIndexPruning = enableIndexPruning;
        this.enableOssCompatible = enableOssCompatible;
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        this.columns = columns;
        this.rexList = rexList;
        this.indexPruneContext = new IndexPruneContext();
        indexPruneContext.setParameters(new Parameters(params));
        indexPruneContext.setZoneId(zoneId);

        // for mock
        this.deletionRatio = deletionRatio;
        this.groupsRatio = groupsRatio;

        // for columnar mode
        this.columnarManager = columnarManager;
        // The checkpoint tso in columnar mode. It will be null if in archive mode.
        this.tso = tso;
        this.columnFieldIdList = columnFieldIdList;

        this.rowGroupMatrix = new HashMap<>();
        this.deletions = new HashMap<>();
        this.sortKeys = sortKeys;
        this.preheatCloseFuture = preheatCloseFuture;
        this.versionStorageStatistics = versionStorageStatistics;
    }

    @Override
    public void addFile(Path filePath) {
        this.filePaths.add(filePath);
    }

    @Override
    public ListenableFuture<?> prepare(ExecutorService executor, String traceId, ColumnarTracer tracer) {
        return prepare(executor, executor, traceId, tracer);
    }

    @Override
    public ListenableFuture<?> prepare(ExecutorService executor, ExecutorService ioExecutor, String traceId,
                                       ColumnarTracer tracer) {
        BlockingFuture<?> future = BlockingFuture.create(BlockingReason.WAIT_FOR_PRE_PREPROCESSOR);
        indexPruneContext.setPruneTracer(tracer);
        final long futureStartTime = System.nanoTime();

        // for statistics
        final long futureStartTimeInMillis = System.currentTimeMillis();

        // The ORC meta preheat tasks are issued in parallel to avoid executing multiple meta
        // fetch I/Os sequentially within a single columnar scan execution.
        Map<Path, CompletableFuture<PreheatFileMeta>> preheatFileMetaFutures = new HashMap<>();
        if (useParallelPreheatFileMeta) {
            for (int filePathIndex = 0; filePathIndex < filePaths.size(); filePathIndex++) {
                final Path filePath = filePaths.get(filePathIndex);
                if (filePath == null || filePath.getName() == null
                    || !filePath.getName().toUpperCase().endsWith(ColumnarFileType.ORC.name())) {
                    // only preheat orc file meta
                    continue;
                }

                CompletableFuture<PreheatFileMeta> fileMetaCompletableFuture =
                    CompletableFuture.supplyAsync(() -> {
                            try {
                                if (preheatCloseFuture.isDone()) {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug(
                                            "scan thread has existed, therefore pre-processor thread need to be cancelled in advance");
                                    }
                                    throw new RuntimeException("ColumnarScanExec has existed");
                                }
                                PreheatFileMeta preheatFileMeta =
                                    PreheatMetaManager.getInstance().get(filePath, fileSystem);
                                return preheatFileMeta;
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                        }, ioExecutor)
                        .exceptionally(ex -> {
                            // Exception will caught by future.get()
                            throw GeneralUtil.nestedException(ex);
                        });

                preheatFileMetaFutures.put(filePath, fileMetaCompletableFuture);
            }
        }

        // Is there a more elegant execution mode?
        Future<?> sequentialPreheatFuture = executor.submit(
            () -> {
                int stripeNum = 0;
                int rgNum = 0;
                int pruneRgLeft = 0;
                int orcFileNum = 0;

                // for metrics
                long maxPreheatRt = Long.MIN_VALUE;
                long minPreheatRt = Long.MAX_VALUE;
                long sumPreheatRt = 0;
                int preheatRtCount = 0;

                try {
                    if (versionStorageStatistics != null) {
                        VersionStorageStatistics.setThreadLocalStatistics(versionStorageStatistics);
                    }

                    // rex+pc -> distribution segment condition + indexes merge tree
                    ColumnPredicatePruningInf columnPredicate =
                        transformRexToIndexMergeTree(rexList, indexPruneContext);
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "column index prune " + schemaName + "," + logicalTableName + "," + PruneUtils.display(
                                columnPredicate, columns, indexPruneContext));
                    }

                    for (int filePathIndex = 0; filePathIndex < filePaths.size(); filePathIndex++) {
                        Path filePath = filePaths.get(filePathIndex);
                        boolean needGenerateDeletion = true;
                        //if query is killed, we need to cancel in advance
                        if (preheatCloseFuture.isDone()) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(
                                    "scan thread has existed, therefore pre-processor thread need to be cancelled in advance");
                            }
                            throw new RuntimeException("ColumnarScanExec has existed");
                        }
                        // only preheat orc file meta
                        if (filePath.getName().toUpperCase().endsWith(ColumnarFileType.ORC.name())) {
                            orcFileNum++;

                            if (enableIndexPruning && columnPredicate != null && tso != null
                                && columnFieldIdList != null) {
                                // prune the row-groups.
                                long loadIndexStart = System.nanoTime();
                                // TODO support multi columns for sort key
                                Map<Long, Integer> orcIndexesMap =
                                    columnarManager.getPhysicalColumnIndexes(filePath.getName());

                                // mapping of physical column
                                List<Integer> orcIndexes = columnFieldIdList.stream()
                                    .map(field -> {
                                        Integer orcIndex = orcIndexesMap.get(field);
                                        return orcIndex == null ? null : orcIndex + 1;
                                    })
                                    .collect(Collectors.toList());

                                // sort key columns
                                Set<Integer> sortKeyColumns = sortKeys.stream()
                                    .map(OrderByOption::getIndex)
                                    .map(orcIndexes::get).collect(Collectors.toSet());

                                // preheat all meta from orc file.
                                PreheatFileMeta preheat;
                                if (useParallelPreheatFileMeta) {
                                    CompletableFuture<PreheatFileMeta> preheatFileMetaFuture =
                                        preheatFileMetaFutures.get(filePath);

                                    if (preheatFileMetaFuture != null) {
                                        preheat = preheatFileMetaFuture.get(); // wait

                                        // metrics
                                        long rt = System.currentTimeMillis() - futureStartTimeInMillis;
                                        preheatRtCount++;
                                        maxPreheatRt = Math.max(maxPreheatRt, rt);
                                        minPreheatRt = Math.min(minPreheatRt, rt);
                                        sumPreheatRt += rt;

                                    } else {
                                        long startTime = System.currentTimeMillis();
                                        try {
                                            preheat = PreheatMetaManager.getInstance()
                                                .get(filePath, fileSystem, sortKeyColumns); // invoke IO
                                        } finally {
                                            // metrics
                                            long rt = System.currentTimeMillis() - startTime;
                                            preheatRtCount++;
                                            maxPreheatRt = Math.max(maxPreheatRt, rt);
                                            minPreheatRt = Math.min(minPreheatRt, rt);
                                            sumPreheatRt += rt;
                                        }
                                    }
                                } else {
                                    long startTime = System.currentTimeMillis();

                                    try {
                                        preheat = PreheatMetaManager.getInstance()
                                            .get(filePath, fileSystem, sortKeyColumns); // invoke IO
                                    } finally {
                                        // metrics
                                        long rt = System.currentTimeMillis() - startTime;
                                        preheatRtCount++;
                                        maxPreheatRt = Math.max(maxPreheatRt, rt);
                                        minPreheatRt = Math.min(minPreheatRt, rt);
                                        sumPreheatRt += rt;
                                    }
                                }

                                IndexPruner indexPruner =
                                    ColumnarPruneManager.getIndexPruner(
                                        filePath, preheat, columns, sortKeys,
                                        orcIndexes, enableOssCompatible
                                    );

                                if (tracer != null) {
                                    tracer.tracePruneInit(logicalTableName,
                                        PruneUtils.display(columnPredicate, columns, indexPruneContext),
                                        System.nanoTime() - loadIndexStart);
                                }
                                long indexPruneStart = System.nanoTime();
                                RoaringBitmap rr =
                                    indexPruner.prune(logicalTableName, columns, columnPredicate, indexPruneContext);
                                SortedMap<Integer, boolean[]> rs = indexPruner.pruneToSortMap(rr);
                                if (rr.isEmpty()) {
                                    needGenerateDeletion = false;
                                }
                                if (tracer != null) {
                                    tracer.tracePruneTime(logicalTableName,
                                        PruneUtils.display(columnPredicate, columns, indexPruneContext),
                                        System.nanoTime() - indexPruneStart);
                                }
                                stripeNum += indexPruner.getStripeRgNum().size();
                                rgNum += indexPruner.getRgNum();
                                pruneRgLeft += rr.getCardinality();
                                // prune stripe&row groups
                                rowGroupMatrix.put(filePath, rs);
                            } else {
                                // if pruning is disabled, mark all row-groups as selected.
                                generateFullMatrix(filePath);
                            }
                        }
                        // generate deletion according to deletion ratio and row count.
                        if (needGenerateDeletion) {
                            generateDeletion(filePath);
                        }
                    }
                    if (tracer != null && columnPredicate != null) {
                        tracer.tracePruneResult(logicalTableName,
                            PruneUtils.display(columnPredicate, columns, indexPruneContext),
                            orcFileNum,
                            stripeNum, rgNum, pruneRgLeft);
                    }
                    if (logger.isDebugEnabled() && columnPredicate != null) {
                        logger.debug(
                            "prune result: " + traceId + "," + logicalTableName + "," + filePaths.size() + ","
                                + stripeNum + "," + rgNum
                                + "," + pruneRgLeft);
                    }

                } catch (Throwable t) {
                    throwable = t;
                    future.complete(null);
                    return;
                } finally {
                    if (versionStorageStatistics != null) {
                        // for metrics
                        versionStorageStatistics.updatePreheatStatisticsInBatch(
                            minPreheatRt, maxPreheatRt, sumPreheatRt, preheatRtCount
                        );

                        VersionStorageStatistics.removeThreadLocalStatistics();
                    }
                }
                future.complete(null);
            }

        );

        if (preheatCloseFuture != null) {
            preheatCloseFuture.addListener(() -> sequentialPreheatFuture.cancel(true), MoreExecutors.directExecutor());
        }

        this.future = future;
        return future;
    }

    @Override
    public boolean isPrepared() {
        throwIfFailed();
        return throwable == null && future != null && future.isDone();
    }

    @Override
    public SortedMap<Integer, boolean[]> getPruningResult(Path filePath) {
        throwIfFailed();
        Preconditions.checkArgument(isPrepared());
        return rowGroupMatrix.get(filePath);
    }

    @Override
    public PreheatFileMeta getPreheated(Path filePath) {
        throwIfFailed();
        Preconditions.checkArgument(isPrepared());
        Set<Integer> sortKeyColumns = getSortKeyColumn(filePath);
        try {
            return PreheatMetaManager.getInstance().get(filePath, fileSystem, sortKeyColumns);
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public RoaringBitmap getDeletion(Path filePath) {
        throwIfFailed();
        Preconditions.checkArgument(isPrepared());
        return deletions.get(filePath);
    }

    @Override
    public void throwIfFailed() {
        if (throwable != null) {
            throw GeneralUtil.nestedException(throwable);
        }
    }

    protected RoaringBitmap generateDeletion(Path filePath) {
        RoaringBitmap bitmap;
        if (tso == null) {
            // in archive mode.
            bitmap = new RoaringBitmap();
        } else {
            // in columnar mode.
            bitmap = columnarManager.getDeleteBitMapOf(tso, filePath.getName(), generalCacheOverride);
        }
        deletions.put(filePath, bitmap);
        return bitmap;
    }

    protected void generateFullMatrix(Path filePath) throws Throwable {
        Set<Integer> sortKeyColumns = getSortKeyColumn(filePath);
        PreheatFileMeta preheatFileMeta = PreheatMetaManager.getInstance().get(filePath, fileSystem, sortKeyColumns);
        OrcTail orcTail = preheatFileMeta.getPreheatTail();

        int indexStride = orcTail.getFooter().getRowIndexStride();

        // but sorted by stripe id.
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        for (StripeInformation stripeInformation : orcTail.getStripes()) {
            int stripeId = (int) stripeInformation.getStripeId();
            int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);

            // build row-group by stripe row count and index stride
            // mark all groups as selected.
            boolean[] groupIncluded = new boolean[groupsInStripe];
            Arrays.fill(groupIncluded, true);

            matrix.put(stripeId, groupIncluded);
        }
        rowGroupMatrix.put(filePath, matrix);
    }

    protected Set<Integer> getSortKeyColumn(Path filePath) {
        if (tso == null) {
            return null;
        }

        Set<Integer> sortKeyColumns = new HashSet<>();

        Map<Long, Integer> orcIndexesMap = columnarManager.getPhysicalColumnIndexes(filePath.getName());

        if (sortKeys == null || sortKeys.isEmpty()) {
            throw new IllegalArgumentException("Sort keys cannot be null or empty");
        }

        for (int i = 0; i < sortKeys.size(); i++) {
            int firstSortKeyPosition = sortKeys.get(i).getIndex();
            if (firstSortKeyPosition < 0 || firstSortKeyPosition >= columnFieldIdList.size()) {
                throw new IndexOutOfBoundsException("Sort key position out of bounds");
            }

            Long fieldId = columnFieldIdList.get(firstSortKeyPosition);
            Integer orcIndex = orcIndexesMap.get(fieldId);
            if (orcIndex != null) {
                sortKeyColumns.add(orcIndex + 1);
            }
        }

        return sortKeyColumns;
    }

}
