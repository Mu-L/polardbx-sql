package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.orc.PreheatFileMeta;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class RowGroupIteratorBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger("mpp_log");
    /**
     * Stripe id of this row-group iterator.
     */
    protected final int stripeId;
    /**
     * The effective row-group count starting from the given startRowGroupId.
     */
    private final int effectiveGroupCount;
    /**
     * The starting row-group id from which the iterator enumerate the logical row-groups.
     */
    private final int startRowGroupId;
    /**
     * The bitmap of selected row-groups, shared by all scan-works in one split.
     * The length of bitmap rowGroupIncluded is equal to count of row-groups in stripe.
     */
    private final boolean[] rowGroupIncluded;
    /**
     * The column ids of primary keys in the file.
     * It may be null.
     */
    private final int[] primaryKeyColIds;
    // parameters for IO processing
    private final ExecutorService ioExecutor;
    private final FileSystem fileSystem;
    private final Configuration configuration;
    protected final Path filePath;
    // for compression
    private final int compressionSize;
    private final CompressionKind compressionKind;
    // preheated meta of this stripe
    private final PreheatFileMeta preheatFileMeta;
    // context for stripe parser
    private final StripeInformation stripeInformation;
    private final long startRowOfStripe;
    protected final TypeDescription fileSchema;
    private final OrcFile.WriterVersion version;
    private final ReaderEncryption encryption;
    protected final OrcProto.ColumnEncoding[] encodings;
    private final boolean ignoreNonUtf8BloomFilter;
    private final long maxBufferSize;
    private final int indexStride;
    protected final boolean[] columnIncluded;
    protected final int chunkLimit;
    protected final BlockCacheManager<Block> blockCacheManager;
    protected final OSSColumnTransformer ossColumnTransformer;
    protected final ExecutionContext context;
    private final boolean enableMetrics;
    private final boolean enableDecimal64;
    private final int maxDiskRangeChunkLimit;
    private final long maxMergeDistance;
    private final boolean enableBlockCache;
    private final MemoryAllocatorCtx memoryAllocatorCtx;
    private final OperatorStatistics operatorStatistics;
    /**
     * Metrics in scan-work level.
     */
    protected RuntimeMetrics metrics;

    public RowGroupIteratorBuilder(
        RuntimeMetrics metrics,

        // selected stripe and row-groups
        int stripeId, int startRowGroupId, int effectiveGroupCount, boolean[] rowGroupIncluded,
        // for primary key
        int[] primaryKeyColIds,
        // to execute the io task
        ExecutorService ioExecutor,
        FileSystem fileSystem, Configuration configuration, Path filePath,
        // for compression
        int compressionSize, CompressionKind compressionKind,
        // preheated meta of this stripe
        PreheatFileMeta preheatFileMeta,
        // context for stripe parser
        StripeInformation stripeInformation, long startRowOfStripe,
        TypeDescription fileSchema, OrcFile.WriterVersion version,
        ReaderEncryption encryption, OrcProto.ColumnEncoding[] encodings, boolean ignoreNonUtf8BloomFilter,
        long maxBufferSize, int maxDiskRangeChunkLimit, long maxMergeDistance, int chunkLimit,
        BlockCacheManager<Block> blockCacheManager,
        OSSColumnTransformer ossColumnTransformer,
        ExecutionContext context, boolean[] columnIncluded, int indexStride,
        // chunk size config
        // global block cache manager
        boolean enableDecimal64, MemoryAllocatorCtx memoryAllocatorCtx,
        OperatorStatistics operatorStatistics) {
        this.metrics = metrics;
        this.stripeId = stripeId;
        this.startRowGroupId = startRowGroupId;
        this.effectiveGroupCount = effectiveGroupCount;
        this.rowGroupIncluded = rowGroupIncluded;
        this.primaryKeyColIds = primaryKeyColIds;
        this.ioExecutor = ioExecutor;
        this.fileSystem = fileSystem;
        this.configuration = configuration;
        this.filePath = filePath;
        this.compressionSize = compressionSize;
        this.compressionKind = compressionKind;
        this.preheatFileMeta = preheatFileMeta;
        this.stripeInformation = stripeInformation;
        this.startRowOfStripe = startRowOfStripe;
        this.fileSchema = fileSchema;
        this.version = version;
        this.encryption = encryption;
        this.encodings = encodings;
        this.ignoreNonUtf8BloomFilter = ignoreNonUtf8BloomFilter;
        this.maxBufferSize = maxBufferSize;
        this.maxDiskRangeChunkLimit = maxDiskRangeChunkLimit;
        this.maxMergeDistance = maxMergeDistance;
        this.indexStride = indexStride;
        this.columnIncluded = columnIncluded;
        this.chunkLimit = chunkLimit;
        this.blockCacheManager = blockCacheManager;
        this.ossColumnTransformer = ossColumnTransformer;
        this.context = context;
        this.enableMetrics = context.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_METRICS);
        this.enableDecimal64 = enableDecimal64;
        this.enableBlockCache = context.getParamManager().getBoolean(ConnectionParams.ENABLE_BLOCK_CACHE);
        this.memoryAllocatorCtx = memoryAllocatorCtx;
        this.operatorStatistics = operatorStatistics;
    }

    public RowGroupIteratorImpl build() {
        return doCreateRowGroupIterator(startRowGroupId, effectiveGroupCount, rowGroupIncluded);
    }

    // split row-group iterators to fine-grained row-group iterators
    public void splitSubRowGroupIterators(
        final List<RowGroupIteratorImpl> subIteratorList,
        final int startSubIteratorIndex,
        final int lastFinishedRowGroupId,
        final int maxRowGroupCountPerTask) {

        Preconditions.checkArgument(startSubIteratorIndex < subIteratorList.size());

        int currentSubIteratorIndex = startSubIteratorIndex;

        RowGroupIteratorImpl startSubRowGroupIterator = subIteratorList.get(startSubIteratorIndex);
        final int subRowGroupIteratorStartRowGroupId = startSubRowGroupIterator.getStartRowGroupId();

        Preconditions.checkArgument(lastFinishedRowGroupId + 1 >= subRowGroupIteratorStartRowGroupId,
            String.format("lastFinishedRowGroupId = %s, subRowGroupIteratorStartRowGroupId = %s",
                lastFinishedRowGroupId, subRowGroupIteratorStartRowGroupId));

        final int rowGroupCount = rowGroupIncluded.length;

        for (int currentIndexInRowGroupArray = lastFinishedRowGroupId + 1;
             currentIndexInRowGroupArray < rowGroupCount; ) {
            final int startRowGroupIdInCurrentTask = currentIndexInRowGroupArray;

            // initialize row-group bitmap of current task.
            boolean[] currentSubTaskRowGroupBitmap = new boolean[rowGroupCount];
            Arrays.fill(currentSubTaskRowGroupBitmap, false);

            // mark row-groups in current task.
            int groupCountInTask = 0;
            for (; groupCountInTask < maxRowGroupCountPerTask && currentIndexInRowGroupArray < rowGroupCount;
                 currentIndexInRowGroupArray++) {
                if (rowGroupIncluded[currentIndexInRowGroupArray]) {
                    currentSubTaskRowGroupBitmap[currentIndexInRowGroupArray] = true;
                    groupCountInTask++;
                }
            }

            RowGroupIteratorImpl subRowGroupIterator =
                doCreateRowGroupIterator(startRowGroupIdInCurrentTask, groupCountInTask, currentSubTaskRowGroupBitmap);

            // set or add.
            if (currentSubIteratorIndex < subIteratorList.size()) {
                subIteratorList.set(currentSubIteratorIndex, subRowGroupIterator);
            } else {
                subIteratorList.add(subRowGroupIterator);
            }
            currentSubIteratorIndex++;
        }

    }

    // append new sub row-group iterators
    public List<RowGroupIteratorImpl> buildSubRowGroupIterators(final int lastFinishedRowGroupId,
                                                                final int maxRowGroupCountPerTask) {

        Preconditions.checkArgument(lastFinishedRowGroupId + 1 < rowGroupIncluded.length);

        List<RowGroupIteratorImpl> subRowGroupIterators = new ArrayList<>();
        final int rowGroupCount = rowGroupIncluded.length;

        for (int currentIndexInRowGroupArray = lastFinishedRowGroupId + 1;
             currentIndexInRowGroupArray < rowGroupCount; ) {
            final int startRowGroupIdInCurrentTask = currentIndexInRowGroupArray;

            // initialize row-group bitmap of current task.
            boolean[] currentSubTaskRowGroupBitmap = new boolean[rowGroupCount];
            Arrays.fill(currentSubTaskRowGroupBitmap, false);

            // mark row-groups in current task.
            int groupCountInTask = 0;
            for (; groupCountInTask < maxRowGroupCountPerTask && currentIndexInRowGroupArray < rowGroupCount;
                 currentIndexInRowGroupArray++) {
                if (rowGroupIncluded[currentIndexInRowGroupArray]) {
                    currentSubTaskRowGroupBitmap[currentIndexInRowGroupArray] = true;
                    groupCountInTask++;
                }
            }

            RowGroupIteratorImpl subRowGroupIterator =
                doCreateRowGroupIterator(startRowGroupIdInCurrentTask, groupCountInTask, currentSubTaskRowGroupBitmap);
            subRowGroupIterators.add(subRowGroupIterator);
        }
        return subRowGroupIterators;
    }

    protected RowGroupIteratorImpl doCreateRowGroupIterator(
        int currentStartRowGroupId, int currentEffectiveGroupCount, boolean[] currentRowGroupBitmap) {
        return new RowGroupIteratorImpl(
            metrics,

            // The range of this row-group iterator.
            stripeId,
            currentStartRowGroupId,
            currentEffectiveGroupCount,
            currentRowGroupBitmap,

            // primary key col ids.
            primaryKeyColIds,

            // parameters for IO task.
            ioExecutor,
            fileSystem,
            configuration,
            filePath,

            // for compression
            compressionSize,
            compressionKind,
            preheatFileMeta,

            // for stripe-level parser
            stripeInformation,
            startRowOfStripe,
            fileSchema,
            version,
            encryption,
            encodings,
            ignoreNonUtf8BloomFilter,
            maxBufferSize,
            maxDiskRangeChunkLimit,
            maxMergeDistance,
            chunkLimit,

            // for cache
            blockCacheManager,

            // for column-mapping
            ossColumnTransformer,

            context,
            columnIncluded,
            indexStride,
            enableDecimal64,
            memoryAllocatorCtx,
            operatorStatistics);
    }
}
