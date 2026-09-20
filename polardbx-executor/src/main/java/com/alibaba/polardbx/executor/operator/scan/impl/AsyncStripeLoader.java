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

import com.alibaba.polardbx.common.collection.MemoryCountableArrayList;
import com.alibaba.polardbx.common.columnar.VersionStorageStatistics;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.memory.ORCMemoryCounterUtil;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.orc.FastColumnEncodingIndex;
import com.alibaba.polardbx.common.orc.FastPositionIndex;
import com.alibaba.polardbx.common.orc.FastPositionIndexPositionProviderBuilder;
import com.alibaba.polardbx.common.columnar.VersionStorageStatistics;
import com.alibaba.polardbx.common.orc.PreheatFileMeta;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.ORCMetricsWrapper;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileKeys;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileUnit;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.customized.ORCProfile;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.BufferChunkList;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.PositionProviderBuilder;
import org.apache.orc.impl.RecordReaderUtils;
import org.apache.orc.impl.StreamName;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.StreamInformation;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.alibaba.polardbx.executor.operator.scan.metrics.MetricsNameBuilder.columnMetricsKey;
import static com.alibaba.polardbx.executor.operator.scan.metrics.MetricsNameBuilder.columnsMetricsKey;
import static com.alibaba.polardbx.executor.operator.scan.metrics.MetricsNameBuilder.streamMetricsKey;
import static io.airlift.compress.lz4.Lz4RawCompressor.MAX_TABLE_SIZE;

public class AsyncStripeLoader implements StripeLoader {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(AsyncStripeLoader.class).instanceSize();

    private static final Logger LOGGER = LoggerFactory.getLogger("mpp_log");

    // Name of metrics
    public static final String ASYNC_STRIPE_LOADER_MEMORY = "AsyncStripeLoader.Memory";
    public static final String ASYNC_STRIPE_LOADER_TIMER = "AsyncStripeLoader.Timer";
    public static final String ASYNC_STRIPE_LOADER_BYTES_RANGE = "AsyncStripeLoader.BytesRange";

    // To sort the InStream with different stream name.
    private static final Comparator<StreamName> STREAM_NAME_COMPARATOR = (s1, s2) -> {
        if (s1.getColumn() != s2.getColumn()) {
            return s1.getColumn() - s2.getColumn();
        } else {
            return s1.getKind().name().compareTo(s2.getKind().name());
        }
    };

    // parameters for IO processing
    @FieldMemoryCounter(value = false)
    private final ExecutorService ioExecutor;

    @FieldMemoryCounter(value = false)
    private final FileSystem fileSystem;

    @FieldMemoryCounter(value = false)
    private final Configuration configuration;

    @FieldMemoryCounter(value = false)
    private final Path filePath;

    @FieldMemoryCounter(value = false)
    private final boolean[] columnIncluded;

    // for compression
    private final int compressionSize;

    @FieldMemoryCounter(value = false)
    private final CompressionKind compressionKind;

    // preheated meta of this stripe
    @FieldMemoryCounter(value = false)
    private final PreheatFileMeta preheatFileMeta;

    // context for stripe parser
    @FieldMemoryCounter(value = false)
    private final StripeInformation stripeInformation;

    @FieldMemoryCounter(value = false)
    private final TypeDescription fileSchema;

    @FieldMemoryCounter(value = false)
    private final OrcFile.WriterVersion version;

    @FieldMemoryCounter(value = false)
    private final ReaderEncryption encryption;

    @FieldMemoryCounter(value = false)
    private final OrcProto.ColumnEncoding[] encodings;
    private final boolean ignoreNonUtf8BloomFilter;
    private final long maxBufferSize;
    private final int maxDiskRangeChunkLimit;
    private final long maxMergeDistance;

    // need initialized
    @FieldMemoryCounter(value = false)
    private StripeContext stripeContext;
    @FieldMemoryCounter(value = false)
    private StreamManager streamManager;
    @FieldMemoryCounter(value = false)
    private InStream.StreamOptions streamOptions;

    // for memory usage count.
    private MemoryCountableArrayList<BufferChunkList> ioPlans;
    private long allocatedBytesForIOPlan = 0L;
    @FieldMemoryCounter(value = false)
    private RoaringBitmap objectBitmap;

    // register loading or loaded columns.
    // NODE: The Stripe-Loader is stateful, and a column can only be loaded once in one stripe.
    @FieldMemoryCounter(value = false)
    private ConcurrentHashMap<Integer, boolean[]> registerMap;

    // for metrics
    @FieldMemoryCounter(value = false)
    private final RuntimeMetrics metrics;
    private final boolean enableMetrics;
    private boolean isOpened;

    @FieldMemoryCounter(value = false)
    private Counter openingTimer;

    // for memory management.
    @FieldMemoryCounter(value = false)
    private final MemoryAllocatorCtx memoryAllocatorCtx;
    @FieldMemoryCounter(value = false)
    private final OperatorStatistics operatorStatistics;
    private AtomicLong totalAllocatedBytes;

    @FieldMemoryCounter(value = false)
    private Set<StreamName> releasedStreams;

    @FieldMemoryCounter(value = false)
    private OperatorMemoryOwnerId operatorMemoryOwnerId;

    @FieldMemoryCounter(value = false)
    private AtomicBoolean released;

    @FieldMemoryCounter(value = false)
    private VersionStorageStatistics versionStorageStatistics;

    @Override
    public long getMemoryUsage() {
        try {
            long memoryUsage = INSTANCE_SIZE;
            memoryUsage += FastMemoryCounter.sizeOf(ioPlans);

            for (int i = 0; i < ioPlans.size(); i++) {
                BufferChunkList ioPlan = ioPlans.get(i);
                memoryUsage += ORCMemoryCounterUtil.sizeOfBufferChunkList(ioPlan, objectBitmap);
            }

            memoryUsage += FastMemoryCounter.sizeOf(totalAllocatedBytes);
            return memoryUsage;
        } finally {
            objectBitmap.clear();
        }
    }

    @Override
    public void setOperatorMemoryOwnerId(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        this.operatorMemoryOwnerId = operatorMemoryOwnerId;
    }

    public AsyncStripeLoader(
        // for file io execution
        ExecutorService ioExecutor, FileSystem fileSystem,
        Configuration configuration, Path filePath, boolean[] columnIncluded,

        // for compression
        int compressionSize, CompressionKind compressionKind,

        // preheated meta of this stripe
        PreheatFileMeta preheatFileMeta,

        // context for stripe parser
        StripeInformation stripeInformation,
        TypeDescription fileSchema, OrcFile.WriterVersion version,
        ReaderEncryption encryption,
        OrcProto.ColumnEncoding[] encodings,
        boolean ignoreNonUtf8BloomFilter, long maxBufferSize,
        int maxDiskRangeChunkLimit, long maxMergeDistance,

        // for metrics
        RuntimeMetrics metrics,
        boolean enableMetrics, MemoryAllocatorCtx memoryAllocatorCtx,
        OperatorStatistics operatorStatistics) {
        this.maxDiskRangeChunkLimit = maxDiskRangeChunkLimit;
        this.maxMergeDistance = maxMergeDistance;
        this.enableMetrics = enableMetrics;
        this.memoryAllocatorCtx = memoryAllocatorCtx;
        this.operatorStatistics = operatorStatistics;
        // NOTE: the 0th column in array is tree-struct.
        Preconditions.checkArgument(columnIncluded != null
            && columnIncluded.length == fileSchema.getMaximumId() + 1);

        this.ioExecutor = ioExecutor;
        this.fileSystem = fileSystem;
        this.configuration = configuration;
        this.filePath = filePath;
        this.columnIncluded = columnIncluded;
        this.compressionSize = compressionSize;
        this.compressionKind = compressionKind;
        this.preheatFileMeta = preheatFileMeta;
        this.stripeInformation = stripeInformation;
        this.fileSchema = fileSchema;
        this.version = version;
        this.encryption = encryption;
        this.encodings = encodings;
        this.ignoreNonUtf8BloomFilter = ignoreNonUtf8BloomFilter;
        this.maxBufferSize = maxBufferSize;

        this.metrics = metrics;

        // internal state
        this.registerMap = new ConcurrentHashMap<>();
        this.isOpened = false;

        if (enableMetrics) {
            this.openingTimer = metrics.addCounter(
                ProfileKeys.ORC_STRIPE_LOADER_OPEN_TIMER.getName(),
                ASYNC_STRIPE_LOADER_TIMER,
                ProfileUnit.NANO_SECOND
            );
        }

        this.totalAllocatedBytes = new AtomicLong(0);
        this.releasedStreams = new HashSet<>();
        this.released = new AtomicBoolean(false);
        this.objectBitmap = new RoaringBitmap();
        this.ioPlans = new MemoryCountableArrayList<>();
    }

    @Override
    public void open() {
        long start = System.nanoTime();

        // Lz4Compressor.<init> allocates a 4KB table.
        MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId,
            VMSupport.align((int) SizeOf.sizeOfIntArray(MAX_TABLE_SIZE)));

        streamOptions = InStream.options()
            .withCodec(OrcCodecPool.getCodec(compressionKind))
            .withBufferSize(compressionSize);

        stripeContext = new StripeContext(
            stripeInformation, fileSchema, encryption, version, streamOptions, ignoreNonUtf8BloomFilter, maxBufferSize
        );

        if (preheatFileMeta.isUseBinaryMeta()) {
            int stripeIndex = (int) stripeInformation.getStripeId();

            // Get all stream information in this stripe.
            streamManager = StaticStripePlanner.parseStripe(
                stripeContext, columnIncluded, preheatFileMeta, stripeIndex
            );
        } else {
            OrcProto.StripeFooter footer =
                preheatFileMeta.findStripeFooterInNonBinaryMode(stripeInformation.getStripeId());

            // Get all stream information in this stripe.
            streamManager = StaticStripePlanner.parseStripe(
                stripeContext, columnIncluded, footer
            );
        }

        releasedStreams.addAll(streamManager.getStreams().keySet());

        isOpened = true;
        if (enableMetrics) {
            openingTimer.inc(System.nanoTime() - start);
        }
    }

    @Override
    public void setVersionStorageStatistics(VersionStorageStatistics versionStorageStatistics) {
        this.versionStorageStatistics = versionStorageStatistics;
    }

    @Override
    public CompletableFuture<Map<StreamName, InStream>> load(List<Integer> columnIds,
                                                             Map<Integer, boolean[]> rowGroupBitmaps,
                                                             Supplier<Boolean> controller) {
        Preconditions.checkArgument(isOpened, "The stripe loader has not already been opened");
        // Column-level parallel data loading is only suitable for columns that size > 2MB in one stripe.
        // In some cases, we need merge all columns in one IO task.

        if (rowGroupBitmaps != null && rowGroupBitmaps.values().stream().allMatch(AsyncStripeLoader::allFalse)) {
            // Directly return empty map to avoid opening file.
            return CompletableFuture.completedFuture(new HashMap<>());
        }

        final long stripeId = stripeInformation.getStripeId();
        PositionProviderBuilder orcIndex = preheatFileMeta.getPositionProviderBuilder(stripeId);

        // build selected columns bitmap
        boolean[] selectedColumns = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(selectedColumns, false);
        columnIds.forEach(col -> selectedColumns[col] = true);

        // Build IO plans for each column with different row group bitmaps
        // and merge them into one buffer-chunk-list.

        // Get the IO plan of all streams in this column.
        BufferChunkList ioPlan;

        if (orcIndex instanceof OrcIndex) {
            ioPlan = StaticStripePlanner.planGroupsInColumn(
                stripeContext,
                streamManager,
                streamOptions,
                (OrcIndex) orcIndex,
                rowGroupBitmaps,
                selectedColumns
            );
        } else {
            ioPlan = StaticStripePlanner.planGroupsInColumn(
                stripeContext,
                streamManager,
                streamOptions,
                (FastPositionIndexPositionProviderBuilder) orcIndex,
                rowGroupBitmaps,
                selectedColumns
            );
        }
        ioPlans.add(ioPlan);

        // check buffer chunk list
        long bytesInIOPlan = 0L;
        long bytesHitStream = 0L;
        for (BufferChunk node = ioPlan.get(); node != null; node = (BufferChunk) node.next) {
            bytesInIOPlan += node.getLength();
        }

        if (operatorStatistics != null) {
            operatorStatistics.addIOReadBytes(bytesInIOPlan);
        }

        // metrics the logical bytes range.
        Counter bytesRangeCounter = enableMetrics ? metrics.addCounter(
            columnsMetricsKey(selectedColumns, ProfileKeys.ORC_LOGICAL_BYTES_RANGE),
            ASYNC_STRIPE_LOADER_BYTES_RANGE,
            ProfileKeys.ORC_LOGICAL_BYTES_RANGE.getProfileUnit()
        ) : null;
        for (Map.Entry<StreamName, StreamInformation> entry : streamManager.getStreams().entrySet()) {
            StreamName streamName = entry.getKey();
            if (streamName.getColumn() < selectedColumns.length && selectedColumns[streamName.getColumn()]) {
                StreamInformation stream = entry.getValue();

                // filter stream with no data.
                if (stream != null && stream.firstChunk != null) {
                    for (DiskRangeList node = stream.firstChunk; node != null; node = node.next) {
                        if (node.getOffset() >= stream.offset
                            && node.getEnd() <= stream.offset + stream.length) {
                            if (enableMetrics) {
                                bytesRangeCounter.inc(node.getLength());
                            }
                            bytesHitStream += node.getLength();
                        }
                    }
                }
            }
        }

        Preconditions.checkArgument(bytesHitStream == bytesInIOPlan,
            String.format(
                "bytesHitStream = %s but bytesInIOPlan = %s",
                bytesHitStream, bytesInIOPlan
            ));

        // large memory allocation: buffer chunk list in stripe-level IO processing.
        // We must multiply by a factor of 2 because the OSS network buffer
        // or file read buffer requires the same memory size as bytesInIOPlan.
        final long allocatedBytes = 2 * bytesInIOPlan;
        memoryAllocatorCtx.allocateReservedMemory(allocatedBytes);
        totalAllocatedBytes.addAndGet(allocatedBytes);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(MessageFormat.format("filePath = {0}, stripeId = {1}, allocatedBytes = {2}",
                filePath, stripeInformation.getStripeId(), allocatedBytes));
        }

        return CompletableFuture.supplyAsync(
            () -> readData(selectedColumns, ioPlan, controller), ioExecutor
        );
    }

    @Override
    public CompletableFuture<Map<StreamName, InStream>> load(int targetColumnId,
                                                             boolean[] targetRowGroups,
                                                             Supplier<Boolean> controller) {
        Preconditions.checkArgument(isOpened, "The stripe loader has not already been opened");

        if (allFalse(targetRowGroups)) {
            // Directly return empty map to avoid opening file.
            return CompletableFuture.completedFuture(new HashMap<>());
        }

        // Column-level parallel data loading is only suitable for columns that size > 2MB in one stripe.
        // In some cases, we need merge all columns in one IO task.

        if (registerMap.putIfAbsent(targetColumnId, targetRowGroups) != null) {
            // The Stripe-Loader is stateful, and a column can only be loaded once in one stripe.
            throw new RuntimeException(
                MessageFormat.format("The column id {0} in stripe can only be planned once", targetColumnId)
            );
        }

        final long stripeId = stripeInformation.getStripeId();
        PositionProviderBuilder orcIndex = preheatFileMeta.getPositionProviderBuilder(stripeId);

        // Get the IO plan of all streams in this column.
        BufferChunkList ioPlan;
        if (orcIndex instanceof OrcIndex) {
            ioPlan = StaticStripePlanner.planGroupsInColumn(
                stripeContext,
                streamManager,
                streamOptions,
                (OrcIndex) orcIndex,
                targetRowGroups,
                targetColumnId
            );
        } else {
            ioPlan = StaticStripePlanner.planGroupsInColumn(
                stripeContext,
                streamManager,
                streamOptions,
                (FastPositionIndexPositionProviderBuilder) orcIndex,
                targetRowGroups,
                targetColumnId
            );
        }
        ioPlans.add(ioPlan);

        // check buffer chunk list
        long bytesInIOPlan = 0L;
        long bytesHitStream = 0L;
        for (BufferChunk node = ioPlan.get(); node != null; node = (BufferChunk) node.next) {
            bytesInIOPlan += node.getLength();
        }

        if (operatorStatistics != null) {
            operatorStatistics.addIOReadBytes(bytesInIOPlan);
        }

        // metrics the logical bytes range.
        Counter bytesRangeCounter = enableMetrics ? metrics.addCounter(
            columnMetricsKey(targetColumnId, ProfileKeys.ORC_LOGICAL_BYTES_RANGE),
            ASYNC_STRIPE_LOADER_BYTES_RANGE,
            ProfileKeys.ORC_LOGICAL_BYTES_RANGE.getProfileUnit()
        ) : null;

        for (Map.Entry<StreamName, StreamInformation> entry : streamManager.getStreams().entrySet()) {
            StreamName streamName = entry.getKey();
            if (streamName.getColumn() == targetColumnId) {
                StreamInformation stream = entry.getValue();

                // filter stream with no data.
                if (stream != null && stream.firstChunk != null) {
                    for (DiskRangeList node = stream.firstChunk; node != null; node = node.next) {
                        if (node.getOffset() >= stream.offset
                            && node.getEnd() <= stream.offset + stream.length) {
                            if (enableMetrics) {
                                bytesRangeCounter.inc(node.getLength());
                            }
                            bytesHitStream += node.getLength();
                        }
                    }
                }
            }
        }

        Preconditions.checkArgument(bytesHitStream == bytesInIOPlan,
            String.format(
                "bytesHitStream = %s but bytesInIOPlan = %s",
                bytesHitStream, bytesInIOPlan
            ));

        // large memory allocation: buffer chunk list in stripe-level IO processing.
        // We must multiply by a factor of 2 because the OSS network buffer
        // or file read buffer requires the same memory size as bytesInIOPlan.
        final long allocatedBytes = 2 * bytesInIOPlan;
        memoryAllocatorCtx.allocateReservedMemory(allocatedBytes);
        totalAllocatedBytes.addAndGet(allocatedBytes);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(MessageFormat.format("filePath = {0}, stripeId = {1}, allocatedBytes = {2}",
                filePath, stripeInformation.getStripeId(), allocatedBytes));
        }

        return CompletableFuture.supplyAsync(
            () -> readData(targetColumnId, ioPlan, controller), ioExecutor
        );
    }

    @Override
    public long getIOMemoryUsage(List<Integer> columnIds, Map<Integer, boolean[]> rowGroupBitmaps) {
        Preconditions.checkArgument(isOpened, "The stripe loader has not already been opened");
        // Column-level parallel data loading is only suitable for columns that size > 2MB in one stripe.
        // In some cases, we need merge all columns in one IO task.

        if (rowGroupBitmaps != null && rowGroupBitmaps.values().stream().allMatch(AsyncStripeLoader::allFalse)) {
            // Directly return empty map to avoid opening file.
            return 0L;
        }

        final long stripeId = stripeInformation.getStripeId();
        PositionProviderBuilder orcIndex = preheatFileMeta.getPositionProviderBuilder(stripeId);

        // build selected columns bitmap
        boolean[] selectedColumns = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(selectedColumns, false);
        columnIds.forEach(col -> selectedColumns[col] = true);

        // Build IO plans for each column with different row group bitmaps
        // and merge them into one buffer-chunk-list.
        // IMPORTANT: Create a local StreamManager to avoid polluting this.streamManager's stream.firstChunk.
        // planGroupsInColumn has a side effect of setting stream.firstChunk via addChunk1,
        // which would corrupt subsequent load() calls if applied to this.streamManager.
        StreamManager localStreamManager;
        if (preheatFileMeta.isUseBinaryMeta()) {
            int stripeIndex = (int) stripeInformation.getStripeId();
            localStreamManager = StaticStripePlanner.parseStripe(
                stripeContext, columnIncluded, preheatFileMeta, stripeIndex
            );
        } else {
            OrcProto.StripeFooter footer =
                preheatFileMeta.findStripeFooterInNonBinaryMode(stripeInformation.getStripeId());
            localStreamManager = StaticStripePlanner.parseStripe(
                stripeContext, columnIncluded, footer
            );
        }

        // Get the IO plan of all streams in this column.
        BufferChunkList result;
        if (orcIndex instanceof OrcIndex) {
            result = StaticStripePlanner.planGroupsInColumn(
                stripeContext,
                localStreamManager,
                streamOptions,
                (OrcIndex) orcIndex,
                rowGroupBitmaps,
                selectedColumns
            );
        } else {
            result = StaticStripePlanner.planGroupsInColumn(
                stripeContext,
                localStreamManager,
                streamOptions,
                (FastPositionIndexPositionProviderBuilder) orcIndex,
                rowGroupBitmaps,
                selectedColumns
            );
        }

        // check buffer chunk list
        long bytesInIOPlan = 0L;
        for (BufferChunk node = result.get(); node != null; node = (BufferChunk) node.next) {
            bytesInIOPlan += node.getLength();
        }

        // large memory allocation: buffer chunk list in stripe-level IO processing.
        // We must multiply by a factor of 2 because the OSS network buffer
        // or file read buffer requires the same memory size as bytesInIOPlan.
        return 2 * bytesInIOPlan;
    }

    @Override
    public long clearStream(StreamName streamName) {
        if (streamManager == null) {
            return 0L;
        }
        // find stream information and clear the buffer chunk list.
        Map<StreamName, StreamInformation> allStreams = streamManager.getStreams();
        StreamInformation streamInformation;
        if ((streamInformation = allStreams.get(streamName)) != null) {
            long releasedBytes = streamInformation.releaseBuffers();

            // allocate the memory of data IO.
            memoryAllocatorCtx.releaseReservedMemory(2 * releasedBytes, true);
            totalAllocatedBytes.getAndAdd(-2 * releasedBytes);

            releasedStreams.remove(streamName);
            if (releasedStreams.isEmpty()) {
                // all streams have been released.
                memoryAllocatorCtx.releaseReservedMemory(totalAllocatedBytes.get(), true);
                totalAllocatedBytes.getAndAdd(-totalAllocatedBytes.get());
            }

            return releasedBytes;
        }
        return 0L;
    }

    /**
     * For validation:
     * The list of {range, data} must be in range of stream [offset, length].
     * And the result RangDiskList must be constructed by linked list of all streams.
     *
     * @return stream manager holding the stream information.
     */
    public StreamManager getStreamManager() {
        return streamManager;
    }

    private Map<StreamName, InStream> readData(boolean[] selectedColumns, BufferChunkList ioPlan,
                                               Supplier<Boolean> controller) {
        VersionStorageStatistics.setThreadLocalStatistics(versionStorageStatistics);
        try (ColumnDataReader dataReader = buildDataReader()) {
            dataReader.setController(controller);
            if (enableMetrics) {
                // build profile for IO processing.
                ORCProfile memoryCounter = new ORCMetricsWrapper(
                    columnsMetricsKey(selectedColumns, ProfileKeys.ORC_IO_RAW_DATA_MEMORY_COUNTER),
                    ASYNC_STRIPE_LOADER_MEMORY,
                    ProfileKeys.ORC_IO_RAW_DATA_MEMORY_COUNTER.getProfileUnit(),
                    metrics);

                ORCProfile ioTimer = new ORCMetricsWrapper(
                    columnsMetricsKey(selectedColumns, ProfileKeys.ORC_IO_RAW_DATA_TIMER),
                    ASYNC_STRIPE_LOADER_TIMER,
                    ProfileKeys.ORC_IO_RAW_DATA_TIMER.getProfileUnit(),
                    metrics);

                // Execute IO tasks within the buffer chunk list.
                dataReader.readFileData(ioPlan, false, memoryCounter, null, ioTimer);
                allocatedBytesForIOPlan += dataReader.getActualAllocatedBytes();
            } else {
                dataReader.readFileData(ioPlan, false);
                allocatedBytesForIOPlan += dataReader.getActualAllocatedBytes();
            }
        } catch (Throwable t) {
            // IO ERROR
            throw GeneralUtil.nestedException(t);
        } finally {
            VersionStorageStatistics.removeThreadLocalStatistics();
        }

        // Build in-streams after IO tasks done
        return buildInStreams((col) -> col < selectedColumns.length && selectedColumns[col]);
    }

    private Map<StreamName, InStream> readData(int targetColumnId, BufferChunkList ioPlan,
                                               Supplier<Boolean> controller) {
        VersionStorageStatistics.setThreadLocalStatistics(versionStorageStatistics);
        try (ColumnDataReader dataReader = buildDataReader()) {
            dataReader.setController(controller);
            if (enableMetrics) {
                // build profile for IO processing.
                ORCProfile memoryCounter = new ORCMetricsWrapper(
                    columnMetricsKey(targetColumnId, ProfileKeys.ORC_IO_RAW_DATA_MEMORY_COUNTER),
                    ASYNC_STRIPE_LOADER_MEMORY,
                    ProfileKeys.ORC_IO_RAW_DATA_MEMORY_COUNTER.getProfileUnit(),
                    metrics);

                ORCProfile ioTimer = new ORCMetricsWrapper(
                    columnMetricsKey(targetColumnId, ProfileKeys.ORC_IO_RAW_DATA_TIMER),
                    ASYNC_STRIPE_LOADER_TIMER,
                    ProfileKeys.ORC_IO_RAW_DATA_TIMER.getProfileUnit(),
                    metrics);

                // Execute IO tasks within the buffer chunk list.
                dataReader.readFileData(ioPlan, false, memoryCounter, null, ioTimer);
                allocatedBytesForIOPlan += dataReader.getActualAllocatedBytes();
            } else {
                dataReader.readFileData(ioPlan, false);
                allocatedBytesForIOPlan += dataReader.getActualAllocatedBytes();
            }
        } catch (Throwable t) {
            // IO ERROR
            throw GeneralUtil.nestedException(t);
        } finally {
            VersionStorageStatistics.removeThreadLocalStatistics();
        }

        // Build in-streams after IO tasks done
        Map<StreamName, InStream> results = buildInStreams((col) -> col == targetColumnId);

        return results;
    }

    @NotNull
    private Map<StreamName, InStream> buildInStreams(Predicate<Integer> columnFilter) {
        Map<StreamName, InStream> results = new TreeMap<>(STREAM_NAME_COMPARATOR);
        for (Map.Entry<StreamName, StreamInformation> entry : streamManager.getStreams().entrySet()) {
            StreamName streamName = entry.getKey();
            if (columnFilter.test(streamName.getColumn())) {
                StreamInformation stream = entry.getValue();

                // filter stream with no data.
                if (stream != null && stream.firstChunk != null) {

                    InStream inStream = InStream.create(
                        streamName,
                        stream.firstChunk,
                        stream.offset,
                        stream.length,
                        streamOptions);

                    // build profile for in stream.
                    ORCProfile memoryCounter = enableMetrics ? new ORCMetricsWrapper(
                        streamMetricsKey(streamName, ProfileKeys.ORC_IN_STREAM_MEMORY_COUNTER),
                        ASYNC_STRIPE_LOADER_MEMORY,
                        ProfileKeys.ORC_IN_STREAM_MEMORY_COUNTER.getProfileUnit(),
                        metrics) : null;

                    ORCProfile decompressTimer = enableMetrics ? new ORCMetricsWrapper(
                        streamMetricsKey(streamName, ProfileKeys.ORC_IN_STREAM_DECOMPRESS_TIMER),
                        ASYNC_STRIPE_LOADER_TIMER,
                        ProfileKeys.ORC_IN_STREAM_DECOMPRESS_TIMER.getProfileUnit(),
                        metrics) : null;

                    inStream.setMemoryCounter(memoryCounter);
                    inStream.setDecompressTimer(decompressTimer);

                    results.put(streamName, inStream);
                }
            }
        }
        return results;
    }

    private ColumnDataReader buildDataReader() throws IOException {
        DataReaderProperties.Builder builder =
            DataReaderProperties.builder()
                .withFileSystemSupplier(() -> fileSystem)
                .withPath(filePath)
                .withMaxMergeDistance(maxMergeDistance)
                .withMaxDiskRangeChunkLimit(maxDiskRangeChunkLimit)
                .withZeroCopy(false);

        FSDataInputStream file = fileSystem.open(filePath); // may have IO rt.
        if (file != null) {
            builder.withFile(file);
        }

        ColumnDataReader dataReader = new ColumnDataReader(builder.build());
        return dataReader;
    }

    @Override
    public void close() throws IOException {
        // nothing should be closed here.
    }

    @Override
    public void release() {
        if (released.compareAndSet(false, true)) {
            MemoryTrackerManager.adjustMemoryUsage(operatorMemoryOwnerId);
        }
    }

    public static boolean allFalse(boolean[] rowGroupIncluded) {
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
