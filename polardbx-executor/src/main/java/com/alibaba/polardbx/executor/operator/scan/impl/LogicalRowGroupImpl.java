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

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.columnar.BlockLoader;
import com.alibaba.polardbx.executor.chunk.columnar.CommonLazyBlock;
import com.alibaba.polardbx.executor.chunk.columnar.LazyBlock;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.CacheReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.executor.operator.scan.RowGroupReader;
import com.alibaba.polardbx.executor.operator.scan.metrics.MetricsNameBuilder;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileKeys;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.utils.TimestampUtils;
import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Booleans;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class LogicalRowGroupImpl implements LogicalRowGroup<Block, ColumnStatistics> {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LogicalRowGroupImpl.class).instanceSize();

    @FieldMemoryCounter(value = false)
    private final RuntimeMetrics metrics;
    @FieldMemoryCounter(value = false)
    private final Path filePath;
    private final int stripeId;

    /**
     * The group id of this row group.
     */
    private final int groupId;

    /**
     * Count of rows in this row group, must <= ORC_INDEX_STRIDE.
     */
    private final int rowCount;
    private final int startRowId;

    /**
     * File-level column schema
     */
    @FieldMemoryCounter(value = false)
    private final TypeDescription fileSchema;
    @FieldMemoryCounter(value = false)
    private final OSSColumnTransformer ossColumnTransformer;

    /**
     * The length of encoding[] array is equal to column count,
     * and the encoding of column that not included is null.
     */
    @FieldMemoryCounter(value = false)
    private final OrcProto.ColumnEncoding[] encodings;

    /**
     * Included column ids.
     */
    @FieldMemoryCounter(value = false)
    private final boolean[] columnIncluded;

    /**
     * Count of selected columns.
     */
    private final int columns;

    /**
     * The maximum rows in one chunk.
     */
    private final int chunkLimit;

    /**
     * A column-level reader responsible for all blocks of all row groups in the stripe.
     */
    @FieldMemoryCounter(value = false)
    private final Map<Integer, ColumnReader> columnReaders;

    /**
     * A column-level cache reader holding the available cached blocks in the stripe
     */
    @FieldMemoryCounter(value = false)
    private final Map<Integer, CacheReader<Block>> cacheReaders;

    /**
     * The global block cache manager shared by all files.
     */
    @FieldMemoryCounter(value = false)
    private final BlockCacheManager<Block> blockCacheManager;

    @FieldMemoryCounter(value = false)
    private final ExecutionContext context;

    /**
     * Enable block cache or not.
     */
    private final boolean useBlockCache;

    private final boolean enableMetrics;

    private final boolean enableColumnReaderLock;

    private final boolean useSelection;

    private final boolean enableCompatible;

    @FieldMemoryCounter(value = false)
    private final TimeZone timeZone;

    private final boolean onlyCachePrimaryKey;

    private final boolean enableSkipCompression;

    @FieldMemoryCounter(value = false)
    private OperatorMemoryOwnerId operatorMemoryOwnerId;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE;
    }

    public LogicalRowGroupImpl(
        RuntimeMetrics metrics,
        Path filePath, int stripeId, int groupId, int rowCount, int startRowId,
        TypeDescription fileSchema, OSSColumnTransformer ossColumnTransformer,
        OrcProto.ColumnEncoding[] encodings, boolean[] columnIncluded, int chunkLimit,
        Map<Integer, ColumnReader> columnReaders, Map<Integer, CacheReader<Block>> cacheReaders,
        BlockCacheManager<Block> blockCacheManager, ExecutionContext context,
        OperatorMemoryOwnerId operatorMemoryOwnerId) {
        this.metrics = metrics;
        this.filePath = filePath;
        this.stripeId = stripeId;
        this.groupId = groupId;
        this.rowCount = rowCount;
        this.startRowId = startRowId;
        this.fileSchema = fileSchema;
        this.ossColumnTransformer = ossColumnTransformer;
        this.encodings = encodings;
        this.columnIncluded = columnIncluded;
        this.chunkLimit = chunkLimit;
        this.columnReaders = columnReaders;
        this.cacheReaders = cacheReaders;
        this.blockCacheManager = blockCacheManager;
        this.context = context;
        this.useBlockCache = context.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_BLOCK_CACHE);
        this.enableMetrics = context.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_COLUMNAR_METRICS);
        this.enableColumnReaderLock = context.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_COLUMN_READER_LOCK);
        this.useSelection = context.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_COLUMNAR_SCAN_SELECTION);
        this.enableCompatible = context.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE);
        this.timeZone = TimestampUtils.getTimeZone(context);
        this.enableSkipCompression = context.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_SKIP_COMPRESSION_IN_ORC);
        this.onlyCachePrimaryKey = context.getParamManager()
            .getBoolean(ConnectionParams.ONLY_CACHE_PRIMARY_KEY_IN_BLOCK_CACHE);

        this.columns = this.ossColumnTransformer.columnCount();
        Preconditions.checkArgument(columns > 0);

        this.operatorMemoryOwnerId = operatorMemoryOwnerId;
    }

    protected class Reader implements RowGroupReader<Chunk> {
        private LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup;

        /**
         * Current row position in this row-group for the next allocation.
         */
        private int currentPosition;

        private int lastChunkRows;
        private int lastPosition;

        public Reader(LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup) {
            this.logicalRowGroup = logicalRowGroup;
            this.currentPosition = 0;
        }

        @Override
        public Chunk nextBatch() {
            // Get the row count of the next lazy chunk.
            int chunkRows;
            if (currentPosition + chunkLimit >= rowCount) {
                chunkRows = rowCount - currentPosition;
            } else {
                chunkRows = chunkLimit;
            }

            if (chunkRows <= 0) {
                // run out.
                return null;
            }

            Block[] blocks = new Block[columns];

            // the col id is precious identifier in orc file schema, while the col index is just the index in list.
            for (int colIndex = 0; colIndex < ossColumnTransformer.columnCount(); colIndex++) {
                Integer colId = ossColumnTransformer.getLocInOrc(colIndex);
                if (colId != null) {
                    OrcProto.ColumnEncoding encoding = encodings[colId];

                    ColumnReader columnReader = columnReaders.get(colId);
                    CacheReader<Block> cacheReader = cacheReaders.get(colId);
                    Preconditions.checkNotNull(columnReader);
                    Preconditions.checkNotNull(cacheReader);

                    Counter loadTimer = enableMetrics ? metrics.addCounter(
                        MetricsNameBuilder.columnMetricsKey(colId, ProfileKeys.SCAN_WORK_BLOCK_LOAD_TIMER),
                        BLOCK_LOAD_TIMER,
                        ProfileKeys.SCAN_WORK_BLOCK_LOAD_TIMER.getProfileUnit()
                    ) : null;
                    Counter memoryCounter = enableMetrics ? metrics.addCounter(
                        MetricsNameBuilder.columnMetricsKey(colId, ProfileKeys.SCAN_WORK_BLOCK_MEMORY_COUNTER),
                        BLOCK_MEMORY_COUNTER,
                        ProfileKeys.SCAN_WORK_BLOCK_MEMORY_COUNTER.getProfileUnit()
                    ) : null;

                    // build block loader for specify position range.
                    BlockLoader loader;
                    if (context.isEnableOrcRawTypeBlock()) {
                        // Special path for check cci consistency.
                        // Normal oss read should not get here.
                        // build block loader for specify position range.
                        loader = new OrcRawTypeBlockLoader(logicalRowGroup, colId, currentPosition, chunkRows,
                            encoding, columnReader, cacheReader, blockCacheManager, context, useBlockCache,
                            enableColumnReaderLock, chunkLimit, loadTimer, memoryCounter,
                            onlyCachePrimaryKey, enableSkipCompression);
                    } else if (context.isCciIncrementalCheck()) {
                        loader = new NoCacheBlockLoader(logicalRowGroup, colId, currentPosition, chunkRows,
                            encoding, columnReader, cacheReader, blockCacheManager, context, useBlockCache,
                            enableColumnReaderLock, chunkLimit, loadTimer, memoryCounter,
                            onlyCachePrimaryKey, enableSkipCompression);
                    } else {
                        // build block loader for specify position range.
                        loader = new ReactiveBlockLoader(logicalRowGroup, colId, currentPosition, chunkRows,
                            encoding, columnReader, cacheReader, blockCacheManager, context, useBlockCache,
                            enableColumnReaderLock, chunkLimit, loadTimer, memoryCounter,
                            onlyCachePrimaryKey, enableSkipCompression);
                        ((ReactiveBlockLoader) loader).setOperatorMemoryOwnerId(operatorMemoryOwnerId);
                    }

                    // build lazy-block with given loader and schema.
                    DataType targetType = ossColumnTransformer.getTargetColumnMeta(colIndex).getDataType();
                    LazyBlock lazyBlock = new CommonLazyBlock(
                        targetType, loader, columnReader, useSelection,
                        enableCompatible, timeZone, context, colIndex, ossColumnTransformer
                    );
                    ((CommonLazyBlock) lazyBlock).setOperatorMemoryOwnerId(operatorMemoryOwnerId);
                    blocks[colIndex] = lazyBlock;
                } else {
                    blocks[colIndex] = new CommonLazyBlock(
                        ossColumnTransformer.getTargetColumnMeta(colIndex).getDataType(),
                        new DummyBlockLoader(currentPosition, chunkRows), null, useSelection,
                        enableCompatible, timeZone, context, colIndex, ossColumnTransformer
                    );
                    ((CommonLazyBlock) blocks[colIndex]).setOperatorMemoryOwnerId(operatorMemoryOwnerId);
                }
            }
            Chunk chunk = new Chunk(chunkRows, blocks);

            MemoryTrackerManager.tryReverseReference(operatorMemoryOwnerId, FastMemoryCounter.sizeOf(chunk));

            // Get the start position of the next chunk.
            lastChunkRows = chunkRows;
            lastPosition = currentPosition;
            currentPosition += chunkRows;

            return chunk;
        }

        @Override
        public int[] batchRange() {
            return new int[] {lastPosition + startRowId, lastChunkRows};
        }

        @Override
        public int groupId() {
            return groupId;
        }

        @Override
        public int rowCount() {
            return rowCount;
        }

        @Override
        public int batches() {
            int batches = rowCount / chunkLimit;
            return rowCount % chunkLimit == 0 ? batches : batches + 1;
        }
    }

    protected class ReversedReader implements RowGroupReader<Chunk> {

        private Reader reader;
        private boolean isInitialized = false;

        private List<Chunk> chunkList;
        private List<int[]> batchRangeList;
        private int currentIndex = 0;

        public ReversedReader(LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup) {
            this.reader = new Reader(logicalRowGroup);
            this.chunkList = new ArrayList<>();
            this.batchRangeList = new ArrayList<>();
        }

        @Override
        public Chunk nextBatch() {
            if (!isInitialized) {

                Chunk chunk;
                while ((chunk = reader.nextBatch()) != null) {
                    int[] batchRange = reader.batchRange();
                    chunkList.add(chunk);
                    batchRangeList.add(batchRange);
                    currentIndex++;
                }

                isInitialized = true;
            }

            if (currentIndex == 0) {
                return null;
            }

            return chunkList.get(--currentIndex);
        }

        @Override
        public int[] batchRange() {
            if (!isInitialized || currentIndex < 0 || currentIndex >= batchRangeList.size()) {
                return null;
            }
            return batchRangeList.get(currentIndex);
        }

        @Override
        public int groupId() {
            return groupId;
        }

        @Override
        public int rowCount() {
            return rowCount;
        }

        @Override
        public int batches() {
            int batches = rowCount / chunkLimit;
            return rowCount % chunkLimit == 0 ? batches : batches + 1;
        }
    }

    @Override
    public Path path() {
        return filePath;
    }

    @Override
    public int stripeId() {
        return stripeId;
    }

    @Override
    public int groupId() {
        return groupId;
    }

    @Override
    public int rowCount() {
        return rowCount;
    }

    @Override
    public int startRowId() {
        return startRowId;
    }

    @Override
    public RowGroupReader<Chunk> getReader() {
        return new Reader(this);
    }

    @Override
    public RowGroupReader<Chunk> getReversedReader() {
        return new ReversedReader(this);
    }

    @Override
    public String toString() {
        return "LogicalRowGroupImpl{" +
            "filePath=" + filePath +
            ", stripeId=" + stripeId +
            ", groupId=" + groupId +
            ", rowCount=" + rowCount +
            ", startRowId=" + startRowId +
            ", columnIncluded=" + Arrays.toString(columnIncluded) +
            ", columns=" + columns +
            '}';
    }
}
