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

package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.oss.filesystem.InputStreamWithBackup;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.rpc.ColumnarDeltaRpcClient;
import com.alibaba.polardbx.rpc.ColumnarDeltaStream;
import com.alibaba.polardbx.rpc.columnar.ColumnarDeltaRequest;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Simple implementation of csv file reader.
 * Try to load csv file from RPC stream first.
 * If failed, load from file system.
 */
public class SimpleCSVFileReader implements CSVFileReader {
    private static final Logger logger = LoggerFactory.getLogger(SimpleCSVFileReader.class);

    private int fieldNum;
    private InputStream inputStream;
    private List<ColumnProvider> columnProviders;
    private List<ColumnMeta> columnMetas;
    private ByteCSVReader rowReader;
    private ExecutionContext context;
    private int chunkLimit;
    private int offset;
    private int length;
    // Track statistics
    private final CSVFileStatistics statistics = CSVFileStatistics.getInstance();
    private boolean isColumnarAccess;
    // Track if fallback occurred for columnar access
    private InputStreamWithBackup statisticsInputStreamWithBackup;

    @Override
    public void open(ExecutionContext context,
                     List<ColumnMeta> columnMetas,
                     int chunkLimit,
                     Engine engine,
                     String csvFileName,
                     int offset,
                     int length) throws IOException {
        open(context, columnMetas, chunkLimit, engine, csvFileName, offset, length, false);
    }

    public void open(ExecutionContext context,
                     List<ColumnMeta> columnMetas,
                     int chunkLimit,
                     Engine engine,
                     String csvFileName,
                     int offset,
                     int length,
                     boolean loadFromColumnar) throws IOException {
        this.chunkLimit = chunkLimit;
        this.context = context;
        this.fieldNum = columnMetas.size();
        this.length = length;
        this.isColumnarAccess = loadFromColumnar;

        if (loadFromColumnar) {
            try {
                ColumnarDeltaStream deltaStream = new ColumnarDeltaStream(
                    ColumnarDeltaRpcClient.getInstance(),
                    ColumnarDeltaRequest.newBuilder()
                        .setFileName(csvFileName)
                        .setOffset(offset)
                        .setLength(length)
                        .build(),
                    context.getTraceId()
                );
                // Create a statistics-aware InputStreamWithBackup
                this.statisticsInputStreamWithBackup = new InputStreamWithBackup(
                    deltaStream,
                    (bytesRead) -> {
                        // Record columnar statistics when fallback occurs
                        long columnarReadTimeMs = statisticsInputStreamWithBackup.getIOTimeFromColumnarMs();
                        statistics.recordReadOperation(bytesRead, true, columnarReadTimeMs);
                        // Reset start time for file system read
                        // Set fallback occurred flag
                        if (statisticsInputStreamWithBackup != null) {
                            statisticsInputStreamWithBackup.setFallbackOccurred();
                        }
                        return readFromFileSystem(engine, csvFileName, offset + bytesRead, length - bytesRead);
                    },
                    csvFileName
                );
                this.inputStream = this.statisticsInputStreamWithBackup;
            } catch (Throwable t) {
                if (inputStream != null) {
                    inputStream.close();
                }
                logger.error("load failed from columnar rpc, fallback to OSS", t);
                this.inputStream = readFromFileSystem(engine, csvFileName, offset, length);
                this.isColumnarAccess = false; // Fallback to file system
            }
        } else {
            this.inputStream = readFromFileSystem(engine, csvFileName, offset, length);
        }
        this.columnProviders = columnMetas.stream()
            .map(ColumnProviders::getProvider).collect(Collectors.toList());
        this.columnMetas = columnMetas;
        this.offset = offset;

        byte[] reusableNulls = new byte[fieldNum];
        int[] reusableOffsets = new int[fieldNum];

        this.rowReader = new ByteCSVReader(csvFileName, inputStream, this.length, reusableNulls, reusableOffsets);
    }

    private InputStream readFromFileSystem(Engine engine, String csvFileName, int offset, int length) {
        long startTime = System.currentTimeMillis();
        try {
            byte[] buffer = new byte[length];
            FileSystemUtils.readFile(csvFileName, offset, length, buffer, engine, true);
            long endTime = System.currentTimeMillis();
            // Record file system statistics
            statistics.recordReadOperation(length, false, endTime - startTime);
            return new ByteArrayInputStream(buffer);
        } catch (Exception e) {
            long endTime = System.currentTimeMillis();
            statistics.recordReadOperation(0, false, endTime - startTime);
            throw e;
        }
    }

    @Override
    public Chunk next() {
        return nextUntilPosition(Long.MAX_VALUE);
    }

    @Override
    public Chunk nextUntilPosition(long pos) {
        return nextUntilPosition(pos, Integer.MAX_VALUE);
    }

    @Override
    public Chunk nextUntilPosition(long pos, int expectedRowCount) {
        long positionBound = Math.min(pos, offset + length);

        try {
            if (offset + rowReader.position() >= positionBound || !rowReader.isReadable()) {
                // return in advance to prevent create block builder with 0 capacity
                return null;
            }
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }

        List<BlockBuilder> blockBuilders = this.columnMetas
            .stream()
            .map(ColumnMeta::getDataType)
            .map(t -> BlockBuilders.create(t, context, Math.min(expectedRowCount, chunkLimit)))
            .collect(Collectors.toList());

        int totalRow = 0;
        try {
            while (offset + rowReader.position() < positionBound && rowReader.isReadable()) {
                CSVRow row = rowReader.nextRow();

                // for each row, parse each column and append onto block-builder
                for (int columnId = 0; columnId < fieldNum; columnId++) {
                    ColumnProvider columnProvider = columnProviders.get(columnId);
                    BlockBuilder blockBuilder = blockBuilders.get(columnId);
                    DataType dataType = columnMetas.get(columnId).getDataType();

                    columnProvider.parseRow(
                        blockBuilder, row, columnId, dataType
                    );
                }

                // reach chunk limit
                if (++totalRow >= chunkLimit) {
                    return buildChunk(blockBuilders, totalRow);
                }
            }
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }

        // flush the remaining rows
        return totalRow == 0 ? null : buildChunk(blockBuilders, totalRow);
    }

    @NotNull
    public static Chunk buildChunk(List<BlockBuilder> blockBuilders, int totalRow) {
        return new Chunk(totalRow, blockBuilders.stream()
            .map(BlockBuilder::build).toArray(Block[]::new));
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }

        // Record statistics when closing the reader (only for non-fallback cases)
        if (isColumnarAccess && inputStream instanceof InputStreamWithBackup) {
            InputStreamWithBackup statisticsInputStreamWithBackup = (InputStreamWithBackup) inputStream;
            if (statisticsInputStreamWithBackup.isFallbackOccurred()) {
                return;
            }
            long bytesRead = statisticsInputStreamWithBackup.getBytesRead();
            long readTimeMs = statisticsInputStreamWithBackup.getIOTimeFromColumnarMs();
            statistics.recordReadOperation(bytesRead, isColumnarAccess, readTimeMs);
        }
    }

    public long position() throws IOException {
        return offset + rowReader.position();
    }
}