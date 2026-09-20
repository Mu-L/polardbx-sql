package com.alibaba.polardbx.executor.operator.external;

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.ExternalTableScanExec;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.io.IOException;
import java.util.List;

/**
 * Connector-specific scan handler for {@link ExternalTableScanExec}.
 *
 * <p>Each {@link com.alibaba.polardbx.optimizer.core.rel.TableSource} type provides
 * its own implementation that encapsulates all I/O, row-layout mapping, filter
 * evaluation, and projection logic.
 *
 * <p>{@link ExternalTableScanExec} calls the three methods in order:
 * <pre>
 *   open()  →  nextChunk(...) [repeated]  →  close()
 * </pre>
 */
public interface ExternalTableScanHandler {

    /**
     * Opens the handler and initialises connector-specific resources.
     * Called once before the first {@link #nextChunk} call.
     *
     * @throws IOException if the underlying source cannot be opened
     */
    void open() throws IOException;

    /**
     * Fills up to {@code chunkLimit} output rows into {@code blockBuilders},
     * applying connector-specific filter and projection logic internally.
     *
     * <p>Returns {@code null} when the source is exhausted.
     *
     * @param blockBuilders one builder per output column, pre-allocated by the caller
     * @param chunkLimit maximum number of rows to write in this call
     * @return a fully built {@link Chunk}, or {@code null} if no more data is available
     */
    Chunk nextChunk(BlockBuilder[] blockBuilders, int chunkLimit);

    /**
     * Closes the handler and releases all connector-specific resources.
     * Called once after the last {@link #nextChunk} call.
     */
    void close();

    /**
     * Returns the output {@link DataType} list for this scan.
     * Used by {@link ExternalTableScanExec} to allocate {@code blockBuilders}.
     */
    List<DataType> getOutputTypes();
}
