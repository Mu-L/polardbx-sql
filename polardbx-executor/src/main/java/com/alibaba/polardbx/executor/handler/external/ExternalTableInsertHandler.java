package com.alibaba.polardbx.executor.handler.external;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.io.IOException;
import java.util.List;

/**
 * Connector-specific insert handler for {@code LogicalExternalInsertHandler}.
 *
 * <p>Each {@link com.alibaba.polardbx.optimizer.core.rel.TableSink} type provides
 * its own implementation that encapsulates all I/O, format serialization, and
 * write logic specific to that external connector.
 *
 * <p>{@code LogicalExternalInsertHandler} calls the three methods in order:
 * <pre>
 *   open()  →  writeRows(rows) [repeated]  →  close()
 * </pre>
 *
 * <p>Adding support for a new connector requires only:
 * <ol>
 *   <li>Implementing this interface for the new sink type.</li>
 *   <li>Adding an {@code instanceof} branch in
 *       {@code LogicalExternalInsertHandler.buildHandler}.</li>
 * </ol>
 */
public interface ExternalTableInsertHandler {

    /**
     * Opens the handler and initialises connector-specific write resources
     * (e.g., creates the output file, acquires a write lock, etc.).
     * Called once before the first {@link #writeRows} call.
     *
     * @throws IOException if the underlying sink cannot be opened
     */
    void open() throws IOException;

    /**
     * Writes a batch of rows to the external sink.
     *
     * <p>Each element of {@code rows} is an ordered list of column values for
     * one input row, following the table's column order.
     *
     * @param rows list of rows, each row being an ordered list of column values
     * @return the number of rows successfully written in this call
     * @throws IOException if writing fails
     */
    int writeRows(List<List<Object>> rows) throws IOException;

    /**
     * Closes the handler and releases all connector-specific write resources.
     * Called once after the last {@link #writeRows} call.
     *
     * @throws IOException if closing fails
     */
    void close() throws IOException;
}
