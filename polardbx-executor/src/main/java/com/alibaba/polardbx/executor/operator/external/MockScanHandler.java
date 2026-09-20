package com.alibaba.polardbx.executor.operator.external;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Mock implementation of {@link ExternalTableScanHandler} for testing.
 * Reads data from the "mock.data" or table-specific data property in options.
 * Data format: rows separated by '|', columns separated by ','.
 * Example: "1,alice,95|2,bob,88|3,carol,72"
 */
public class MockScanHandler implements ExternalTableScanHandler {

    private final Map<String, String> options;
    private final List<DataType> outputTypes;

    private List<String[]> rows;
    private int cursor;

    public MockScanHandler(Map<String, String> options, List<DataType> outputTypes) {
        this.options = options;
        this.outputTypes = outputTypes;
    }

    @Override
    public void open() throws IOException {
        rows = new ArrayList<>();
        String dataStr = resolveDataString();
        if (dataStr != null && !dataStr.isEmpty()) {
            for (String row : dataStr.split("\\|")) {
                if (!row.trim().isEmpty()) {
                    rows.add(row.split(",", -1));
                }
            }
        }
        cursor = 0;
    }

    @Override
    public Chunk nextChunk(BlockBuilder[] blockBuilders, int chunkLimit) {
        if (cursor >= rows.size()) {
            return null;
        }

        int count = 0;
        while (cursor < rows.size() && count < chunkLimit) {
            String[] row = rows.get(cursor);
            for (int col = 0; col < blockBuilders.length; col++) {
                if (col < row.length) {
                    writeValue(blockBuilders[col], outputTypes.get(col), row[col].trim());
                } else {
                    blockBuilders[col].appendNull();
                }
            }
            cursor++;
            count++;
        }

        if (count == 0) {
            return null;
        }

        Block[] blocks =
            new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        return new Chunk(blocks);
    }

    @Override
    public void close() {
        rows = null;
    }

    @Override
    public List<DataType> getOutputTypes() {
        return outputTypes;
    }

    private String resolveDataString() {
        // Try generic mock.data first (used by FILES())
        String data = options.get("mock.data");
        if (data != null) {
            return data;
        }

        // Try native_query data
        if (options.containsKey("native_query_sql")) {
            data = options.get("mock.native_query.data");
            if (data != null) {
                return data;
            }
        }

        // Scan for any mock.<db>.<table>.data key (for catalog table queries)
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("mock.") && key.endsWith(".data")
                && !key.equals("mock.data")
                && !key.equals("mock.native_query.data")) {
                return entry.getValue();
            }
        }

        return null;
    }

    private void writeValue(BlockBuilder builder, DataType type, String value) {
        if (value == null || value.isEmpty() || "null".equalsIgnoreCase(value)) {
            builder.appendNull();
            return;
        }
        try {
            if (type == DataTypes.IntegerType) {
                builder.writeInt(Integer.parseInt(value));
            } else if (type == DataTypes.LongType) {
                builder.writeLong(Long.parseLong(value));
            } else if (type == DataTypes.DoubleType) {
                builder.writeDouble(Double.parseDouble(value));
            } else {
                // Default: write as string (varchar, datetime, etc.)
                builder.writeString(value);
            }
        } catch (NumberFormatException e) {
            // Fallback to string for parse failures
            builder.writeString(value);
        }
    }
}
