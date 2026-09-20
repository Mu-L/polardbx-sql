package com.alibaba.polardbx.optimizer.external.connector;

import java.util.Collections;
import java.util.Map;

public class ConnectorTableStatistics {

    private final long rowCount;
    private final long dataSize;
    private final Map<String, ConnectorColumnStatistics> columnStats;

    public ConnectorTableStatistics(long rowCount, long dataSize,
                                    Map<String, ConnectorColumnStatistics> columnStats) {
        this.rowCount = rowCount;
        this.dataSize = dataSize;
        this.columnStats = columnStats != null
            ? Collections.unmodifiableMap(columnStats)
            : Collections.emptyMap();
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getDataSize() {
        return dataSize;
    }

    public Map<String, ConnectorColumnStatistics> getColumnStats() {
        return columnStats;
    }
}
