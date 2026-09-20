package com.alibaba.polardbx.optimizer.external.connector;

import java.util.Collections;
import java.util.Map;

public class ConnectorPartitionInfo {

    private final String name;
    private final Map<String, String> values;
    private final long rowCount;
    private final long dataSize;

    public ConnectorPartitionInfo(String name, Map<String, String> values,
                                  long rowCount, long dataSize) {
        this.name = name;
        this.values = values != null
            ? Collections.unmodifiableMap(values)
            : Collections.emptyMap();
        this.rowCount = rowCount;
        this.dataSize = dataSize;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getValues() {
        return values;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getDataSize() {
        return dataSize;
    }
}
