package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ConnectorTable {
    public final List<ColumnMeta> columns;
    public final List<String> partitionColumns;
    public final String comment;
    public final List<String> primaryKey;
    public final Map<String, String> properties;
    public final long estimatedRowCount;

    public ConnectorTable(List<ColumnMeta> columns, List<String> partitionColumns) {
        this.columns = Collections.unmodifiableList(columns);
        this.partitionColumns = partitionColumns != null
            ? Collections.unmodifiableList(partitionColumns) : Collections.emptyList();
        this.comment = null;
        this.primaryKey = Collections.emptyList();
        this.properties = Collections.emptyMap();
        this.estimatedRowCount = -1;
    }

    private ConnectorTable(Builder builder) {
        this.columns = Collections.unmodifiableList(builder.columns);
        this.partitionColumns = Collections.unmodifiableList(builder.partitionColumns);
        this.comment = builder.comment;
        this.primaryKey = Collections.unmodifiableList(builder.primaryKey);
        this.properties = Collections.unmodifiableMap(builder.properties);
        this.estimatedRowCount = builder.estimatedRowCount;
    }

    public static Builder builder(List<ColumnMeta> columns) {
        return new Builder(columns);
    }

    public static class Builder {
        private final List<ColumnMeta> columns;
        private List<String> partitionColumns = Collections.emptyList();
        private String comment;
        private List<String> primaryKey = Collections.emptyList();
        private Map<String, String> properties = Collections.emptyMap();
        private long estimatedRowCount = -1;

        private Builder(List<ColumnMeta> columns) {
            this.columns = columns;
        }

        public Builder partitionColumns(List<String> partitionColumns) {
            this.partitionColumns = partitionColumns;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder primaryKey(List<String> primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public Builder estimatedRowCount(long estimatedRowCount) {
            this.estimatedRowCount = estimatedRowCount;
            return this;
        }

        public ConnectorTable build() {
            return new ConnectorTable(this);
        }
    }
}
