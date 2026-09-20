/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
public class ColumnarTableMeta {
    private final String schemaName;
    private final String tableName;
    private final String indexName;
    private final long schemaTso;
    private final PartitionInfo partitionInfo;

    private final List<ColumnMeta> columns;
    private final List<String> primaryKeys;
    private final List<String> sortKeys;

    private final Map<String, String> options;
    private final String status;

    public ColumnarTableMeta(String schemaName, String tableName, String indexName, long schemaTso,
                             PartitionInfo partitionInfo, List<ColumnMeta> columns, List<String> primaryKeys,
                             List<String> sortKeys, Map<String, String> options, String status) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexName = indexName;
        this.schemaTso = schemaTso;
        this.partitionInfo = partitionInfo;
        this.columns = columns;
        this.primaryKeys = primaryKeys;
        this.sortKeys = sortKeys;
        this.options = options;
        this.status = status;
    }

    public static final class Builder {
        private String schemaName;
        private String tableName;
        private String indexName;
        private long schemaTso;
        private PartitionInfo partitionInfo;
        private List<ColumnMeta> columns;
        private List<String> primaryKeys;
        private List<String> sortKeys;
        private Map<String, String> options;
        private String status;

        public Builder() {
        }

        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder schemaTso(long schemaTso) {
            this.schemaTso = schemaTso;
            return this;
        }

        public Builder partitionInfo(PartitionInfo partitionInfo) {
            this.partitionInfo = partitionInfo;
            return this;
        }

        public Builder columns(List<ColumnMeta> columns) {
            this.columns = columns;
            return this;
        }

        public Builder primaryKeys(List<String> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        public Builder sortKeys(List<String> sortKeys) {
            this.sortKeys = sortKeys;
            return this;
        }

        public Builder options(Map<String, String> options) {
            this.options = options;
            return this;
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public ColumnarTableMeta build() {
            return new ColumnarTableMeta(schemaName, tableName, indexName, schemaTso, partitionInfo, columns,
                primaryKeys, sortKeys, options, status);
        }
    }
}
