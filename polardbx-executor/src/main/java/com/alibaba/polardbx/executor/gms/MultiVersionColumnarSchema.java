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

package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.schemaevolution.ColumnMetaWithTs;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.gms.DynamicColumnarManager.MAXIMUM_SIZE_OF_SNAPSHOT_CACHE;

public class MultiVersionColumnarSchema implements Purgeable {
    private final static Logger logger = LoggerFactory.getLogger(MultiVersionColumnarSchema.class);

    private final DynamicColumnarManager columnarManager;

    /**
     * Cache mapping from table id to multi-version columnar table meta
     */
    private final LoadingCache<Long, MultiVersionColumnarTableMeta> columnarTableMetas;

    public MultiVersionColumnarSchema(DynamicColumnarManager columnarManager) {
        this.columnarManager = columnarManager;

        this.columnarTableMetas = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_SIZE_OF_SNAPSHOT_CACHE)
            .build(new CacheLoader<Long, MultiVersionColumnarTableMeta>() {
                @Override
                public MultiVersionColumnarTableMeta load(@NotNull Long tableId) {
                    return new MultiVersionColumnarTableMeta(tableId);
                }
            });
    }

    public Long getTableId(long tso, String logicalSchema, String logicalTable, TableMeta tableMeta)
        throws ExecutionException {
        TableMeta.MultiVersionedId multiVersionedId =
            tableMeta.getTableMappingCache().get(Pair.of(logicalSchema.toLowerCase(), logicalTable.toLowerCase()));
        return multiVersionedId.getId(tso);
    }

    public List<Long> getTableIds(long tso, String logicalSchema, String logicalTable) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
            accessor.setConnection(connection);
            List<ColumnarTableMappingRecord> records = accessor.querySchemaTable(logicalSchema, logicalTable);

            return records.stream().map(r -> r.tableId).collect(Collectors.toList());
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SCHEMA, e.getCause(),
                String.format("Failed to fetch table id, tso: %d, schema name: %s, table name: %s",
                    tso, logicalSchema, logicalTable));
        }
    }

    private MultiVersionColumnarTableMeta getColumnarTableMeta(long tableId) {
        try {
            return columnarTableMetas.get(tableId);
        } catch (ExecutionException e) {
            columnarTableMetas.invalidate(tableId);
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SCHEMA, e.getCause(),
                String.format("Failed to fetch column meta of table, table id: %d", tableId));
        }
    }

    @NotNull
    public List<ColumnMeta> getColumnMetas(long schemaTso, long tableId) {
        MultiVersionColumnarTableMeta columnarTableMeta = getColumnarTableMeta(tableId);

        List<ColumnMeta> columnMetas = columnarTableMeta.getColumnMetaListByTso(schemaTso);
        if (columnMetas != null) {
            return columnMetas;
        }

        // Case: column meta cache missed
        Lock writeLock = columnarTableMeta.getLock();
        writeLock.lock();
        try {
            columnarTableMeta.loadUntilTso(schemaTso);
            return Objects.requireNonNull(columnarTableMeta.getColumnMetaListByTso(schemaTso));
        } finally {
            writeLock.unlock();
        }
    }

    @NotNull
    public Map<Long, Integer> getColumnIndexMap(long schemaTso, long tableId) {
        MultiVersionColumnarTableMeta columnarTableMeta = getColumnarTableMeta(tableId);

        Map<Long, Integer> columnIndex = columnarTableMeta.getFieldIdMapByTso(schemaTso);
        if (columnIndex != null) {
            return columnIndex;
        }

        // Case: column cache missed
        Lock writeLock = columnarTableMeta.getLock();
        writeLock.lock();
        try {
            columnarTableMeta.loadUntilTso(schemaTso);
            return Objects.requireNonNull(columnarTableMeta.getFieldIdMapByTso(schemaTso));
        } finally {
            writeLock.unlock();
        }
    }

    @NotNull
    public List<Long> getColumnFieldIdList(long versionId, long tableId) {
        MultiVersionColumnarTableMeta columnarTableMeta = getColumnarTableMeta(tableId);

        List<Long> fieldIdList = columnarTableMeta.getColumnFieldIdList(versionId);
        if (fieldIdList != null) {
            return fieldIdList;
        }

        // Case:columns cache missed
        Lock writeLock = columnarTableMeta.getLock();
        writeLock.lock();
        try {
            columnarTableMeta.loadUntilTso(Long.MAX_VALUE);
            return Objects.requireNonNull(columnarTableMeta.getColumnFieldIdList(versionId));
        } finally {
            writeLock.unlock();
        }
    }

    @NotNull
    public ColumnMetaWithTs getInitColumnMeta(long tableId, long fieldId) {
        MultiVersionColumnarTableMeta columnarTableMeta = getColumnarTableMeta(tableId);

        ColumnMetaWithTs columnMeta = columnarTableMeta.getInitColumnMeta(fieldId);
        if (columnMeta != null) {
            return columnMeta;
        }

        // Case: default column meta cache missed
        Lock writeLock = columnarTableMeta.getLock();
        writeLock.lock();
        try {
            columnarTableMeta.loadUntilTso(Long.MAX_VALUE);
            return Objects.requireNonNull(columnarTableMeta.getInitColumnMeta(fieldId));
        } finally {
            writeLock.unlock();
        }
    }

    public int @NotNull [] getPrimaryKeyColumns(long schemaTso, long tableId) {
        MultiVersionColumnarTableMeta columnarTableMeta = getColumnarTableMeta(tableId);

        int[] primaryKeyColumns = columnarTableMeta.getPrimaryKeyColumns(schemaTso);
        if (primaryKeyColumns != null) {
            return primaryKeyColumns;
        }

        Lock writeLock = columnarTableMeta.getLock();
        writeLock.lock();
        try {
            columnarTableMeta.loadUntilTso(Long.MAX_VALUE);
            return Objects.requireNonNull(columnarTableMeta.getPrimaryKeyColumns(schemaTso));
        } finally {
            writeLock.unlock();
        }
    }

    @NotNull
    public ConcurrentNavigableMap<Long, PartitionInfo> getPartitionInfos(long schemaTso, long tableId) {
        MultiVersionColumnarTableMeta columnarTableMeta = getColumnarTableMeta(tableId);

        ConcurrentNavigableMap<Long, PartitionInfo> partitionInfos = columnarTableMeta.getPartitionInfos(schemaTso);
        if (partitionInfos != null) {
            return partitionInfos;
        }

        Lock writeLock = columnarTableMeta.getLock();
        writeLock.lock();
        try {
            columnarTableMeta.loadUntilTso(schemaTso);
            return Objects.requireNonNull(columnarTableMeta.getPartitionInfos(schemaTso));
        } finally {
            writeLock.unlock();
        }
    }

    public ColumnarTableMeta getColumnarTableMetaByTso(long tableId, long tso) throws SQLException {
        MultiVersionColumnarTableMeta multiSchema = getColumnarTableMeta(tableId);
        Triple<String, String, String> triple;

        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarTableEvolutionAccessor accessor = new ColumnarTableEvolutionAccessor();
            accessor.setConnection(connection);
            List<ColumnarTableEvolutionRecord> records = accessor.queryTableIdAndLessThanCommitTs(tableId, tso);
            if (records != null && !records.isEmpty()) {
                ColumnarTableEvolutionRecord record = records.get(0);
                triple = Triple.of(record.tableSchema, record.tableName, record.indexName);
            } else {
                throw new RuntimeException("cci not found");
            }
        }

        String tableSchema = triple.getLeft();
        String tableName = triple.getMiddle();
        String indexName = triple.getRight();

        // always get latest
        Lock writeLock = multiSchema.getLock();
        writeLock.lock();
        try {
            multiSchema.loadUntilTso(tso);
        } finally {
            writeLock.unlock();
        }

        PartitionInfo partitionInfo = multiSchema.getPartitionInfoByAnyTso(tso);
        List<ColumnMeta> columnMetas = multiSchema.getColumnMetasByAnyTso(tso);
        List<String> primaryKeys = multiSchema.getPrimaryKeyColumnsByAnyTso(tso);
        List<String> sortKeys = multiSchema.getSortKeyColumnsByAnyTso(tso);
        Map<String, String> options = getColumnarIndexOptions(tableSchema, indexName);

        Objects.requireNonNull(columnMetas, "columns must be specified.");
        Objects.requireNonNull(primaryKeys, "columnTable must be specified.");
        Objects.requireNonNull(sortKeys, "columnTable must be specified.");
        Preconditions.checkArgument(!primaryKeys.isEmpty(), "the length of primaryKes > 0.");
        Preconditions.checkArgument(!sortKeys.isEmpty(), "the length of sortKeys > 0.");

        // first is TSO_COLUMN, second is POSITION_COLUMN
        columnMetas = columnMetas.subList(2, columnMetas.size());

        ColumnarTableMeta.Builder builder = new ColumnarTableMeta.Builder();

        builder.schemaName(tableSchema).
            tableName(tableName).
            indexName(indexName).
            schemaTso(tso).
            partitionInfo(partitionInfo).
            columns(columnMetas).
            primaryKeys(primaryKeys).
            sortKeys(sortKeys).
            options(options).
            status(getColumnarStatus(tableId));
        return builder.build();
    }

    private Map<String, String> getColumnarIndexOptions(String schemaName,
                                                        String indexName) throws SQLException {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ColumnarTableEvolutionAccessor accessor = new ColumnarTableEvolutionAccessor();
            accessor.setConnection(metaDbConn);
            List<ColumnarTableEvolutionRecord> records =
                accessor.querySchemaIndexLatest(schemaName, indexName);
            if (CollectionUtils.isEmpty(records)) {
                return null;
            }
            return records.get(0).options;
        }
    }

    private String getColumnarStatus(long tableId) throws SQLException {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
            accessor.setConnection(metaDbConn);
            List<ColumnarTableMappingRecord> records = accessor.queryTableId(tableId);
            if (CollectionUtils.isEmpty(records)) {
                return null;
            }
            return records.get(0).status;
        }
    }

    @Override
    public void purge(long tso) {
        // TODO(siyun):
        throw new NotSupportException("purge columnar schema not supported now!");
    }
}
