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

package com.alibaba.polardbx.executor.columnar.pruning;

import com.alibaba.polardbx.common.orc.FastColumnStatistics;
import com.alibaba.polardbx.common.orc.FastDateColumnStatistics;
import com.alibaba.polardbx.common.orc.FastLongColumnStatistics;
import com.alibaba.polardbx.common.orc.FastStringColumnStatistics;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruner;
import com.alibaba.polardbx.common.orc.PreheatFileMeta;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.engine.FileStoreStatistics.CACHE_STATS_FIELD_COUNT;

/**
 * @author fangwu
 */
public class ColumnarPruneManager {

    private static final String PRUNER_CACHE_NAME = "PRUNER_CACHE";

    private static final int PRUNER_CACHE_MAX_ENTRY = 1024 * 256;

    private static final int PRUNER_CACHE_TTL_HOURS = 12;

    private static final AtomicLong PRUNER_CACHE_SIZE_IN_BYTES = new AtomicLong(0);

    private static final Cache<Path, IndexPruner> PRUNER_CACHE =
        CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(PRUNER_CACHE_MAX_ENTRY)
            .expireAfterAccess(PRUNER_CACHE_TTL_HOURS, TimeUnit.HOURS)
            .removalListener(removalNotification -> {
                PRUNER_CACHE_SIZE_IN_BYTES.addAndGet(-((IndexPruner) removalNotification.getValue()).getSizeInBytes());
            })
            .build();

    public static IndexPruner getIndexPruner(Path targetFile,
                                             PreheatFileMeta preheat,
                                             List<ColumnMeta> columns, List<OrderByOption> sortKeys,
                                             List<Integer> orcIndexes,
                                             boolean enableOssCompatible)
        throws ExecutionException {

        // use fast column statistics array in prior
        if (preheat.getFastColumnStatisticsArray() != null) {
            return PRUNER_CACHE.get(targetFile,
                () -> getIndexPrunerFromBinaryMeta(targetFile, preheat, columns, sortKeys, orcIndexes,
                    enableOssCompatible));
        }

        return PRUNER_CACHE.get(targetFile, () -> {
            IndexPruner.IndexPrunerBuilder builder =
                new IndexPruner.IndexPrunerBuilder(targetFile.toString(), enableOssCompatible);
            int rgNum = 0;
            int clusteringKeyPosition = sortKeys.get(0).getIndex();

            // for multi sort key
            IntArraySet sortKeySet = new IntArraySet(sortKeys.size());
            sortKeys.stream().forEach(sortKey -> sortKeySet.add(sortKey.getIndex()));

            for (int i = 0; i < preheat.getStripeSize(); i++) {

                // for single-column sort key.
                // init sort key index if exist
                if (clusteringKeyPosition != -1) {
                    // alter sort key column is not supported
                    Preconditions.checkArgument(orcIndexes.get(clusteringKeyPosition) != null);
                    OrcProto.RowIndex rgIndex = preheat.getRowGroupIndex(i, orcIndexes.get(clusteringKeyPosition));
                    builder.setSortKeyColId(clusteringKeyPosition);
                    builder.setSortKeyDataType(columns.get(clusteringKeyPosition).getDataType());
                    builder.setSortKeyAsc(sortKeys.get(0).isAsc());
                    // init sort key index
                    for (OrcProto.RowIndexEntry rowIndexEntry : rgIndex.getEntryList()) {
                        rgNum++;
                        builder.appendSortKeyIndex(rowIndexEntry.getStatistics());
                    }
                } else {
                    // error log
                    ModuleLogInfo.getInstance().logRecord(
                        Module.COLUMNAR_PRUNE,
                        LogPattern.UNEXPECTED,
                        new String[] {"sort key load", "neg clustering key position " + clusteringKeyPosition},
                        LogLevel.CRITICAL);
                }

                // for multi-column sort key.
                if (sortKeySet.size() > 1) {
                    for (int sortKeyOptionIndex = 0; sortKeyOptionIndex < sortKeys.size(); sortKeyOptionIndex++) {
                        int sortKeyColumnIndex = sortKeys.get(sortKeyOptionIndex).getIndex();

                        if (sortKeyColumnIndex == -1) {
                            // error log
                            ModuleLogInfo.getInstance().logRecord(
                                Module.COLUMNAR_PRUNE,
                                LogPattern.UNEXPECTED,
                                new String[] {"sort key load", "neg clustering key position " + clusteringKeyPosition},
                                LogLevel.CRITICAL);

                            continue;
                        }

                        ColumnMeta cm = columns.get(sortKeyColumnIndex);
                        // get orc column index by table meta column index
                        Integer orcIndex = orcIndexes.get(sortKeyColumnIndex);
                        if (orcIndex == null) {
                            continue;
                        }

                        DataType dataType = cm.getDataType();
                        if (DataTypeUtil.isUnderIntType(dataType) ||
                            DataTypeUtil.equalsSemantically(DataTypes.IntegerType, dataType) ||
                            DataTypeUtil.equalsSemantically(DataTypes.LongType, dataType) ||
                            DataTypeUtil.equalsSemantically(DataTypes.DateType, dataType) ||
                            DataTypeUtil.equalsSemantically(DataTypes.DatetimeType, dataType) ||
                            DataTypeUtil.equalsSemantically(DataTypes.VarcharType, dataType) ||
                            DataTypeUtil.equalsSemantically(DataTypes.CharType, dataType)) {
                            builder.appendMultiSortKeyColumn(sortKeyColumnIndex, dataType);
                            OrcProto.RowIndex rgIndex = preheat.getRowGroupIndex(i, orcIndex);
                            for (OrcProto.RowIndexEntry rowIndexEntry : rgIndex.getEntryList()) {
                                OrcProto.ColumnStatistics columnStatistics = rowIndexEntry.getStatistics();
                                // multi sort key index build
                                if (columnStatistics.hasIntStatistics()) {
                                    // int
                                    builder.appendMultiSortKeyColumn(sortKeyColumnIndex,
                                        rowIndexEntry.getStatistics().getIntStatistics());
                                } else if (columnStatistics.hasDateStatistics()) {
                                    // date
                                    builder.appendMultiSortKeyColumn(sortKeyColumnIndex,
                                        rowIndexEntry.getStatistics().getDateStatistics());
                                } else if (columnStatistics.hasStringStatistics()) {
                                    // char / varchar
                                    builder.appendMultiSortKeyColumn(sortKeyColumnIndex,
                                        rowIndexEntry.getStatistics().getStringStatistics());
                                }
                                if (columnStatistics.hasHasNull()) {
                                    builder.appendMultiSortKeyColumn(sortKeyColumnIndex,
                                        rowIndexEntry.getStatistics().getHasNull());
                                }

                            }
                        }

                    }
                }

                // for zone map.
                for (int m = 0; m < columns.size(); m++) {
                    // skip sort key index
                    if (m == clusteringKeyPosition || sortKeySet.contains(m)) {
                        continue;
                    }
                    ColumnMeta cm = columns.get(m);
                    // get orc column index by table meta column index
                    Integer orcIndex = orcIndexes.get(m);
                    if (orcIndex == null) {
                        continue;
                    }

                    DataType dataType = cm.getDataType();// zone map
                    if (DataTypeUtil.equalsSemantically(DataTypes.IntegerType, dataType) ||
                        DataTypeUtil.equalsSemantically(DataTypes.LongType, dataType) ||
                        DataTypeUtil.equalsSemantically(DataTypes.DateType, dataType) ||
                        DataTypeUtil.equalsSemantically(DataTypes.DatetimeType, dataType)) {
                        builder.appendZoneMap(m, dataType);
                        OrcProto.RowIndex rgIndex = preheat.getRowGroupIndex(i, orcIndex);
                        for (OrcProto.RowIndexEntry rowIndexEntry : rgIndex.getEntryList()) {
                            OrcProto.ColumnStatistics columnStatistics = rowIndexEntry.getStatistics();
                            // zone map index build
                            if (DynamicConfig.getInstance().enableZoneMapPrune()) {
                                if (columnStatistics.hasIntStatistics()) {
                                    builder.appendZoneMap(m, rowIndexEntry.getStatistics().getIntStatistics());
                                } else if (columnStatistics.hasDateStatistics()) {
                                    builder.appendZoneMap(m, rowIndexEntry.getStatistics().getDateStatistics());
                                }
                                if (columnStatistics.hasHasNull()) {
                                    builder.appendZoneMap(m, rowIndexEntry.getStatistics().getHasNull());
                                }
                            }

                        }
                    } else if (DataTypes.DecimalType.equals(dataType)) {
                        // TODO build zone map index for DecimalType
                    } else {
                        continue;
                    }
                }

                // bitmap index was built by stripe index

                // TODO bloom filter build
                builder.stripeEnd();
            }

            builder.setRgNum(rgNum);
            IndexPruner pruner = builder.build();
            PRUNER_CACHE_SIZE_IN_BYTES.getAndAdd(pruner.getSizeInBytes());
            return pruner;
        });
    }

    public static IndexPruner getIndexPrunerFromBinaryMeta(Path targetFile,
                                                           PreheatFileMeta preheat,
                                                           List<ColumnMeta> columns, List<OrderByOption> sortKeys,
                                                           List<Integer> orcIndexes,
                                                           boolean enableOssCompatible) {
        IndexPruner.IndexPrunerBuilder builder =
            new IndexPruner.IndexPrunerBuilder(targetFile.toString(), enableOssCompatible);
        int rgNum = 0;
        int clusteringKeyPosition = sortKeys.get(0).getIndex();

        // for multi sort key
        IntArraySet sortKeySet = new IntArraySet(sortKeys.size());
        sortKeys.stream().forEach(sortKey -> sortKeySet.add(sortKey.getIndex()));

        FastColumnStatistics[] fastColumnStatisticsArray = preheat.getFastColumnStatisticsArray();
        for (int i = 0; i < preheat.getStripeSize(); i++) {

            // for single-column sort key.
            // init sort key index if exist
            if (clusteringKeyPosition != -1) {
                // alter sort key column is not supported
                Preconditions.checkArgument(orcIndexes.get(clusteringKeyPosition) != null);

                int columnId = orcIndexes.get(clusteringKeyPosition);
                FastColumnStatistics fastColumnStatistics = fastColumnStatisticsArray[columnId];

                final int startRowGroupId = fastColumnStatistics.getAccumulatedRowGroupCountPerStripe()[i];
                final int endRowGroupId = i + 1 >= preheat.getStripeSize()
                    ? fastColumnStatistics.getTotalRowGroupCount()
                    : fastColumnStatistics.getAccumulatedRowGroupCountPerStripe()[i + 1];

                builder.setSortKeyColId(clusteringKeyPosition);
                builder.setSortKeyDataType(columns.get(clusteringKeyPosition).getDataType());
                builder.setSortKeyAsc(sortKeys.get(0).isAsc());

                // init sort key index
                if (fastColumnStatistics instanceof FastStringColumnStatistics) {
                    for (int rowGroupIndex = startRowGroupId; rowGroupIndex < endRowGroupId; rowGroupIndex++) {
                        String min = fastColumnStatistics.getMinString(rowGroupIndex);
                        String max = fastColumnStatistics.getMaxString(rowGroupIndex);
                        builder.appendSortKeyIndex(min, max);
                        rgNum++;
                    }
                } else if (fastColumnStatistics instanceof FastLongColumnStatistics
                    || fastColumnStatistics instanceof FastDateColumnStatistics) {
                    for (int rowGroupIndex = startRowGroupId; rowGroupIndex < endRowGroupId; rowGroupIndex++) {
                        long min = fastColumnStatistics.getMinLong(rowGroupIndex);
                        long max = fastColumnStatistics.getMaxLong(rowGroupIndex);
                        builder.appendSortKeyIndex(min, max);
                        rgNum++;
                    }
                } else {
                    // Sort-key index is not buildable for this FastColumnStatistics variant
                    // (e.g. NormalColumnStatistics for DECIMAL/TIMESTAMP/BINARY, or
                    // FastDoubleColumnStatistics for FLOAT/DOUBLE which does not expose long
                    // min/max nor override hasNull). Skip sort-key index construction so the
                    // query falls back to full scan + filter. LongSortKeyIndex.checkSupport
                    // rejects these types anyway, so no pruning capability is lost. Keep rgNum
                    // accurate so downstream indexes (zone-map / multi-sort / bitmap) stay
                    // correctly sized.
                    rgNum += endRowGroupId - startRowGroupId;
                }
            } else {
                // error log
                ModuleLogInfo.getInstance().logRecord(
                    Module.COLUMNAR_PRUNE,
                    LogPattern.UNEXPECTED,
                    new String[] {"sort key load", "neg clustering key position " + clusteringKeyPosition},
                    LogLevel.CRITICAL);
            }

            // for multi-column sort key.
            if (sortKeySet.size() > 1) {
                for (int sortKeyOptionIndex = 0; sortKeyOptionIndex < sortKeys.size(); sortKeyOptionIndex++) {
                    int sortKeyColumnIndex = sortKeys.get(sortKeyOptionIndex).getIndex();

                    if (sortKeyColumnIndex == -1) {
                        // error log
                        ModuleLogInfo.getInstance().logRecord(
                            Module.COLUMNAR_PRUNE,
                            LogPattern.UNEXPECTED,
                            new String[] {"sort key load", "neg clustering key position " + clusteringKeyPosition},
                            LogLevel.CRITICAL);

                        continue;
                    }

                    ColumnMeta cm = columns.get(sortKeyColumnIndex);
                    // get orc column index by table meta column index
                    Integer orcIndex = orcIndexes.get(sortKeyColumnIndex);
                    if (orcIndex == null) {
                        continue;
                    }

                    DataType dataType = cm.getDataType();
                    if (DataTypeUtil.isUnderIntType(dataType) ||
                        DataTypeUtil.equalsSemantically(DataTypes.IntegerType, dataType) ||
                        DataTypeUtil.equalsSemantically(DataTypes.LongType, dataType) ||
                        DataTypeUtil.equalsSemantically(DataTypes.DateType, dataType) ||
                        DataTypeUtil.equalsSemantically(DataTypes.DatetimeType, dataType) ||
                        DataTypeUtil.equalsSemantically(DataTypes.VarcharType, dataType) ||
                        DataTypeUtil.equalsSemantically(DataTypes.CharType, dataType)) {
                        builder.appendMultiSortKeyColumn(sortKeyColumnIndex, dataType);

                        FastColumnStatistics fastColumnStatistics = fastColumnStatisticsArray[orcIndex];
                        final int startRowGroupId = fastColumnStatistics.getAccumulatedRowGroupCountPerStripe()[i];
                        final int endRowGroupId = i + 1 >= preheat.getStripeSize()
                            ? fastColumnStatistics.getTotalRowGroupCount()
                            : fastColumnStatistics.getAccumulatedRowGroupCountPerStripe()[i + 1];

                        // handle null
                        for (int rowGroupIndex = startRowGroupId; rowGroupIndex < endRowGroupId; rowGroupIndex++) {
                            boolean hasNull = fastColumnStatistics.hasNull(rowGroupIndex);
                            builder.appendMultiSortKeyColumn(sortKeyColumnIndex, hasNull);
                        }

                        // handle min-max
                        if (fastColumnStatistics instanceof FastStringColumnStatistics) {
                            // char / varchar
                            for (int rowGroupIndex = startRowGroupId; rowGroupIndex < endRowGroupId; rowGroupIndex++) {
                                String min = fastColumnStatistics.getMinString(rowGroupIndex);
                                String max = fastColumnStatistics.getMaxString(rowGroupIndex);
                                builder.appendMultiSortKeyColumn(sortKeyColumnIndex, min, max);
                            }
                        } else if (fastColumnStatistics instanceof FastDateColumnStatistics) {
                            // date
                            for (int rowGroupIndex = startRowGroupId; rowGroupIndex < endRowGroupId; rowGroupIndex++) {
                                int min = fastColumnStatistics.getMinInt(rowGroupIndex);
                                int max = fastColumnStatistics.getMaxInt(rowGroupIndex);
                                builder.appendMultiSortKeyColumn(sortKeyColumnIndex, min, max);
                            }
                        } else {
                            // int/long
                            for (int rowGroupIndex = startRowGroupId; rowGroupIndex < endRowGroupId; rowGroupIndex++) {
                                long min = fastColumnStatistics.getMinLong(rowGroupIndex);
                                long max = fastColumnStatistics.getMaxLong(rowGroupIndex);
                                builder.appendMultiSortKeyColumn(sortKeyColumnIndex, min, max);
                            }
                        }
                    }

                }
            }

            // for zone map.
            for (int m = 0; m < columns.size(); m++) {
                // skip sort key index
                if (m == clusteringKeyPosition || sortKeySet.contains(m)) {
                    continue;
                }
                ColumnMeta cm = columns.get(m);
                // get orc column index by table meta column index
                Integer orcIndex = orcIndexes.get(m);
                if (orcIndex == null) {
                    continue;
                }

                DataType dataType = cm.getDataType();// zone map
                if (DataTypeUtil.equalsSemantically(DataTypes.IntegerType, dataType) ||
                    DataTypeUtil.equalsSemantically(DataTypes.LongType, dataType) ||
                    DataTypeUtil.equalsSemantically(DataTypes.DateType, dataType) ||
                    DataTypeUtil.equalsSemantically(DataTypes.DatetimeType, dataType)) {
                    builder.appendZoneMap(m, dataType);

                    if (DynamicConfig.getInstance().enableZoneMapPrune()) {
                        FastColumnStatistics fastColumnStatistics = fastColumnStatisticsArray[orcIndex];
                        final int startRowGroupId = fastColumnStatistics.getAccumulatedRowGroupCountPerStripe()[i];
                        final int endRowGroupId = i + 1 >= preheat.getStripeSize()
                            ? fastColumnStatistics.getTotalRowGroupCount()
                            : fastColumnStatistics.getAccumulatedRowGroupCountPerStripe()[i + 1];

                        // handle has_null
                        for (int rowGroupIndex = startRowGroupId; rowGroupIndex < endRowGroupId; rowGroupIndex++) {
                            boolean hasNull = fastColumnStatistics.hasNull(rowGroupIndex);
                            builder.appendZoneMap(m, hasNull);
                        }

                        // zone map index build
                        if (fastColumnStatistics instanceof FastDateColumnStatistics) {
                            for (int rowGroupIndex = startRowGroupId; rowGroupIndex < endRowGroupId; rowGroupIndex++) {
                                int min = fastColumnStatistics.getMinInt(rowGroupIndex);
                                int max = fastColumnStatistics.getMaxInt(rowGroupIndex);
                                builder.appendZoneMap(m, min, max);
                            }
                        } else if (fastColumnStatistics instanceof FastLongColumnStatistics) {
                            for (int rowGroupIndex = startRowGroupId; rowGroupIndex < endRowGroupId; rowGroupIndex++) {
                                long min = fastColumnStatistics.getMinLong(rowGroupIndex);
                                long max = fastColumnStatistics.getMaxLong(rowGroupIndex);
                                builder.appendZoneMap(m, min, max);
                            }
                        }
                    }

                }
            }
            builder.stripeEnd();
        }

        builder.setRgNum(rgNum);
        IndexPruner pruner = builder.build();
        PRUNER_CACHE_SIZE_IN_BYTES.getAndAdd(pruner.getSizeInBytes());
        return pruner;
    }

    public static byte[][] getCacheStat() {
        CacheStats cacheStats = PRUNER_CACHE.stats();

        byte[][] results = new byte[CACHE_STATS_FIELD_COUNT][];
        int pos = 0;
        results[pos++] = PRUNER_CACHE_NAME.getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(PRUNER_CACHE.size()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(cacheStats.hitCount()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(cacheStats.missCount()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = "IN MEMORY".getBytes();
        results[pos++] = new StringBuilder().append(PRUNER_CACHE_TTL_HOURS).append(" h").toString().getBytes();
        results[pos++] = String.valueOf(PRUNER_CACHE_MAX_ENTRY).getBytes();
        results[pos++] = new StringBuilder().append(-1).append(" BYTES").toString().getBytes();
        return results;
    }

    public static long getPruneCacheUsedSize() {
        return PRUNER_CACHE_SIZE_IN_BYTES.get();
    }
}
