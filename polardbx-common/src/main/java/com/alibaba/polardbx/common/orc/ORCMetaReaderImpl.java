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

package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CachingFileSystem;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.DataReader;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.MetadataDeserializeUtils;
import org.apache.orc.impl.MetadataSerializeUtils;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.RecordReaderUtils;
import org.apache.orc.impl.RowIndexUtilBase;
import org.apache.orc.impl.reader.StripePlanner;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jol.info.GraphLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ORCMetaReaderImpl implements ORCMetaReader {
    private static final Logger LOG = LoggerFactory.getLogger(ORCMetaReaderImpl.class);

    private final Configuration configuration;
    private final FileSystem preheatFileSystem;
    private final Set<Integer> sortKeyColumns;

    public ORCMetaReaderImpl(Configuration configuration, FileSystem fileSystem, Set<Integer> sortKeyColumns) {
        this.configuration = configuration;
        this.preheatFileSystem = fileSystem;
        this.sortKeyColumns = sortKeyColumns;
    }

    @Override
    public PreheatFileMeta preheat(Path path) throws IOException {
        PreheatFileMeta result = new PreheatFileMeta();
        // 0. build and cache file reader
        ReaderImpl fileReader = null;
        try {
            // BugFix: This method is called by the preheat cache's load process.
            // It should not use the filesystem's getStatus method to obtain file status,
            // as it may recursively call the preheat cache.
            FileStatus fileStatus;
            if (preheatFileSystem instanceof OSSFileSystem) {
                fileStatus = ((OSSFileSystem) preheatFileSystem).getFileStatusImpl(path);
            } else if (preheatFileSystem instanceof CachingFileSystem
                && ((CachingFileSystem) preheatFileSystem).getDataTier() instanceof OSSFileSystem) {
                fileStatus = ((OSSFileSystem) ((CachingFileSystem) preheatFileSystem).getDataTier())
                    .getFileStatusImpl(path);
            } else {
                fileStatus = preheatFileSystem.getFileStatus(path);
            }
            result.setFileStatus(fileStatus);

            fileReader = (ReaderImpl) OrcFile.createReader(path,
                OrcFile.readerOptions(configuration).filesystem(preheatFileSystem)
            );

            // 1. extract orc tail and cache it
            OrcTail orcTail = fileReader.getOrcTail();

            // 2. build and cache each stripe metadata in file.
            Map<Long, PreheatStripeMeta> preheatContextMap;
            OrcProto.RedundantMetadata redundantMetadata = orcTail.getFooter().getRedundantMetadata();
            OrcProto.BinaryMetadata binaryMetadata = orcTail.getFooter().getBinaryMetadata();

            if (DynamicConfig.getInstance().useRedundantMetaData()
                && redundantMetadata != null
                && redundantMetadata.getStripesCount() > 0) {
                TypeDescription schema = orcTail.getSchema();
                preheatContextMap = preheatStripeFromRedundantMetadata(redundantMetadata, schema);

                // remove redundant metadata from footer.
                OrcProto.Footer newFooter = OrcProto.Footer
                    .newBuilder(orcTail.getFooter())
                    .clearRedundantMetadata().build();

                OrcProto.FileTail newFileTail = OrcProto.FileTail
                    .newBuilder(orcTail.getFileTail())
                    .clearFooter()
                    .setFooter(newFooter)
                    .build();

                orcTail.resetTail(newFileTail);
                orcTail.clearForPreheat();
                result.setPreheatTail(orcTail);

                result.setPreheatStripes(preheatContextMap);

            } else if (DynamicConfig.getInstance().useRedundantMetaData()
                && DynamicConfig.getInstance().useBinaryMetaData()
                && binaryMetadata.getStripeSize() > 0) {

                // remove redundant metadata from footer.
                OrcProto.Footer newFooter = OrcProto.Footer
                    .newBuilder(orcTail.getFooter())
                    .clearBinaryMetadata().build();

                OrcProto.FileTail newFileTail = OrcProto.FileTail
                    .newBuilder(orcTail.getFileTail())
                    .clearFooter()
                    .setFooter(newFooter)
                    .build();
                orcTail.resetTail(newFileTail);
                orcTail.clearForPreheat();
                result.setPreheatTail(orcTail);

                // parse binary meta data.
                TypeDescription schema = orcTail.getSchema();
                try {
                    preheatStripeFromBinaryMetadata(path, binaryMetadata, schema, result);
                } catch (BinaryMetadataCorruptException ex) {
                    // N4 defensive fallback: on any BinaryMetadata invariant violation,
                    // degrade to the legacy preheatStripe path. Report key locator fields
                    // so that production incidents stay diagnosable while the read path continues.
                    LOG.warn("BinaryMetadata corruption detected, fallback to preheatStripe: {}", ex.getMessage(), ex);
                    result.setUseBinaryMeta(false);
                    result.setFastPositionIndexArray(null);
                    result.setFastColumnEncodingIndexArray(null);
                    result.setFastStreamIndexArray(null);
                    result.setFastColumnStatisticsArray(null);
                    result.setWriterTimeZoneArray(null);
                    preheatContextMap = preheatStripe(path, fileReader, preheatFileSystem);
                    result.setPreheatStripes(preheatContextMap);
                }
            } else {
                // compatible for old version without redundant metadata.
                preheatContextMap = preheatStripe(path, fileReader, preheatFileSystem);
                result.setPreheatStripes(preheatContextMap);

                orcTail.clearForPreheat();
                result.setPreheatTail(orcTail);
            }

        } finally {
            // prevent from IO resource leak
            if (fileReader != null) {
                fileReader.close();
            }
        }

        if (result != null) {
            long memorySize;
            if (DynamicConfig.getInstance().enablePreheatMemoryPreciseCount()) {
                memorySize = GraphLayout.parseInstance(result).totalSize();
            } else {
                memorySize = PreheatMetaMemoryUtils.estimatedMemorySizeOf(result);
            }
            result.setMemorySize(memorySize);
        }

        return result;
    }

    private Map<Long, PreheatStripeMeta> preheatStripeFromRedundantMetadata(
        OrcProto.RedundantMetadata redundantMetadata, TypeDescription schema) {
        boolean enableZoneMapPrune = DynamicConfig.getInstance().enableZoneMapPrune();

        Map<Long, PreheatStripeMeta> result = new ConcurrentHashMap<>();

        //  redundant meta store all row-index and footer for each stripe.
        List<OrcProto.RedundantStripeMetadata> redundantStripeMetadataList = redundantMetadata.getStripesList();
        for (int i = 0; i < redundantStripeMetadataList.size(); i++) {
            OrcProto.RedundantStripeMetadata redundantStripeMetadata = redundantStripeMetadataList.get(i);

            // Get stripe footer from redundant meta in file tail.
            OrcProto.StripeFooterWithId stripeFooterWithId = redundantStripeMetadata.getStripeFooter();
            int stripeNumber = stripeFooterWithId.getStripe();
            OrcProto.StripeFooter stripeFooter = stripeFooterWithId.getStripeFooter();

            List<OrcProto.RowIndexWithColumn> rowIndexWithColumnList = redundantStripeMetadata.getRowIndexList();

            int typeCount = schema.getMaximumId() + 1;
            OrcIndex orcIndex = new OrcIndex(new OrcProto.RowIndex[typeCount],
                new OrcProto.Stream.Kind[typeCount],
                new OrcProto.BloomFilterIndex[typeCount],
                new OrcProto.BitmapIndex[typeCount]);
            OrcProto.RowIndex[] indexes = orcIndex.getRowGroupIndex();

            for (int index = 0; index < rowIndexWithColumnList.size(); index++) {
                OrcProto.RowIndexWithColumn rowIndexWithColumn = rowIndexWithColumnList.get(index);
                int column = rowIndexWithColumn.getColumn();
                OrcProto.RowIndex rowIndex = rowIndexWithColumn.getRowIndex();

                // if this column is not in sort key.
                if (sortKeyColumns != null && !sortKeyColumns.contains(column)) {
                    // simplified row index
                    OrcProto.RowIndex.Builder rowIndexBuilder = OrcProto.RowIndex.newBuilder();

                    for (OrcProto.RowIndexEntry rowIndexEntry : rowIndex.getEntryList()) {
                        OrcProto.RowIndexEntry.Builder entryBuilder = OrcProto.RowIndexEntry.newBuilder(rowIndexEntry);
                        entryBuilder.setUnknownFields(null);

                        if (rowIndexEntry.hasStatistics()) {
                            if (!enableZoneMapPrune) {
                                entryBuilder.clearStatistics();
                            } else {
                                // Allow zone map but some column statistics types are not supported.
                                OrcProto.ColumnStatistics.Builder statsBuilder = entryBuilder.getStatisticsBuilder();

                                // preserve intStatistics，and clear other type.
                                statsBuilder.clearDoubleStatistics();
                                statsBuilder.clearStringStatistics();
                                statsBuilder.clearBucketStatistics();

                                statsBuilder.clearDecimalStatistics();
                                statsBuilder.clearBinaryStatistics();
                                statsBuilder.clearTimestampStatistics();
                                statsBuilder.clearCollectionStatistics();
                                statsBuilder.setUnknownFields(null);

                                // clear bytes on disk.
                                statsBuilder.clearBytesOnDisk();
                            }

                        }
                        OrcProto.RowIndexEntry clearedEntry = entryBuilder.build();
                        rowIndexBuilder.addEntry(clearedEntry);
                    }

                    // rebuild row index without unsupported statistics.
                    rowIndex = rowIndexBuilder.build();
                }

                indexes[column] = rowIndex;
            }

            PreheatStripeMeta preheatStripeMeta = new PreheatStripeMeta(
                stripeNumber, orcIndex, stripeFooter);
            result.put((long) stripeNumber, preheatStripeMeta);
        }

        return result;
    }

    private void preheatStripeFromBinaryMetadata(Path path,
                                                 OrcProto.BinaryMetadata binaryMetadata,
                                                 TypeDescription schema,
                                                 PreheatFileMeta preheatFileMeta) throws IOException {
        boolean enableZoneMapPrune = DynamicConfig.getInstance().enableZoneMapPrune();

        Map<Long, PreheatStripeMeta> result = new ConcurrentHashMap<>();

        final int totalRowGroupCount = binaryMetadata.getTotalRowGroupCount();
        List<Integer> accumulatedRowGroupCountPerStripeList = binaryMetadata.getAccumulatedRowGroupCountPerStripeList();
        int[] accumulatedRowGroupCountPerStripeArray = new int[accumulatedRowGroupCountPerStripeList.size()];
        for (int i = 0; i < accumulatedRowGroupCountPerStripeList.size(); i++) {
            accumulatedRowGroupCountPerStripeArray[i] = accumulatedRowGroupCountPerStripeList.get(i);
        }

        // ============ N4 defensive pre-check: BinaryMetadata global invariants ============
        // Any violation below throws BinaryMetadataCorruptException, which is caught at the
        // preheat entry and falls back to preheatStripe.
        final String pathStr = path == null ? "<unknown>" : path.toString();
        final int declaredStripeSize = binaryMetadata.getStripeSize();
        if (declaredStripeSize <= 0) {
            throw new BinaryMetadataCorruptException(pathStr, "stripeSize",
                "binaryMetadata.stripeSize=" + declaredStripeSize + ", must be > 0");
        }
        if (accumulatedRowGroupCountPerStripeArray.length != declaredStripeSize) {
            throw new BinaryMetadataCorruptException(pathStr, "accumulatedLength",
                "accumulated.length=" + accumulatedRowGroupCountPerStripeArray.length
                    + ", stripeSize=" + declaredStripeSize);
        }
        if (accumulatedRowGroupCountPerStripeArray[0] < 0) {
            throw new BinaryMetadataCorruptException(pathStr, "accumulatedNegative",
                "accumulated[0]=" + accumulatedRowGroupCountPerStripeArray[0]);
        }
        for (int i = 1; i < accumulatedRowGroupCountPerStripeArray.length; i++) {
            if (accumulatedRowGroupCountPerStripeArray[i] < accumulatedRowGroupCountPerStripeArray[i - 1]) {
                throw new BinaryMetadataCorruptException(pathStr, "accumulatedMonotonic",
                    "accumulated[" + i + "]=" + accumulatedRowGroupCountPerStripeArray[i]
                        + " < accumulated[" + (i - 1) + "]=" + accumulatedRowGroupCountPerStripeArray[i - 1]);
            }
        }
        final int lastAccumulated = accumulatedRowGroupCountPerStripeArray[
            accumulatedRowGroupCountPerStripeArray.length - 1];
        if (totalRowGroupCount < lastAccumulated) {
            throw new BinaryMetadataCorruptException(pathStr, "totalRowGroupCount",
                "totalRowGroupCount=" + totalRowGroupCount + " < accumulated[last]=" + lastAccumulated);
        }
        // ============ end of pre-check ============

        // Column dimension
        List<ByteString> binaryPositionsList = binaryMetadata.getBinaryPositionsList();
        List<ByteString> columnPositionUnitSizeList = binaryMetadata.getColumnPositionUnitSizeList();
        List<ByteString> binaryStatisticsList = binaryMetadata.getBinaryStatisticsList();
        List<ByteString> binaryStatisticsNullBitmapList = binaryMetadata.getBinaryStatisticsNullBitmapList();
        List<Integer> columnStatisticsTypeList = binaryMetadata.getColumnStatisticsTypeList();

        // Check column count.
        final int expectedColumnCount = columnStatisticsTypeList.size();
        if (expectedColumnCount != (schema.getMaximumId() + 1)) {
            throw new IllegalArgumentException(
                "columnStatisticsTypeList.size() != schema.getMaximumId() + 1, columnStatisticsTypeList.size()="
                    + columnStatisticsTypeList.size() + ", schema.getMaximumId() + 1=" + (schema.getMaximumId() + 1));
        }
        if (binaryPositionsList.size() != expectedColumnCount) {
            throw new IllegalArgumentException(
                "binaryPositionsList.size() != expectedColumnCount, binaryPositionsList.size()="
                    + binaryPositionsList.size() + ", expectedColumnCount=" + expectedColumnCount);
        }
        if (columnPositionUnitSizeList.size() != expectedColumnCount) {
            throw new IllegalArgumentException(
                "columnPositionUnitSizeList.size() != expectedColumnCount, columnPositionUnitSizeList.size()="
                    + columnPositionUnitSizeList.size() + ", expectedColumnCount=" + expectedColumnCount);
        }
        if (binaryStatisticsList.size() != expectedColumnCount) {
            throw new IllegalArgumentException(
                "binaryStatisticsList.size() != expectedColumnCount, binaryStatisticsList.size()="
                    + binaryStatisticsList.size() + ", expectedColumnCount=" + expectedColumnCount);
        }
        if (binaryStatisticsNullBitmapList.size() != expectedColumnCount) {
            throw new IllegalArgumentException(
                "binaryStatisticsNullBitmapList.size() != expectedColumnCount, binaryStatisticsNullBitmapList.size()="
                    + binaryStatisticsNullBitmapList.size() + ", expectedColumnCount=" + expectedColumnCount);
        }

        FastPositionIndex[] fastPositionIndexArray = new FastPositionIndex[expectedColumnCount];
        FastColumnStatistics[] fastColumnStatisticsArray = new FastColumnStatistics[expectedColumnCount];

        for (int columnIndex = 0; columnIndex < expectedColumnCount; columnIndex++) {
            ByteString binaryPositions = binaryPositionsList.get(columnIndex);
            ByteString columnPositionUnitSize = columnPositionUnitSizeList.get(columnIndex);
            ByteString binaryStatistics = binaryStatisticsList.get(columnIndex);
            ByteString binaryStatisticsNullBitmap = binaryStatisticsNullBitmapList.get(columnIndex);
            Integer columnStatisticsType = columnStatisticsTypeList.get(columnIndex);

            // convert columnPositionUnitSize
            byte[] columnPositionUnitSizeByteArray = columnPositionUnitSize.toByteArray();
            // N4 defense: unitSize array length must equal the stripe count (covers C5 empty-array case).
            if (columnPositionUnitSizeByteArray.length != accumulatedRowGroupCountPerStripeArray.length) {
                throw new BinaryMetadataCorruptException(pathStr, "unitSizeLength", -1, columnIndex,
                    "columnPositionUnitSize.length=" + columnPositionUnitSizeByteArray.length
                        + ", stripeCount=" + accumulatedRowGroupCountPerStripeArray.length);
            }
            // N4 defense: each unitSize byte must be non-negative (raw byte in 0..127).
            // Matches the original bug report: unitSize=0xFF is interpreted as -1 in Java,
            // which drives positionListSize negative and triggers NegativeArraySizeException.
            for (int i = 0; i < columnPositionUnitSizeByteArray.length; i++) {
                if (columnPositionUnitSizeByteArray[i] < 0) {
                    throw new BinaryMetadataCorruptException(pathStr, "unitSizeNegative", i, columnIndex,
                        "columnPositionUnitSize[" + i + "]=" + columnPositionUnitSizeByteArray[i]
                            + " (rawByte=" + (columnPositionUnitSizeByteArray[i] & 0xFF) + ")");
                }
            }
            int positionListSize = 0;
            for (int i = 0; i < accumulatedRowGroupCountPerStripeArray.length; i++) {
                int rowGroupCountInStripe;
                if (i == accumulatedRowGroupCountPerStripeArray.length - 1) {
                    rowGroupCountInStripe = totalRowGroupCount - accumulatedRowGroupCountPerStripeArray[i];
                } else {
                    rowGroupCountInStripe =
                        accumulatedRowGroupCountPerStripeArray[i + 1] - accumulatedRowGroupCountPerStripeArray[i];
                }
                positionListSize += rowGroupCountInStripe * columnPositionUnitSizeByteArray[i];
            }
            // N4 defense: final safety net on positionListSize. Pre-checks cover the primary
            // sources; this guards against overflow or unknown combinations.
            if (positionListSize < 0) {
                throw new BinaryMetadataCorruptException(pathStr, "positionListSize", -1, columnIndex,
                    "positionListSize=" + positionListSize);
            }

            // convert to ByteBuffer
            ByteBuffer binaryPositionsReadOnlyByteBuffer = binaryPositions.asReadOnlyByteBuffer();
            binaryPositionsReadOnlyByteBuffer.rewind();
            ByteBuffer binaryStatisticsReadOnlyByteBuffer = binaryStatistics.asReadOnlyByteBuffer();
            binaryStatisticsReadOnlyByteBuffer.rewind();
            ByteBuffer binaryStatisticsNullBitmapReadOnlyByteBuffer = binaryStatisticsNullBitmap.asReadOnlyByteBuffer();
            binaryStatisticsNullBitmapReadOnlyByteBuffer.rewind();

            // for recording position index
            long[] positionList = new long[positionListSize];
            int positionListIndex = 0;
            long maxPositionValue = -1;
            // NOTE: hotpot
            int longCount = binaryPositionsReadOnlyByteBuffer.remaining() / 8;
            if (longCount > 0) {
                // 使用LongBuffer进行批量读取
                LongBuffer longBuffer = binaryPositionsReadOnlyByteBuffer.asLongBuffer();
                longBuffer.get(positionList, positionListIndex, longCount);

                // 单独计算最大值，减少循环中的比较操作
                for (int i = positionListIndex; i < positionListIndex + longCount; i++) {
                    if (positionList[i] > maxPositionValue) {
                        maxPositionValue = positionList[i];
                    }
                }

                positionListIndex += longCount;
                binaryPositionsReadOnlyByteBuffer.position(
                    binaryPositionsReadOnlyByteBuffer.position() + longCount * 8);
            }
            if (maxPositionValue <= Integer.MAX_VALUE) {
                // NOTE: hotpot
                int[] intPositionList = new int[positionListSize];
                for (int i = 0; i < positionList.length; i++) {
                    intPositionList[i] = (int) positionList[i];
                }
                FastPositionIndexImpl fastPositionIndex = new FastPositionIndexImpl(totalRowGroupCount,
                    columnPositionUnitSizeByteArray, accumulatedRowGroupCountPerStripeArray, intPositionList, null);
                fastPositionIndexArray[columnIndex] = fastPositionIndex;
            } else {
                FastPositionIndexImpl fastPositionIndex = new FastPositionIndexImpl(totalRowGroupCount,
                    columnPositionUnitSizeByteArray, accumulatedRowGroupCountPerStripeArray, null, positionList);
                fastPositionIndexArray[columnIndex] = fastPositionIndex;
            }

            // NOTE: hotpot
            // for has_null bitmap
            byte[] hasNullBitmap = new byte[totalRowGroupCount];
            int remaining = binaryStatisticsNullBitmapReadOnlyByteBuffer.remaining();
            if (remaining > 0) {
                binaryStatisticsNullBitmapReadOnlyByteBuffer.get(
                    hasNullBitmap, 0, Math.min(remaining, hasNullBitmap.length));
            }

            // for recording column statistics
            // if this column is not in sort key, and zone map prune is disabled.
            if (sortKeyColumns != null && !sortKeyColumns.contains(columnIndex) && !enableZoneMapPrune) {
                fastColumnStatisticsArray[columnIndex] = null;
            } else if (columnStatisticsType == RowIndexUtilBase.FLAG_INT_STATISTICS) {
                FastLongColumnStatistics fastLongColumnStatistics = new FastLongColumnStatistics();
                fastLongColumnStatistics.setTotalRowGroupCount(totalRowGroupCount);
                fastLongColumnStatistics.setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripeArray);
                fastLongColumnStatistics.setHasNullBitmap(hasNullBitmap);

                // NOTE: hotpot
                long[] minMaxValues = new long[totalRowGroupCount * 2];
                int binaryStatisticsReadOnlyByteBufferLongCount = binaryStatisticsReadOnlyByteBuffer.remaining() / 8;
                if (binaryStatisticsReadOnlyByteBufferLongCount > 0) {
                    LongBuffer longBuffer = binaryStatisticsReadOnlyByteBuffer.asLongBuffer();
                    longBuffer.get(minMaxValues, 0, binaryStatisticsReadOnlyByteBufferLongCount);
                }
                fastLongColumnStatistics.setMinMaxValues(minMaxValues);
                fastColumnStatisticsArray[columnIndex] = fastLongColumnStatistics;
            } else if (columnStatisticsType == RowIndexUtilBase.FLAG_DOUBLE_STATISTICS) {
                FastDoubleColumnStatistics fastDoubleColumnStatistics = new FastDoubleColumnStatistics();
                fastDoubleColumnStatistics.setTotalRowGroupCount(totalRowGroupCount);
                fastDoubleColumnStatistics.setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripeArray);
                fastDoubleColumnStatistics.setHasNullBitmap(hasNullBitmap);

                // NOTE: hotpot
                double[] minMaxValues = new double[totalRowGroupCount * 2];
                int binaryStatisticsReadOnlyByteBufferDoubleCount = binaryStatisticsReadOnlyByteBuffer.remaining() / 8;
                if (binaryStatisticsReadOnlyByteBufferDoubleCount > 0) {
                    DoubleBuffer doubleBuffer = binaryStatisticsReadOnlyByteBuffer.asDoubleBuffer();
                    doubleBuffer.get(minMaxValues, 0, binaryStatisticsReadOnlyByteBufferDoubleCount);
                }
                fastDoubleColumnStatistics.setMinMaxValues(minMaxValues);
                fastColumnStatisticsArray[columnIndex] = fastDoubleColumnStatistics;
            } else if (columnStatisticsType == RowIndexUtilBase.FLAG_DATE_STATISTICS) {
                FastDateColumnStatistics fastDateColumnStatistics = new FastDateColumnStatistics();
                fastDateColumnStatistics.setTotalRowGroupCount(totalRowGroupCount);
                fastDateColumnStatistics.setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripeArray);
                fastDateColumnStatistics.setHasNullBitmap(hasNullBitmap);

                // NOTE: hotpot
                int[] minMaxValues = new int[totalRowGroupCount * 2];
                int binaryStatisticsReadOnlyByteBufferIntCount = binaryStatisticsReadOnlyByteBuffer.remaining() / 4;
                if (binaryStatisticsReadOnlyByteBufferIntCount > 0) {
                    IntBuffer intBuffer = binaryStatisticsReadOnlyByteBuffer.asIntBuffer();
                    intBuffer.get(minMaxValues, 0, binaryStatisticsReadOnlyByteBufferIntCount);
                }
                fastDateColumnStatistics.setMinMaxValues(minMaxValues);
                fastColumnStatisticsArray[columnIndex] = fastDateColumnStatistics;
            } else if (columnStatisticsType == RowIndexUtilBase.FLAG_STRING_STATISTICS) {
                FastStringColumnStatistics fastStringColumnStatistics = new FastStringColumnStatistics();
                fastStringColumnStatistics.setTotalRowGroupCount(totalRowGroupCount);
                fastStringColumnStatistics.setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripeArray);
                fastStringColumnStatistics.setHasNullBitmap(hasNullBitmap);

                // NOTE: hotpot
                int totalDataSize = binaryStatisticsReadOnlyByteBuffer.remaining();
                byte[] allData = new byte[totalDataSize];
                binaryStatisticsReadOnlyByteBuffer.get(allData);
                ByteBuffer dataBuffer = ByteBuffer.wrap(allData);

                String[] minMaxValueArray = new String[totalRowGroupCount * 2];
                for (int rowGroupIndex = 0; rowGroupIndex < totalRowGroupCount; rowGroupIndex++) {
                    int totalLength = dataBuffer.getInt();
                    int lowerBoundLength = dataBuffer.getInt();
                    int upperBoundLength = totalLength - lowerBoundLength;

                    minMaxValueArray[rowGroupIndex * 2] =
                        new String(allData, dataBuffer.position(), lowerBoundLength, StandardCharsets.UTF_8);
                    dataBuffer.position(dataBuffer.position() + lowerBoundLength);

                    minMaxValueArray[rowGroupIndex * 2 + 1] =
                        new String(allData, dataBuffer.position(), upperBoundLength, StandardCharsets.UTF_8);
                    dataBuffer.position(dataBuffer.position() + upperBoundLength);
                }
                fastStringColumnStatistics.setMinMaxValues(minMaxValueArray);
                fastColumnStatisticsArray[columnIndex] = fastStringColumnStatistics;

            } else if (columnStatisticsType == RowIndexUtilBase.FLAG_DECIMAL_STATISTICS) {

                OrcProto.ColumnStatistics[] columnStatisticsArray = new OrcProto.ColumnStatistics[totalRowGroupCount];
                for (int rowGroupIndex = 0; rowGroupIndex < totalRowGroupCount; rowGroupIndex++) {
                    // parse decimal value from binaryStatistics
                    OrcProto.DecimalStatistics.Builder decimalStatisticsBuilder =
                        OrcProto.DecimalStatistics.newBuilder();
                    int flag = binaryStatisticsReadOnlyByteBuffer.getInt();
                    if (flag != RowIndexUtilBase.FLAG_DECIMAL_STATISTICS) {
                        throw new IllegalArgumentException(
                            "flag != RowIndexUtilBase.FLAG_DECIMAL_STATISTICS, flag=" + flag);
                    }
                    boolean hasMinimum = binaryStatisticsReadOnlyByteBuffer.get() == (byte) 1;
                    if (hasMinimum) {
                        int minimumLength = binaryStatisticsReadOnlyByteBuffer.getInt();
                        byte[] minimumBytes = new byte[minimumLength];
                        binaryStatisticsReadOnlyByteBuffer.get(minimumBytes);
                        decimalStatisticsBuilder.setMinimum(new String(minimumBytes));
                    }
                    boolean hasMaximum = binaryStatisticsReadOnlyByteBuffer.get() == (byte) 1;
                    if (hasMaximum) {
                        int maximumLength = binaryStatisticsReadOnlyByteBuffer.getInt();
                        byte[] maximumBytes = new byte[maximumLength];
                        binaryStatisticsReadOnlyByteBuffer.get(maximumBytes);
                        decimalStatisticsBuilder.setMaximum(new String(maximumBytes));
                    }
                    boolean hasSum = binaryStatisticsReadOnlyByteBuffer.get() == (byte) 1;
                    if (hasSum) {
                        int sumLength = binaryStatisticsReadOnlyByteBuffer.getInt();
                        byte[] sumBytes = new byte[sumLength];
                        binaryStatisticsReadOnlyByteBuffer.get(sumBytes);
                        decimalStatisticsBuilder.setSum(new String(sumBytes));
                    }
                    OrcProto.ColumnStatistics.Builder columnStatisticsBuilder = OrcProto.ColumnStatistics.newBuilder();
                    columnStatisticsBuilder.setHasNull(hasNullBitmap[rowGroupIndex] == (byte) 1);
                    columnStatisticsBuilder.setDecimalStatistics(decimalStatisticsBuilder.build());
                    columnStatisticsArray[rowGroupIndex] = columnStatisticsBuilder.build();
                }
                NormalColumnStatistics normalColumnStatistics = new NormalColumnStatistics();
                normalColumnStatistics.setTotalRowGroupCount(totalRowGroupCount);
                normalColumnStatistics.setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripeArray);
                normalColumnStatistics.setHasNullBitmap(hasNullBitmap);
                normalColumnStatistics.setColumnStatisticsArray(columnStatisticsArray);

                fastColumnStatisticsArray[columnIndex] = normalColumnStatistics;
            } else if (columnStatisticsType == RowIndexUtilBase.FLAG_TIMESTAMP_STATISTICS) {
                OrcProto.ColumnStatistics[] columnStatisticsArray = new OrcProto.ColumnStatistics[totalRowGroupCount];
                for (int rowGroupIndex = 0; rowGroupIndex < totalRowGroupCount; rowGroupIndex++) {
                    // parse timestamp value from binaryStatistics
                    OrcProto.TimestampStatistics.Builder timestampStatisticsBuilder =
                        OrcProto.TimestampStatistics.newBuilder();
                    int flag = binaryStatisticsReadOnlyByteBuffer.getInt();
                    if (flag != RowIndexUtilBase.FLAG_TIMESTAMP_STATISTICS) {
                        throw new IllegalArgumentException(
                            "flag != RowIndexUtilBase.FLAG_TIMESTAMP_STATISTICS, flag=" + flag);
                    }
                    timestampStatisticsBuilder.setMinimum(binaryStatisticsReadOnlyByteBuffer.getLong());
                    timestampStatisticsBuilder.setMaximum(binaryStatisticsReadOnlyByteBuffer.getLong());
                    timestampStatisticsBuilder.setMinimumUtc(binaryStatisticsReadOnlyByteBuffer.getLong());
                    timestampStatisticsBuilder.setMaximumUtc(binaryStatisticsReadOnlyByteBuffer.getLong());
                    timestampStatisticsBuilder.setMinimumNanos(binaryStatisticsReadOnlyByteBuffer.getInt());
                    timestampStatisticsBuilder.setMaximumNanos(binaryStatisticsReadOnlyByteBuffer.getInt());

                    OrcProto.ColumnStatistics.Builder columnStatisticsBuilder = OrcProto.ColumnStatistics.newBuilder();
                    columnStatisticsBuilder.setHasNull(hasNullBitmap[rowGroupIndex] == (byte) 1);
                    columnStatisticsBuilder.setTimestampStatistics(timestampStatisticsBuilder.build());
                    columnStatisticsArray[rowGroupIndex] = columnStatisticsBuilder.build();
                }
                NormalColumnStatistics normalColumnStatistics = new NormalColumnStatistics();
                normalColumnStatistics.setTotalRowGroupCount(totalRowGroupCount);
                normalColumnStatistics.setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripeArray);
                normalColumnStatistics.setHasNullBitmap(hasNullBitmap);
                normalColumnStatistics.setColumnStatisticsArray(columnStatisticsArray);

                fastColumnStatisticsArray[columnIndex] = normalColumnStatistics;
            } else if (columnStatisticsType == RowIndexUtilBase.FLAG_BINARY_STATISTICS) {
                OrcProto.ColumnStatistics[] columnStatisticsArray = new OrcProto.ColumnStatistics[totalRowGroupCount];
                for (int rowGroupIndex = 0; rowGroupIndex < totalRowGroupCount; rowGroupIndex++) {
                    OrcProto.BinaryStatistics.Builder binaryStatisticsBuilder =
                        OrcProto.BinaryStatistics.newBuilder();
                    int flag = binaryStatisticsReadOnlyByteBuffer.getInt();
                    if (flag != RowIndexUtilBase.FLAG_BINARY_STATISTICS) {
                        throw new IllegalArgumentException(
                            "flag != RowIndexUtilBase.FLAG_BINARY_STATISTICS, flag=" + flag);
                    }
                    binaryStatisticsBuilder.setSum(binaryStatisticsReadOnlyByteBuffer.getLong());

                    OrcProto.ColumnStatistics.Builder columnStatisticsBuilder = OrcProto.ColumnStatistics.newBuilder();
                    columnStatisticsBuilder.setHasNull(hasNullBitmap[rowGroupIndex] == (byte) 1);
                    columnStatisticsBuilder.setBinaryStatistics(binaryStatisticsBuilder.build());
                    columnStatisticsArray[rowGroupIndex] = columnStatisticsBuilder.build();
                }
                NormalColumnStatistics normalColumnStatistics = new NormalColumnStatistics();
                normalColumnStatistics.setTotalRowGroupCount(totalRowGroupCount);
                normalColumnStatistics.setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripeArray);
                normalColumnStatistics.setHasNullBitmap(hasNullBitmap);
                normalColumnStatistics.setColumnStatisticsArray(columnStatisticsArray);

                fastColumnStatisticsArray[columnIndex] = normalColumnStatistics;
            } else if (columnStatisticsType == RowIndexUtilBase.FLAG_COLLECTION_STATISTICS) {
                OrcProto.ColumnStatistics[] columnStatisticsArray = new OrcProto.ColumnStatistics[totalRowGroupCount];
                for (int rowGroupIndex = 0; rowGroupIndex < totalRowGroupCount; rowGroupIndex++) {
                    OrcProto.CollectionStatistics.Builder collectionStatisticsBuilder =
                        OrcProto.CollectionStatistics.newBuilder();
                    int flag = binaryStatisticsReadOnlyByteBuffer.getInt();
                    if (flag != RowIndexUtilBase.FLAG_COLLECTION_STATISTICS) {
                        throw new IllegalArgumentException(
                            "flag != RowIndexUtilBase.FLAG_COLLECTION_STATISTICS, flag=" + flag);
                    }
                    collectionStatisticsBuilder.setMinChildren(binaryStatisticsReadOnlyByteBuffer.getLong());
                    collectionStatisticsBuilder.setMaxChildren(binaryStatisticsReadOnlyByteBuffer.getLong());
                    collectionStatisticsBuilder.setTotalChildren(binaryStatisticsReadOnlyByteBuffer.getLong());
                    OrcProto.ColumnStatistics.Builder columnStatisticsBuilder = OrcProto.ColumnStatistics.newBuilder();
                    columnStatisticsBuilder.setHasNull(hasNullBitmap[rowGroupIndex] == (byte) 1);
                    columnStatisticsBuilder.setCollectionStatistics(collectionStatisticsBuilder.build());
                    columnStatisticsArray[rowGroupIndex] = columnStatisticsBuilder.build();
                }
                NormalColumnStatistics normalColumnStatistics = new NormalColumnStatistics();
                normalColumnStatistics.setTotalRowGroupCount(totalRowGroupCount);
                normalColumnStatistics.setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripeArray);
                normalColumnStatistics.setHasNullBitmap(hasNullBitmap);
                normalColumnStatistics.setColumnStatisticsArray(columnStatisticsArray);

                fastColumnStatisticsArray[columnIndex] = normalColumnStatistics;
            } else if (columnStatisticsType == RowIndexUtilBase.FLAG_BUCKET_STATISTICS) {
                OrcProto.ColumnStatistics[] columnStatisticsArray = new OrcProto.ColumnStatistics[totalRowGroupCount];
                for (int rowGroupIndex = 0; rowGroupIndex < totalRowGroupCount; rowGroupIndex++) {
                    OrcProto.BucketStatistics.Builder bucketStatisticsBuilder =
                        OrcProto.BucketStatistics.newBuilder();
                    int flag = binaryStatisticsReadOnlyByteBuffer.getInt();
                    if (flag != RowIndexUtilBase.FLAG_BUCKET_STATISTICS) {
                        throw new IllegalArgumentException(
                            "flag != RowIndexUtilBase.FLAG_BUCKET_STATISTICS, flag=" + flag);
                    }
                    int countListSize = binaryStatisticsReadOnlyByteBuffer.getInt();
                    for (int i = 0; i < countListSize; i++) {
                        bucketStatisticsBuilder.addCount(binaryStatisticsReadOnlyByteBuffer.getLong());
                    }

                    OrcProto.ColumnStatistics.Builder columnStatisticsBuilder = OrcProto.ColumnStatistics.newBuilder();
                    columnStatisticsBuilder.setHasNull(hasNullBitmap[rowGroupIndex] == (byte) 1);
                    columnStatisticsBuilder.setBucketStatistics(bucketStatisticsBuilder.build());
                    columnStatisticsArray[rowGroupIndex] = columnStatisticsBuilder.build();
                }
                NormalColumnStatistics normalColumnStatistics = new NormalColumnStatistics();
                normalColumnStatistics.setTotalRowGroupCount(totalRowGroupCount);
                normalColumnStatistics.setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripeArray);
                normalColumnStatistics.setHasNullBitmap(hasNullBitmap);
                normalColumnStatistics.setColumnStatisticsArray(columnStatisticsArray);

                fastColumnStatisticsArray[columnIndex] = normalColumnStatistics;
            } else {
                OrcProto.ColumnStatistics[] columnStatisticsArray = new OrcProto.ColumnStatistics[totalRowGroupCount];
                for (int rowGroupIndex = 0; rowGroupIndex < totalRowGroupCount; rowGroupIndex++) {
                    int flag = binaryStatisticsReadOnlyByteBuffer.getInt();
                    if (flag != RowIndexUtilBase.FLAG_NO_STATISTICS) {
                        throw new IllegalArgumentException(
                            "flag != RowIndexUtilBase.FLAG_NO_STATISTICS, flag=" + flag);
                    }
                    columnStatisticsArray[rowGroupIndex] = null;
                }
                NormalColumnStatistics normalColumnStatistics = new NormalColumnStatistics();
                normalColumnStatistics.setTotalRowGroupCount(totalRowGroupCount);
                normalColumnStatistics.setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripeArray);
                normalColumnStatistics.setHasNullBitmap(hasNullBitmap);
                normalColumnStatistics.setColumnStatisticsArray(columnStatisticsArray);

                fastColumnStatisticsArray[columnIndex] = normalColumnStatistics;
            }
        }

        // Stripe dimension
        // for each stripe
        final int stripeSize = binaryMetadata.getStripeSize();
        FastStreamIndex[] fastStreamIndexArray = new FastStreamIndex[stripeSize];
        String[] writerTimeZoneArray = new String[stripeSize];
        for (int stripeIndex = 0; stripeIndex < stripeSize; stripeIndex++) {

            // set writer time zone
            String writerTimeZone = binaryMetadata.getWriterTimeZones(stripeIndex);
            writerTimeZoneArray[stripeIndex] = writerTimeZone;

            // set stream
            ByteString streamByteString = binaryMetadata.getStreams(stripeIndex);
            ByteBuffer streamBuffer = streamByteString.asReadOnlyByteBuffer();
            streamBuffer.rewind();

            // NOTE: hotpot
            int totalElements = streamBuffer.remaining() / Long.BYTES;
            long[] streamIndexArray = new long[totalElements];

            LongBuffer longBuffer = streamBuffer.asLongBuffer();
            longBuffer.get(streamIndexArray);

            long maxStreamLength = -1;
            for (int i = 1; i < streamIndexArray.length; i += 2) {
                maxStreamLength = Math.max(maxStreamLength, streamIndexArray[i]);
            }

            // NOTE: hotpot
            if (maxStreamLength <= Integer.MAX_VALUE) {
                int[] intStreamIndexArray = new int[streamIndexArray.length];
                for (int i = 0; i < streamIndexArray.length / 2; i++) {
                    intStreamIndexArray[i * 2] =
                        MetadataSerializeUtils.serializeStreamColumnAndKindToInt(streamIndexArray[i * 2]);
                    intStreamIndexArray[i * 2 + 1] = (int) streamIndexArray[i * 2 + 1];
                }
                FastStreamIndexImpl fastStreamIndex =
                    new FastStreamIndexImpl(streamIndexArray.length / 2, intStreamIndexArray, null);
                fastStreamIndexArray[stripeIndex] = fastStreamIndex;
            } else {
                FastStreamIndexImpl fastStreamIndex =
                    new FastStreamIndexImpl(streamIndexArray.length / 2, null, streamIndexArray);
                fastStreamIndexArray[stripeIndex] = fastStreamIndex;
            }
        }

        List<ByteString> columnEncodingByteStringList = binaryMetadata.getColumnEncodingsList();
        FastColumnEncodingIndex[] fastColumnEncodingIndexArray =
            new FastColumnEncodingIndex[columnEncodingByteStringList.size()];
        for (int columnIndex = 0; columnIndex < columnEncodingByteStringList.size(); columnIndex++) {

            int[] columnEncodingList = new int[stripeSize * 2];
            int columnEncodingListIndex = 0;

            ByteString columnEncodingByteString = columnEncodingByteStringList.get(columnIndex);
            ByteBuffer columnEncodingBuffer = columnEncodingByteString.asReadOnlyByteBuffer();
            columnEncodingBuffer.rewind();

            // NOTE: hotpot
            // for each column, deserialize kind and bloomEncoding and set into every stripe.
            IntBuffer intBuffer = columnEncodingBuffer.asIntBuffer();
            intBuffer.get(columnEncodingList);

            int maxDictionarySize = -1;
            for (int i = 1; i < columnEncodingList.length; i += 2) {
                maxDictionarySize = Math.max(maxDictionarySize, columnEncodingList[i]);
            }

            // NOTE: hotpot
            if (maxDictionarySize < Short.MAX_VALUE) {
                // compact to short list.
                short[] shortColumnEncodingList = new short[columnEncodingList.length];
                for (int i = 0; i < columnEncodingList.length / 2; i++) {
                    shortColumnEncodingList[i * 2] = MetadataSerializeUtils
                        .serializeColumnEncodingKindAndBloomEncodingToShort(columnEncodingList[i * 2]);
                    shortColumnEncodingList[i * 2 + 1] = (short) columnEncodingList[i * 2 + 1];
                }
                FastColumnEncodingIndexImpl fastColumnEncodingIndex =
                    new FastColumnEncodingIndexImpl(shortColumnEncodingList, null);
                fastColumnEncodingIndexArray[columnIndex] = fastColumnEncodingIndex;
            } else {
                // use int list
                FastColumnEncodingIndexImpl fastColumnEncodingIndex =
                    new FastColumnEncodingIndexImpl(null, columnEncodingList);
                fastColumnEncodingIndexArray[columnIndex] = fastColumnEncodingIndex;
            }
        }

        preheatFileMeta.setUseBinaryMeta(true);
        preheatFileMeta.setWriterTimeZoneArray(writerTimeZoneArray);
        preheatFileMeta.setFastPositionIndexArray(fastPositionIndexArray);
        preheatFileMeta.setFastColumnEncodingIndexArray(fastColumnEncodingIndexArray);
        preheatFileMeta.setFastStreamIndexArray(fastStreamIndexArray);
        preheatFileMeta.setFastColumnStatisticsArray(fastColumnStatisticsArray);
    }

    private Map<Long, PreheatStripeMeta> preheatStripe(Path path, ReaderImpl fileReader, FileSystem preheatFileSystem)
        throws IOException {
        Map<Long, PreheatStripeMeta> result = new ConcurrentHashMap<>();

        // 1. build data reader
        try (DataReader dataReader = buildDataReader(path, fileReader, preheatFileSystem)) {
            for (StripeInformation stripe : fileReader.getStripes()) {

                // 2. build stripe planner for each stripe.
                StripePlanner planner = buildStripePlanner(fileReader, dataReader);

                boolean[] allColumns = new boolean[fileReader.getSchema().getMaximumId() + 1];
                Arrays.fill(allColumns, true);

                // 3. get stripe footer
                OrcProto.StripeFooter stripeFooter = dataReader.readStripeFooter(stripe);

                // 4. planner parse meta info of Stripe
                // get info of data streams + index streams and cache it in planner object.
                planner.parseStripe(stripe, allColumns, stripeFooter);

                // 5. get row index
                // NOTE: Stripe Planner will NOT cache the row indexes already fetched.
                OrcIndex index;
                if (sortKeyColumns == null) {
                    index = planner.readRowIndex(allColumns, null);
                } else {
                    boolean enableZoneMapPrune = DynamicConfig.getInstance().enableZoneMapPrune();
                    index = planner.readRowIndex(allColumns, null, sortKeyColumns, enableZoneMapPrune);
                }

                // 6. for preheated meta, don't cache bitmap index.
                index.clearBitmapIndex();

                planner.clearDataReader();
                PreheatStripeMeta preheatStripeMeta = new PreheatStripeMeta(
                    stripe.getStripeId(), index, stripeFooter);

                result.put(stripe.getStripeId(), preheatStripeMeta);
            }
        }

        return result;
    }

    @NotNull
    private StripePlanner buildStripePlanner(ReaderImpl fileReader, DataReader dataReader) {
        int maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(configuration);
        boolean ignoreNonUtf8BloomFilter = OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(configuration);

        StripePlanner planner = new StripePlanner(
            fileReader.getSchema(),
            fileReader.getEncryption(),
            dataReader,
            fileReader.getWriterVersion(),
            ignoreNonUtf8BloomFilter,
            maxDiskRangeChunkLimit);
        return planner;
    }

    private DataReader buildDataReader(Path path, ReaderImpl fileReader, FileSystem fileSystem) throws IOException {
        int maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(configuration);
        Reader.Options options = fileReader.options();

        InStream.StreamOptions unencryptedOptions =
            InStream.options()
                .withCodec(OrcCodecPool.getCodec(fileReader.getCompressionKind()))
                .withBufferSize(fileReader.getCompressionSize());
        DataReaderProperties.Builder builder =
            DataReaderProperties.builder()
                .withCompression(unencryptedOptions)
                .withFileSystemSupplier(() -> fileSystem)
                .withPath(path)
                .withMaxDiskRangeChunkLimit(maxDiskRangeChunkLimit)
                .withZeroCopy(options.getUseZeroCopy());
        FSDataInputStream file = fileSystem.open(path);
        if (file != null) {
            builder.withFile(file);
        }

        DataReader dataReader = RecordReaderUtils.createDefaultDataReader(
            builder.build());
        return dataReader;
    }

    @Override
    public void close() throws IOException {

    }
}
