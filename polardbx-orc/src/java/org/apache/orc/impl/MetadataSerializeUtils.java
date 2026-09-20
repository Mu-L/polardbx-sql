package org.apache.orc.impl;

import com.google.protobuf.ByteString;
import org.apache.orc.OrcProto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.orc.impl.RowIndexUtilBase.*;

public class MetadataSerializeUtils {
    public static OrcProto.BinaryMetadata transformToRedundantMetadata(
        OrcProto.RedundantMetadata redundantMetadata) {
        OrcProto.BinaryMetadata.Builder builder = OrcProto.BinaryMetadata.newBuilder();

        // [col_id] -> binary positions of (stripe_id, rg_id)
        TreeMap<Integer, DynamicByteBuffer> positionsOfAllColumn = new TreeMap<>();

        // [col_id] -> binary positions address of (stripe_id, rg_id)
        TreeMap<Integer, DynamicByteBuffer> positionsAddressOfAllColumn = new TreeMap<>();

        // [col_id] -> [stripe_id][position_size]
        TreeMap<Integer, List<Integer>> positionsSizeListOfAllColumn = new TreeMap<>();

        // [col_id] -> binary statistics of (stripe_id, rg_id)
        TreeMap<Integer, DynamicByteBuffer> statisticsOfAllColumn = new TreeMap<>();

        // [col_id] -> binary statistics null bitmap of (stripe_id, rg_id)
        TreeMap<Integer, DynamicByteBuffer> statisticsNullBitmapOfAllColumn = new TreeMap<>();

        // [col_id] -> binary statistics type of (stripe_id, rg_id)
        TreeMap<Integer, Integer> statisticsTypeOfAllColumn = new TreeMap<>();

        // address management: <stripe_id, rg_id> -> address = total_rg_id
        TreeMap<Integer, Integer> rowGroupCountOfAllStripes = new TreeMap<>();
        int[] rowGroupCountUtilThisStripe = new int[redundantMetadata.getStripesCount()];

        //  redundant meta store all row-index and footer for each stripe.
        List<OrcProto.RedundantStripeMetadata> redundantStripeMetadataList = redundantMetadata.getStripesList();

        Map<Integer, String> stripeIndexToWriterTimeZone = new TreeMap<>();
        Map<Integer, DynamicByteBuffer> columnIdToColumnEncodingMap = new TreeMap<>();
        Map<Integer, DynamicByteBuffer> stripeIndexToStreamListMap = new TreeMap<>();

        final int totalStripeCount = redundantStripeMetadataList.size();
        builder.setStripeSize(totalStripeCount);

        for (int stripeIndex = 0; stripeIndex < redundantStripeMetadataList.size(); stripeIndex++) {
            OrcProto.RedundantStripeMetadata redundantStripeMetadata = redundantStripeMetadataList.get(stripeIndex);

            OrcProto.StripeFooterWithId stripeFooterWithId = redundantStripeMetadata.getStripeFooter();

            // serialize stripe footer:
            OrcProto.StripeFooter stripeFooter = stripeFooterWithId.getStripeFooter();

            // writerTimeZone
            String writerTimeZone = stripeFooter.getWriterTimezone();
            stripeIndexToWriterTimeZone.put(stripeIndex, writerTimeZone);

            // column encoding
            List<OrcProto.ColumnEncoding> columnEncodingList = stripeFooter.getColumnsList();
            for (int columnIndex = 0; columnIndex < columnEncodingList.size(); columnIndex++) {
                OrcProto.ColumnEncoding columnEncoding = columnEncodingList.get(columnIndex);

                DynamicByteBuffer columnEncodingBuffer =
                    columnIdToColumnEncodingMap.computeIfAbsent(columnIndex, k -> new DynamicByteBuffer());

                int kindAndBloomEncoding = serializeColumnEncodingKindAndBloomEncoding(columnEncoding.getKind(),
                    columnEncoding.getBloomEncoding());
                int dictionarySize = columnEncoding.getDictionarySize();
                columnEncodingBuffer.appendInt(kindAndBloomEncoding);
                columnEncodingBuffer.appendInt(dictionarySize);
            }

            // stream
            List<OrcProto.Stream> streamList = stripeFooter.getStreamsList();
            for (int streamIndex = 0; streamIndex < streamList.size(); streamIndex++) {
                OrcProto.Stream stream = streamList.get(streamIndex);

                long streamColumnAndKind = serializeStreamColumnAndKind(stream.getColumn(), stream.getKind());
                long streamLength = stream.getLength();

                DynamicByteBuffer streamListBuffer =
                    stripeIndexToStreamListMap.computeIfAbsent(stripeIndex, k -> new DynamicByteBuffer());

                streamListBuffer.appendLong(streamColumnAndKind);
                streamListBuffer.appendLong(streamLength);
            }
        }

        // build writer time zones for binary metadata
        for (Map.Entry<Integer, String> entry : stripeIndexToWriterTimeZone.entrySet()) {
            int stripeIndex = entry.getKey();
            String writerTimeZone = entry.getValue();
            builder.addWriterTimeZones(writerTimeZone);
        }

        // build streams for binary metadata
        for (Map.Entry<Integer, DynamicByteBuffer> entry : stripeIndexToStreamListMap.entrySet()) {
            int stripeIndex = entry.getKey();
            DynamicByteBuffer streamListBuffer = entry.getValue();
            builder.addStreams(streamListBuffer.getByteString());
        }

        // build column encodings for binary metadata
        for (Map.Entry<Integer, DynamicByteBuffer> entry : columnIdToColumnEncodingMap.entrySet()) {
            int columnIndex = entry.getKey();
            DynamicByteBuffer columnEncodingBuffer = entry.getValue();
            builder.addColumnEncodings(columnEncodingBuffer.getByteString());
        }

        // Get column count.
        int totalColumnCount = -1;
        int lastStripeRowGroupCount = 0;
        int totalRowGroupCount = 0;
        // for each stripe, management address of <stripe_id, rg_id>.
        for (int stripeIndex = 0; stripeIndex < redundantStripeMetadataList.size(); stripeIndex++) {
            OrcProto.RedundantStripeMetadata redundantStripeMetadata = redundantStripeMetadataList.get(stripeIndex);
            // Get stripe footer from redundant meta in file tail.
            OrcProto.StripeFooterWithId stripeFooterWithId = redundantStripeMetadata.getStripeFooter();
            int stripeNumber = stripeFooterWithId.getStripe();

            OrcProto.StripeFooter stripeFooter = stripeFooterWithId.getStripeFooter();
            totalColumnCount = stripeFooter.getColumnsCount();

            // get column on [0] in this stripe
            List<OrcProto.RowIndexWithColumn> rowIndexWithColumnList = redundantStripeMetadata.getRowIndexList();
            OrcProto.RowIndexWithColumn rowIndexWithColumn = rowIndexWithColumnList.get(0);

            // RowIndex： 一个列的所有rg的所有row index
            OrcProto.RowIndex rowIndex = rowIndexWithColumn.getRowIndex();

            List<OrcProto.RowIndexEntry> rowIndexEntryList = rowIndex.getEntryList();

            // record row group count of this stripe
            // rowGroupCountUtilThisStripe[0] = 0
            // rowGroupCountUtilThisStripe[1] = 10
            // rowGroupCountUtilThisStripe[2] = 20
            // rowGroupCountUtilThisStripe[3] = 25
            rowGroupCountOfAllStripes.put(stripeNumber, rowIndexEntryList.size());
            rowGroupCountUtilThisStripe[stripeNumber] =
                stripeNumber == 0 ? stripeNumber :
                    rowGroupCountUtilThisStripe[stripeNumber - 1] + lastStripeRowGroupCount;

            lastStripeRowGroupCount = rowIndexEntryList.size();
            totalRowGroupCount += lastStripeRowGroupCount;
        }

        // for each column
        for (int columnIndex = 0; columnIndex < totalColumnCount; columnIndex++) {

            // for each stripe
            for (int stripeIndex = 0; stripeIndex < redundantStripeMetadataList.size(); stripeIndex++) {
                OrcProto.RedundantStripeMetadata redundantStripeMetadata = redundantStripeMetadataList.get(stripeIndex);

                // Get stripe footer from redundant meta in file tail.
                OrcProto.StripeFooterWithId stripeFooterWithId = redundantStripeMetadata.getStripeFooter();
                int stripeNumber = stripeFooterWithId.getStripe();
                OrcProto.StripeFooter stripeFooter = stripeFooterWithId.getStripeFooter();

                // get column on [columnIndex] in this stripe
                List<OrcProto.RowIndexWithColumn> rowIndexWithColumnList = redundantStripeMetadata.getRowIndexList();
                OrcProto.RowIndexWithColumn rowIndexWithColumn = rowIndexWithColumnList.get(columnIndex);
                // column id
                int column = rowIndexWithColumn.getColumn();

                // RowIndex： 一个列的所有rg的所有row index
                OrcProto.RowIndex rowIndex = rowIndexWithColumn.getRowIndex();

                List<OrcProto.RowIndexEntry> rowIndexEntryList = rowIndex.getEntryList();

                // for each row group in this stripe & column
                for (int i = 0; i < rowIndexEntryList.size(); i++) {
                    // RowIndexEntry： 一个列内部，具体的某一个row group所具有的min-max statistics 和 positions信息
                    OrcProto.RowIndexEntry rowIndexEntry = rowIndexEntryList.get(i);

                    // positions信息
                    List<Long> positionList = rowIndexEntry.getPositionsList();

                    // record positions size
                    if (i == 0) {
                        List<Integer> positionsSizeList = positionsSizeListOfAllColumn
                            .computeIfAbsent(column, k -> new ArrayList<>());
                        positionsSizeList.add(positionList.size());
                    }

                    // serialize position list
                    DynamicByteBuffer binaryPositionsOfColumn =
                        positionsOfAllColumn.computeIfAbsent(column, k -> new DynamicByteBuffer());
                    for (int j = 0; j < positionList.size(); j++) {
                        binaryPositionsOfColumn.appendLong(positionList.get(j));
                    }

                    // ColumnStatistics： statistics信息汇总
                    OrcProto.ColumnStatistics columnStatistics = rowIndexEntry.getStatistics();

                    DynamicByteBuffer binaryStatisticsOfColumn =
                        statisticsOfAllColumn.computeIfAbsent(column, k -> new DynamicByteBuffer());
                    DynamicByteBuffer binaryStatisticsNullBitmapOfColumn =
                        statisticsNullBitmapOfAllColumn.computeIfAbsent(column, k -> new DynamicByteBuffer());

                    // append statistics null info
                    if (columnStatistics.hasHasNull() && columnStatistics.getHasNull()) {
                        binaryStatisticsNullBitmapOfColumn.appendBoolean(true);
                    } else {
                        // no has null
                        binaryStatisticsNullBitmapOfColumn.appendBoolean(false);
                    }

                    // append statistics min-max info
                    // XxxColumnStatistics: 类型特定min-max信息
                    if (columnStatistics.hasIntStatistics()) {
                        statisticsTypeOfAllColumn.put(column, FLAG_INT_STATISTICS);
                        OrcProto.IntegerStatistics integerStatistics = columnStatistics.getIntStatistics();
                        binaryStatisticsOfColumn.appendLong(integerStatistics.getMinimum());
                        binaryStatisticsOfColumn.appendLong(integerStatistics.getMaximum());
                    } else if (columnStatistics.hasDoubleStatistics()) {
                        statisticsTypeOfAllColumn.put(column, FLAG_DOUBLE_STATISTICS);
                        OrcProto.DoubleStatistics doubleStatistics = columnStatistics.getDoubleStatistics();
                        binaryStatisticsOfColumn.appendDouble(doubleStatistics.getMinimum());
                        binaryStatisticsOfColumn.appendDouble(doubleStatistics.getMaximum());
                    } else if (columnStatistics.hasDateStatistics()) {
                        statisticsTypeOfAllColumn.put(column, FLAG_DATE_STATISTICS);
                        OrcProto.DateStatistics dateStatistics = columnStatistics.getDateStatistics();
                        binaryStatisticsOfColumn.appendInt(dateStatistics.getMinimum());
                        binaryStatisticsOfColumn.appendInt(dateStatistics.getMaximum());
                    } else if (columnStatistics.hasStringStatistics()) {
                        statisticsTypeOfAllColumn.put(column, FLAG_STRING_STATISTICS);
                        OrcProto.StringStatistics stringStatistics = columnStatistics.getStringStatistics();

                        byte[] lowerBound =
                            stringStatistics.getMinimum().isEmpty() ? stringStatistics.getLowerBound().getBytes() :
                                stringStatistics.getMinimum().getBytes();

                        byte[] upperBound =
                            stringStatistics.getMaximum().isEmpty() ? stringStatistics.getUpperBound().getBytes() :
                                stringStatistics.getMaximum().getBytes();

                        binaryStatisticsOfColumn.appendInt(lowerBound.length + upperBound.length);
                        binaryStatisticsOfColumn.appendInt(lowerBound.length);
                        binaryStatisticsOfColumn.appendBytes(lowerBound);
                        binaryStatisticsOfColumn.appendBytes(upperBound);
                    } else if (columnStatistics.hasDecimalStatistics()) {
                        statisticsTypeOfAllColumn.put(column, FLAG_DECIMAL_STATISTICS);
                        // impossible type.
                        OrcProto.DecimalStatistics decimalStatistics = columnStatistics.getDecimalStatistics();
                        binaryStatisticsOfColumn.appendInt(FLAG_DECIMAL_STATISTICS);

                        // serialize minimum
                        if (decimalStatistics.hasMinimum() && decimalStatistics.getMinimum() != null) {
                            binaryStatisticsOfColumn.appendBoolean(true);
                            byte[] minimumBytes = decimalStatistics.getMinimum().getBytes();
                            binaryStatisticsOfColumn.appendInt(minimumBytes.length);
                            binaryStatisticsOfColumn.appendBytes(minimumBytes);
                        } else {
                            binaryStatisticsOfColumn.appendBoolean(false);
                        }

                        // serialize maximum
                        if (decimalStatistics.hasMaximum() && decimalStatistics.getMaximum() != null) {
                            binaryStatisticsOfColumn.appendBoolean(true);
                            byte[] maximumBytes = decimalStatistics.getMaximum().getBytes();
                            binaryStatisticsOfColumn.appendInt(maximumBytes.length);
                            binaryStatisticsOfColumn.appendBytes(maximumBytes);
                        } else {
                            binaryStatisticsOfColumn.appendBoolean(false);
                        }

                        // serialize sum
                        if (decimalStatistics.hasSum() && decimalStatistics.getSum() != null) {
                            binaryStatisticsOfColumn.appendBoolean(true);
                            byte[] sumBytes = decimalStatistics.getSum().getBytes();
                            binaryStatisticsOfColumn.appendInt(sumBytes.length);
                            binaryStatisticsOfColumn.appendBytes(sumBytes);
                        } else {
                            binaryStatisticsOfColumn.appendBoolean(false);
                        }
                    } else if (columnStatistics.hasTimestampStatistics()) {
                        statisticsTypeOfAllColumn.put(column, FLAG_TIMESTAMP_STATISTICS);
                        // impossible type.
                        OrcProto.TimestampStatistics timestampStatistics = columnStatistics.getTimestampStatistics();
                        binaryStatisticsOfColumn.appendInt(FLAG_TIMESTAMP_STATISTICS);
                        binaryStatisticsOfColumn.appendLong(timestampStatistics.getMinimum());
                        binaryStatisticsOfColumn.appendLong(timestampStatistics.getMaximum());
                        binaryStatisticsOfColumn.appendLong(timestampStatistics.getMinimumUtc());
                        binaryStatisticsOfColumn.appendLong(timestampStatistics.getMaximumUtc());
                        binaryStatisticsOfColumn.appendInt(timestampStatistics.getMinimumNanos());
                        binaryStatisticsOfColumn.appendInt(timestampStatistics.getMaximumNanos());
                    } else if (columnStatistics.hasBinaryStatistics()) {
                        statisticsTypeOfAllColumn.put(column, FLAG_BINARY_STATISTICS);
                        // impossible type.
                        OrcProto.BinaryStatistics binaryStatistics = columnStatistics.getBinaryStatistics();
                        binaryStatisticsOfColumn.appendInt(FLAG_BINARY_STATISTICS);
                        binaryStatisticsOfColumn.appendLong(binaryStatistics.getSum());
                    } else if (columnStatistics.hasCollectionStatistics()) {
                        statisticsTypeOfAllColumn.put(column, FLAG_COLLECTION_STATISTICS);
                        // impossible type.
                        OrcProto.CollectionStatistics collectionStatistics = columnStatistics.getCollectionStatistics();
                        binaryStatisticsOfColumn.appendInt(FLAG_COLLECTION_STATISTICS);
                        binaryStatisticsOfColumn.appendLong(collectionStatistics.getMinChildren());
                        binaryStatisticsOfColumn.appendLong(collectionStatistics.getMaxChildren());
                        binaryStatisticsOfColumn.appendLong(collectionStatistics.getTotalChildren());
                    } else if (columnStatistics.hasBucketStatistics()) {
                        statisticsTypeOfAllColumn.put(column, FLAG_BUCKET_STATISTICS);
                        // impossible type.
                        OrcProto.BucketStatistics bucketStatistics = columnStatistics.getBucketStatistics();
                        binaryStatisticsOfColumn.appendInt(FLAG_BUCKET_STATISTICS);
                        List<Long> countList = bucketStatistics.getCountList();
                        binaryStatisticsOfColumn.appendInt(countList.size());
                        for (int j = 0; j < countList.size(); j++) {
                            binaryStatisticsOfColumn.appendLong(countList.get(j));
                        }
                    } else {
                        statisticsTypeOfAllColumn.put(column, FLAG_NO_STATISTICS);
                        // first struct type, unknown statistics
                        binaryStatisticsOfColumn.appendInt(FLAG_NO_STATISTICS);
                    }

                }

            }

        }

        // Summary:
        // 1. check column consistency
        if (totalColumnCount != positionsOfAllColumn.size()) {
            throw new AssertionError("column count is not consistent, totalColumnCount: " + totalColumnCount
                + ", positionsOfAllColumn.size(): " + positionsOfAllColumn.size());
        }
        if (totalColumnCount != positionsSizeListOfAllColumn.size()) {
            throw new AssertionError("column count is not consistent, totalColumnCount: " + totalColumnCount
                + ", positionsSizeListOfAllColumn.size(): " + positionsSizeListOfAllColumn.size());
        }
        if (totalColumnCount != statisticsOfAllColumn.size()) {
            throw new AssertionError("column count is not consistent, totalColumnCount: " + totalColumnCount
                + ", statisticsOfAllColumn.size(): " + statisticsOfAllColumn.size());
        }
        if (totalColumnCount != statisticsNullBitmapOfAllColumn.size()) {
            throw new AssertionError("column count is not consistent, totalColumnCount: " + totalColumnCount
                + ", statisticsNullBitmapOfAllColumn.size(): " + statisticsNullBitmapOfAllColumn.size());
        }
        if (totalColumnCount != statisticsTypeOfAllColumn.size()) {
            throw new AssertionError("column count is not consistent, totalColumnCount: " + totalColumnCount
                + ", statisticsTypeOfAllColumn.size(): " + statisticsTypeOfAllColumn.size());
        }

        // 2. check row group count consistency
        for (Map.Entry<Integer, List<Integer>> entry : positionsSizeListOfAllColumn.entrySet()) {
            int column = entry.getKey();
            List<Integer> positionsSizeList = entry.getValue();
            if (positionsSizeList.size() != totalStripeCount) {
                throw new AssertionError(
                    "total Stripe Count is not consistent, column: " + column + ", positionsSizeList.size(): "
                        + positionsSizeList.size() + ", totalStripeCount: " + totalStripeCount);
            }
        }
        for (Map.Entry<Integer, DynamicByteBuffer> entry : statisticsNullBitmapOfAllColumn.entrySet()) {
            int column = entry.getKey();
            DynamicByteBuffer statisticsNullBitmap = entry.getValue();
            if (statisticsNullBitmap.size() != totalRowGroupCount) {
                throw new AssertionError("statistics null bitmap size is not consistent, column: " + column
                    + ", statisticsNullBitmap.size(): " + statisticsNullBitmap.size() + ", totalRowGroupCount: "
                    + totalRowGroupCount);
            }
        }

        // set total row group count
        builder.setTotalRowGroupCount(totalRowGroupCount);

        // build accumulated row group count for all stripes
        for (int i = 0; i < rowGroupCountUtilThisStripe.length; i++) {
            builder.addAccumulatedRowGroupCountPerStripe(rowGroupCountUtilThisStripe[i]);
        }

        // build position list for all columns
        for (Map.Entry<Integer, DynamicByteBuffer> entry : positionsOfAllColumn.entrySet()) {
            int columnId = entry.getKey();
            DynamicByteBuffer positionList = entry.getValue();
            ByteString positionByteString = positionList.getByteString();
            builder.addBinaryPositions(positionByteString);
        }

        // build position list unit size for all columns
        for (Map.Entry<Integer, List<Integer>> entry : positionsSizeListOfAllColumn.entrySet()) {
            int columnId = entry.getKey();
            List<Integer> positionsSizeList = entry.getValue();

            // convert positionsSizeList into positionsSizeByteString
            ByteString positionsSizeByteString = null;
            byte[] sizeBytes = new byte[positionsSizeList.size()];
            for (int i = 0; i < positionsSizeList.size(); i++) {
                int size = positionsSizeList.get(i);
                sizeBytes[i] = (byte) size;
            }
            positionsSizeByteString = ByteString.copyFrom(sizeBytes);

            // we assume that positions size are consistent
            builder.addColumnPositionUnitSize(positionsSizeByteString);
        }

        // build binary statistics for all columns
        for (Map.Entry<Integer, DynamicByteBuffer> entry : statisticsOfAllColumn.entrySet()) {
            int columnId = entry.getKey();
            DynamicByteBuffer statistics = entry.getValue();
            ByteString statisticsByteString = statistics.getByteString();
            builder.addBinaryStatistics(statisticsByteString);
        }

        // build binary statistics null bitmap for all columns
        for (Map.Entry<Integer, DynamicByteBuffer> entry : statisticsNullBitmapOfAllColumn.entrySet()) {
            int columnId = entry.getKey();
            DynamicByteBuffer statisticsNullBitmap = entry.getValue();
            ByteString statisticsNullBitmapByteString = statisticsNullBitmap.getByteString();
            builder.addBinaryStatisticsNullBitmap(statisticsNullBitmapByteString);
        }

        // build binary statistics type for all columns
        for (Map.Entry<Integer, Integer> entry : statisticsTypeOfAllColumn.entrySet()) {
            int columnId = entry.getKey();
            int statisticsType = entry.getValue();
            builder.addColumnStatisticsType(statisticsType);
        }

        return builder.build();
    }

    public static int serializeColumnEncodingKindAndBloomEncoding(OrcProto.ColumnEncoding.Kind kind,
                                                                  int bloomEncoding) {
        // kind int8, bloomEncoding int8
        // -> int32 (kind, bloomEncoding)
        int kindNumber = kind == null ? -1 : kind.getNumber();
        int result = ((kindNumber & 0xFF) << 8) | (bloomEncoding & 0xFF);
        return result;
    }

    public static short serializeColumnEncodingKindAndBloomEncodingToShort(int columnEncodingKindAndBloomEncoding) {
        // kind int8, bloomEncoding int8
        // -> int32 (kind, bloomEncoding)
        // int kindNumber = kind == null ? -1 : kind.getNumber();
        // int columnEncodingKindAndBloomEncoding = ((kindNumber & 0xFF) << 8) | (bloomEncoding & 0xFF);

        // 解出(kind, bloomEncoding)
        int kind = (columnEncodingKindAndBloomEncoding >> 8) & 0xFF;
        int bloomEncoding = columnEncodingKindAndBloomEncoding & 0xFF;

        // 压缩为short，kind 取值-1 ~ 3, bloomEncoding 取值0 ~ 1
        short result = (short) (((kind & 0x7F) << 1) | (bloomEncoding & 0x01));

        return result;
    }

    public static long serializeStreamColumnAndKind(int columnId, OrcProto.Stream.Kind kind) {
        // column int32, kind int8
        // -> long (column, kind)
        int kindNumber = kind == null ? -1 : kind.getNumber();
        long result = ((columnId & 0xFFFFFFFFL) << 8) | (kindNumber & 0xFF);
        return result;
    }

    public static int serializeStreamColumnAndKindToInt(long streamColumnAndKind) {
        // 从 long 中解出 columnId 和 kindNumber
        int columnId = (int) (streamColumnAndKind >> 8);
        int kindNumber = (int) (streamColumnAndKind & 0xFF);

        // 确保 columnId 在有效范围内
        if (columnId > 0xFFF) {
            throw new IllegalArgumentException("ColumnId " + columnId + " exceeds maximum value 4095");
        }

        // 压缩为 int
        int result = ((columnId & 0xFFF) << 8) | (kindNumber & 0xFF);
        return result;
    }
}
