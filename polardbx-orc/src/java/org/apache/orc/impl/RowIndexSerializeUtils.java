package org.apache.orc.impl;

import com.google.protobuf.ByteString;
import org.apache.orc.OrcProto;

import java.util.List;

import static org.apache.orc.impl.RowIndexUtilBase.*;

public class RowIndexSerializeUtils {

    public static OrcProto.BinaryRowIndex transformToBinaryRowIndex(OrcProto.RowIndex rowIndex) {
        DynamicByteBuffer output = new DynamicByteBuffer();

        // RowIndex： 一个列的所有rg的所有row index
        List<OrcProto.RowIndexEntry> rowIndexEntryList = rowIndex.getEntryList();

        // append number of row group
        output.appendInt(rowIndexEntryList.size());

        for (int i = 0; i < rowIndexEntryList.size(); i++) {

            // RowIndexEntry： 一个列内部，具体的某一个row group所具有的min-max statistics 和 positions信息
            OrcProto.RowIndexEntry rowIndexEntry = rowIndexEntryList.get(i);

            // positions信息
            List<Long> positionList = rowIndexEntry.getPositionsList();

            // 依次append positions信息总长度 + 依次append positions信息
            output.appendInt(positionList.size());
            for (int j = 0; j < positionList.size(); j++) {
                output.appendLong(positionList.get(j));
            }

            // ColumnStatistics： statistics信息汇总
            OrcProto.ColumnStatistics columnStatistics = rowIndexEntry.getStatistics();

            // append statistics 基础信息
            if (columnStatistics.hasNumberOfValues()) {
                // has number of values
                output.appendBoolean(true);
                output.appendLong(columnStatistics.getNumberOfValues());
            } else {
                // no number of values
                output.appendBoolean(false);
            }

            if (columnStatistics.hasHasNull()) {
                // has has null
                output.appendBoolean(true);
                output.appendBoolean(columnStatistics.getHasNull());
            } else {
                // no has null
                output.appendBoolean(false);
            }

            if (columnStatistics.hasBytesOnDisk()) {
                // has bytes on disk
                output.appendBoolean(true);
                output.appendLong(columnStatistics.getBytesOnDisk());
            } else {
                // no bytes on disk
                output.appendBoolean(false);
            }

            // XxxColumnStatistics: 类型特定min-max信息
            if (columnStatistics.hasIntStatistics()) {
                OrcProto.IntegerStatistics integerStatistics = columnStatistics.getIntStatistics();
                output.appendInt(FLAG_INT_STATISTICS);
                output.appendLong(integerStatistics.getMinimum());
                output.appendLong(integerStatistics.getMaximum());
                output.appendLong(integerStatistics.getSum());
                output.appendLong(integerStatistics.getFirst());
                output.appendLong(integerStatistics.getLatest());
            } else if (columnStatistics.hasDoubleStatistics()) {
                OrcProto.DoubleStatistics doubleStatistics = columnStatistics.getDoubleStatistics();
                output.appendInt(FLAG_DOUBLE_STATISTICS);
                output.appendDouble(doubleStatistics.getMinimum());
                output.appendDouble(doubleStatistics.getMaximum());
                output.appendDouble(doubleStatistics.getSum());
                output.appendDouble(doubleStatistics.getFirst());
                output.appendDouble(doubleStatistics.getLatest());
            } else if (columnStatistics.hasStringStatistics()) {

                handleStringStatistics(columnStatistics, output);

            } else if (columnStatistics.hasDecimalStatistics()) {

                handleDecimalStatistics(columnStatistics, output);

            } else if (columnStatistics.hasDateStatistics()) {
                OrcProto.DateStatistics dateStatistics = columnStatistics.getDateStatistics();
                output.appendInt(FLAG_DATE_STATISTICS);
                output.appendInt(dateStatistics.getMinimum());
                output.appendInt(dateStatistics.getMaximum());
                output.appendInt(dateStatistics.getFirst());
                output.appendInt(dateStatistics.getLatest());
            } else if (columnStatistics.hasTimestampStatistics()) {
                OrcProto.TimestampStatistics timestampStatistics = columnStatistics.getTimestampStatistics();
                output.appendInt(FLAG_TIMESTAMP_STATISTICS);
                output.appendLong(timestampStatistics.getMinimum());
                output.appendLong(timestampStatistics.getMaximum());
                output.appendLong(timestampStatistics.getMinimumUtc());
                output.appendLong(timestampStatistics.getMaximumUtc());
                output.appendInt(timestampStatistics.getMinimumNanos());
                output.appendInt(timestampStatistics.getMaximumNanos());
            } else if (columnStatistics.hasBinaryStatistics()) {
                OrcProto.BinaryStatistics binaryStatistics = columnStatistics.getBinaryStatistics();
                output.appendInt(FLAG_BINARY_STATISTICS);
                output.appendLong(binaryStatistics.getSum());
            } else if (columnStatistics.hasCollectionStatistics()) {
                OrcProto.CollectionStatistics collectionStatistics = columnStatistics.getCollectionStatistics();
                output.appendInt(FLAG_COLLECTION_STATISTICS);
                output.appendLong(collectionStatistics.getMinChildren());
                output.appendLong(collectionStatistics.getMaxChildren());
                output.appendLong(collectionStatistics.getTotalChildren());
            } else if (columnStatistics.hasBucketStatistics()) {
                OrcProto.BucketStatistics bucketStatistics = columnStatistics.getBucketStatistics();
                output.appendInt(FLAG_BUCKET_STATISTICS);
                List<Long> countList = bucketStatistics.getCountList();
                output.appendInt(countList.size());
                for (int j = 0; j < countList.size(); j++) {
                    output.appendLong(countList.get(j));
                }
            } else {
                // unknown statistics
                output.appendInt(FLAG_NO_STATISTICS);
            }
        }

        ByteString byteString = output.getByteString();

        // build BinaryRowIndexWithColumn
        OrcProto.BinaryRowIndex.Builder binaryRowIndexBuilder
            = OrcProto.BinaryRowIndex.newBuilder();
        binaryRowIndexBuilder.setBinaryEntry(byteString);

        return binaryRowIndexBuilder.build();
    }

    public static OrcProto.BinaryRowIndexWithColumn transformToBinaryRowIndexWithColumn(OrcProto.RowIndexWithColumn rowIndexWithColumn) {
        DynamicByteBuffer output = new DynamicByteBuffer();

        // column id
        int column = rowIndexWithColumn.getColumn();
        output.appendInt(column);

        // RowIndex： 一个列的所有rg的所有row index
        OrcProto.RowIndex rowIndex = rowIndexWithColumn.getRowIndex();

        List<OrcProto.RowIndexEntry> rowIndexEntryList = rowIndex.getEntryList();

        // append number of row group
        output.appendInt(rowIndexEntryList.size());

        for (int i = 0; i < rowIndexEntryList.size(); i++) {

            // RowIndexEntry： 一个列内部，具体的某一个row group所具有的min-max statistics 和 positions信息
            OrcProto.RowIndexEntry rowIndexEntry = rowIndexEntryList.get(i);

            // positions信息
            List<Long> positionList = rowIndexEntry.getPositionsList();

            // 依次append positions信息总长度 + 依次append positions信息
            output.appendInt(positionList.size());
            for (int j = 0; j < positionList.size(); j++) {
                output.appendLong(positionList.get(j));
            }

            // ColumnStatistics： statistics信息汇总
            OrcProto.ColumnStatistics columnStatistics = rowIndexEntry.getStatistics();

            // append statistics 基础信息
            if (columnStatistics.hasNumberOfValues()) {
                // has number of values
                output.appendBoolean(true);
                output.appendLong(columnStatistics.getNumberOfValues());
            } else {
                // no number of values
                output.appendBoolean(false);
            }

            if (columnStatistics.hasHasNull()) {
                // has has null
                output.appendBoolean(true);
                output.appendBoolean(columnStatistics.getHasNull());
            } else {
                // no has null
                output.appendBoolean(false);
            }

            if (columnStatistics.hasBytesOnDisk()) {
                // has bytes on disk
                output.appendBoolean(true);
                output.appendLong(columnStatistics.getBytesOnDisk());
            } else {
                // no bytes on disk
                output.appendBoolean(false);
            }

            // XxxColumnStatistics: 类型特定min-max信息
            if (columnStatistics.hasIntStatistics()) {
                OrcProto.IntegerStatistics integerStatistics = columnStatistics.getIntStatistics();
                output.appendInt(FLAG_INT_STATISTICS);
                output.appendLong(integerStatistics.getMinimum());
                output.appendLong(integerStatistics.getMaximum());
                output.appendLong(integerStatistics.getSum());
                output.appendLong(integerStatistics.getFirst());
                output.appendLong(integerStatistics.getLatest());
            } else if (columnStatistics.hasDoubleStatistics()) {
                OrcProto.DoubleStatistics doubleStatistics = columnStatistics.getDoubleStatistics();
                output.appendInt(FLAG_DOUBLE_STATISTICS);
                output.appendDouble(doubleStatistics.getMinimum());
                output.appendDouble(doubleStatistics.getMaximum());
                output.appendDouble(doubleStatistics.getSum());
                output.appendDouble(doubleStatistics.getFirst());
                output.appendDouble(doubleStatistics.getLatest());
            } else if (columnStatistics.hasStringStatistics()) {

                handleStringStatistics(columnStatistics, output);

            } else if (columnStatistics.hasDecimalStatistics()) {

                handleDecimalStatistics(columnStatistics, output);

            } else if (columnStatistics.hasDateStatistics()) {
                OrcProto.DateStatistics dateStatistics = columnStatistics.getDateStatistics();
                output.appendInt(FLAG_DATE_STATISTICS);
                output.appendInt(dateStatistics.getMinimum());
                output.appendInt(dateStatistics.getMaximum());
                output.appendInt(dateStatistics.getFirst());
                output.appendInt(dateStatistics.getLatest());
            } else if (columnStatistics.hasTimestampStatistics()) {
                OrcProto.TimestampStatistics timestampStatistics = columnStatistics.getTimestampStatistics();
                output.appendInt(FLAG_TIMESTAMP_STATISTICS);
                output.appendLong(timestampStatistics.getMinimum());
                output.appendLong(timestampStatistics.getMaximum());
                output.appendLong(timestampStatistics.getMinimumUtc());
                output.appendLong(timestampStatistics.getMaximumUtc());
                output.appendInt(timestampStatistics.getMinimumNanos());
                output.appendInt(timestampStatistics.getMaximumNanos());
            } else if (columnStatistics.hasBinaryStatistics()) {
                OrcProto.BinaryStatistics binaryStatistics = columnStatistics.getBinaryStatistics();
                output.appendInt(FLAG_BINARY_STATISTICS);
                output.appendLong(binaryStatistics.getSum());
            } else if (columnStatistics.hasCollectionStatistics()) {
                OrcProto.CollectionStatistics collectionStatistics = columnStatistics.getCollectionStatistics();
                output.appendInt(FLAG_COLLECTION_STATISTICS);
                output.appendLong(collectionStatistics.getMinChildren());
                output.appendLong(collectionStatistics.getMaxChildren());
                output.appendLong(collectionStatistics.getTotalChildren());
            } else if (columnStatistics.hasBucketStatistics()) {
                OrcProto.BucketStatistics bucketStatistics = columnStatistics.getBucketStatistics();
                output.appendInt(FLAG_BUCKET_STATISTICS);
                List<Long> countList = bucketStatistics.getCountList();
                output.appendInt(countList.size());
                for (int j = 0; j < countList.size(); j++) {
                    output.appendLong(countList.get(j));
                }
            } else {
                // unknown statistics
                output.appendInt(FLAG_NO_STATISTICS);
            }
        }

        ByteString byteString = output.getByteString();

        // build BinaryRowIndexWithColumn
        OrcProto.BinaryRowIndexWithColumn.Builder binaryRowIndexWithColumnBuilder
            = OrcProto.BinaryRowIndexWithColumn.newBuilder();
        binaryRowIndexWithColumnBuilder.setColumn(column);
        binaryRowIndexWithColumnBuilder.setBinaryEntry(byteString);

        return binaryRowIndexWithColumnBuilder.build();
    }

    private static void handleDecimalStatistics(OrcProto.ColumnStatistics columnStatistics, DynamicByteBuffer output) {
        OrcProto.DecimalStatistics decimalStatistics = columnStatistics.getDecimalStatistics();
        output.appendInt(FLAG_DECIMAL_STATISTICS);

        // serialize minimum
        if (decimalStatistics.hasMinimum() && decimalStatistics.getMinimum() != null) {
            output.appendBoolean(true);
            byte[] minimumBytes = decimalStatistics.getMinimum().getBytes();
            output.appendInt(minimumBytes.length);
            output.appendBytes(minimumBytes);
        } else {
            output.appendBoolean(false);
        }

        // serialize maximum
        if (decimalStatistics.hasMaximum() && decimalStatistics.getMaximum() != null) {
            output.appendBoolean(true);
            byte[] maximumBytes = decimalStatistics.getMaximum().getBytes();
            output.appendInt(maximumBytes.length);
            output.appendBytes(maximumBytes);
        } else {
            output.appendBoolean(false);
        }

        // serialize sum
        if (decimalStatistics.hasSum() && decimalStatistics.getSum() != null) {
            output.appendBoolean(true);
            byte[] sumBytes = decimalStatistics.getSum().getBytes();
            output.appendInt(sumBytes.length);
            output.appendBytes(sumBytes);
        } else {
            output.appendBoolean(false);
        }
    }

    private static void handleStringStatistics(OrcProto.ColumnStatistics columnStatistics, DynamicByteBuffer output) {
        OrcProto.StringStatistics stringStatistics = columnStatistics.getStringStatistics();
        output.appendInt(FLAG_STRING_STATISTICS);

        // serialize minimum
        if (stringStatistics.hasMinimum() && stringStatistics.getMinimum().getBytes() != null) {
            output.appendBoolean(true);
            byte[] minimumBytes = stringStatistics.getMinimum().getBytes();
            output.appendInt(minimumBytes.length);
            output.appendBytes(stringStatistics.getMinimum().getBytes());
        } else {
            output.appendBoolean(false);
        }

        // serialize maximum
        if (stringStatistics.hasMaximum() && stringStatistics.getMaximum().getBytes() != null) {
            output.appendBoolean(true);
            byte[] maximumBytes = stringStatistics.getMaximum().getBytes();
            output.appendInt(maximumBytes.length);
            output.appendBytes(stringStatistics.getMaximum().getBytes());
        } else {
            output.appendBoolean(false);
        }

        // serialize sum
        if (stringStatistics.hasSum()) {
            output.appendBoolean(true);
            output.appendLong(stringStatistics.getSum());
        } else {
            output.appendBoolean(false);
        }

        // serialize lower bound
        if (stringStatistics.hasLowerBound() && stringStatistics.getLowerBound().getBytes() != null) {
            output.appendBoolean(true);
            byte[] lowerBoundBytes = stringStatistics.getLowerBound().getBytes();
            output.appendInt(lowerBoundBytes.length);
            output.appendBytes(stringStatistics.getLowerBound().getBytes());
        } else {
            output.appendBoolean(false);
        }

        // serialize upper bound
        if (stringStatistics.hasUpperBound() && stringStatistics.getUpperBound().getBytes() != null) {
            output.appendBoolean(true);
            byte[] upperBoundBytes = stringStatistics.getUpperBound().getBytes();
            output.appendInt(upperBoundBytes.length);
            output.appendBytes(stringStatistics.getUpperBound().getBytes());
        } else {
            output.appendBoolean(false);
        }

        // serialize first
        if (stringStatistics.hasFirst() && stringStatistics.getFirst().getBytes() != null) {
            output.appendBoolean(true);
            byte[] firstBytes = stringStatistics.getFirst().getBytes();
            output.appendInt(firstBytes.length);
            output.appendBytes(stringStatistics.getFirst().getBytes());
        } else {
            output.appendBoolean(false);
        }

        // serialize latest
        if (stringStatistics.hasLatest() && stringStatistics.getLatest().getBytes() != null) {
            output.appendBoolean(true);
            byte[] latestBytes = stringStatistics.getLatest().getBytes();
            output.appendInt(latestBytes.length);
            output.appendBytes(stringStatistics.getLatest().getBytes());
        } else {
            output.appendBoolean(false);
        }

        // serialize first bound
        if (stringStatistics.hasFirstBound() && stringStatistics.getFirstBound().getBytes() != null) {
            output.appendBoolean(true);
            byte[] firstBoundBytes = stringStatistics.getFirstBound().getBytes();
            output.appendInt(firstBoundBytes.length);
            output.appendBytes(stringStatistics.getFirstBound().getBytes());
        } else {
            output.appendBoolean(false);
        }

        // serialize latest bound
        if (stringStatistics.hasLatestBound() && stringStatistics.getLatestBound().getBytes() != null) {
            output.appendBoolean(true);
            byte[] latestBoundBytes = stringStatistics.getLatestBound().getBytes();
            output.appendInt(latestBoundBytes.length);
            output.appendBytes(stringStatistics.getLatestBound().getBytes());
        } else {
            output.appendBoolean(false);
        }
    }
}
