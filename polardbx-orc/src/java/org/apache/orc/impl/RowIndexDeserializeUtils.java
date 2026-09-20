package org.apache.orc.impl;

import com.google.protobuf.ByteString;
import org.apache.orc.OrcProto;

import java.nio.ByteBuffer;

import static org.apache.orc.impl.RowIndexUtilBase.FLAG_BINARY_STATISTICS;
import static org.apache.orc.impl.RowIndexUtilBase.FLAG_BUCKET_STATISTICS;
import static org.apache.orc.impl.RowIndexUtilBase.FLAG_COLLECTION_STATISTICS;
import static org.apache.orc.impl.RowIndexUtilBase.FLAG_DATE_STATISTICS;
import static org.apache.orc.impl.RowIndexUtilBase.FLAG_DECIMAL_STATISTICS;
import static org.apache.orc.impl.RowIndexUtilBase.FLAG_DOUBLE_STATISTICS;
import static org.apache.orc.impl.RowIndexUtilBase.FLAG_INT_STATISTICS;
import static org.apache.orc.impl.RowIndexUtilBase.FLAG_NO_STATISTICS;
import static org.apache.orc.impl.RowIndexUtilBase.FLAG_STRING_STATISTICS;
import static org.apache.orc.impl.RowIndexUtilBase.FLAG_TIMESTAMP_STATISTICS;

public class RowIndexDeserializeUtils {

    public static OrcProto.RowIndex transformToRowIndex(OrcProto.BinaryRowIndex binaryRowIndex) {
        ByteString byteString = binaryRowIndex.getBinaryEntry();

        ByteBuffer byteBuffer = byteString.asReadOnlyByteBuffer();
        byteBuffer.rewind();

        OrcProto.RowIndex.Builder rowIndexBuilder = OrcProto.RowIndex.newBuilder();

        int numRowGroups = byteBuffer.getInt();
        for (int i = 0; i < numRowGroups; i++) {
            OrcProto.RowIndexEntry.Builder rowIndexEntryBuilder = OrcProto.RowIndexEntry.newBuilder();

            // positions
            int numPositions = byteBuffer.getInt();
            for (int j = 0; j < numPositions; j++) {
                rowIndexEntryBuilder.addPositions(byteBuffer.getLong());
            }

            // ColumnStatistics
            OrcProto.ColumnStatistics.Builder columnStatisticsBuilder = OrcProto.ColumnStatistics.newBuilder();
            boolean hasNumberOfValues = byteBuffer.get() == (byte) 1;
            if (hasNumberOfValues) {
                columnStatisticsBuilder.setNumberOfValues(byteBuffer.getLong());
            }
            boolean hasHasNull = byteBuffer.get() == (byte) 1;
            if (hasHasNull) {
                columnStatisticsBuilder.setHasNull(byteBuffer.get() == (byte) 1);
            }
            boolean hasBytesOnDisk = byteBuffer.get() == (byte) 1;
            if (hasBytesOnDisk) {
                columnStatisticsBuilder.setBytesOnDisk(byteBuffer.getLong());
            }

            // Deserialize XxxColumnStatistics
            int statisticsFlag = byteBuffer.getInt();
            if (statisticsFlag == FLAG_INT_STATISTICS) {
                OrcProto.IntegerStatistics.Builder integerStatisticsBuilder = OrcProto.IntegerStatistics.newBuilder();
                integerStatisticsBuilder.setMinimum(byteBuffer.getLong());
                integerStatisticsBuilder.setMaximum(byteBuffer.getLong());
                integerStatisticsBuilder.setSum(byteBuffer.getLong());
                integerStatisticsBuilder.setFirst(byteBuffer.getLong());
                integerStatisticsBuilder.setLatest(byteBuffer.getLong());
                columnStatisticsBuilder.setIntStatistics(integerStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_DOUBLE_STATISTICS) {
                OrcProto.DoubleStatistics.Builder doubleStatisticsBuilder = OrcProto.DoubleStatistics.newBuilder();
                doubleStatisticsBuilder.setMinimum(byteBuffer.getDouble());
                doubleStatisticsBuilder.setMaximum(byteBuffer.getDouble());
                doubleStatisticsBuilder.setSum(byteBuffer.getDouble());
                doubleStatisticsBuilder.setFirst(byteBuffer.getDouble());
                doubleStatisticsBuilder.setLatest(byteBuffer.getDouble());
                columnStatisticsBuilder.setDoubleStatistics(doubleStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_STRING_STATISTICS) {
                buildStringStatistics(columnStatisticsBuilder, byteBuffer);
            } else if (statisticsFlag == FLAG_DECIMAL_STATISTICS) {
                buildDecimalStatistics(columnStatisticsBuilder, byteBuffer);
            } else if (statisticsFlag == FLAG_DATE_STATISTICS) {
                OrcProto.DateStatistics.Builder dateStatisticsBuilder = OrcProto.DateStatistics.newBuilder();
                dateStatisticsBuilder.setMinimum(byteBuffer.getInt());
                dateStatisticsBuilder.setMaximum(byteBuffer.getInt());
                dateStatisticsBuilder.setFirst(byteBuffer.getInt());
                dateStatisticsBuilder.setLatest(byteBuffer.getInt());
                columnStatisticsBuilder.setDateStatistics(dateStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_TIMESTAMP_STATISTICS) {
                OrcProto.TimestampStatistics.Builder timestampStatisticsBuilder =
                    OrcProto.TimestampStatistics.newBuilder();
                timestampStatisticsBuilder.setMinimum(byteBuffer.getLong());
                timestampStatisticsBuilder.setMaximum(byteBuffer.getLong());
                timestampStatisticsBuilder.setMinimumUtc(byteBuffer.getLong());
                timestampStatisticsBuilder.setMaximumUtc(byteBuffer.getLong());
                timestampStatisticsBuilder.setMinimumNanos(byteBuffer.getInt());
                timestampStatisticsBuilder.setMaximumNanos(byteBuffer.getInt());
                columnStatisticsBuilder.setTimestampStatistics(timestampStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_BINARY_STATISTICS) {
                OrcProto.BinaryStatistics.Builder binaryStatisticsBuilder = OrcProto.BinaryStatistics.newBuilder();
                binaryStatisticsBuilder.setSum(byteBuffer.getLong());
                columnStatisticsBuilder.setBinaryStatistics(binaryStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_COLLECTION_STATISTICS) {
                OrcProto.CollectionStatistics.Builder collectionStatisticsBuilder =
                    OrcProto.CollectionStatistics.newBuilder();
                collectionStatisticsBuilder.setMinChildren(byteBuffer.getLong());
                collectionStatisticsBuilder.setMaxChildren(byteBuffer.getLong());
                collectionStatisticsBuilder.setTotalChildren(byteBuffer.getLong());
                columnStatisticsBuilder.setCollectionStatistics(collectionStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_BUCKET_STATISTICS) {
                OrcProto.BucketStatistics.Builder bucketStatisticsBuilder = OrcProto.BucketStatistics.newBuilder();
                int numCounts = byteBuffer.getInt();
                for (int j = 0; j < numCounts; j++) {
                    bucketStatisticsBuilder.addCount(byteBuffer.getLong());
                }
                columnStatisticsBuilder.setBucketStatistics(bucketStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_NO_STATISTICS) {
                // no statistics
            }

            rowIndexEntryBuilder.setStatistics(columnStatisticsBuilder.build());
            rowIndexBuilder.addEntry(rowIndexEntryBuilder.build());
        }
        return rowIndexBuilder.build();
    }

    public static OrcProto.RowIndexWithColumn transformToRowIndexWithColumn(
        OrcProto.BinaryRowIndexWithColumn binaryRowIndexWithColumn) {
        int column = binaryRowIndexWithColumn.getColumn();
        ByteString byteString = binaryRowIndexWithColumn.getBinaryEntry();

        ByteBuffer byteBuffer = byteString.asReadOnlyByteBuffer();
        byteBuffer.rewind();

        int deserializedColumn = byteBuffer.getInt();

        OrcProto.RowIndexWithColumn.Builder rowIndexWithColumnBuilder = OrcProto.RowIndexWithColumn.newBuilder();
        rowIndexWithColumnBuilder.setColumn(column);

        OrcProto.RowIndex.Builder rowIndexBuilder = OrcProto.RowIndex.newBuilder();

        int numRowGroups = byteBuffer.getInt();
        for (int i = 0; i < numRowGroups; i++) {
            OrcProto.RowIndexEntry.Builder rowIndexEntryBuilder = OrcProto.RowIndexEntry.newBuilder();

            // positions
            int numPositions = byteBuffer.getInt();
            for (int j = 0; j < numPositions; j++) {
                rowIndexEntryBuilder.addPositions(byteBuffer.getLong());
            }

            // ColumnStatistics
            OrcProto.ColumnStatistics.Builder columnStatisticsBuilder = OrcProto.ColumnStatistics.newBuilder();
            boolean hasNumberOfValues = byteBuffer.get() == (byte) 1;
            if (hasNumberOfValues) {
                columnStatisticsBuilder.setNumberOfValues(byteBuffer.getLong());
            }
            boolean hasHasNull = byteBuffer.get() == (byte) 1;
            if (hasHasNull) {
                columnStatisticsBuilder.setHasNull(byteBuffer.get() == (byte) 1);
            }
            boolean hasBytesOnDisk = byteBuffer.get() == (byte) 1;
            if (hasBytesOnDisk) {
                columnStatisticsBuilder.setBytesOnDisk(byteBuffer.getLong());
            }

            // Deserialize XxxColumnStatistics
            int statisticsFlag = byteBuffer.getInt();
            if (statisticsFlag == FLAG_INT_STATISTICS) {
                OrcProto.IntegerStatistics.Builder integerStatisticsBuilder = OrcProto.IntegerStatistics.newBuilder();
                integerStatisticsBuilder.setMinimum(byteBuffer.getLong());
                integerStatisticsBuilder.setMaximum(byteBuffer.getLong());
                integerStatisticsBuilder.setSum(byteBuffer.getLong());
                integerStatisticsBuilder.setFirst(byteBuffer.getLong());
                integerStatisticsBuilder.setLatest(byteBuffer.getLong());
                columnStatisticsBuilder.setIntStatistics(integerStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_DOUBLE_STATISTICS) {
                OrcProto.DoubleStatistics.Builder doubleStatisticsBuilder = OrcProto.DoubleStatistics.newBuilder();
                doubleStatisticsBuilder.setMinimum(byteBuffer.getDouble());
                doubleStatisticsBuilder.setMaximum(byteBuffer.getDouble());
                doubleStatisticsBuilder.setSum(byteBuffer.getDouble());
                doubleStatisticsBuilder.setFirst(byteBuffer.getDouble());
                doubleStatisticsBuilder.setLatest(byteBuffer.getDouble());
                columnStatisticsBuilder.setDoubleStatistics(doubleStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_STRING_STATISTICS) {
                buildStringStatistics(columnStatisticsBuilder, byteBuffer);
            } else if (statisticsFlag == FLAG_DECIMAL_STATISTICS) {
                buildDecimalStatistics(columnStatisticsBuilder, byteBuffer);
            } else if (statisticsFlag == FLAG_DATE_STATISTICS) {
                OrcProto.DateStatistics.Builder dateStatisticsBuilder = OrcProto.DateStatistics.newBuilder();
                dateStatisticsBuilder.setMinimum(byteBuffer.getInt());
                dateStatisticsBuilder.setMaximum(byteBuffer.getInt());
                dateStatisticsBuilder.setFirst(byteBuffer.getInt());
                dateStatisticsBuilder.setLatest(byteBuffer.getInt());
                columnStatisticsBuilder.setDateStatistics(dateStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_TIMESTAMP_STATISTICS) {
                OrcProto.TimestampStatistics.Builder timestampStatisticsBuilder =
                    OrcProto.TimestampStatistics.newBuilder();
                timestampStatisticsBuilder.setMinimum(byteBuffer.getLong());
                timestampStatisticsBuilder.setMaximum(byteBuffer.getLong());
                timestampStatisticsBuilder.setMinimumUtc(byteBuffer.getLong());
                timestampStatisticsBuilder.setMaximumUtc(byteBuffer.getLong());
                timestampStatisticsBuilder.setMinimumNanos(byteBuffer.getInt());
                timestampStatisticsBuilder.setMaximumNanos(byteBuffer.getInt());
                columnStatisticsBuilder.setTimestampStatistics(timestampStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_BINARY_STATISTICS) {
                OrcProto.BinaryStatistics.Builder binaryStatisticsBuilder = OrcProto.BinaryStatistics.newBuilder();
                binaryStatisticsBuilder.setSum(byteBuffer.getLong());
                columnStatisticsBuilder.setBinaryStatistics(binaryStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_COLLECTION_STATISTICS) {
                OrcProto.CollectionStatistics.Builder collectionStatisticsBuilder =
                    OrcProto.CollectionStatistics.newBuilder();
                collectionStatisticsBuilder.setMinChildren(byteBuffer.getLong());
                collectionStatisticsBuilder.setMaxChildren(byteBuffer.getLong());
                collectionStatisticsBuilder.setTotalChildren(byteBuffer.getLong());
                columnStatisticsBuilder.setCollectionStatistics(collectionStatisticsBuilder.build());
            } else if (statisticsFlag == FLAG_BUCKET_STATISTICS) {
                OrcProto.BucketStatistics.Builder bucketStatisticsBuilder = OrcProto.BucketStatistics.newBuilder();
                int numCounts = byteBuffer.getInt();
                for (int j = 0; j < numCounts; j++) {
                    bucketStatisticsBuilder.addCount(byteBuffer.getLong());
                }
                columnStatisticsBuilder.setBucketStatistics(bucketStatisticsBuilder.build());
            }

            rowIndexEntryBuilder.setStatistics(columnStatisticsBuilder.build());
            rowIndexBuilder.addEntry(rowIndexEntryBuilder.build());
        }
        rowIndexWithColumnBuilder.setRowIndex(rowIndexBuilder.build());
        return rowIndexWithColumnBuilder.build();
    }

    private static void buildStringStatistics(OrcProto.ColumnStatistics.Builder columnStatisticsBuilder,
                                              ByteBuffer byteBuffer) {
        OrcProto.StringStatistics.Builder stringStatisticsBuilder =
            columnStatisticsBuilder.getStringStatisticsBuilder();
        boolean hasMinimum = byteBuffer.get() == (byte) 1;
        if (hasMinimum) {
            int minimumLength = byteBuffer.getInt();
            byte[] minimumBytes = new byte[minimumLength];
            byteBuffer.get(minimumBytes);
            stringStatisticsBuilder.setMinimum(new String(minimumBytes));
        }
        boolean hasMaximum = byteBuffer.get() == (byte) 1;
        if (hasMaximum) {
            int maximumLength = byteBuffer.getInt();
            byte[] maximumBytes = new byte[maximumLength];
            byteBuffer.get(maximumBytes);
            stringStatisticsBuilder.setMaximum(new String(maximumBytes));
        }
        boolean hasSum = byteBuffer.get() == (byte) 1;
        if (hasSum) {
            stringStatisticsBuilder.setSum(byteBuffer.getLong());
        }
        boolean hasLowerBound = byteBuffer.get() == (byte) 1;
        if (hasLowerBound) {
            int lowerBoundLength = byteBuffer.getInt();
            byte[] lowerBoundBytes = new byte[lowerBoundLength];
            byteBuffer.get(lowerBoundBytes);
            stringStatisticsBuilder.setLowerBound(new String(lowerBoundBytes));
        }
        boolean hasUpperBound = byteBuffer.get() == (byte) 1;
        if (hasUpperBound) {
            int upperBoundLength = byteBuffer.getInt();
            byte[] upperBoundBytes = new byte[upperBoundLength];
            byteBuffer.get(upperBoundBytes);
            stringStatisticsBuilder.setUpperBound(new String(upperBoundBytes));
        }
        boolean hasFirst = byteBuffer.get() == (byte) 1;
        if (hasFirst) {
            int firstLength = byteBuffer.getInt();
            byte[] firstBytes = new byte[firstLength];
            byteBuffer.get(firstBytes);
            stringStatisticsBuilder.setFirst(new String(firstBytes));
        }
        boolean hasLatest = byteBuffer.get() == (byte) 1;
        if (hasLatest) {
            int latestLength = byteBuffer.getInt();
            byte[] latestBytes = new byte[latestLength];
            byteBuffer.get(latestBytes);
            stringStatisticsBuilder.setLatest(new String(latestBytes));
        }
        boolean hasFirstBound = byteBuffer.get() == (byte) 1;
        if (hasFirstBound) {
            int firstBoundLength = byteBuffer.getInt();
            byte[] firstBoundBytes = new byte[firstBoundLength];
            byteBuffer.get(firstBoundBytes);
            stringStatisticsBuilder.setFirstBound(new String(firstBoundBytes));
        }
        boolean hasLatestBound = byteBuffer.get() == (byte) 1;
        if (hasLatestBound) {
            int latestBoundLength = byteBuffer.getInt();
            byte[] latestBoundBytes = new byte[latestBoundLength];
            byteBuffer.get(latestBoundBytes);
            stringStatisticsBuilder.setLatestBound(new String(latestBoundBytes));
        }
        columnStatisticsBuilder.setStringStatistics(stringStatisticsBuilder);
    }

    private static void buildDecimalStatistics(OrcProto.ColumnStatistics.Builder columnStatisticsBuilder,
                                               ByteBuffer byteBuffer) {
        OrcProto.DecimalStatistics.Builder decimalStatisticsBuilder =
            columnStatisticsBuilder.getDecimalStatisticsBuilder();
        boolean hasMinimum = byteBuffer.get() == (byte) 1;
        if (hasMinimum) {
            int minimumLength = byteBuffer.getInt();
            byte[] minimumBytes = new byte[minimumLength];
            byteBuffer.get(minimumBytes);
            decimalStatisticsBuilder.setMinimum(new String(minimumBytes));
        }
        boolean hasMaximum = byteBuffer.get() == (byte) 1;
        if (hasMaximum) {
            int maximumLength = byteBuffer.getInt();
            byte[] maximumBytes = new byte[maximumLength];
            byteBuffer.get(maximumBytes);
            decimalStatisticsBuilder.setMaximum(new String(maximumBytes));
        }
        boolean hasSum = byteBuffer.get() == (byte) 1;
        if (hasSum) {
            int sumLength = byteBuffer.getInt();
            byte[] sumBytes = new byte[sumLength];
            byteBuffer.get(sumBytes);
            decimalStatisticsBuilder.setSum(new String(sumBytes));
        }
        columnStatisticsBuilder.setDecimalStatistics(decimalStatisticsBuilder);
    }

}
