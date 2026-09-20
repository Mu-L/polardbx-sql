package org.apache.orc.impl;

import com.google.protobuf.ByteString;
import org.apache.orc.OrcProto;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MetadataDeserializeUtils {
    public static OrcProto.RedundantMetadata transformToRedundantMetadata(
        OrcProto.BinaryMetadata binaryMetadata) {
        OrcProto.RedundantMetadata.Builder redundantMetadataBuilder = OrcProto.RedundantMetadata.newBuilder();

        List<OrcProto.RedundantStripeMetadata.Builder> redundantStripeMetadataBuilderList = new ArrayList<>();

        final int totalRowGroupCount = binaryMetadata.getTotalRowGroupCount();

        List<OrcProto.StripeFooterWithId> stripeFooterWithIdList = new ArrayList<>();
        List<OrcProto.StripeFooter.Builder> stripeFooterBuilderList = new ArrayList<>();

        // for each stripe
        final int stripeSize = binaryMetadata.getStripeSize();
        for (int stripeIndex = 0; stripeIndex < stripeSize; stripeIndex++) {
            OrcProto.StripeFooterWithId.Builder stripeFooterWithIdBuilder = OrcProto.StripeFooterWithId.newBuilder();
            stripeFooterWithIdBuilder.setStripe(stripeIndex);

            OrcProto.StripeFooter.Builder stripeFooterBuilder = OrcProto.StripeFooter.newBuilder();
            stripeFooterBuilderList.add(stripeFooterBuilder);

            // set writer time zone
            String writerTimeZone = binaryMetadata.getWriterTimeZones(stripeIndex);
            stripeFooterBuilder.setWriterTimezone(writerTimeZone);

            // set stream
            List<OrcProto.Stream.Builder> streamBuilderList = new ArrayList<>();
            ByteString streamByteString = binaryMetadata.getStreams(stripeIndex);
            ByteBuffer streamBuffer = streamByteString.asReadOnlyByteBuffer();
            streamBuffer.rewind();
            while (streamBuffer.hasRemaining()) {
                OrcProto.Stream.Builder streamBuilder = OrcProto.Stream.newBuilder();
                long streamColumnAndKind = streamBuffer.getLong();
                long streamLength = streamBuffer.getLong();

                // deserialize column and kind
                deserializeColumnAndKind(streamBuilder, streamColumnAndKind);
                streamBuilder.setLength(streamLength);
                streamBuilderList.add(streamBuilder);
            }
            for (OrcProto.Stream.Builder streamBuilder : streamBuilderList) {
                OrcProto.Stream stream = streamBuilder.build();
                stripeFooterBuilder.addStreams(stream);
            }
        }

        List<ByteString> columnEncodingByteStringList = binaryMetadata.getColumnEncodingsList();
        for (int columnIndex = 0; columnIndex < columnEncodingByteStringList.size(); columnIndex++) {
            ByteString columnEncodingByteString = columnEncodingByteStringList.get(columnIndex);
            ByteBuffer columnEncodingBuffer = columnEncodingByteString.asReadOnlyByteBuffer();
            columnEncodingBuffer.rewind();

            // for each column, deserialize kind and bloomEncoding and set into every stripe.
            int stripeIndex = 0;
            while (columnEncodingBuffer.hasRemaining()) {
                OrcProto.ColumnEncoding.Builder columnEncodingBuilder = OrcProto.ColumnEncoding.newBuilder();
                int kindAndBloomEncoding = columnEncodingBuffer.getInt();
                int dictionarySize = columnEncodingBuffer.getInt();

                deserializeColumnEncodingKindAndBloomEncoding(columnEncodingBuilder, kindAndBloomEncoding);
                columnEncodingBuilder.setDictionarySize(dictionarySize);
                OrcProto.ColumnEncoding columnEncoding = columnEncodingBuilder.build();

                OrcProto.StripeFooter.Builder stripeFooterBuilder = stripeFooterBuilderList.get(stripeIndex);
                stripeFooterBuilder.addColumns(columnEncoding);

                stripeIndex++;
            }
        }

        for (int stripeIndex = 0; stripeIndex < stripeFooterBuilderList.size(); stripeIndex++) {
            OrcProto.StripeFooter.Builder stripeFooterBuilder = stripeFooterBuilderList.get(stripeIndex);
            OrcProto.StripeFooter stripeFooter = stripeFooterBuilder.build();
            OrcProto.StripeFooterWithId.Builder stripeFooterWithIdBuilder = OrcProto.StripeFooterWithId.newBuilder();
            stripeFooterWithIdBuilder.setStripe(stripeIndex);
            stripeFooterWithIdBuilder.setStripeFooter(stripeFooter);
            OrcProto.StripeFooterWithId stripeFooterWithId = stripeFooterWithIdBuilder.build();
            stripeFooterWithIdList.add(stripeFooterWithId);
        }

        List<Integer> accumulatedRowGroupCountPerStripeList = binaryMetadata.getAccumulatedRowGroupCountPerStripeList();
        int[] accumulatedRowGroupCountPerStripeArray = new int[accumulatedRowGroupCountPerStripeList.size()];
        for (int i = 0; i < accumulatedRowGroupCountPerStripeList.size(); i++) {
            accumulatedRowGroupCountPerStripeArray[i] = accumulatedRowGroupCountPerStripeList.get(i);
        }

        // Strip dimension
        for (int stripeIndex = 0; stripeIndex < stripeFooterWithIdList.size(); stripeIndex++) {
            OrcProto.StripeFooterWithId stripeFooterWithId = stripeFooterWithIdList.get(stripeIndex);
            OrcProto.RedundantStripeMetadata.Builder redundantStripeMetadataBuilder =
                OrcProto.RedundantStripeMetadata.newBuilder();
            redundantStripeMetadataBuilder.setStripeFooter(stripeFooterWithId);

            redundantStripeMetadataBuilderList.add(redundantStripeMetadataBuilder);
        }

        // Column dimension
        List<ByteString> binaryPositionsList = binaryMetadata.getBinaryPositionsList();
        List<ByteString> columnPositionUnitSizeList = binaryMetadata.getColumnPositionUnitSizeList();
        List<ByteString> binaryStatisticsList = binaryMetadata.getBinaryStatisticsList();
        List<ByteString> binaryStatisticsNullBitmapList = binaryMetadata.getBinaryStatisticsNullBitmapList();
        List<Integer> columnStatisticsTypeList = binaryMetadata.getColumnStatisticsTypeList();

        // Check column count.
        final int expectedColumnCount = columnStatisticsTypeList.size();
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

        OrcProto.RowIndexWithColumn.Builder[][] rowIndexWithColumnBuilderArray
            = new OrcProto.RowIndexWithColumn.Builder[stripeFooterWithIdList.size()][expectedColumnCount];
        OrcProto.RowIndex.Builder[][] rowIndexBuilderArray
            = new OrcProto.RowIndex.Builder[stripeFooterWithIdList.size()][expectedColumnCount];

        for (int columnIndex = 0; columnIndex < expectedColumnCount; columnIndex++) {
            ByteString binaryPositions = binaryPositionsList.get(columnIndex);
            ByteString columnPositionUnitSize = columnPositionUnitSizeList.get(columnIndex);
            ByteString binaryStatistics = binaryStatisticsList.get(columnIndex);
            ByteString binaryStatisticsNullBitmap = binaryStatisticsNullBitmapList.get(columnIndex);
            Integer columnStatisticsType = columnStatisticsTypeList.get(columnIndex);

            // convert columnPositionUnitSize to byte array
            byte[] columnPositionUnitSizeByteArray = columnPositionUnitSize.toByteArray();

            // convert to ByteBuffer
            ByteBuffer binaryPositionsReadOnlyByteBuffer = binaryPositions.asReadOnlyByteBuffer();
            binaryPositionsReadOnlyByteBuffer.rewind();
            ByteBuffer binaryStatisticsReadOnlyByteBuffer = binaryStatistics.asReadOnlyByteBuffer();
            binaryStatisticsReadOnlyByteBuffer.rewind();
            ByteBuffer binaryStatisticsNullBitmapReadOnlyByteBuffer = binaryStatisticsNullBitmap.asReadOnlyByteBuffer();
            binaryStatisticsNullBitmapReadOnlyByteBuffer.rewind();

            // ByteString[x] ~ ByteString[y] -> stripe
            // column=column_index, stripe=stripe_index, rowGroup=startRowGroupIncluded ~ endRowGroupExcluded
            for (int stripeIndex = 0; stripeIndex < stripeFooterWithIdList.size(); stripeIndex++) {
                final int startRowGroupIncluded = accumulatedRowGroupCountPerStripeArray[stripeIndex];
                final int endRowGroupExcluded =
                    stripeIndex + 1 >= accumulatedRowGroupCountPerStripeArray.length
                        ? totalRowGroupCount : accumulatedRowGroupCountPerStripeArray[stripeIndex + 1];

                OrcProto.RedundantStripeMetadata.Builder redundantStripeMetadataBuilder =
                    redundantStripeMetadataBuilderList.get(stripeIndex);

                OrcProto.RowIndexWithColumn.Builder rowIndexWithColumnBuilder =
                    rowIndexWithColumnBuilderArray[stripeIndex][columnIndex];
                if (rowIndexWithColumnBuilder == null) {
                    rowIndexWithColumnBuilder = OrcProto.RowIndexWithColumn.newBuilder();
                    rowIndexWithColumnBuilderArray[stripeIndex][columnIndex] = rowIndexWithColumnBuilder;
                }

                rowIndexWithColumnBuilder.setColumn(columnIndex);

                OrcProto.RowIndex.Builder rowIndexBuilder = rowIndexBuilderArray[stripeIndex][columnIndex];
                if (rowIndexBuilder == null) {
                    rowIndexBuilder = OrcProto.RowIndex.newBuilder();
                    rowIndexBuilderArray[stripeIndex][columnIndex] = rowIndexBuilder;
                }

                // for each row group
                for (int rowGroupIndex = startRowGroupIncluded; rowGroupIndex < endRowGroupExcluded; rowGroupIndex++) {
                    OrcProto.RowIndexEntry.Builder rowIndexEntryBuilder = OrcProto.RowIndexEntry.newBuilder();

                    // rowIndexEntryBuilder.setStatistics() get from binaryStatistics, binaryStatisticsNullBitmap,
                    // rowIndexEntryBuilder.setPositions()

//                    final int startBinaryPositionIndex = rowGroupIndex * columnPositionUnitSize;
//                    final int endBinaryPositionIndex = (rowGroupIndex + 1) * columnPositionUnitSize;
//                    ByteString binaryPositionsSubset = binaryPositions.substring(startBinaryPositionIndex, endBinaryPositionIndex);
//                    ByteBuffer binaryPositionsSubsetReadOnlyByteBuffer = binaryPositionsSubset.asReadOnlyByteBuffer();
//                    binaryPositionsSubsetReadOnlyByteBuffer.rewind();
//
//                    // parse long value from binaryPositionsSubset
//                    while (binaryPositionsSubsetReadOnlyByteBuffer.hasRemaining()) {
//                        rowIndexEntryBuilder.addPositions(binaryPositionsSubsetReadOnlyByteBuffer.getLong());
//                    }

                    for (int i = 0; i < columnPositionUnitSizeByteArray[stripeIndex]; i++) {
                        rowIndexEntryBuilder.addPositions(binaryPositionsReadOnlyByteBuffer.getLong());
                    }

                    OrcProto.ColumnStatistics.Builder columnStatisticsBuilder = OrcProto.ColumnStatistics.newBuilder();
                    byte binaryNullBitmap = binaryStatisticsNullBitmap.byteAt(rowGroupIndex);
                    columnStatisticsBuilder.setHasNull(binaryNullBitmap == (byte) 1);

                    if (columnStatisticsType == RowIndexUtilBase.FLAG_INT_STATISTICS) {
//                        final int startColumnStatisticsIndex = rowGroupIndex * 8;
//                        final int endColumnStatisticsIndex = (rowGroupIndex + 1) * 8;
//                        ByteString binaryStatisticsSubset = binaryStatistics.substring(startColumnStatisticsIndex, endColumnStatisticsIndex);
//                        ByteBuffer binaryStatisticsSubsetReadOnlyByteBuffer = binaryStatisticsSubset.asReadOnlyByteBuffer();
//                        binaryStatisticsSubsetReadOnlyByteBuffer.rewind();

                        // parse int value from binaryStatistics
                        OrcProto.IntegerStatistics.Builder integerStatisticsBuilder =
                            OrcProto.IntegerStatistics.newBuilder();
                        integerStatisticsBuilder.setMinimum(binaryStatisticsReadOnlyByteBuffer.getLong());
                        integerStatisticsBuilder.setMaximum(binaryStatisticsReadOnlyByteBuffer.getLong());
                        columnStatisticsBuilder.setIntStatistics(integerStatisticsBuilder.build());
                    } else if (columnStatisticsType == RowIndexUtilBase.FLAG_DOUBLE_STATISTICS) {
//                        final int startColumnStatisticsIndex = rowGroupIndex * 8;
//                        final int endColumnStatisticsIndex = (rowGroupIndex + 1) * 8;
//                        ByteString binaryStatisticsSubset = binaryStatistics.substring(startColumnStatisticsIndex, endColumnStatisticsIndex);
//                        ByteBuffer binaryStatisticsSubsetReadOnlyByteBuffer = binaryStatisticsSubset.asReadOnlyByteBuffer();
//                        binaryStatisticsSubsetReadOnlyByteBuffer.rewind();

                        // parse double value from binaryStatistics
                        OrcProto.DoubleStatistics.Builder doubleStatisticsBuilder =
                            OrcProto.DoubleStatistics.newBuilder();
                        doubleStatisticsBuilder.setMinimum(binaryStatisticsReadOnlyByteBuffer.getDouble());
                        doubleStatisticsBuilder.setMaximum(binaryStatisticsReadOnlyByteBuffer.getDouble());
                        columnStatisticsBuilder.setDoubleStatistics(doubleStatisticsBuilder.build());
                    } else if (columnStatisticsType == RowIndexUtilBase.FLAG_DATE_STATISTICS) {
//                        final int startColumnStatisticsIndex = rowGroupIndex * 4;
//                        final int endColumnStatisticsIndex = (rowGroupIndex + 1) * 4;
//                        ByteString binaryStatisticsSubset = binaryStatistics.substring(startColumnStatisticsIndex, endColumnStatisticsIndex);
//                        ByteBuffer binaryStatisticsSubsetReadOnlyByteBuffer = binaryStatisticsSubset.asReadOnlyByteBuffer();
//                        binaryStatisticsSubsetReadOnlyByteBuffer.rewind();

                        // parse int value from binaryStatistics
                        OrcProto.DateStatistics.Builder dateStatisticsBuilder = OrcProto.DateStatistics.newBuilder();
                        dateStatisticsBuilder.setMinimum(binaryStatisticsReadOnlyByteBuffer.getInt());
                        dateStatisticsBuilder.setMaximum(binaryStatisticsReadOnlyByteBuffer.getInt());
                        columnStatisticsBuilder.setDateStatistics(dateStatisticsBuilder.build());
                    } else if (columnStatisticsType == RowIndexUtilBase.FLAG_STRING_STATISTICS) {

                        int totalLength = binaryStatisticsReadOnlyByteBuffer.getInt();
                        int lowerBoundLength = binaryStatisticsReadOnlyByteBuffer.getInt();
                        int upperBoundLength = totalLength - lowerBoundLength;
                        byte[] lowerBound = new byte[lowerBoundLength];
                        binaryStatisticsReadOnlyByteBuffer.get(lowerBound);
                        byte[] upperBound = new byte[upperBoundLength];
                        binaryStatisticsReadOnlyByteBuffer.get(upperBound);

                        OrcProto.StringStatistics.Builder stringStatisticsBuilder =
                            columnStatisticsBuilder.getStringStatisticsBuilder();
                        stringStatisticsBuilder.setLowerBound(new String(lowerBound));
                        stringStatisticsBuilder.setUpperBound(new String(upperBound));
                        columnStatisticsBuilder.setStringStatistics(stringStatisticsBuilder.build());
                    } else if (columnStatisticsType == RowIndexUtilBase.FLAG_DECIMAL_STATISTICS) {
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
                        columnStatisticsBuilder.setDecimalStatistics(decimalStatisticsBuilder.build());
                    } else if (columnStatisticsType == RowIndexUtilBase.FLAG_TIMESTAMP_STATISTICS) {
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
                        columnStatisticsBuilder.setTimestampStatistics(timestampStatisticsBuilder.build());
                    } else if (columnStatisticsType == RowIndexUtilBase.FLAG_BINARY_STATISTICS) {
                        OrcProto.BinaryStatistics.Builder binaryStatisticsBuilder =
                            OrcProto.BinaryStatistics.newBuilder();
                        int flag = binaryStatisticsReadOnlyByteBuffer.getInt();
                        if (flag != RowIndexUtilBase.FLAG_BINARY_STATISTICS) {
                            throw new IllegalArgumentException(
                                "flag != RowIndexUtilBase.FLAG_BINARY_STATISTICS, flag=" + flag);
                        }
                        binaryStatisticsBuilder.setSum(binaryStatisticsReadOnlyByteBuffer.getLong());
                        columnStatisticsBuilder.setBinaryStatistics(binaryStatisticsBuilder.build());
                    } else if (columnStatisticsType == RowIndexUtilBase.FLAG_COLLECTION_STATISTICS) {
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
                        columnStatisticsBuilder.setCollectionStatistics(collectionStatisticsBuilder.build());
                    } else if (columnStatisticsType == RowIndexUtilBase.FLAG_BUCKET_STATISTICS) {
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
                        columnStatisticsBuilder.setBucketStatistics(bucketStatisticsBuilder.build());
                    } else {
                        int flag = binaryStatisticsReadOnlyByteBuffer.getInt();
                        if (flag != RowIndexUtilBase.FLAG_NO_STATISTICS) {
                            throw new IllegalArgumentException(
                                "flag != RowIndexUtilBase.FLAG_NO_STATISTICS, flag=" + flag);
                        }
                    }
                    rowIndexEntryBuilder.setStatistics(columnStatisticsBuilder.build());
                    rowIndexBuilder.addEntry(rowIndexEntryBuilder.build());
                }
                rowIndexWithColumnBuilder.setRowIndex(rowIndexBuilder.build());
                redundantStripeMetadataBuilder.addRowIndex(rowIndexWithColumnBuilder.build());
            }

        }

        for (int i = 0; i < redundantStripeMetadataBuilderList.size(); i++) {
            redundantMetadataBuilder.addStripes(redundantStripeMetadataBuilderList.get(i).build());
        }

        return redundantMetadataBuilder.build();
    }

    public static void deserializeColumnEncodingKindAndBloomEncoding(
        OrcProto.ColumnEncoding.Builder columnEncodingBuilder, int kindAndBloomEncoding) {
        // serialize method:
        // kind int8, bloomEncoding int8
        // -> int32 (kind, bloomEncoding)
        int kindNumber = (kindAndBloomEncoding >> 8) & 0xFF; // deserialize kind
        int bloomEncoding = kindAndBloomEncoding & 0xFF; // deserialize bloomEncoding

        if (kindNumber >= 0) {
            OrcProto.ColumnEncoding.Kind kind = OrcProto.ColumnEncoding.Kind.forNumber(kindNumber);
            columnEncodingBuilder.setKind(kind);
        }
        columnEncodingBuilder.setBloomEncoding(bloomEncoding);
    }

    public static void deserializeColumnEncodingKindAndBloomEncodingShort(
        OrcProto.ColumnEncoding.Builder columnEncodingBuilder, short kindAndBloomEncoding) {
        // 解出(kind, bloomEncoding)
        int kind = (kindAndBloomEncoding >> 1) & 0x7F; // 解出 kind
        int bloomEncoding = kindAndBloomEncoding & 0x01; // 解出 bloomEncoding

        if (kind >= 0) {
            OrcProto.ColumnEncoding.Kind kindEnum = OrcProto.ColumnEncoding.Kind.forNumber(kind);
            columnEncodingBuilder.setKind(kindEnum);
        }
        columnEncodingBuilder.setBloomEncoding(bloomEncoding);
    }

    public static void deserializeColumnAndKind(OrcProto.Stream.Builder streamBuilder, long columnAndKind) {
        // serialize method:
        // column int32, kind int8
        // -> long (column, kind)
        // columnAndKind = ((columnId & 0xFFFFFFFFL) << 8) | (kind.getNumber() & 0xFF);

        int columnId = (int) (columnAndKind >> 8); // deserialize columnId
        int kindNumber = (byte) (columnAndKind & 0xFF); // deserialize kind

        if (kindNumber >= 0) {
            OrcProto.Stream.Kind kind = OrcProto.Stream.Kind.forNumber(kindNumber);
            streamBuilder.setKind(kind);
        }

        OrcProto.Stream.Kind kind = OrcProto.Stream.Kind.forNumber(kindNumber);

        streamBuilder.setColumn(columnId);
        streamBuilder.setKind(kind);
    }

    public static void deserializeColumnAndKindShort(OrcProto.Stream.Builder streamBuilder, short columnAndKind) {
        // 从 short 中解出 columnId 和 kindNumber
        int columnId = (columnAndKind >>> 8) & 0xFF; // 解出 columnId
        int kindNumber = columnAndKind & 0xFF; // 解出 kindNumber

        // 确保 columnId 在有效范围内
        if (columnId > 0xFFF) {
            throw new IllegalArgumentException("ColumnId " + columnId + " 超过最大值 4095");
        }

        if (kindNumber >= 0) {
            OrcProto.Stream.Kind kind = OrcProto.Stream.Kind.forNumber(kindNumber);
            streamBuilder.setKind(kind);
        }

        streamBuilder.setColumn(columnId);
    }
}
