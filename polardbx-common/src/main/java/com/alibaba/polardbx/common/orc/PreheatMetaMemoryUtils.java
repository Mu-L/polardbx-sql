package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import io.airlift.slice.SizeOf;
import org.apache.hadoop.fs.FileStatus;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.OrcTail;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class PreheatMetaMemoryUtils {

    // level 1
    private static final int PREHEAT_FILE_META_SIZE = ClassLayout.parseClass(PreheatFileMeta.class).instanceSize();

    // level 2
    private static final int PREHEAT_STRIPE_META_SIZE = ClassLayout.parseClass(PreheatStripeMeta.class).instanceSize();
    private static final int ORC_TAIL_SIZE = ClassLayout.parseClass(OrcTail.class).instanceSize();
    private static final int FILE_STATUS_SIZE = ClassLayout.parseClass(FileStatus.class).instanceSize();

    // level 3
    private static final int ORC_INDEX_SIZE = ClassLayout.parseClass(OrcIndex.class).instanceSize();
    private static final int STRIPE_FOOTER_SIZE = ClassLayout.parseClass(OrcProto.StripeFooter.class).instanceSize();

    private static final int PROTO_FILE_TAIL = ClassLayout.parseClass(OrcProto.FileTail.class).instanceSize();
    private static final int PROTO_POST_SCRIPT = ClassLayout.parseClass(OrcProto.PostScript.class).instanceSize();

    // level 4
    private static final int ROW_INDEX_ENTRY_SIZE = ClassLayout.parseClass(OrcProto.RowIndexEntry.class).instanceSize();
    public static final int PROTO_COLUMN_STATISTICS =
        ClassLayout.parseClass(OrcProto.ColumnStatistics.class).instanceSize();
    public static final int PROTO_INTEGER_STATISTICS =
        ClassLayout.parseClass(OrcProto.IntegerStatistics.class).instanceSize();
    public static final int PROTO_DOUBLE_STATISTICS =
        ClassLayout.parseClass(OrcProto.DoubleStatistics.class).instanceSize();
    public static final int PROTO_STRING_STATISTICS =
        ClassLayout.parseClass(OrcProto.StringStatistics.class).instanceSize();
    public static final int PROTO_BUCKET_STATISTICS =
        ClassLayout.parseClass(OrcProto.BucketStatistics.class).instanceSize();
    public static final int PROTO_DECIMAL_STATISTICS =
        ClassLayout.parseClass(OrcProto.DecimalStatistics.class).instanceSize();
    public static final int PROTO_DATE_STATISTICS =
        ClassLayout.parseClass(OrcProto.DateStatistics.class).instanceSize();
    public static final int PROTO_BINARY_STATISTICS =
        ClassLayout.parseClass(OrcProto.BinaryStatistics.class).instanceSize();
    public static final int PROTO_TIMESTAMP_STATISTICS =
        ClassLayout.parseClass(OrcProto.TimestampStatistics.class).instanceSize();

    private static final int PROTO_STREAM = ClassLayout.parseClass(OrcProto.Stream.class).instanceSize();
    private static final int PROTO_COLUMN_ENCODING =
        ClassLayout.parseClass(OrcProto.ColumnEncoding.class).instanceSize();

    private static final int PROTO_STRIPE_INFORMATION =
        ClassLayout.parseClass(OrcProto.StripeInformation.class).instanceSize();
    private static final int PROTO_TYPE = ClassLayout.parseClass(OrcProto.Type.class).instanceSize();

    // Type Description
    private static final int TYPE_DESCRIPTION_SIZE = ClassLayout.parseClass(TypeDescription.class).instanceSize();

    public static final int UNKNOWN_FIELDS_SIZE = 48;

    public static long estimatedMemorySizeOf(PreheatFileMeta preheatFileMeta) {
        long memorySize = PREHEAT_FILE_META_SIZE;

        // For binary metadata
        memorySize += FastMemoryCounter.sizeOf(preheatFileMeta.getFastColumnStatisticsArray());
        memorySize += FastMemoryCounter.sizeOf(preheatFileMeta.getFastPositionIndexArray());
        memorySize += FastMemoryCounter.sizeOf(preheatFileMeta.getFastColumnEncodingIndexArray());
        memorySize += FastMemoryCounter.sizeOf(preheatFileMeta.getFastStreamIndexArray());
        if (preheatFileMeta.getWriterTimeZoneArray() != null) {
            for (String writerTimeZone : preheatFileMeta.getWriterTimeZoneArray()) {
                memorySize += FastMemoryCounter.sizeOf(writerTimeZone);
            }
            memorySize += VMSupport.align((int) sizeOf(preheatFileMeta.getWriterTimeZoneArray()));
        }

        // Stripe-Level
        for (PreheatStripeMeta preheatStripeMeta :
            (preheatFileMeta.getPreheatStripes() == null
                ? new ArrayList<PreheatStripeMeta>()
                : preheatFileMeta.getPreheatStripes().values())) {
            memorySize += PREHEAT_STRIPE_META_SIZE;

            // orc index
            memorySize += ORC_INDEX_SIZE;
            OrcProto.RowIndex[] rowGroupIndex = preheatStripeMeta.getOrcIndex().getRowGroupIndex();
            memorySize += SizeOf.sizeOfObjectArray(rowGroupIndex.length);
            for (int column = 0; column < rowGroupIndex.length; column++) {

                long memorySizeOfColumn = 0;
                OrcProto.RowIndex rowIndexOfColumn = rowGroupIndex[column];

                // message RowIndexEntry {
                //  repeated uint64 positions = 1 [packed=true];
                //  optional ColumnStatistics statistics = 2;
                // }
                OrcProto.RowIndexEntry rowIndexEntry = rowIndexOfColumn.getEntry(0);
                memorySizeOfColumn += ROW_INDEX_ENTRY_SIZE;
                if (rowIndexEntry.getUnknownFields() != null) {
                    memorySizeOfColumn += UNKNOWN_FIELDS_SIZE;
                }
                memorySizeOfColumn += SizeOf.sizeOfLongArray(rowIndexEntry.getPositionsCount());
                memorySizeOfColumn += SizeOf.sizeOfObjectArray(rowIndexEntry.getPositionsCount());

                OrcProto.ColumnStatistics columnStatistics = rowIndexEntry.getStatistics();
                memorySizeOfColumn += memorySizeOfColumnStatistics(columnStatistics);

                // memory usage of this column row group 0 * row group count.
                memorySizeOfColumn *= rowIndexOfColumn.getEntryCount();
                memorySizeOfColumn += SizeOf.sizeOfObjectArray(rowIndexOfColumn.getEntryCount());
                memorySize += memorySizeOfColumn;
            }

            OrcProto.StripeFooter stripeFooter = preheatStripeMeta.getStripeFooter();
            memorySize += STRIPE_FOOTER_SIZE;
            memorySize += UNKNOWN_FIELDS_SIZE;

            memorySize += (PROTO_STREAM + UNKNOWN_FIELDS_SIZE) * stripeFooter.getStreamsCount();
            memorySize += SizeOf.sizeOfObjectArray(stripeFooter.getStreamsCount());

            memorySize += (PROTO_COLUMN_ENCODING + UNKNOWN_FIELDS_SIZE) * stripeFooter.getColumnsCount();
            memorySize += SizeOf.sizeOfObjectArray(stripeFooter.getColumnsCount());
        }

        // File Tail
        memorySize += ORC_TAIL_SIZE;
        memorySize += (PROTO_FILE_TAIL + UNKNOWN_FIELDS_SIZE);
        memorySize += (PROTO_POST_SCRIPT + UNKNOWN_FIELDS_SIZE);

        // footer in File Tail
        OrcProto.Footer footer = preheatFileMeta.getPreheatTail().getFooter();
        memorySize += (PROTO_STRIPE_INFORMATION + UNKNOWN_FIELDS_SIZE) * footer.getStripesCount();
        memorySize += SizeOf.sizeOfObjectArray(footer.getStripesCount());

        memorySize += (PROTO_TYPE + UNKNOWN_FIELDS_SIZE) * footer.getTypesCount();
        memorySize += SizeOf.sizeOfObjectArray(footer.getTypesCount());

        memorySize += SizeOf.sizeOfObjectArray(footer.getStatisticsCount());
        for (int column = 0; column < footer.getStatisticsCount(); column++) {
            OrcProto.ColumnStatistics columnStatistics = footer.getStatistics(column);
            memorySize += memorySizeOfColumnStatistics(columnStatistics);
        }

        // type description
        TypeDescription schema = preheatFileMeta.getPreheatTail().getSchema();
        memorySize += TYPE_DESCRIPTION_SIZE * (1 + schema.getChildren().size());
        memorySize += SizeOf.sizeOfObjectArray(schema.getChildren().size());

        // OSS File Status
        memorySize += FILE_STATUS_SIZE;
        return memorySize;
    }

    public static long memorySizeOfColumnStatistics(OrcProto.ColumnStatistics columnStatistics) {
        long memorySize = PROTO_COLUMN_STATISTICS;
        if (columnStatistics.getUnknownFields() != null) {
            memorySize += UNKNOWN_FIELDS_SIZE;
        }

        if (columnStatistics != null) {
            if (columnStatistics.hasIntStatistics()) {
                memorySize += PROTO_INTEGER_STATISTICS;
                if (columnStatistics.getIntStatistics().getUnknownFields() != null) {
                    memorySize += UNKNOWN_FIELDS_SIZE;
                }
            } else if (columnStatistics.hasDoubleStatistics()) {
                memorySize += PROTO_DOUBLE_STATISTICS;
                if (columnStatistics.getDoubleStatistics().getUnknownFields() != null) {
                    memorySize += UNKNOWN_FIELDS_SIZE;
                }
            } else if (columnStatistics.hasStringStatistics()) {
                memorySize += PROTO_STRING_STATISTICS;
                if (columnStatistics.getStringStatistics().getUnknownFields() != null) {
                    memorySize += UNKNOWN_FIELDS_SIZE;
                }
            } else if (columnStatistics.hasBucketStatistics()) {
                memorySize += PROTO_BUCKET_STATISTICS;
                if (columnStatistics.getBucketStatistics().getUnknownFields() != null) {
                    memorySize += UNKNOWN_FIELDS_SIZE;
                }
            } else if (columnStatistics.hasDecimalStatistics()) {
                memorySize += PROTO_DECIMAL_STATISTICS;
                if (columnStatistics.getDecimalStatistics().getUnknownFields() != null) {
                    memorySize += UNKNOWN_FIELDS_SIZE;
                }
            } else if (columnStatistics.hasDateStatistics()) {
                memorySize += PROTO_DATE_STATISTICS;
                if (columnStatistics.getDateStatistics().getUnknownFields() != null) {
                    memorySize += UNKNOWN_FIELDS_SIZE;
                }
            } else if (columnStatistics.hasBinaryStatistics()) {
                memorySize += PROTO_BINARY_STATISTICS;
                if (columnStatistics.getBinaryStatistics().getUnknownFields() != null) {
                    memorySize += UNKNOWN_FIELDS_SIZE;
                }
            } else if (columnStatistics.hasTimestampStatistics()) {
                memorySize += PROTO_TIMESTAMP_STATISTICS;
                if (columnStatistics.getTimestampStatistics().getUnknownFields() != null) {
                    memorySize += UNKNOWN_FIELDS_SIZE;
                }
            }
        }
        return memorySize;
    }
}
