package com.alibaba.polardbx.common.orc;

import org.apache.orc.OrcProto;
import org.junit.Assert;
import org.junit.Test;

import static com.alibaba.polardbx.common.orc.PreheatMetaMemoryUtils.*;

public class PreheatMetaMemoryUtilsTest {

    @Test
    public void test1() {
        OrcProto.ColumnStatistics.Builder builder = OrcProto.ColumnStatistics.newBuilder();
        builder.setIntStatistics(OrcProto.IntegerStatistics.newBuilder().build());
        OrcProto.ColumnStatistics columnStatistics = builder.build();
        Assert.assertEquals(
            PROTO_INTEGER_STATISTICS + PROTO_COLUMN_STATISTICS + UNKNOWN_FIELDS_SIZE + UNKNOWN_FIELDS_SIZE,
            PreheatMetaMemoryUtils.memorySizeOfColumnStatistics(columnStatistics));
    }

    @Test
    public void test2() {
        OrcProto.ColumnStatistics.Builder builder = OrcProto.ColumnStatistics.newBuilder();
        builder.setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder().build());
        OrcProto.ColumnStatistics columnStatistics = builder.build();
        Assert.assertEquals(
            PROTO_DOUBLE_STATISTICS + PROTO_COLUMN_STATISTICS + UNKNOWN_FIELDS_SIZE + UNKNOWN_FIELDS_SIZE,
            PreheatMetaMemoryUtils.memorySizeOfColumnStatistics(columnStatistics));
    }

    @Test
    public void test3() {
        OrcProto.ColumnStatistics.Builder builder = OrcProto.ColumnStatistics.newBuilder();
        builder.setStringStatistics(OrcProto.StringStatistics.newBuilder().build());
        OrcProto.ColumnStatistics columnStatistics = builder.build();
        Assert.assertEquals(
            PROTO_STRING_STATISTICS + PROTO_COLUMN_STATISTICS + UNKNOWN_FIELDS_SIZE + UNKNOWN_FIELDS_SIZE,
            PreheatMetaMemoryUtils.memorySizeOfColumnStatistics(columnStatistics));
    }

    @Test
    public void test4() {
        OrcProto.ColumnStatistics.Builder builder = OrcProto.ColumnStatistics.newBuilder();
        builder.setBucketStatistics(OrcProto.BucketStatistics.newBuilder().build());
        OrcProto.ColumnStatistics columnStatistics = builder.build();
        Assert.assertEquals(
            PROTO_BUCKET_STATISTICS + PROTO_COLUMN_STATISTICS + UNKNOWN_FIELDS_SIZE + UNKNOWN_FIELDS_SIZE,
            PreheatMetaMemoryUtils.memorySizeOfColumnStatistics(columnStatistics));
    }

    @Test
    public void test5() {
        OrcProto.ColumnStatistics.Builder builder = OrcProto.ColumnStatistics.newBuilder();
        builder.setDecimalStatistics(OrcProto.DecimalStatistics.newBuilder().build());
        OrcProto.ColumnStatistics columnStatistics = builder.build();
        Assert.assertEquals(
            PROTO_DECIMAL_STATISTICS + PROTO_COLUMN_STATISTICS + UNKNOWN_FIELDS_SIZE + UNKNOWN_FIELDS_SIZE,
            PreheatMetaMemoryUtils.memorySizeOfColumnStatistics(columnStatistics));
    }

    @Test
    public void test6() {
        OrcProto.ColumnStatistics.Builder builder = OrcProto.ColumnStatistics.newBuilder();
        builder.setDateStatistics(OrcProto.DateStatistics.newBuilder().build());
        OrcProto.ColumnStatistics columnStatistics = builder.build();
        Assert.assertEquals(PROTO_DATE_STATISTICS + PROTO_COLUMN_STATISTICS + UNKNOWN_FIELDS_SIZE + UNKNOWN_FIELDS_SIZE,
            PreheatMetaMemoryUtils.memorySizeOfColumnStatistics(columnStatistics));
    }

    @Test
    public void test7() {
        OrcProto.ColumnStatistics.Builder builder = OrcProto.ColumnStatistics.newBuilder();
        builder.setBinaryStatistics(OrcProto.BinaryStatistics.newBuilder().build());
        OrcProto.ColumnStatistics columnStatistics = builder.build();
        Assert.assertEquals(
            PROTO_BINARY_STATISTICS + PROTO_COLUMN_STATISTICS + UNKNOWN_FIELDS_SIZE + UNKNOWN_FIELDS_SIZE,
            PreheatMetaMemoryUtils.memorySizeOfColumnStatistics(columnStatistics));
    }

    @Test
    public void test8() {
        OrcProto.ColumnStatistics.Builder builder = OrcProto.ColumnStatistics.newBuilder();
        builder.setTimestampStatistics(OrcProto.TimestampStatistics.newBuilder().build());
        OrcProto.ColumnStatistics columnStatistics = builder.build();
        Assert.assertEquals(
            PROTO_TIMESTAMP_STATISTICS + PROTO_COLUMN_STATISTICS + UNKNOWN_FIELDS_SIZE + UNKNOWN_FIELDS_SIZE,
            PreheatMetaMemoryUtils.memorySizeOfColumnStatistics(columnStatistics));
    }
}