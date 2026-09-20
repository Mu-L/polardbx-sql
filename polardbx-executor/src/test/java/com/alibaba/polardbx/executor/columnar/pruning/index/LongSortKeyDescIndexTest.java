package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

public class LongSortKeyDescIndexTest {
    @Test
    public void testEqual() {
        long[] data = new long[] {80000, 100000, 70000, 70050, 40000, 70000, 10000, 20000};
        LongSortKeyIndex sortKeyIndex = LongSortKeyIndex.build(1, data, DataTypes.LongType, false);
        IndexPruneContext ipc = new IndexPruneContext();

        // test size
        org.junit.Assert.assertEquals(data.length * 8, sortKeyIndex.getSizeInBytes());

        RoaringBitmap rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual(50000L, rs, ipc);

        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(2));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual(30000L, rs, ipc);

        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual(70000L, rs, ipc);

        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(RoaringBitmap.bitmapOf(1, 2)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual(70001L, rs, ipc);

        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(RoaringBitmap.bitmapOf(1)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual(79999L, rs, ipc);

        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual(80000L, rs, ipc);

        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(RoaringBitmap.bitmapOf(0)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual(100000L, rs, ipc);

        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(RoaringBitmap.bitmapOf(0)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual(100001L, rs, ipc);

        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual(10000L, rs, ipc);

        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(RoaringBitmap.bitmapOf(3)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual(9999L, rs, ipc);

        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 0);
    }

    @Test
    public void testRange() {
        long[] data = new long[] {80000, 100000, 70000, 70050, 40000, 70000, 10000, 20000};
        LongSortKeyIndex sortKeyIndex = LongSortKeyIndex.build(1, data, DataTypes.LongType, false);
        IndexPruneContext ipc = new IndexPruneContext();


        // test start obj less than the lowest value
        RoaringBitmap rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(-10L, -1L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(-1L, 10L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(-1L, 10000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(-1L, 10001L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(-1L, 20000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(-1L, 25000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(-1L, 40000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(-1L, 40001L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(3) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(-1L, 70000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 3 && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(-1L, 80000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(-1L, 100000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(-1L, 100001L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(10000L, 10001L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(10000L, 20000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(10000L, 20010L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(10000L, 40000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(10000L, 70000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 3 && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(20001L, 70000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(1) && rs.contains(2));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(70000L, 100000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 3 && rs.contains(0) && rs.contains(1) && rs.contains(2));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(100000L, 100001L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(100001L, 110001L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(null, 100000L, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(10000L, null, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

    }

    @Test
    public void testInvalidRange() {
        long[] data = new long[] {80000, 100000, 70000, 70050, 40000, 70000, 10000, 20000};
        LongSortKeyIndex sortKeyIndex = LongSortKeyIndex.build(1, data, DataTypes.LongType, false);
        IndexPruneContext ipc = new IndexPruneContext();


        //start > end
        RoaringBitmap rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(20000, 10000, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);
    }

    @Test
    public void testRandomPrune() {
        long[] data = new long[] {
            887477183, 1073721405, 700990606, 887477183, 515709375, 700958778, 331412912, 515680940, 145935433,
            331412912, 0, 145897436};
        LongSortKeyIndex sortKeyIndex = LongSortKeyIndex.build(1, data, DataTypes.LongType, false);
        IndexPruneContext ipc = new IndexPruneContext();


        // test start obj less than the lowest value
        RoaringBitmap rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(1071533168, null, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(19051085, 19051085, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(5));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(19051085, 235832645, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(4) && rs.contains(5));
    }
}
