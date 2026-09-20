package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

public class StringSortKeyIndexAescTest {
    private final String[] data = new String[] {"aaa", "bbb", "ddd", "ggg", "ggg", "jjj", "xxx", "zzz"};

    private final StringSortKeyIndex sortKeyIndex = StringSortKeyIndex.build(1, data, DataTypes.StringType, true);

    @Test
    public void testEqual() {
        RoaringBitmap rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        IndexPruneContext ipc = new IndexPruneContext();
        sortKeyIndex.pruneEqual("eee", rs, ipc);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(1));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("ccc", rs, ipc);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("ggg", rs, ipc);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(RoaringBitmap.bitmapOf(1, 2)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("hhh", rs, ipc);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(RoaringBitmap.bitmapOf(2)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("www", rs, ipc);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("xxx", rs, ipc);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(RoaringBitmap.bitmapOf(3)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("zzz", rs, ipc);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(RoaringBitmap.bitmapOf(3)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("zzza", rs, ipc);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("aaa", rs, ipc);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(RoaringBitmap.bitmapOf(0)));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneEqual("aa", rs, ipc);
        rs.stream().forEachOrdered(System.out::print);
        Assert.assertTrue(rs.getCardinality() == 0);
    }

    @Test
    public void testRange() {
        // test start obj less than the lowest value
        RoaringBitmap rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        IndexPruneContext ipc = new IndexPruneContext();
        sortKeyIndex.pruneRange("a", "aa", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aa", "aa", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("a", "aaa", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("a", "b", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("a", "bb", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("a", "bbb", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bbb", "bbb", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bb", "bbb", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bb", "ddd", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(0) && rs.contains(1));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bbb", "eee", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(0) && rs.contains(1));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bba", "ggg", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 3 && rs.contains(0) && rs.contains(1) && rs.contains(2));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bba", "xxxa", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bba", "xxx", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bba", "xxxy", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aaa", "bb", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aaa", "bbb", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aaa", "bbbd", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(0));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aaa", "gga", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(0) && rs.contains(1));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("aaa", "ggg", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 3 && rs.contains(0) && rs.contains(1) && rs.contains(2));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("ddd", "ggg", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 2 && rs.contains(1) && rs.contains(2));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("ddd", "xxx", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 3 && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("xxx", "zzz", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("zzza", "zzzb", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("zzzzzzzz", "zzzzzzzz", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("bbb", "zzzz", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(
            rs.getCardinality() == 4 && rs.contains(0) && rs.contains(1) && rs.contains(2) && rs.contains(3));

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange(null, "zzz", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == sortKeyIndex.rgNum());

        rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        sortKeyIndex.pruneRange("zzz", null, rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 1 && rs.contains(3));
    }

    @Test
    public void testInvalidRange() {
        // test start obj less than the lowest value
        RoaringBitmap rs = RoaringBitmap.bitmapOfRange(0, sortKeyIndex.rgNum());
        IndexPruneContext ipc = new IndexPruneContext();
        sortKeyIndex.pruneRange("aa", "a", rs, ipc);
        rs.stream().forEachOrdered(System.out::println);
        Assert.assertTrue(rs.getCardinality() == 0);
    }

    @Test
    public void testSize() {
        long expectedSize = 0;
        for (String datum : data) {
            expectedSize += datum.getBytes().length;
        }
        Assert.assertEquals(expectedSize, sortKeyIndex.getSizeInBytes());
    }
}
