package com.alibaba.polardbx.common.orc;

import org.junit.Test;
import org.junit.Assert;

public class FastDateColumnStatisticsTest {

    @Test
    public void testDefaultConstructor() {
        FastDateColumnStatistics stats = new FastDateColumnStatistics();

        // Test default values
        Assert.assertEquals(0, stats.getTotalRowGroupCount());
        Assert.assertNull(stats.getAccumulatedRowGroupCountPerStripe());
        Assert.assertNull(stats.getHasNullBitmap());
        Assert.assertNull(stats.getMinMaxValues());
    }

    @Test
    public void testParameterizedConstructor() {
        int totalRowGroupCount = 5;
        int[] accumulatedRowGroupCountPerStripe = {2, 5};
        byte[] hasNullBitmap = {0, 1, 0, 1, 0};
        int[] minMaxValues = {100, 200, 150, 250, 80, 180, 120, 220, 90, 190};

        FastDateColumnStatistics stats = new FastDateColumnStatistics(
            totalRowGroupCount,
            accumulatedRowGroupCountPerStripe,
            hasNullBitmap,
            minMaxValues
        );

        Assert.assertEquals(totalRowGroupCount, stats.getTotalRowGroupCount());
        Assert.assertArrayEquals(accumulatedRowGroupCountPerStripe, stats.getAccumulatedRowGroupCountPerStripe());
        Assert.assertArrayEquals(hasNullBitmap, stats.getHasNullBitmap());
        Assert.assertArrayEquals(minMaxValues, stats.getMinMaxValues());
    }

    @Test
    public void testGetMemoryUsage() {
        // Test with null arrays
        FastDateColumnStatistics stats = new FastDateColumnStatistics();
        long memoryUsage = stats.getMemoryUsage();
        Assert.assertTrue("Memory usage should be positive", memoryUsage > 0);

        // Test with non-null arrays
        byte[] hasNullBitmap = {0, 1, 0, 1, 0};
        int[] minMaxValues = {100, 200, 150, 250, 80, 180, 120, 220, 90, 190};
        stats = new FastDateColumnStatistics(5, new int[] {2, 5}, hasNullBitmap, minMaxValues);

        long memoryUsageWithArrays = stats.getMemoryUsage();
        Assert.assertTrue("Memory usage with arrays should be greater than without arrays",
            memoryUsageWithArrays > memoryUsage);
    }

    @Test
    public void testHasNull() {
        byte[] hasNullBitmap = {0, 1, 0, 1, 0};
        FastDateColumnStatistics stats = new FastDateColumnStatistics(
            5, new int[] {2, 5}, hasNullBitmap, new int[10]
        );

        Assert.assertFalse("Row group 0 should not have null", stats.hasNull(0));
        Assert.assertTrue("Row group 1 should have null", stats.hasNull(1));
        Assert.assertFalse("Row group 2 should not have null", stats.hasNull(2));
        Assert.assertTrue("Row group 3 should have null", stats.hasNull(3));
        Assert.assertFalse("Row group 4 should not have null", stats.hasNull(4));
    }

    @Test
    public void testGetMinLong() {
        int[] minMaxValues = {100, 200, 150, 250, 80, 180, 120, 220, 90, 190};
        FastDateColumnStatistics stats = new FastDateColumnStatistics(
            5, new int[] {2, 5}, new byte[5], minMaxValues
        );

        Assert.assertEquals(100L, stats.getMinLong(0));
        Assert.assertEquals(150L, stats.getMinLong(1));
        Assert.assertEquals(80L, stats.getMinLong(2));
        Assert.assertEquals(120L, stats.getMinLong(3));
        Assert.assertEquals(90L, stats.getMinLong(4));
    }

    @Test
    public void testGetMaxLong() {
        int[] minMaxValues = {100, 200, 150, 250, 80, 180, 120, 220, 90, 190};
        FastDateColumnStatistics stats = new FastDateColumnStatistics(
            5, new int[] {2, 5}, new byte[5], minMaxValues
        );

        Assert.assertEquals(200L, stats.getMaxLong(0));
        Assert.assertEquals(250L, stats.getMaxLong(1));
        Assert.assertEquals(180L, stats.getMaxLong(2));
        Assert.assertEquals(220L, stats.getMaxLong(3));
        Assert.assertEquals(190L, stats.getMaxLong(4));
    }

    @Test
    public void testGetMinInt() {
        int[] minMaxValues = {100, 200, 150, 250, 80, 180, 120, 220, 90, 190};
        FastDateColumnStatistics stats = new FastDateColumnStatistics(
            5, new int[] {2, 5}, new byte[5], minMaxValues
        );

        Assert.assertEquals(100, stats.getMinInt(0));
        Assert.assertEquals(150, stats.getMinInt(1));
        Assert.assertEquals(80, stats.getMinInt(2));
        Assert.assertEquals(120, stats.getMinInt(3));
        Assert.assertEquals(90, stats.getMinInt(4));
    }

    @Test
    public void testGetMaxInt() {
        int[] minMaxValues = {100, 200, 150, 250, 80, 180, 120, 220, 90, 190};
        FastDateColumnStatistics stats = new FastDateColumnStatistics(
            5, new int[] {2, 5}, new byte[5], minMaxValues
        );

        Assert.assertEquals(200, stats.getMaxInt(0));
        Assert.assertEquals(250, stats.getMaxInt(1));
        Assert.assertEquals(180, stats.getMaxInt(2));
        Assert.assertEquals(220, stats.getMaxInt(3));
        Assert.assertEquals(190, stats.getMaxInt(4));
    }

    @Test
    public void testSetTotalRowGroupCount() {
        FastDateColumnStatistics stats = new FastDateColumnStatistics();

        FastDateColumnStatistics result = stats.setTotalRowGroupCount(10);

        Assert.assertEquals(10, stats.getTotalRowGroupCount());
        Assert.assertSame("Should return same instance for method chaining", stats, result);
    }

    @Test
    public void testSetAccumulatedRowGroupCountPerStripe() {
        FastDateColumnStatistics stats = new FastDateColumnStatistics();
        int[] stripeData = {3, 7, 10};

        FastDateColumnStatistics result = stats.setAccumulatedRowGroupCountPerStripe(stripeData);

        Assert.assertArrayEquals(stripeData, stats.getAccumulatedRowGroupCountPerStripe());
        Assert.assertSame("Should return same instance for method chaining", stats, result);
    }

    @Test
    public void testSetHasNullBitmap() {
        FastDateColumnStatistics stats = new FastDateColumnStatistics();
        byte[] nullBitmap = {1, 0, 1, 0, 1};

        FastDateColumnStatistics result = stats.setHasNullBitmap(nullBitmap);

        Assert.assertArrayEquals(nullBitmap, stats.getHasNullBitmap());
        Assert.assertSame("Should return same instance for method chaining", stats, result);
    }

    @Test
    public void testSetMinMaxValues() {
        FastDateColumnStatistics stats = new FastDateColumnStatistics();
        int[] minMaxData = {50, 150, 75, 175, 25, 125};

        FastDateColumnStatistics result = stats.setMinMaxValues(minMaxData);

        Assert.assertArrayEquals(minMaxData, stats.getMinMaxValues());
        Assert.assertSame("Should return same instance for method chaining", stats, result);
    }

    @Test
    public void testMethodChaining() {
        FastDateColumnStatistics stats = new FastDateColumnStatistics();
        int[] stripeData = {2, 4};
        byte[] nullBitmap = {0, 1, 0, 1};
        int[] minMaxData = {10, 20, 30, 40, 50, 60, 70, 80};

        FastDateColumnStatistics result = stats
            .setTotalRowGroupCount(4)
            .setAccumulatedRowGroupCountPerStripe(stripeData)
            .setHasNullBitmap(nullBitmap)
            .setMinMaxValues(minMaxData);

        Assert.assertSame("Method chaining should return same instance", stats, result);
        Assert.assertEquals(4, stats.getTotalRowGroupCount());
        Assert.assertArrayEquals(stripeData, stats.getAccumulatedRowGroupCountPerStripe());
        Assert.assertArrayEquals(nullBitmap, stats.getHasNullBitmap());
        Assert.assertArrayEquals(minMaxData, stats.getMinMaxValues());
    }

    @Test
    public void testEdgeCases() {
        // Test with empty arrays
        FastDateColumnStatistics stats = new FastDateColumnStatistics(
            0, new int[0], new byte[0], new int[0]
        );

        Assert.assertEquals(0, stats.getTotalRowGroupCount());
        Assert.assertEquals(0, stats.getAccumulatedRowGroupCountPerStripe().length);
        Assert.assertEquals(0, stats.getHasNullBitmap().length);
        Assert.assertEquals(0, stats.getMinMaxValues().length);

        // Test memory usage with empty arrays
        long memoryUsage = stats.getMemoryUsage();
        Assert.assertTrue("Memory usage should be positive even with empty arrays", memoryUsage > 0);
    }

    @Test
    public void testNullArraysInConstructor() {
        FastDateColumnStatistics stats = new FastDateColumnStatistics(5, null, null, null);

        Assert.assertEquals(5, stats.getTotalRowGroupCount());
        Assert.assertNull(stats.getAccumulatedRowGroupCountPerStripe());
        Assert.assertNull(stats.getHasNullBitmap());
        Assert.assertNull(stats.getMinMaxValues());
    }

    @Test
    public void testLargeValues() {
        // Test with large values to ensure proper handling
        int[] minMaxValues = {Integer.MIN_VALUE, Integer.MAX_VALUE, -1000000, 1000000};
        FastDateColumnStatistics stats = new FastDateColumnStatistics(
            2, new int[] {1, 2}, new byte[] {0, 1}, minMaxValues
        );

        Assert.assertEquals(Integer.MIN_VALUE, stats.getMinInt(0));
        Assert.assertEquals(Integer.MAX_VALUE, stats.getMaxInt(0));
        Assert.assertEquals(-1000000, stats.getMinInt(1));
        Assert.assertEquals(1000000, stats.getMaxInt(1));

        // Test long conversion
        Assert.assertEquals((long) Integer.MIN_VALUE, stats.getMinLong(0));
        Assert.assertEquals((long) Integer.MAX_VALUE, stats.getMaxLong(0));
    }

    @Test
    public void testSingleRowGroup() {
        // Test with single row group
        byte[] hasNullBitmap = {1};
        int[] minMaxValues = {42, 84};
        FastDateColumnStatistics stats = new FastDateColumnStatistics(
            1, new int[] {1}, hasNullBitmap, minMaxValues
        );

        Assert.assertEquals(1, stats.getTotalRowGroupCount());
        Assert.assertTrue(stats.hasNull(0));
        Assert.assertEquals(42, stats.getMinInt(0));
        Assert.assertEquals(84, stats.getMaxInt(0));
        Assert.assertEquals(42L, stats.getMinLong(0));
        Assert.assertEquals(84L, stats.getMaxLong(0));
    }
}