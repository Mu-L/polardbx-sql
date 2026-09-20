package com.alibaba.polardbx.common.orc;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for FastLongColumnStatistics class.
 * This test class covers all methods and code lines without using mocks.
 */
public class FastLongColumnStatisticsTest {

    @Test
    public void testDefaultConstructor() {
        FastLongColumnStatistics stats = new FastLongColumnStatistics();

        // Verify default values
        assertEquals(0, stats.getTotalRowGroupCount());
        assertNull(stats.getAccumulatedRowGroupCountPerStripe());
        assertNull(stats.getHasNullBitmap());
        assertNull(stats.getMinMaxValues());
    }

    @Test
    public void testParameterizedConstructor() {
        int totalRowGroupCount = 5;
        int[] accumulatedRowGroupCountPerStripe = {2, 5};
        byte[] hasNullBitmap = {0, 1, 0, 1, 0};
        long[] minMaxValues = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};

        FastLongColumnStatistics stats = new FastLongColumnStatistics(
            totalRowGroupCount,
            accumulatedRowGroupCountPerStripe,
            hasNullBitmap,
            minMaxValues
        );

        assertEquals(totalRowGroupCount, stats.getTotalRowGroupCount());
        assertArrayEquals(accumulatedRowGroupCountPerStripe, stats.getAccumulatedRowGroupCountPerStripe());
        assertArrayEquals(hasNullBitmap, stats.getHasNullBitmap());
        assertArrayEquals(minMaxValues, stats.getMinMaxValues());
    }

    @Test
    public void testHasNull() {
        byte[] hasNullBitmap = {0, 1, 0, 1, 0};
        FastLongColumnStatistics stats = new FastLongColumnStatistics(
            5, null, hasNullBitmap, null
        );

        assertFalse(stats.hasNull(0));
        assertTrue(stats.hasNull(1));
        assertFalse(stats.hasNull(2));
        assertTrue(stats.hasNull(3));
        assertFalse(stats.hasNull(4));
    }

    @Test
    public void testGetMinLong() {
        long[] minMaxValues = {10, 20, 30, 40, 50, 60};
        FastLongColumnStatistics stats = new FastLongColumnStatistics(
            3, null, null, minMaxValues
        );

        assertEquals(10, stats.getMinLong(0));
        assertEquals(30, stats.getMinLong(1));
        assertEquals(50, stats.getMinLong(2));
    }

    @Test
    public void testGetMaxLong() {
        long[] minMaxValues = {10, 20, 30, 40, 50, 60};
        FastLongColumnStatistics stats = new FastLongColumnStatistics(
            3, null, null, minMaxValues
        );

        assertEquals(20, stats.getMaxLong(0));
        assertEquals(40, stats.getMaxLong(1));
        assertEquals(60, stats.getMaxLong(2));
    }

    @Test
    public void testSetTotalRowGroupCount() {
        FastLongColumnStatistics stats = new FastLongColumnStatistics();

        FastLongColumnStatistics result = stats.setTotalRowGroupCount(10);

        assertEquals(10, stats.getTotalRowGroupCount());
        assertSame(stats, result); // Test fluent interface
    }

    @Test
    public void testSetAccumulatedRowGroupCountPerStripe() {
        FastLongColumnStatistics stats = new FastLongColumnStatistics();
        int[] accumulatedRowGroupCountPerStripe = {3, 7, 10};

        FastLongColumnStatistics result = stats.setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripe);

        assertArrayEquals(accumulatedRowGroupCountPerStripe, stats.getAccumulatedRowGroupCountPerStripe());
        assertSame(stats, result); // Test fluent interface
    }

    @Test
    public void testSetHasNullBitmap() {
        FastLongColumnStatistics stats = new FastLongColumnStatistics();
        byte[] hasNullBitmap = {1, 0, 1, 0};

        FastLongColumnStatistics result = stats.setHasNullBitmap(hasNullBitmap);

        assertArrayEquals(hasNullBitmap, stats.getHasNullBitmap());
        assertSame(stats, result); // Test fluent interface
    }

    @Test
    public void testSetMinMaxValues() {
        FastLongColumnStatistics stats = new FastLongColumnStatistics();
        long[] minMaxValues = {100, 200, 300, 400};

        FastLongColumnStatistics result = stats.setMinMaxValues(minMaxValues);

        assertArrayEquals(minMaxValues, stats.getMinMaxValues());
        assertSame(stats, result); // Test fluent interface
    }

    @Test
    public void testGetMemoryUsageWithNullArrays() {
        FastLongColumnStatistics stats = new FastLongColumnStatistics();

        long memoryUsage = stats.getMemoryUsage();

        // Should return instance size when arrays are null
        assertTrue(memoryUsage > 0);
    }

    @Test
    public void testGetMemoryUsageWithNonNullArrays() {
        byte[] hasNullBitmap = {0, 1, 0, 1, 0};
        long[] minMaxValues = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};

        FastLongColumnStatistics stats = new FastLongColumnStatistics(
            5, null, hasNullBitmap, minMaxValues
        );

        long memoryUsage = stats.getMemoryUsage();

        // Should include instance size plus aligned array sizes
        assertTrue(memoryUsage > 0);
    }

    @Test
    public void testCompleteWorkflow() {
        // Test a complete workflow with all operations
        FastLongColumnStatistics stats = new FastLongColumnStatistics();

        // Set up data using fluent interface
        int[] accumulatedRowGroupCountPerStripe = {2, 5, 8};
        byte[] hasNullBitmap = {0, 1, 0, 1, 0, 1, 0, 0};
        long[] minMaxValues = {1, 10, 2, 20, 3, 30, 4, 40, 5, 50, 6, 60, 7, 70, 8, 80};

        stats.setTotalRowGroupCount(8)
            .setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripe)
            .setHasNullBitmap(hasNullBitmap)
            .setMinMaxValues(minMaxValues);

        // Verify all data is set correctly
        assertEquals(8, stats.getTotalRowGroupCount());
        assertArrayEquals(accumulatedRowGroupCountPerStripe, stats.getAccumulatedRowGroupCountPerStripe());
        assertArrayEquals(hasNullBitmap, stats.getHasNullBitmap());
        assertArrayEquals(minMaxValues, stats.getMinMaxValues());

        // Test statistical operations
        for (int i = 0; i < 8; i++) {
            assertEquals(hasNullBitmap[i] == 1, stats.hasNull(i));
            assertEquals(minMaxValues[i * 2], stats.getMinLong(i));
            assertEquals(minMaxValues[i * 2 + 1], stats.getMaxLong(i));
        }

        // Test memory usage calculation
        long memoryUsage = stats.getMemoryUsage();
        assertTrue(memoryUsage > 0);
    }

    @Test
    public void testEdgeCases() {
        // Test with empty arrays
        FastLongColumnStatistics stats = new FastLongColumnStatistics(
            0, new int[0], new byte[0], new long[0]
        );

        assertEquals(0, stats.getTotalRowGroupCount());
        assertEquals(0, stats.getAccumulatedRowGroupCountPerStripe().length);
        assertEquals(0, stats.getHasNullBitmap().length);
        assertEquals(0, stats.getMinMaxValues().length);

        long memoryUsage = stats.getMemoryUsage();
        assertTrue(memoryUsage > 0);
    }

    @Test
    public void testLargeValues() {
        // Test with large values
        long[] minMaxValues = {Long.MIN_VALUE, Long.MAX_VALUE, -1000000L, 1000000L};
        FastLongColumnStatistics stats = new FastLongColumnStatistics(
            2, null, null, minMaxValues
        );

        assertEquals(Long.MIN_VALUE, stats.getMinLong(0));
        assertEquals(Long.MAX_VALUE, stats.getMaxLong(0));
        assertEquals(-1000000L, stats.getMinLong(1));
        assertEquals(1000000L, stats.getMaxLong(1));
    }

    @Test
    public void testNullBitmapVariations() {
        // Test different null bitmap patterns
        byte[] allNulls = {1, 1, 1, 1};
        byte[] noNulls = {0, 0, 0, 0};
        byte[] mixed = {1, 0, 1, 0};

        FastLongColumnStatistics statsAllNulls = new FastLongColumnStatistics(4, null, allNulls, null);
        FastLongColumnStatistics statsNoNulls = new FastLongColumnStatistics(4, null, noNulls, null);
        FastLongColumnStatistics statsMixed = new FastLongColumnStatistics(4, null, mixed, null);

        // Test all nulls
        for (int i = 0; i < 4; i++) {
            assertTrue(statsAllNulls.hasNull(i));
        }

        // Test no nulls
        for (int i = 0; i < 4; i++) {
            assertFalse(statsNoNulls.hasNull(i));
        }

        // Test mixed pattern
        assertTrue(statsMixed.hasNull(0));
        assertFalse(statsMixed.hasNull(1));
        assertTrue(statsMixed.hasNull(2));
        assertFalse(statsMixed.hasNull(3));
    }
}