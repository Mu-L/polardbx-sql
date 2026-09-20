package com.alibaba.polardbx.common.orc;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for FastDoubleColumnStatistics class.
 * This test class covers all methods and code lines without using mocks.
 */
public class FastDoubleColumnStatisticsTest {

    @Test
    public void testDefaultConstructor() {
        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics();

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
        double[] minMaxValues = {
            10.5, 20.7, 30.2, 40.8, 50.1, 60.9, 70.3, 80.6, 90.4, 100.0
        };

        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics(
            totalRowGroupCount,
            accumulatedRowGroupCountPerStripe,
            hasNullBitmap,
            minMaxValues
        );

        assertEquals(totalRowGroupCount, stats.getTotalRowGroupCount());
        assertArrayEquals(accumulatedRowGroupCountPerStripe, stats.getAccumulatedRowGroupCountPerStripe());
        assertArrayEquals(hasNullBitmap, stats.getHasNullBitmap());
        assertArrayEquals(minMaxValues, stats.getMinMaxValues(), 0.0);
    }

    @Test
    public void testSetTotalRowGroupCount() {
        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics();

        FastDoubleColumnStatistics result = stats.setTotalRowGroupCount(10);

        assertEquals(10, stats.getTotalRowGroupCount());
        assertSame(stats, result); // Test fluent interface
    }

    @Test
    public void testSetAccumulatedRowGroupCountPerStripe() {
        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics();
        int[] accumulatedRowGroupCountPerStripe = {3, 7, 10};

        FastDoubleColumnStatistics result =
            stats.setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripe);

        assertArrayEquals(accumulatedRowGroupCountPerStripe, stats.getAccumulatedRowGroupCountPerStripe());
        assertSame(stats, result); // Test fluent interface
    }

    @Test
    public void testSetHasNullBitmap() {
        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics();
        byte[] hasNullBitmap = {1, 0, 1, 0};

        FastDoubleColumnStatistics result = stats.setHasNullBitmap(hasNullBitmap);

        assertArrayEquals(hasNullBitmap, stats.getHasNullBitmap());
        assertSame(stats, result); // Test fluent interface
    }

    @Test
    public void testSetMinMaxValues() {
        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics();
        double[] minMaxValues = {100.5, 200.7, 300.2, 400.8};

        FastDoubleColumnStatistics result = stats.setMinMaxValues(minMaxValues);

        assertArrayEquals(minMaxValues, stats.getMinMaxValues(), 0.0);
        assertSame(stats, result); // Test fluent interface
    }

    @Test
    public void testGetMemoryUsageWithNullArrays() {
        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics();

        long memoryUsage = stats.getMemoryUsage();

        // Should return instance size when arrays are null
        assertTrue(memoryUsage > 0);
    }

    @Test
    public void testGetMemoryUsageWithNonNullArrays() {
        byte[] hasNullBitmap = {0, 1, 0, 1, 0};
        double[] minMaxValues = {
            10.1, 20.2, 30.3, 40.4, 50.5, 60.6, 70.7, 80.8, 90.9, 100.0
        };

        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics(
            5, null, hasNullBitmap, minMaxValues
        );

        long memoryUsage = stats.getMemoryUsage();

        // Should include instance size plus aligned array sizes
        assertTrue(memoryUsage > 0);
    }

    @Test
    public void testCompleteWorkflow() {
        // Test a complete workflow with all operations
        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics();

        // Set up data using fluent interface
        int[] accumulatedRowGroupCountPerStripe = {2, 5, 8};
        byte[] hasNullBitmap = {0, 1, 0, 1, 0, 1, 0, 0};
        double[] minMaxValues = {
            1.1, 10.9, 2.2, 20.8, 3.3, 30.7, 4.4, 40.6, 5.5, 50.5, 6.6, 60.4, 7.7, 70.3, 8.8, 80.2
        };

        stats.setTotalRowGroupCount(8)
            .setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripe)
            .setHasNullBitmap(hasNullBitmap)
            .setMinMaxValues(minMaxValues);

        // Verify all data is set correctly
        assertEquals(8, stats.getTotalRowGroupCount());
        assertArrayEquals(accumulatedRowGroupCountPerStripe, stats.getAccumulatedRowGroupCountPerStripe());
        assertArrayEquals(hasNullBitmap, stats.getHasNullBitmap());
        assertArrayEquals(minMaxValues, stats.getMinMaxValues(), 0.0);

        // Test memory usage calculation
        long memoryUsage = stats.getMemoryUsage();
        assertTrue(memoryUsage > 0);
    }

    @Test
    public void testEdgeCases() {
        // Test with empty arrays
        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics(
            0, new int[0], new byte[0], new double[0]
        );

        assertEquals(0, stats.getTotalRowGroupCount());
        assertEquals(0, stats.getAccumulatedRowGroupCountPerStripe().length);
        assertEquals(0, stats.getHasNullBitmap().length);
        assertEquals(0, stats.getMinMaxValues().length);

        long memoryUsage = stats.getMemoryUsage();
        assertTrue(memoryUsage > 0);
    }

    @Test
    public void testSpecialDoubleValues() {
        // Test with special double values
        double[] minMaxValues = {
            Double.MIN_VALUE, Double.MAX_VALUE,
            Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
            -0.0, 0.0,
            Double.NaN, Double.NaN
        };
        int[] accumulatedRowGroupCountPerStripe = {};
        byte[] hasNullBitmap = {};

        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics(
            4, accumulatedRowGroupCountPerStripe, hasNullBitmap, minMaxValues
        );

        assertArrayEquals(minMaxValues, stats.getMinMaxValues(), 0.0);

        // Verify special values are preserved
        assertEquals(Double.MIN_VALUE, stats.getMinMaxValues()[0], 0.0);
        assertEquals(Double.MAX_VALUE, stats.getMinMaxValues()[1], 0.0);
        assertEquals(Double.NEGATIVE_INFINITY, stats.getMinMaxValues()[2], 0.0);
        assertEquals(Double.POSITIVE_INFINITY, stats.getMinMaxValues()[3], 0.0);
        assertEquals(-0.0, stats.getMinMaxValues()[4], 0.0);
        assertEquals(0.0, stats.getMinMaxValues()[5], 0.0);
        assertTrue(Double.isNaN(stats.getMinMaxValues()[6]));
        assertTrue(Double.isNaN(stats.getMinMaxValues()[7]));
    }

    @Test
    public void testPrecisionValues() {
        // Test with high precision values
        double[] minMaxValues = {
            1.23456789012345, 9.87654321098765,
            0.000000000000001, 999999999999999.0,
            -1.23456789012345, -9.87654321098765
        };
        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics(
            3, null, null, minMaxValues
        );

        assertArrayEquals(minMaxValues, stats.getMinMaxValues(), 0.0);

        // Verify precision is maintained
        assertEquals(1.23456789012345, stats.getMinMaxValues()[0], 0.0);
        assertEquals(9.87654321098765, stats.getMinMaxValues()[1], 0.0);
        assertEquals(0.000000000000001, stats.getMinMaxValues()[2], 0.0);
        assertEquals(999999999999999.0, stats.getMinMaxValues()[3], 0.0);
        assertEquals(-1.23456789012345, stats.getMinMaxValues()[4], 0.0);
        assertEquals(-9.87654321098765, stats.getMinMaxValues()[5], 0.0);
    }

    @Test
    public void testNullBitmapVariations() {
        // Test different null bitmap patterns
        byte[] allNulls = {1, 1, 1, 1};
        byte[] noNulls = {0, 0, 0, 0};
        byte[] mixed = {1, 0, 1, 0};

        FastDoubleColumnStatistics statsAllNulls = new FastDoubleColumnStatistics(4, null, allNulls, null);
        FastDoubleColumnStatistics statsNoNulls = new FastDoubleColumnStatistics(4, null, noNulls, null);
        FastDoubleColumnStatistics statsMixed = new FastDoubleColumnStatistics(4, null, mixed, null);

        // Verify bitmap arrays are correctly stored
        assertArrayEquals(allNulls, statsAllNulls.getHasNullBitmap());
        assertArrayEquals(noNulls, statsNoNulls.getHasNullBitmap());
        assertArrayEquals(mixed, statsMixed.getHasNullBitmap());
    }

    @Test
    public void testFluentInterfaceChaining() {
        // Test that all setter methods can be chained
        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics();
        int[] accumulatedRowGroupCountPerStripe = {1, 3, 5};
        byte[] hasNullBitmap = {0, 1, 0};
        double[] minMaxValues = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};

        FastDoubleColumnStatistics result = stats
            .setTotalRowGroupCount(3)
            .setAccumulatedRowGroupCountPerStripe(accumulatedRowGroupCountPerStripe)
            .setHasNullBitmap(hasNullBitmap)
            .setMinMaxValues(minMaxValues);

        // Verify all methods return the same instance
        assertSame(stats, result);

        // Verify all values are set correctly
        assertEquals(3, stats.getTotalRowGroupCount());
        assertArrayEquals(accumulatedRowGroupCountPerStripe, stats.getAccumulatedRowGroupCountPerStripe());
        assertArrayEquals(hasNullBitmap, stats.getHasNullBitmap());
        assertArrayEquals(minMaxValues, stats.getMinMaxValues(), 0.0);
    }

    @Test
    public void testMemoryUsageWithDifferentArraySizes() {
        // Test memory usage with different array sizes
        FastDoubleColumnStatistics smallStats = new FastDoubleColumnStatistics(
            1, new int[1], new byte[1], new double[2]
        );

        FastDoubleColumnStatistics largeStats = new FastDoubleColumnStatistics(
            100, new int[10], new byte[100], new double[200]
        );

        long smallMemory = smallStats.getMemoryUsage();
        long largeMemory = largeStats.getMemoryUsage();

        assertTrue(smallMemory > 0);
        assertTrue(largeMemory > 0);
        assertTrue(largeMemory > smallMemory); // Larger arrays should use more memory
    }

    @Test
    public void testZeroValues() {
        // Test with zero and negative zero values
        double[] minMaxValues = {0.0, -0.0, 0.0, -0.0};
        FastDoubleColumnStatistics stats = new FastDoubleColumnStatistics(
            2, null, null, minMaxValues
        );

        assertArrayEquals(minMaxValues, stats.getMinMaxValues(), 0.0);
        assertEquals(0.0, stats.getMinMaxValues()[0], 0.0);
        assertEquals(-0.0, stats.getMinMaxValues()[1], 0.0);
    }
}