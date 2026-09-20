package com.alibaba.polardbx.common.orc;

import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.RecordReaderImpl;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for FastPositionIndexImpl class.
 * This test class covers all methods and code lines without using mocks.
 */
public class FastPositionIndexImplTest {

    @Test
    public void testConstructorAndBasicGetters() {
        // Test data setup
        int totalRowGroupCount = 10;
        byte[] positionListUnitSizeArray = {4, 6, 8};
        int[] accumulatedRowGroupCountPerStripe = {0, 3, 7, 10};
        int[] intPositionList = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        long[] positionList = {100L, 200L, 300L, 400L, 500L, 600L, 700L, 800L, 900L, 1000L};

        // Create instance with long array
        FastPositionIndexImpl indexWithLong = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        // Test basic getters
        assertEquals(totalRowGroupCount, indexWithLong.getTotalRowGroupCount());
        assertArrayEquals(accumulatedRowGroupCountPerStripe, indexWithLong.getAccumulatedRowGroupCountPerStripe());

        // Test position list unit size
        assertEquals(4, indexWithLong.getPositionListUnitSize(0));
        assertEquals(6, indexWithLong.getPositionListUnitSize(1));
        assertEquals(8, indexWithLong.getPositionListUnitSize(2));
    }

    @Test
    public void testConstructorWithNullPositionList() {
        // Test data setup with null positionList
        int totalRowGroupCount = 6;
        byte[] positionListUnitSizeArray = {2, 3};
        int[] accumulatedRowGroupCountPerStripe = {0, 2, 6};
        int[] intPositionList = {10, 20, 30, 40, 50, 60};
        long[] positionList = null;

        // Create instance with null long array
        FastPositionIndexImpl indexWithInt = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        assertEquals(totalRowGroupCount, indexWithInt.getTotalRowGroupCount());
        assertArrayEquals(accumulatedRowGroupCountPerStripe, indexWithInt.getAccumulatedRowGroupCountPerStripe());
    }

    @Test
    public void testGetMemoryUsage() {
        int totalRowGroupCount = 4;
        byte[] positionListUnitSizeArray = {2, 2};
        int[] accumulatedRowGroupCountPerStripe = {0, 2, 4};
        int[] intPositionList = {1, 2, 3, 4};
        long[] positionList = {100L, 200L, 300L, 400L};

        FastPositionIndexImpl index = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        long memoryUsage = index.getMemoryUsage();
        assertTrue("Memory usage should be positive", memoryUsage > 0);
    }

    @Test
    public void testGetMemoryUsageWithNullPositionList() {
        int totalRowGroupCount = 4;
        byte[] positionListUnitSizeArray = {2, 2};
        int[] accumulatedRowGroupCountPerStripe = {0, 2, 4};
        int[] intPositionList = {1, 2, 3, 4};
        long[] positionList = null;

        FastPositionIndexImpl index = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        long memoryUsage = index.getMemoryUsage();
        assertTrue("Memory usage should be positive", memoryUsage > 0);
    }

    @Test
    public void testGetPositionWithLongArray() {
        int totalRowGroupCount = 6;
        byte[] positionListUnitSizeArray = {2, 3, 1};
        int[] accumulatedRowGroupCountPerStripe = {2, 4, 6};
        int[] intPositionList = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        long[] positionList = {100L, 200L, 300L, 400L, 500L, 600L, 700L, 800L, 900L, 1000L, 1100L, 1200L};

        FastPositionIndexImpl index = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        // Test getting position from stripe 0, rowGroup 0
        assertEquals(100L, index.getPosition(0, 0, 0));
        assertEquals(200L, index.getPosition(0, 0, 1));

        // Test getting position from stripe 0, rowGroup 1
        assertEquals(300L, index.getPosition(0, 1, 0));
        assertEquals(400L, index.getPosition(0, 1, 1));

        // Test getting position from stripe 1, rowGroup 0
        assertEquals(500L, index.getPosition(1, 0, 0));
        assertEquals(600L, index.getPosition(1, 0, 1));
        assertEquals(700L, index.getPosition(1, 0, 2));
    }

    @Test
    public void testGetPositionWithIntArray() {
        int totalRowGroupCount = 4;
        byte[] positionListUnitSizeArray = {2, 2};
        int[] accumulatedRowGroupCountPerStripe = {0, 2, 4};
        int[] intPositionList = {10, 20, 30, 40, 50, 60, 70, 80};
        long[] positionList = null; // Use int array instead

        FastPositionIndexImpl index = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        // Test getting position from stripe 0, rowGroup 0
        assertEquals(10, index.getPosition(0, 0, 0));
        assertEquals(20, index.getPosition(0, 0, 1));

        // Test getting position from stripe 0, rowGroup 1
        assertEquals(30, index.getPosition(0, 1, 0));
        assertEquals(40, index.getPosition(0, 1, 1));

        // Test getting position from stripe 1, rowGroup 0
        assertEquals(50, index.getPosition(1, 0, 0));
        assertEquals(60, index.getPosition(1, 0, 1));
    }

    @Test
    public void testGetPositionProviderWithLongArray() {
        int totalRowGroupCount = 4;
        byte[] positionListUnitSizeArray = {2, 2};
        int[] accumulatedRowGroupCountPerStripe = {0, 2, 4};
        int[] intPositionList = {1, 2, 3, 4, 5, 6, 7, 8};
        long[] positionList = {100L, 200L, 300L, 400L, 500L, 600L, 700L, 800L};

        FastPositionIndexImpl index = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        PositionProvider provider = index.getPositionProvider(0, 0);
        assertNotNull(provider);
        assertTrue(provider instanceof RecordReaderImpl.LongArrayPositionProviderImpl);

        // Test next() method to verify the provider works correctly
        assertEquals(100L, provider.getNext());
        assertEquals(200L, provider.getNext());
    }

    @Test
    public void testGetPositionProviderWithIntArray() {
        int totalRowGroupCount = 4;
        byte[] positionListUnitSizeArray = {2, 2};
        int[] accumulatedRowGroupCountPerStripe = {0, 2, 4};
        int[] intPositionList = {10, 20, 30, 40, 50, 60, 70, 80};
        long[] positionList = null; // Use int array instead

        FastPositionIndexImpl index = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        PositionProvider provider = index.getPositionProvider(0, 0);
        assertNotNull(provider);
        assertTrue(provider instanceof RecordReaderImpl.IntArrayPositionProviderImpl);

        // Test next() method to verify the provider works correctly
        assertEquals(10L, provider.getNext());
        assertEquals(20L, provider.getNext());
    }

    @Test
    public void testGetPositionListUnitSizeWithByteValues() {
        // Test with byte values that need unsigned conversion
        byte[] positionListUnitSizeArray = {-1, -128, 127, 0}; // -1 = 255, -128 = 128
        int totalRowGroupCount = 4;
        int[] accumulatedRowGroupCountPerStripe = {0, 1, 2, 3, 4};
        int[] intPositionList = {1, 2, 3, 4};
        long[] positionList = {1L, 2L, 3L, 4L};

        FastPositionIndexImpl index = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        assertEquals(255, index.getPositionListUnitSize(0)); // -1 & 0xFF = 255
        assertEquals(128, index.getPositionListUnitSize(1)); // -128 & 0xFF = 128
        assertEquals(127, index.getPositionListUnitSize(2)); // 127 & 0xFF = 127
        assertEquals(0, index.getPositionListUnitSize(3));   // 0 & 0xFF = 0
    }

    @Test
    public void testComplexStripeCalculation() {
        // Test the complex stripe calculation in getStartIndexInPositionArray
        int totalRowGroupCount = 10;
        byte[] positionListUnitSizeArray = {2, 3, 4};
        int[] accumulatedRowGroupCountPerStripe = {0, 3, 6, 10};
        int[] intPositionList = new int[50]; // Large enough array
        for (int i = 0; i < intPositionList.length; i++) {
            intPositionList[i] = i + 1;
        }
        long[] positionList = null;

        FastPositionIndexImpl index = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        // Test positions in different stripes
        // Stripe 0: 3 row groups, unit size 2
        assertEquals(1, index.getPosition(0, 0, 0)); // Start at index 0
        assertEquals(3, index.getPosition(0, 1, 0)); // Start at index 2
        assertEquals(5, index.getPosition(0, 2, 0)); // Start at index 4

        // Stripe 1: 3 row groups (6-3=3), unit size 3
        // Start index = 3 * 2 = 6
        assertEquals(7, index.getPosition(1, 0, 0)); // Start at index 6
        assertEquals(10, index.getPosition(1, 1, 0)); // Start at index 9

        // Stripe 2: 4 row groups (10-6=4), unit size 4
        // Start index = 3 * 2 + 3 * 3 = 15
        assertEquals(16, index.getPosition(2, 0, 0)); // Start at index 15
        assertEquals(20, index.getPosition(2, 1, 0)); // Start at index 19
    }

    @Test
    public void testLastStripeCalculation() {
        // Test the special case for last stripe in getStartIndexInPositionArray
        int totalRowGroupCount = 8;
        byte[] positionListUnitSizeArray = {2, 3};
        int[] accumulatedRowGroupCountPerStripe = {0, 3, 8}; // Last stripe has 8-3=5 row groups
        int[] intPositionList = new int[30];
        for (int i = 0; i < intPositionList.length; i++) {
            intPositionList[i] = (i + 1) * 10;
        }
        long[] positionList = null;

        FastPositionIndexImpl index = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        // Test that the last stripe calculation works correctly
        // Stripe 1 (last stripe): should have 5 row groups (8-3=5)
        // Start index = 3 * 2 = 6
        assertEquals(70, index.getPosition(1, 0, 0)); // Start at index 6
        assertEquals(100, index.getPosition(1, 1, 0)); // Start at index 9 (6 + 1*3)
        assertEquals(130, index.getPosition(1, 2, 0)); // Start at index 12 (6 + 2*3)
    }

    @Test
    public void testEdgeCaseWithSingleStripe() {
        // Test edge case with only one stripe
        int totalRowGroupCount = 2;
        byte[] positionListUnitSizeArray = {3};
        int[] accumulatedRowGroupCountPerStripe = {0, 2};
        int[] intPositionList = {1, 2, 3, 4, 5, 6};
        long[] positionList = null;

        FastPositionIndexImpl index = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        assertEquals(1, index.getPosition(0, 0, 0));
        assertEquals(2, index.getPosition(0, 0, 1));
        assertEquals(3, index.getPosition(0, 0, 2));
        assertEquals(4, index.getPosition(0, 1, 0));
        assertEquals(5, index.getPosition(0, 1, 1));
        assertEquals(6, index.getPosition(0, 1, 2));
    }

    @Test
    public void testPositionProviderForDifferentStripes() {
        int totalRowGroupCount = 6;
        byte[] positionListUnitSizeArray = {2, 2, 2};
        int[] accumulatedRowGroupCountPerStripe = {0, 2, 4, 6};
        int[] intPositionList = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120};
        long[] positionList = {100L, 200L, 300L, 400L, 500L, 600L, 700L, 800L, 900L, 1000L, 1100L, 1200L};

        FastPositionIndexImpl index = new FastPositionIndexImpl(
            totalRowGroupCount, positionListUnitSizeArray, accumulatedRowGroupCountPerStripe,
            intPositionList, positionList);

        // Test position provider for stripe 1, rowGroup 1
        PositionProvider provider = index.getPositionProvider(1, 1);
        assertNotNull(provider);
        assertTrue(provider instanceof RecordReaderImpl.LongArrayPositionProviderImpl);

        // The start index should be: stripe 0 (2 groups * 2 units) + rowGroup 1 (1 * 2 units) = 4 + 2 = 6
        assertEquals(700L, provider.getNext()); // positionList[6]
        assertEquals(800L, provider.getNext()); // positionList[7]
    }
}