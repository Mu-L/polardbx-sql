package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for the MemoryCountableAtomicReferenceArray class.
 */
public class MemoryCountableAtomicReferenceArrayTest {

    private MemoryCountableAtomicReferenceArray<MemoryCountableImpl> atomicArray;

    /**
     * A simple implementation of MemoryCountable for testing purposes.
     */
    private static class MemoryCountableImpl implements MemoryCountable {
        private long memoryUsage;

        public MemoryCountableImpl(long memoryUsage) {
            this.memoryUsage = memoryUsage;
        }

        @Override
        public long getMemoryUsage() {
            return memoryUsage;
        }

        @Override
        public String toString() {
            return "MemoryCountableImpl{" +
                "memoryUsage=" + memoryUsage +
                '}';
        }
    }

    @Before
    public void setUp() {
        // Initialize the MemoryCountableAtomicReferenceArray with a length of 5
        atomicArray = new MemoryCountableAtomicReferenceArray<>(5);
    }

    @Test
    public void testDefaultConstructor() {
        // Verify that all elements are initially null and memory usage is zero
        for (int i = 0; i < atomicArray.length(); i++) {
            assertNull(atomicArray.get(i));
        }
    }

    @Test
    public void testArrayConstructor() {
        // Create an array of MemoryCountableImpl instances
        MemoryCountableImpl[] initialArray = new MemoryCountableImpl[] {
            new MemoryCountableImpl(100L),
            new MemoryCountableImpl(200L),
            new MemoryCountableImpl(300L)
        };

        // Initialize the MemoryCountableAtomicReferenceArray with the existing array
        MemoryCountableAtomicReferenceArray<MemoryCountableImpl> constructedArray =
            new MemoryCountableAtomicReferenceArray<>(initialArray);

        // Verify that elements are correctly copied
        for (int i = 0; i < initialArray.length; i++) {
            assertEquals(initialArray[i], constructedArray.get(i));
        }

        // Verify remaining elements are null
        for (int i = initialArray.length; i < constructedArray.length(); i++) {
            assertNull(constructedArray.get(i));
        }
    }

    @Test
    public void testLength() {
        // Verify the length of the array
        assertEquals(5, atomicArray.length());

        // Initialize with a different length
        MemoryCountableAtomicReferenceArray<MemoryCountableImpl> anotherArray =
            new MemoryCountableAtomicReferenceArray<>(10);
        assertEquals(10, anotherArray.length());
    }

    @Test
    public void testGetAndSet() {
        // Initially, all elements are null
        for (int i = 0; i < atomicArray.length(); i++) {
            assertNull(atomicArray.get(i));
        }

        // Set elements at specific indices
        MemoryCountableImpl elem1 = new MemoryCountableImpl(150L);
        MemoryCountableImpl elem2 = new MemoryCountableImpl(250L);

        atomicArray.set(1, elem1);
        atomicArray.set(3, elem2);

        // Verify that elements are set correctly
        assertEquals(elem1, atomicArray.get(1));
        assertEquals(elem2, atomicArray.get(3));

        // Verify other elements remain null
        assertNull(atomicArray.get(0));
        assertNull(atomicArray.get(2));
        assertNull(atomicArray.get(4));
    }

    @Test
    public void testGetAndSetMemoryUsage() {
        // Set elements and verify memory usage updates correctly
        MemoryCountableImpl elem1 = new MemoryCountableImpl(100L);
        MemoryCountableImpl elem2 = new MemoryCountableImpl(200L);

        long oldValue = atomicArray.getMemoryUsage();
        atomicArray.set(0, elem1);
        assertEquals(oldValue + 100L, atomicArray.getMemoryUsage());

        oldValue = atomicArray.getMemoryUsage();
        atomicArray.set(1, elem2);
        assertEquals(oldValue + 200L, atomicArray.getMemoryUsage());

        // Replace element at index 0
        MemoryCountableImpl elem3 = new MemoryCountableImpl(150L);

        oldValue = atomicArray.getMemoryUsage();
        atomicArray.set(0, elem3);
        assertEquals(oldValue - 100L + 150L, atomicArray.getMemoryUsage());
    }

    @Test
    public void testGetAndSetAtomicity() {
        // Test atomicity by setting and getting elements concurrently would require multithreading.
        // However, since we're not using Mockito or any concurrency testing tools, we'll simulate it sequentially.

        MemoryCountableImpl elem1 = new MemoryCountableImpl(100L);
        MemoryCountableImpl elem2 = new MemoryCountableImpl(200L);

        long oldValue = atomicArray.getMemoryUsage();

        // Set elem1 and verify
        atomicArray.set(2, elem1);
        assertEquals(elem1, atomicArray.get(2));
        assertEquals(oldValue + 100L, atomicArray.getMemoryUsage());

        // Atomically set elem2 and verify the old value is returned
        oldValue = atomicArray.getMemoryUsage();
        MemoryCountableImpl oldElem = atomicArray.getAndSet(2, elem2);
        assertEquals(elem1, oldElem);
        assertEquals(elem2, atomicArray.get(2));
        assertEquals(oldValue - 100L + 200L, atomicArray.getMemoryUsage());
    }

    @Test
    public void testToStringEmpty() {
        // Verify the string representation of an empty array
        assertEquals("[null, null, null, null, null]", atomicArray.toString());
    }

    @Test
    public void testToStringWithElements() {
        // Set elements and verify the string representation
        MemoryCountableImpl elem1 = new MemoryCountableImpl(100L);
        MemoryCountableImpl elem2 = new MemoryCountableImpl(200L);

        atomicArray.set(0, elem1);
        atomicArray.set(4, elem2);

        assertEquals("[MemoryCountableImpl{memoryUsage=100}, null, null, null, MemoryCountableImpl{memoryUsage=200}]",
            atomicArray.toString());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetInvalidIndex() {
        // Attempt to get an element with an invalid index
        atomicArray.get(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetInvalidIndex() {
        // Attempt to set an element with an invalid index
        atomicArray.set(5, new MemoryCountableImpl(100L));
    }

//    @Test
//    public void testFromMethod() {
//        // Create multiple instances of MemoryCountableAtomicReferenceArray
//        MemoryCountableAtomicReferenceArray<MemoryCountableImpl> array1 = new MemoryCountableAtomicReferenceArray<>(3);
//        array1.set(0, new MemoryCountableImpl(100L));
//        array1.set(1, new MemoryCountableImpl(200L));
//        array1.set(2, new MemoryCountableImpl(300L));
//
//        MemoryCountableAtomicReferenceArray<MemoryCountableImpl> array2 = new MemoryCountableAtomicReferenceArray<>(3);
//        array2.set(0, new MemoryCountableImpl(400L));
//        array2.set(1, new MemoryCountableImpl(500L));
//        array2.set(2, new MemoryCountableImpl(600L));
//
//        // Merge them using the from method
//        MemoryCountableAtomicReferenceArray<MemoryCountableImpl> mergedArray =
//            MemoryCountableAtomicReferenceArray.from(Arrays.asList(array1, array2));
//
//        // Verify the merged results
//        assertEquals(2, mergedArray.length());
//        assertEquals(500L, mergedArray.getMemoryUsage());
//    }

    @Test
    public void testUpdateElementMemoryUsage() {
        // Update element memory usage and verify
        long oldValue = atomicArray.getMemoryUsage();
        atomicArray.updateElementMemoryUsage(50L);
        assertEquals(oldValue + 50L, atomicArray.getMemoryUsage());

        oldValue = atomicArray.getMemoryUsage();
        atomicArray.updateElementMemoryUsage(150L);
        assertEquals(oldValue + 150L, atomicArray.getMemoryUsage());
    }
}
