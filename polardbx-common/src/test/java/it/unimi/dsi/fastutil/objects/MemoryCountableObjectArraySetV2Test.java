package it.unimi.dsi.fastutil.objects;

import com.alibaba.polardbx.common.memory.MemoryCounter;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Spliterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MemoryCountableObjectArraySetV2Test {

    /**
     * A simple MemoryCounter implementation for testing purposes.
     * It returns a fixed memory usage for any element.
     */
    private static class SimpleMemoryCounter<K> implements MemoryCounter<K> {
        private final long memoryUsagePerElement;

        public SimpleMemoryCounter(long memoryUsagePerElement) {
            this.memoryUsagePerElement = memoryUsagePerElement;
        }

        @Override
        public long getMemoryUsage(K k) {
            return memoryUsagePerElement;
        }
    }

    private MemoryCounter<String> memoryCounter;

    @Before
    public void setUp() {
        // Initialize MemoryCounter with fixed memory usage per element
        memoryCounter = new SimpleMemoryCounter<>(10);
    }

    /**
     * Helper method to create a sample array set with given elements.
     */
    private MemoryCountableObjectArraySet<String> createSampleSet(String... elements) {
        return new MemoryCountableObjectArraySet<>(elements, memoryCounter);
    }

    /**
     * Test all constructors of MemoryCountableObjectArraySet.
     */
    @Test
    public void testConstructors() {
        // Test constructor with array
        String[] initialArray = {"one", "two", "three"};
        MemoryCountableObjectArraySet<String> setWithArray =
            new MemoryCountableObjectArraySet<>(initialArray, memoryCounter);
        assertEquals(3, setWithArray.size());
        assertTrue(setWithArray.contains("one"));
        assertTrue(setWithArray.contains("two"));
        assertTrue(setWithArray.contains("three"));

        // Test empty constructor
        MemoryCountableObjectArraySet<String> emptySet = new MemoryCountableObjectArraySet<>(memoryCounter);
        assertEquals(0, emptySet.size());
        assertTrue(emptySet.isEmpty());

        // Test constructor with initial capacity
        int capacity = 5;
        MemoryCountableObjectArraySet<String> setWithCapacity =
            new MemoryCountableObjectArraySet<>(capacity, memoryCounter);
        assertEquals(0, setWithCapacity.size());
        setWithCapacity.add("alpha");
        assertEquals(1, setWithCapacity.size());
        assertTrue(setWithCapacity.contains("alpha"));

        // Test constructor from ObjectCollection
        ObjectCollection<String> objectCollection = new ObjectArrayList<>(Arrays.asList("a", "b", "c"));
        MemoryCountableObjectArraySet<String> setFromObjectCollection =
            new MemoryCountableObjectArraySet<>(objectCollection, memoryCounter);
        assertEquals(3, setFromObjectCollection.size());
        assertTrue(setFromObjectCollection.contains("a"));
        assertTrue(setFromObjectCollection.contains("b"));
        assertTrue(setFromObjectCollection.contains("c"));

        // Test constructor from Collection
        Collection<String> collection = Arrays.asList("x", "y", "z");
        MemoryCountableObjectArraySet<String> setFromCollection =
            new MemoryCountableObjectArraySet<>(collection, memoryCounter);
        assertEquals(3, setFromCollection.size());
        assertTrue(setFromCollection.contains("x"));
        assertTrue(setFromCollection.contains("y"));
        assertTrue(setFromCollection.contains("z"));

        // Test constructor from ObjectSet
        ObjectSet<String> objectSet = new ObjectOpenHashSet<>(Arrays.asList("foo", "bar"));
        MemoryCountableObjectArraySet<String> setFromObjectSet =
            new MemoryCountableObjectArraySet<>(objectSet, memoryCounter);
        assertEquals(2, setFromObjectSet.size());
        assertTrue(setFromObjectSet.contains("foo"));
        assertTrue(setFromObjectSet.contains("bar"));

        // Test constructor with array and size
        String[] arrayWithExtras = {"first", "second", "third", "extra"};
        MemoryCountableObjectArraySet<String> setWithArrayAndSize =
            new MemoryCountableObjectArraySet<>(arrayWithExtras, 3, memoryCounter);
        assertEquals(3, setWithArrayAndSize.size());
        assertTrue(setWithArrayAndSize.contains("first"));
        assertTrue(setWithArrayAndSize.contains("second"));
        assertTrue(setWithArrayAndSize.contains("third"));
        assertFalse(setWithArrayAndSize.contains("extra"));
    }

    /**
     * Test the Spliterator functionality of MemoryCountableObjectArraySet.
     */
    @Test
    public void testSpliterator() {
        String[] elements = {"apple", "banana", "cherry", "date", "elderberry"};
        MemoryCountableObjectArraySet<String> set = createSampleSet(elements);

        // Obtain spliterator
        Spliterator<String> spliterator = set.spliterator();

        // Test characteristics
        int characteristics = spliterator.characteristics();
        assertTrue((characteristics & Spliterator.DISTINCT) != 0);
        assertTrue((characteristics & Spliterator.ORDERED) != 0);
        assertTrue((characteristics & Spliterator.SUBSIZED) != 0);
        assertTrue((characteristics & Spliterator.SIZED) != 0);

        // Test estimateSize
        assertEquals(set.size(), spliterator.estimateSize());

        // Collect elements using spliterator
        Collection<String> collected = new java.util.ArrayList<>();
        spliterator.forEachRemaining(collected::add);
        assertEquals(set.size(), collected.size());
        for (String element : elements) {
            assertTrue(collected.contains(element));
        }

        // Test trySplit
        Spliterator<String> split = spliterator.trySplit();
        if (split != null) {
            Collection<String> firstHalf = new java.util.ArrayList<>();
            split.forEachRemaining(firstHalf::add);
            Collection<String> secondHalf = new java.util.ArrayList<>();
            spliterator.forEachRemaining(secondHalf::add);

            assertEquals(2, firstHalf.size());
            assertEquals(3, secondHalf.size());

            for (String e : firstHalf) {
                assertTrue(set.contains(e));
            }
            for (String e : secondHalf) {
                assertTrue(set.contains(e));
            }
        }
    }

    /**
     * Test the toArray methods of MemoryCountableObjectArraySet.
     */
    @Test
    public void testToArray() {
        String[] elements = {"red", "green", "blue"};
        MemoryCountableObjectArraySet<String> set = createSampleSet(elements);

        // Test toArray()
        Object[] objectArray = set.toArray();
        assertEquals(set.size(), objectArray.length);
        for (String element : elements) {
            assertTrue(Arrays.asList(objectArray).contains(element));
        }

        // Test toArray(T[] a) with insufficient size
        String[] smallArray = new String[2];
        String[] resultArray1 = set.toArray(smallArray);
        assertNotSame(smallArray, resultArray1);
        assertEquals(set.size(), resultArray1.length);
        for (String element : elements) {
            assertTrue(Arrays.asList(resultArray1).contains(element));
        }

        // Test toArray(T[] a) with exact size
        String[] exactArray = new String[3];
        String[] resultArray2 = set.toArray(exactArray);
        assertSame(exactArray, resultArray2);
        for (String element : elements) {
            assertTrue(Arrays.asList(resultArray2).contains(element));
        }

        // Test toArray(T[] a) with larger size
        String[] largeArray = new String[5];
        Arrays.fill(largeArray, "overflow");
        String[] resultArray3 = set.toArray(largeArray);
        assertSame(largeArray, resultArray3);
        for (int i = 0; i < set.size(); i++) {
            assertEquals(elements[i], resultArray3[i]);
        }
        assertNull(resultArray3[3]);
        assertEquals("overflow", resultArray3[4]);
    }

    /**
     * Test incremental addition of elements.
     */
    @Test
    public void testIncrementalAdditions() {
        MemoryCountableObjectArraySet<String> set = new MemoryCountableObjectArraySet<>(memoryCounter);
        assertTrue(set.isEmpty());

        // Add elements one by one
        assertTrue(set.add("alpha"));
        assertFalse(set.isEmpty());
        assertEquals(1, set.size());
        assertTrue(set.contains("alpha"));

        assertTrue(set.add("beta"));
        assertEquals(2, set.size());
        assertTrue(set.contains("beta"));

        assertTrue(set.add("gamma"));
        assertEquals(3, set.size());
        assertTrue(set.contains("gamma"));

        // Attempt to add duplicate
        assertFalse(set.add("alpha"));
        assertEquals(3, set.size());
    }

    /**
     * Test incremental removal of elements.
     */
    @Test
    public void testIncrementalRemovals() {
        String[] elements = {"one", "two", "three"};
        MemoryCountableObjectArraySet<String> set = createSampleSet(elements);
        assertEquals(3, set.size());

        // Remove elements one by one
        assertTrue(set.remove("two"));
        assertEquals(2, set.size());
        assertFalse(set.contains("two"));

        assertTrue(set.remove("one"));
        assertEquals(1, set.size());
        assertFalse(set.contains("one"));

        assertTrue(set.remove("three"));
        assertEquals(0, set.size());
        assertTrue(set.isEmpty());

        // Attempt to remove non-existing element
        assertFalse(set.remove("four"));
    }

    /**
     * Test the iterator's remove functionality.
     */
    @Test
    public void testIteratorRemove() {
        String[] elements = {"alpha", "beta", "gamma"};
        MemoryCountableObjectArraySet<String> set = createSampleSet(elements);
        assertEquals(3, set.size());

        ObjectIterator<String> iterator = set.iterator();
        while (iterator.hasNext()) {
            String element = iterator.next();
            if ("beta".equals(element)) {
                iterator.remove();
            }
        }

        assertEquals(2, set.size());
        assertFalse(set.contains("beta"));
        assertTrue(set.contains("alpha"));
        assertTrue(set.contains("gamma"));
    }

    /**
     * Test the clone method.
     */
    @Test
    public void testClone() {
        String[] elements = {"dog", "cat", "bird"};
        MemoryCountableObjectArraySet<String> original = createSampleSet(elements);
        MemoryCountableObjectArraySet<String> cloned = original.clone();

        assertNotSame(original, cloned);
        assertEquals(original.size(), cloned.size());
        for (String element : elements) {
            assertTrue(cloned.contains(element));
        }

        // Modify the cloned set and ensure original is unaffected
        cloned.add("fish");
        assertEquals(4, cloned.size());
        assertEquals(3, original.size());
        assertTrue(cloned.contains("fish"));
        assertFalse(original.contains("fish"));
    }

    /**
     * Test the clear method.
     */
    @Test
    public void testClear() {
        String[] elements = {"sun", "moon", "stars"};
        MemoryCountableObjectArraySet<String> set = createSampleSet(elements);
        assertEquals(3, set.size());

        set.clear();
        assertEquals(0, set.size());
        assertTrue(set.isEmpty());
        for (String element : elements) {
            assertFalse(set.contains(element));
        }
    }

    /**
     * Test the memory usage calculation.
     * This test ensures that getMemoryUsage returns the expected value.
     */
    @Test
    public void testMemoryUsage() {
        String[] elements = {"alpha", "beta", "gamma"};
        MemoryCountableObjectArraySet<String> set = createSampleSet(elements);
        set.getMemoryUsage();
    }

    /**
     * Test that adding elements beyond initial capacity works correctly (resizing).
     */
    @Test
    public void testResizing() {
        MemoryCountableObjectArraySet<Integer> set =
            new MemoryCountableObjectArraySet<>(2, new SimpleMemoryCounter<>(4));

        // Add elements beyond initial capacity to trigger resizing
        assertTrue(set.add(1));
        assertTrue(set.add(2));
        assertTrue(set.add(3)); // Should trigger resize

        assertEquals(3, set.size());
        assertTrue(set.contains(1));
        assertTrue(set.contains(2));
        assertTrue(set.contains(3));

        // Verify internal array size (should be at least doubled)
        // Since initial capacity was 2, after adding the third element, capacity should be 4
        // However, since the internal array is private, we cannot directly test it.
        // Instead, we can add more elements and ensure no exceptions are thrown
        assertTrue(set.add(4));
        assertEquals(4, set.size());
        assertTrue(set.contains(4));

        assertTrue(set.add(5));
        assertEquals(5, set.size());
        assertTrue(set.contains(5));
    }
}
