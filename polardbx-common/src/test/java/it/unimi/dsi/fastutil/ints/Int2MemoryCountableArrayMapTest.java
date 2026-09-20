package it.unimi.dsi.fastutil.ints;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the Int2MemoryCountableArrayMap class.
 */
public class Int2MemoryCountableArrayMapTest {

    private Int2MemoryCountableArrayMap<TestMemoryCountable> map;

    @Before
    public void setUp() {
        // Initialize the map
        map = new Int2MemoryCountableArrayMap<>();
    }

    @Test
    public void test() {
        Int2MemoryCountableArrayMap<TestMemoryCountable> map1 = new Int2MemoryCountableArrayMap<>(
            new int[] {1, 2, 3},
            new Object[] {new TestMemoryCountable(1), new TestMemoryCountable(2), new TestMemoryCountable(3)}
        );

        Int2MemoryCountableArrayMap<TestMemoryCountable> map2 = new Int2MemoryCountableArrayMap<>(map1);

        Map<Integer, TestMemoryCountable> hashMap = new HashMap<>();
        hashMap.put(1, new TestMemoryCountable(1));
        hashMap.put(2, new TestMemoryCountable(2));
        Int2MemoryCountableArrayMap<TestMemoryCountable> map3 = new Int2MemoryCountableArrayMap<>(hashMap);

        Int2MemoryCountableArrayMap<TestMemoryCountable> map4 = new Int2MemoryCountableArrayMap<>(
            new int[] {1, 2, 3},
            new Object[] {new TestMemoryCountable(1), new TestMemoryCountable(2), new TestMemoryCountable(3)},
            3
        );

        map3.keySet().intStream().forEach(System.out::println);
        map3.keySet().iterator().forEachRemaining(System.out::println);

        map3.values().iterator().forEachRemaining(System.out::println);
        map3.values().stream().iterator().forEachRemaining(System.out::println);

        map3.int2ObjectEntrySet().stream().forEach(System.out::println);
        map3.int2ObjectEntrySet().stream().iterator().forEachRemaining(System.out::println);
        map3.int2ObjectEntrySet().iterator().forEachRemaining(System.out::println);

        map3.int2ObjectEntrySet().fastIterator().forEachRemaining(System.out::println);

        map3.int2ObjectEntrySet().fastIterator().hasNext();
        map3.int2ObjectEntrySet().fastIterator().next();
    }

    @Test
    public void testKeySet() {
        Int2MemoryCountableArrayMap<TestMemoryCountable> map = new Int2MemoryCountableArrayMap<>(
            new int[] {1, 2, 3},
            new Object[] {new TestMemoryCountable(1), new TestMemoryCountable(2), new TestMemoryCountable(3)}
        );

        map.keySet().intStream().forEach(System.out::println);
        map.keySet().iterator().forEachRemaining(System.out::println);
        map.keySet().stream().iterator().forEachRemaining(System.out::println);
        for (Iterator iterator = map.keySet().iterator(); iterator.hasNext(); ) {
            iterator.next();
            iterator.remove();
        }
    }

    @Test
    public void testValues() {
        Int2MemoryCountableArrayMap<TestMemoryCountable> map = new Int2MemoryCountableArrayMap<>(
            new int[] {1, 2, 3},
            new Object[] {new TestMemoryCountable(1), new TestMemoryCountable(2), new TestMemoryCountable(3)}
        );

        map.values().stream().forEach(System.out::println);
        map.values().stream().iterator().forEachRemaining(System.out::println);
        map.values().iterator().forEachRemaining(System.out::println);

        for (Iterator iterator = map.values().iterator(); iterator.hasNext(); ) {
            iterator.next();
            iterator.remove();
        }
    }

    @Test
    public void testEntrySet() {
        Int2MemoryCountableArrayMap<TestMemoryCountable> map = new Int2MemoryCountableArrayMap<>(
            new int[] {1, 2, 3},
            new Object[] {new TestMemoryCountable(1), new TestMemoryCountable(2), new TestMemoryCountable(3)}
        );

        map.int2ObjectEntrySet().stream().forEach(System.out::println);
        map.int2ObjectEntrySet().stream().iterator().forEachRemaining(System.out::println);
        map.int2ObjectEntrySet().iterator().forEachRemaining(System.out::println);
        for (Iterator iterator = map.int2ObjectEntrySet().iterator(); iterator.hasNext(); ) {
            iterator.next();
            iterator.remove();
        }

        map = new Int2MemoryCountableArrayMap<>(
            new int[] {1, 2, 3},
            new Object[] {new TestMemoryCountable(1), new TestMemoryCountable(2), new TestMemoryCountable(3)}
        );
        for (Iterator iterator = map.int2ObjectEntrySet().fastIterator(); iterator.hasNext(); ) {
            iterator.next();
            iterator.remove();
        }
    }

    /**
     * Test adding elements to the map and verify containment and size.
     */
    @Test
    public void testPut() {
        TestMemoryCountable value = new TestMemoryCountable(50L);
        TestMemoryCountable previous = map.put(1, value);
        assertNull("Previous value should be null", previous);
        assertTrue("Map should contain key 1", map.containsKey(1));
        assertEquals("Map size should be 1", 1, map.size());
        assertEquals("Value for key 1 should be the inserted value", value, map.get(1));
    }

    /**
     * Test adding duplicate keys to ensure value is updated and size remains the same.
     */
    @Test
    public void testPutDuplicate() {
        TestMemoryCountable value1 = new TestMemoryCountable(50L);
        TestMemoryCountable value2 = new TestMemoryCountable(100L);
        map.put(1, value1);
        TestMemoryCountable previous = map.put(1, value2);
        assertEquals("Previous value should be value1", value1, previous);
        assertEquals("Map size should remain 1", 1, map.size());
        assertEquals("Value for key 1 should be updated to value2", value2, map.get(1));
    }

    /**
     * Test removing existing keys and verify containment and size.
     */
    @Test
    public void testRemove() {
        TestMemoryCountable value1 = new TestMemoryCountable(50L);
        TestMemoryCountable value2 = new TestMemoryCountable(100L);
        map.put(1, value1);
        map.put(2, value2);

        TestMemoryCountable removed = map.remove(1);
        assertEquals("Removed value should be value1", value1, removed);
        assertFalse("Map should no longer contain key 1", map.containsKey(1));
        assertEquals("Map size should be 1", 1, map.size());
    }

    /**
     * Test removing non-existent keys to ensure default value is returned and size remains unchanged.
     */
    @Test
    public void testRemoveNonExistent() {
        TestMemoryCountable value1 = new TestMemoryCountable(50L);
        map.put(1, value1);

        TestMemoryCountable removed = map.remove(2);
        assertNull("Removing non-existent key should return null", removed);
        assertEquals("Map size should remain 1", 1, map.size());
    }

    /**
     * Test the containsKey method.
     */
    @Test
    public void testContainsKey() {
        TestMemoryCountable value1 = new TestMemoryCountable(50L);
        map.put(1, value1);

        assertTrue("Map should contain key 1", map.containsKey(1));
        assertFalse("Map should not contain key 2", map.containsKey(2));
    }

    /**
     * Test the containsValue method.
     */
    @Test
    public void testContainsValue() {
        TestMemoryCountable value1 = new TestMemoryCountable(50L);
        TestMemoryCountable value2 = new TestMemoryCountable(100L);
        map.put(1, value1);
        map.put(2, value2);

        assertTrue("Map should contain value1", map.containsValue(value1));
        assertTrue("Map should contain value2", map.containsValue(value2));
        assertFalse("Map should not contain a non-inserted value", map.containsValue(new TestMemoryCountable(150L)));
    }

    /**
     * Test retrieving values for existing and non-existing keys.
     */
    @Test
    public void testGet() {
        TestMemoryCountable value1 = new TestMemoryCountable(50L);
        map.put(1, value1);

        assertEquals("Value for key 1 should be value1", value1, map.get(1));
        assertNull("Value for non-existent key should be null", map.get(2));
    }

    /**
     * Test the isEmpty and size methods.
     */
    @Test
    public void testIsEmptyAndSize() {
        assertTrue("Map should be empty initially", map.isEmpty());

        TestMemoryCountable value1 = new TestMemoryCountable(50L);
        map.put(1, value1);

        assertFalse("Map should not be empty after adding an element", map.isEmpty());
        assertEquals("Map size should be 1", 1, map.size());

        map.remove(1);

        assertTrue("Map should be empty after removing the element", map.isEmpty());
        assertEquals("Map size should be 0", 0, map.size());
    }

    /**
     * Test clearing the map.
     */
    @Test
    public void testClear() {
        TestMemoryCountable value1 = new TestMemoryCountable(50L);
        TestMemoryCountable value2 = new TestMemoryCountable(100L);
        map.put(1, value1);
        map.put(2, value2);

        assertFalse("Map should not be empty before clear", map.isEmpty());

        map.clear();

        assertTrue("Map should be empty after clear", map.isEmpty());
        assertEquals("Map size should be 0 after clear", 0, map.size());
        assertFalse("Map should not contain key 1 after clear", map.containsKey(1));
        assertFalse("Map should not contain key 2 after clear", map.containsKey(2));
    }

    /**
     * Test if the iterator can traverse all elements.
     */
    @Test
    public void testIterator() {
        TestMemoryCountable value1 = new TestMemoryCountable(50L);
        TestMemoryCountable value2 = new TestMemoryCountable(100L);
        TestMemoryCountable value3 = new TestMemoryCountable(150L);
        map.put(1, value1);
        map.put(2, value2);
        map.put(3, value3);

        ObjectIterator<Int2ObjectMap.Entry<TestMemoryCountable>> iterator = map.int2ObjectEntrySet().iterator();

        int count = 0;
        while (iterator.hasNext()) {
            Int2MemoryCountableArrayMap.Entry<TestMemoryCountable> entry = iterator.next();
            assertNotNull("Entry should not be null", entry);
            int key = entry.getIntKey();
            TestMemoryCountable value = entry.getValue();
            assertTrue("Key should be 1, 2, or 3", key == 1 || key == 2 || key == 3);
            assertTrue("Value should match the key", value.getMemoryUsage() == key * 50L);
            count++;
        }
        assertEquals("Should have iterated over 3 entries", 3, count);
    }

    /**
     * Test the clone method.
     */
    @Test
    public void testClone() {
        TestMemoryCountable value1 = new TestMemoryCountable(50L);
        TestMemoryCountable value2 = new TestMemoryCountable(100L);
        map.put(1, value1);
        map.put(2, value2);

        Int2MemoryCountableArrayMap<TestMemoryCountable> clonedMap = map.clone();
        assertNotSame("Cloned map should be a different instance", map, clonedMap);
        assertEquals("Cloned map should have the same size", map.size(), clonedMap.size());
        assertEquals("Cloned map should contain key 1 with value1", value1, clonedMap.get(1));
        assertEquals("Cloned map should contain key 2 with value2", value2, clonedMap.get(2));

        // Modify the cloned map and ensure the original map is unaffected
        TestMemoryCountable value3 = new TestMemoryCountable(150L);
        clonedMap.put(3, value3);
        assertFalse("Original map should not contain key 3", map.containsKey(3));
        assertTrue("Cloned map should contain key 3", clonedMap.containsKey(3));
        assertEquals("Cloned map size should be 3", 3, clonedMap.size());
    }

    /**
     * Simulates the VMSupport.align method by aligning to 8-byte boundaries.
     *
     * @param size the original size
     * @return the aligned size
     */
    private long align(int size) {
        int alignment = 8;
        return ((size + alignment - 1) / alignment) * alignment;
    }

    /**
     * Test that the iterator throws NoSuchElementException when no more elements are available.
     */
    @Test(expected = NoSuchElementException.class)
    public void testIteratorNoSuchElementException() {
        TestMemoryCountable value1 = new TestMemoryCountable(50L);
        map.put(1, value1);
        ObjectIterator<Int2ObjectMap.Entry<TestMemoryCountable>> iterator = map.int2ObjectEntrySet().iterator();
        assertTrue("Iterator should have next element", iterator.hasNext());
        iterator.next();
        assertFalse("Iterator should have no more elements", iterator.hasNext());
        // The following call should throw NoSuchElementException
        iterator.next();
    }

    // TestMemoryCountable.java

    /**
     * A simple implementation of the MemoryCountable interface for testing purposes.
     */
    public static class TestMemoryCountable implements MemoryCountable {
        private final long memoryUsage;

        /**
         * Constructs a TestMemoryCountable with the specified memory usage.
         *
         * @param memoryUsage the memory usage in bytes
         */
        public TestMemoryCountable(long memoryUsage) {
            this.memoryUsage = memoryUsage;
        }

        @Override
        public long getMemoryUsage() {
            return memoryUsage;
        }
    }

}

