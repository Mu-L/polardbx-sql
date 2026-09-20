package it.unimi.dsi.fastutil.objects;

import com.alibaba.polardbx.common.memory.MemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCounterUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openjdk.jol.info.ClassLayout;

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
 * Unit tests for the MemoryCountableObject2ObjectArrayMap class.
 */
public class MemoryCountableObject2ObjectArrayMapTest {

    private MemoryCountableObject2ObjectArrayMap<String, String> map;
    private SimpleMemoryCounter<String> keyMemoryCounter;
    private SimpleMemoryCounter<String> valueMemoryCounter;
    private long instanceSize;

    @Before
    public void setUp() {
        // Initialize the memory counters
        keyMemoryCounter = new SimpleMemoryCounter<>();
        valueMemoryCounter = new SimpleMemoryCounter<>();

        // Set predefined memory usage for specific keys and values
        keyMemoryCounter.setMemoryUsage("one", 10L);
        keyMemoryCounter.setMemoryUsage("two", 20L);
        keyMemoryCounter.setMemoryUsage("three", 30L);

        valueMemoryCounter.setMemoryUsage("uno", 15L);
        valueMemoryCounter.setMemoryUsage("dos", 25L);
        valueMemoryCounter.setMemoryUsage("tres", 35L);

        // Initialize the map with the memory counters
        map = new MemoryCountableObject2ObjectArrayMap<>(keyMemoryCounter, valueMemoryCounter);

        // Get the instance size using JOL
        instanceSize = ClassLayout.parseClass(MemoryCountableObject2ObjectArrayMap.class).instanceSize();
    }

    @Test
    public void testKeySet() {
        MemoryCountableObject2ObjectArrayMap<String, String> map = new MemoryCountableObject2ObjectArrayMap<>(
            10,
            MemoryCounterUtils.getMemoryCounter(String.class),
            MemoryCounterUtils.getMemoryCounter(String.class));

        map.put("a", "1");
        map.put("b", "2");
        map.put("c", "3");
        map.put("d", "4");
        map.put("e", "5");
        map.put("f", "6");

        Throwable throwable = null;
        try {
            map.object2ObjectEntrySet();
        } catch (Throwable t) {
            throwable = t;
        }
        Assert.assertNotNull(throwable);

        map.keySet().stream().iterator().forEachRemaining(System.out::println);
        map.keySet().stream().forEach(System.out::println);

        ObjectSet<String> keySet = map.keySet();
        Assert.assertTrue(keySet.contains("a"));
        keySet.remove("a");
        Assert.assertFalse(keySet.contains("a"));

        keySet.forEach(System.out::println);

        ObjectIterator<String> iterator = keySet.iterator();
        iterator.next();
        iterator.remove();
        iterator.forEachRemaining(System.out::println);

        Assert.assertEquals(4, keySet.size());
        keySet.clear();
    }

    @Test
    public void testValues() {
        MemoryCountableObject2ObjectArrayMap<String, String> map = new MemoryCountableObject2ObjectArrayMap<>(
            10,
            MemoryCounterUtils.getMemoryCounter(String.class),
            MemoryCounterUtils.getMemoryCounter(String.class));

        map.put("a", "1");
        map.put("b", "2");
        map.put("c", "3");
        map.put("d", "4");
        map.put("e", "5");
        map.put("f", "6");

        map.values().stream().forEach(System.out::println);
        map.values().stream().iterator().forEachRemaining(System.out::println);

        ObjectCollection<String> values = map.values();
        ObjectIterator<String> iterator = values.iterator();

        if (iterator.hasNext()) {
            iterator.next();
        }
        iterator.remove();
        iterator.forEachRemaining(System.out::println);

    }

    /**
     * Test adding elements to the map and verify containment and size.
     */
    @Test
    public void testPut() {
        String previous = map.put("one", "uno");
        assertNull("Previous value should be null", previous);
        assertTrue("Map should contain key 'one'", map.containsKey("one"));
        assertEquals("Map size should be 1", 1, map.size());
        assertEquals("Value for key 'one' should be 'uno'", "uno", map.get("one"));
    }

    /**
     * Test adding duplicate keys to ensure value is updated and size remains the same.
     */
    @Test
    public void testPutDuplicate() {
        map.put("one", "uno");
        String previous = map.put("one", "un");
        assertEquals("Previous value should be 'uno'", "uno", previous);
        assertEquals("Map size should remain 1", 1, map.size());
        assertEquals("Value for key 'one' should be updated to 'un'", "un", map.get("one"));
    }

    /**
     * Test removing existing keys and verify containment and size.
     */
    @Test
    public void testRemove() {
        map.put("one", "uno");
        map.put("two", "dos");
        String removed = map.remove("one");
        assertEquals("Removed value should be 'uno'", "uno", removed);
        assertFalse("Map should no longer contain key 'one'", map.containsKey("one"));
        assertEquals("Map size should be 1", 1, map.size());
    }

    /**
     * Test removing non-existent keys to ensure default value is returned and size remains unchanged.
     */
    @Test
    public void testRemoveNonExistent() {
        map.put("one", "uno");
        String removed = map.remove("three");
        assertNull("Removing non-existent key should return null", removed);
        assertEquals("Map size should remain 1", 1, map.size());
    }

    /**
     * Test the containsKey method.
     */
    @Test
    public void testContainsKey() {
        map.put("one", "uno");
        assertTrue("Map should contain key 'one'", map.containsKey("one"));
        assertFalse("Map should not contain key 'two'", map.containsKey("two"));
    }

    /**
     * Test the containsValue method.
     */
    @Test
    public void testContainsValue() {
        map.put("one", "uno");
        map.put("two", "dos");
        assertTrue("Map should contain value 'uno'", map.containsValue("uno"));
        assertTrue("Map should contain value 'dos'", map.containsValue("dos"));
        assertFalse("Map should not contain value 'tres'", map.containsValue("tres"));
    }

    /**
     * Test retrieving values for existing and non-existing keys.
     */
    @Test
    public void testGet() {
        map.put("one", "uno");
        assertEquals("Value for key 'one' should be 'uno'", "uno", map.get("one"));
        assertNull("Value for non-existent key 'three' should be null", map.get("three"));
    }

    /**
     * Test the isEmpty and size methods.
     */
    @Test
    public void testIsEmptyAndSize() {
        assertTrue("Map should be empty initially", map.isEmpty());
        map.put("one", "uno");
        assertFalse("Map should not be empty after adding an element", map.isEmpty());
        assertEquals("Map size should be 1", 1, map.size());
        map.remove("one");
        assertTrue("Map should be empty after removing the element", map.isEmpty());
        assertEquals("Map size should be 0", 0, map.size());
    }

    /**
     * Test clearing the map.
     */
    @Test
    public void testClear() {
        map.put("one", "uno");
        map.put("two", "dos");
        assertFalse("Map should not be empty before clear", map.isEmpty());
        map.clear();
        assertTrue("Map should be empty after clear", map.isEmpty());
        assertEquals("Map size should be 0 after clear", 0, map.size());
        assertFalse("Map should not contain key 'one' after clear", map.containsKey("one"));
        assertFalse("Map should not contain key 'two' after clear", map.containsKey("two"));
    }

    /**
     * Test if the iterator can traverse all elements.
     */
    @Test
    public void testIterator() {
        map.put("one", "uno");
        map.put("two", "dos");
        map.put("three", "tres");

        Iterator<String> iterator = map.keySet().iterator();
        int count = 0;
        while (iterator.hasNext()) {
            String key = iterator.next();
            assertNotNull("Key should not be null", key);
            assertTrue("Key should be 'one', 'two', or 'three'",
                key.equals("one") || key.equals("two") || key.equals("three"));
            count++;
        }
        assertEquals("Should have iterated over 3 keys", 3, count);
    }

    /**
     * Test the clone method.
     */
    @Test
    public void testClone() {
        map.put("one", "uno");
        map.put("two", "dos");

        MemoryCountableObject2ObjectArrayMap<String, String> clonedMap = map.clone();
        assertNotSame("Cloned map should be a different instance", map, clonedMap);
        assertEquals("Cloned map should have the same size", map.size(), clonedMap.size());
        assertEquals("Cloned map should contain key 'one' with value 'uno'", "uno", clonedMap.get("one"));
        assertEquals("Cloned map should contain key 'two' with value 'dos'", "dos", clonedMap.get("two"));

        // Modify the cloned map and ensure the original map is unaffected
        clonedMap.put("three", "tres");
        assertFalse("Original map should not contain key 'three'", map.containsKey("three"));
        assertTrue("Cloned map should contain key 'three'", clonedMap.containsKey("three"));
    }

    /**
     * Test that the iterator throws NoSuchElementException when no more elements are available.
     */
    @Test(expected = NoSuchElementException.class)
    public void testIteratorNoSuchElementException() {
        map.put("one", "uno");
        Iterator<String> iterator = map.keySet().iterator();
        assertTrue("Iterator should have next element", iterator.hasNext());
        iterator.next();
        assertFalse("Iterator should have no more elements", iterator.hasNext());
        // The following call should throw NoSuchElementException
        iterator.next();
    }

    /**
     * A simple implementation of the MemoryCounter interface for testing purposes.
     */
    public static class SimpleMemoryCounter<K> implements MemoryCounter<K> {
        private final Map<K, Long> memoryUsageMap = new HashMap<>();

        /**
         * Sets the memory usage for a specific key.
         *
         * @param key the key
         * @param memoryUsage the memory usage to associate with the key
         */
        public void setMemoryUsage(K key, long memoryUsage) {
            memoryUsageMap.put(key, memoryUsage);
        }

        @Override
        public long getMemoryUsage(K key) {
            return memoryUsageMap.getOrDefault(key, 0L);
        }
    }
}

