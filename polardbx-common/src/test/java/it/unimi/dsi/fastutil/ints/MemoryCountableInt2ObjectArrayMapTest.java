package it.unimi.dsi.fastutil.ints;

import com.alibaba.polardbx.common.memory.MemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCounterUtils;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
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
 * Unit tests for the MemoryCountableInt2ObjectArrayMap class.
 */
public class MemoryCountableInt2ObjectArrayMapTest {

    private MemoryCountableInt2ObjectArrayMap<String> map;
    private SimpleMemoryCounter<String> memoryCounter;
    private long instanceSize;

    @Before
    public void setUp() {
        // Initialize the memory counter
        memoryCounter = new SimpleMemoryCounter<>();
        // Set predefined memory usage for specific values
        memoryCounter.setMemoryUsage("uno", 15L);
        memoryCounter.setMemoryUsage("dos", 25L);
        memoryCounter.setMemoryUsage("tres", 35L);
        memoryCounter.setMemoryUsage("quatro", 45L);

        // Initialize the map with the memory counter
        map = new MemoryCountableInt2ObjectArrayMap<>(memoryCounter);

        // Get the instance size using JOL
        instanceSize = ClassLayout.parseClass(MemoryCountableInt2ObjectArrayMap.class).instanceSize();
    }

    @Test
    public void testKeySet() {
        Map<Integer, String> hashMap = new HashMap<>();
        for (int i = 0; i < 16; i++) {
            hashMap.put(i, String.valueOf(i));
        }
        MemoryCountableInt2ObjectArrayMap<String> map = new MemoryCountableInt2ObjectArrayMap<>(
            hashMap, MemoryCounterUtils.getMemoryCounter(String.class));

        map.keySet().iterator().forEachRemaining(System.out::println);
        map.keySet().intStream().forEach(System.out::println);
        for (Iterator iterator = map.keySet().iterator(); iterator.hasNext(); ) {
            iterator.next();
            iterator.remove();
        }
    }

    @Test
    public void testValues() {
        Map<Integer, String> hashMap = new HashMap<>();
        for (int i = 0; i < 16; i++) {
            hashMap.put(i, String.valueOf(i));
        }
        MemoryCountableInt2ObjectArrayMap<String> map = new MemoryCountableInt2ObjectArrayMap<>(
            hashMap, MemoryCounterUtils.getMemoryCounter(String.class));

        map.values().iterator().forEachRemaining(System.out::println);
        map.values().stream().forEach(System.out::println);

        for (Iterator iterator = map.values().iterator(); iterator.hasNext(); ) {
            iterator.next();
            iterator.remove();
        }
    }

    @Test
    public void testEntrySet() {
        Map<Integer, String> hashMap = new HashMap<>();
        for (int i = 0; i < 16; i++) {
            hashMap.put(i, String.valueOf(i));
        }
        MemoryCountableInt2ObjectArrayMap<String> map = new MemoryCountableInt2ObjectArrayMap<>(
            hashMap, MemoryCounterUtils.getMemoryCounter(String.class));

        map.int2ObjectEntrySet().stream().forEach(System.out::println);
        map.int2ObjectEntrySet().stream().iterator().forEachRemaining(System.out::println);

        for (Iterator iterator = map.int2ObjectEntrySet().iterator(); iterator.hasNext(); ) {
            iterator.next();
            iterator.remove();
        }

        for (Iterator iterator = map.int2ObjectEntrySet().fastIterator(); iterator.hasNext(); ) {
            iterator.next();
            iterator.remove();
        }
    }

    @Test
    public void testConstructor() {
        MemoryCountableInt2ObjectArrayMap<String> map1 = new MemoryCountableInt2ObjectArrayMap<>(
            MemoryCounterUtils.getMemoryCounter(String.class), new int[16], new String[16]);

        MemoryCountableInt2ObjectArrayMap<String> map2 = new MemoryCountableInt2ObjectArrayMap<>(
            MemoryCounterUtils.getMemoryCounter(String.class), 16);

        MemoryCountableInt2ObjectArrayMap<String> map3 = new MemoryCountableInt2ObjectArrayMap<>(
            map1, MemoryCounterUtils.getMemoryCounter(String.class));

        Map<Integer, String> hashMap = new HashMap<>();
        for (int i = 0; i < 16; i++) {
            hashMap.put(i, String.valueOf(i));
        }
        MemoryCountableInt2ObjectArrayMap<String> map4 = new MemoryCountableInt2ObjectArrayMap<>(
            hashMap, MemoryCounterUtils.getMemoryCounter(String.class));

        MemoryCountableInt2ObjectArrayMap<String> map5 = new MemoryCountableInt2ObjectArrayMap<>(
            MemoryCounterUtils.getMemoryCounter(String.class),
            new int[] {1, 2, 3, 4},
            new String[] {"1", "2", "3", "4"},
            4);

        MemoryCountableInt2ObjectArrayMap.FastEntrySet<String> fastEntrySet = map4.int2ObjectEntrySet();
        for (ObjectIterator<Int2ObjectMap.Entry<String>> iterator = fastEntrySet.iterator(); iterator.hasNext(); ) {
            iterator.next();
            iterator.remove();
            iterator.forEachRemaining(System.out::println);
        }

        IntSet intSet = map5.keySet();
        Assert.assertEquals(intSet.size(), map5.size());
        Assert.assertTrue(intSet.contains(1));
        intSet.remove(1);
        Assert.assertFalse(intSet.contains(1));
        for (IntIterator iterator = intSet.iterator(); iterator.hasNext(); ) {
            iterator.nextInt();
            iterator.remove();
            iterator.forEachRemaining(System.out::println);
        }
    }

    /**
     * Test adding elements to the map and verify containment and size.
     */
    @Test
    public void testPut() {
        assertNull("Previous value should be null", map.put(1, "uno"));
        assertTrue("Map should contain key 1", map.containsKey(1));
        assertEquals("Map size should be 1", 1, map.size());
        assertEquals("Value for key 1 should be 'uno'", "uno", map.get(1));
    }

    /**
     * Test adding duplicate keys to ensure value is updated and size remains the same.
     */
    @Test
    public void testPutDuplicate() {
        map.put(1, "uno");
        assertEquals("Previous value should be 'uno'", "uno", map.put(1, "un"));
        assertEquals("Map size should remain 1", 1, map.size());
        assertEquals("Value for key 1 should be updated to 'un'", "un", map.get(1));
    }

    /**
     * Test removing existing keys and verify containment and size.
     */
    @Test
    public void testRemove() {
        map.put(1, "uno");
        map.put(2, "dos");
        assertEquals("Removed value should be 'uno'", "uno", map.remove(1));
        assertFalse("Map should no longer contain key 1", map.containsKey(1));
        assertEquals("Map size should be 1", 1, map.size());
    }

    /**
     * Test removing non-existent keys to ensure default value is returned and size remains unchanged.
     */
    @Test
    public void testRemoveNonExistent() {
        map.put(1, "uno");
        assertNull("Removing non-existent key should return null", map.remove(3));
        assertEquals("Map size should remain 1", 1, map.size());
    }

    /**
     * Test the containsKey method.
     */
    @Test
    public void testContainsKey() {
        map.put(1, "uno");
        assertTrue("Map should contain key 1", map.containsKey(1));
        assertFalse("Map should not contain key 2", map.containsKey(2));
    }

    /**
     * Test the containsValue method.
     */
    @Test
    public void testContainsValue() {
        map.put(1, "uno");
        map.put(2, "dos");
        assertTrue("Map should contain value 'uno'", map.containsValue("uno"));
        assertTrue("Map should contain value 'dos'", map.containsValue("dos"));
        assertFalse("Map should not contain value 'tres'", map.containsValue("tres"));
    }

    /**
     * Test retrieving values for existing and non-existing keys.
     */
    @Test
    public void testGet() {
        map.put(1, "uno");
        assertEquals("Value for key 1 should be 'uno'", "uno", map.get(1));
        assertNull("Value for non-existent key 3 should be null", map.get(3));
    }

    /**
     * Test the isEmpty and size methods.
     */
    @Test
    public void testIsEmptyAndSize() {
        assertTrue("Map should be empty initially", map.isEmpty());

        map.put(1, "uno");
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
        map.put(1, "uno");
        map.put(2, "dos");
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
        map.put(1, "uno");
        map.put(2, "dos");
        map.put(3, "tres");

        Iterator<Int2ObjectMap.Entry<String>> iterator = map.int2ObjectEntrySet().iterator();
        int count = 0;
        while (iterator.hasNext()) {
            Int2ObjectMap.Entry<String> entry = iterator.next();
            assertNotNull("Entry should not be null", entry);
            int key = entry.getIntKey();
            String value = entry.getValue();
            assertTrue("Key should be 1, 2, or 3", key == 1 || key == 2 || key == 3);
            assertTrue("Value should match the key",
                value.equals("uno") || value.equals("dos") || value.equals("tres"));
            count++;
        }
        assertEquals("Should have iterated over 3 entries", 3, count);
    }

    /**
     * Test the clone method.
     */
    @Test
    public void testClone() {
        map.put(1, "uno");
        map.put(2, "dos");

        MemoryCountableInt2ObjectArrayMap<String> clonedMap = map.clone();
        assertNotSame("Cloned map should be a different instance", map, clonedMap);
        assertEquals("Cloned map should have the same size", map.size(), clonedMap.size());
        assertEquals("Cloned map should contain key 1 with value 'uno'", "uno", clonedMap.get(1));
        assertEquals("Cloned map should contain key 2 with value 'dos'", "dos", clonedMap.get(2));

        // Modify the cloned map and ensure the original map is unaffected
        clonedMap.put(3, "tres");
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
        map.put(1, "uno");
        Iterator<Int2ObjectMap.Entry<String>> iterator = map.int2ObjectEntrySet().iterator();
        assertTrue("Iterator should have next element", iterator.hasNext());
        iterator.next();
        assertFalse("Iterator should have no more elements", iterator.hasNext());
        // The following call should throw NoSuchElementException
        iterator.next();
    }

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

