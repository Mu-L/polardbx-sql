package it.unimi.dsi.fastutil.objects;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import org.junit.Before;
import org.junit.Test;
import org.openjdk.jol.info.ClassLayout;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the MemoryCountableInt2Int2OpenHashMap class.
 */
public class MemoryCountableInt2Int2OpenHashMapTest {

    private MemoryCountableInt2Int2OpenHashMap map;

    @Before
    public void setUp() {
        // Initialize the map with an expected size of 16 and default load factor
        map = new MemoryCountableInt2Int2OpenHashMap(16, 0.75f);
    }

    @Test
    public void testDefaultConstructor() {
        // Verify initial size is zero
        assertEquals(0, map.size());
        // Verify the map is empty
        assertTrue(map.isEmpty());
        // Verify memory usage includes instance size and arrays
        long expectedMemory = ClassLayout.parseClass(MemoryCountableInt2Int2OpenHashMap.class).instanceSize()
            + FastMemoryCounter.sizeOf(map.key)
            + FastMemoryCounter.sizeOf(map.value);
        assertEquals(expectedMemory, map.getMemoryUsage());
    }

    @Test
    public void testConstructor() {
        MemoryCountableInt2Int2OpenHashMap map1 = new MemoryCountableInt2Int2OpenHashMap(16);
        MemoryCountableInt2Int2OpenHashMap map2 = new MemoryCountableInt2Int2OpenHashMap();
        MemoryCountableInt2Int2OpenHashMap map3 = new MemoryCountableInt2Int2OpenHashMap(16, 0.75f);
    }

    @Test
    public void testPutAndGet() {
        // Put key-value pairs into the map
        map.put(1, 100);
        map.put(2, 200);
        map.put(3, 300);

        // Verify size
        assertEquals(3, map.size());

        // Retrieve values and verify
        assertEquals(100, map.get(1));
        assertEquals(200, map.get(2));
        assertEquals(300, map.get(3));
        // Verify a non-existent key returns default value
        assertEquals(0, map.get(4));
    }

    @Test
    public void testOverwriteValue() {
        // Put a key with an initial value
        map.put(1, 100);
        assertEquals(100, map.get(1));

        // Overwrite the value for the same key
        int oldValue = map.put(1, 200);
        assertEquals(100, oldValue);
        assertEquals(200, map.get(1));

        // Verify size remains the same
        assertEquals(1, map.size());
    }

    @Test
    public void testRemove() {
        // Populate the map
        map.put(1, 100);
        map.put(2, 200);
        map.put(3, 300);

        // Remove a key and verify
        int removedValue = map.remove(2);
        assertEquals(200, removedValue);
        assertFalse(map.containsKey(2));
        assertEquals(2, map.size());

        // Attempt to remove a non-existent key
        removedValue = map.remove(4);
        assertEquals(0, removedValue);
        assertEquals(2, map.size());
    }

    @Test
    public void testContainsKey() {
        // Populate the map
        map.put(1, 100);
        map.put(2, 200);

        // Verify existing keys
        assertTrue(map.containsKey(1));
        assertTrue(map.containsKey(2));

        // Verify non-existent key
        assertFalse(map.containsKey(3));
    }

    @Test
    public void testContainsValue() {
        // Populate the map
        map.put(1, 100);
        map.put(2, 200);
        map.put(3, 300);

        // Verify existing values
        assertTrue(map.containsValue(100));
        assertTrue(map.containsValue(200));
        assertTrue(map.containsValue(300));

        // Verify non-existent value
        assertFalse(map.containsValue(400));
    }

    @Test
    public void testClear() {
        // Populate the map
        map.put(1, 100);
        map.put(2, 200);

        // Clear the map
        map.clear();

        // Verify the map is empty
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        assertFalse(map.containsKey(1));
        assertFalse(map.containsKey(2));
    }

    @Test
    public void testPutAll() {
        // Create another map and populate it
        Map<Integer, Integer> anotherMap = new HashMap<>();
        anotherMap.put(1, 100);
        anotherMap.put(2, 200);

        // Put all entries from anotherMap into the original map
        map.putAll(anotherMap);

        // Verify size and contents
        assertEquals(2, map.size());
        assertEquals(100, map.get(1));
        assertEquals(200, map.get(2));
    }

    @Test
    public void testComputeIfAbsent() {
        // Compute value for a non-existent key
        int value = map.computeIfAbsent(1, key -> key * 100);
        assertEquals(100, value);
        assertEquals(100, map.get(1));

        // Compute value for an existing key
        value = map.computeIfAbsent(1, key -> key * 200);
        assertEquals(100, value); // Should not overwrite
        assertEquals(100, map.get(1));
    }

    @Test
    public void testComputeIfPresent() {
        // Populate the map
        map.put(1, 100);
        map.put(2, 200);

        // Compute new value for an existing key
        int value = map.computeIfPresent(1, (key, oldValue) -> oldValue + 50);
        assertEquals(150, value);
        assertEquals(150, map.get(1));

        // Attempt to compute for a non-existent key
        value = map.computeIfPresent(3, (key, oldValue) -> oldValue + 50);
        assertEquals(0, value);
        assertFalse(map.containsKey(3));
    }

    @Test
    public void testCompute() {
        // Compute for a non-existent key
        int value = map.compute(1, (key, oldValue) -> (oldValue != null ? oldValue : 0) + 100);
        assertEquals(100, value);
        assertEquals(100, map.get(1));

        // Compute for an existing key
        value = map.compute(1, (key, oldValue) -> (oldValue != null ? oldValue : 0) + 50);
        assertEquals(150, value);
        assertEquals(150, map.get(1));

        // Compute to remove a key
        value = map.compute(1, (key, oldValue) -> null);
        assertEquals(0, value);
        assertFalse(map.containsKey(1));
    }

    @Test
    public void testAddTo() {
        // Add to a non-existent key
        int oldValue = map.addTo(1, 100);
        assertEquals(0, oldValue);
        assertEquals(100, map.get(1));

        // Add to an existing key
        oldValue = map.addTo(1, 50);
        assertEquals(100, oldValue);
        assertEquals(150, map.get(1));
    }

    @Test
    public void testGetOrDefault() {
        // Populate the map
        map.put(1, 100);

        // Get existing key
        int value = map.getOrDefault(1, 200);
        assertEquals(100, value);

        // Get non-existent key with default
        value = map.getOrDefault(2, 200);
        assertEquals(200, value);
    }

    @Test
    public void testReplace() {
        // Populate the map
        map.put(1, 100);
        map.put(2, 200);

        // Replace existing key
        int oldValue = map.replace(1, 150);
        assertEquals(100, oldValue);
        assertEquals(150, map.get(1));

        // Attempt to replace non-existent key
        oldValue = map.replace(3, 300);
        assertEquals(0, oldValue);
        assertFalse(map.containsKey(3));

        // Conditional replace with matching old value
        boolean replaced = map.replace(2, 200, 250);
        assertTrue(replaced);
        assertEquals(250, map.get(2));

        // Conditional replace with non-matching old value
        replaced = map.replace(2, 200, 300);
        assertFalse(replaced);
        assertEquals(250, map.get(2));
    }

    @Test
    public void testHashCode() {
        // Populate the map
        map.put(1, 100);
        map.put(2, 200);
        map.put(3, 300);

        // Calculate hash code manually
        int expectedHash = 0;
        expectedHash += (1 ^ 100);
        expectedHash += (2 ^ 200);
        expectedHash += (3 ^ 300);
        // No null key

        assertEquals(expectedHash, map.hashCode());

        // Add a null key (key = 0)
        map.put(0, 400);
        expectedHash += 400;
        assertEquals(expectedHash, map.hashCode());
    }

    @Test
    public void testRehash() {
        // Insert elements to trigger rehash
        for (int i = 1; i <= 20; i++) {
            map.put(i, i * 100);
        }

        // Verify size
        assertEquals(20, map.size());

        // Verify all elements are accessible
        for (int i = 1; i <= 20; i++) {
            assertTrue(map.containsKey(i));
            assertEquals(i * 100, map.get(i));
        }

        // Verify memory usage has increased due to rehash
        long expectedMemory = ClassLayout.parseClass(MemoryCountableInt2Int2OpenHashMap.class).instanceSize()
            + FastMemoryCounter.sizeOf(map.key)
            + FastMemoryCounter.sizeOf(map.value);
        assertTrue(map.getMemoryUsage() >= expectedMemory);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidLoadFactor() {
        // Attempt to create a map with invalid load factor
        new MemoryCountableInt2Int2OpenHashMap(16, 1.5f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeExpectedSize() {
        // Attempt to create a map with negative expected size
        new MemoryCountableInt2Int2OpenHashMap(-5, 0.75f);
    }
}
