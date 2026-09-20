package it.unimi.dsi.fastutil.objects;

import com.alibaba.polardbx.common.memory.MemoryCounter;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for the MemoryCountableObjectArraySet class.
 */
public class MemoryCountableObjectArraySetTest {

    private SimpleMemoryCounter<String> memoryCounter;
    private MemoryCountableObjectArraySet<String> set;

    @Before
    public void setUp() {
        // Initialize the simple memory counter
        memoryCounter = new SimpleMemoryCounter<>();
        // Set predefined memory usage for specific keys
        memoryCounter.setMemoryUsage("one", 10L);
        memoryCounter.setMemoryUsage("two", 20L);
        memoryCounter.setMemoryUsage("three", 30L);
        // Initialize the MemoryCountableObjectArraySet with the memory counter
        set = new MemoryCountableObjectArraySet<>(memoryCounter);
    }

    @Test
    public void testAdd() {
        assertTrue("Adding element 'one' should return true", set.add("one"));
        assertTrue("Set should contain 'one'", set.contains("one"));
        assertEquals("Set size should be 1", 1, set.size());
    }

    @Test
    public void testAddDuplicate() {
        assertTrue("First addition of 'one' should return true", set.add("one"));
        assertFalse("Adding duplicate 'one' should return false", set.add("one"));
        assertEquals("Set size should remain 1", 1, set.size());
    }

    @Test
    public void testRemove() {
        set.add("one");
        set.add("two");
        assertTrue("Removing existing element 'one' should return true", set.remove("one"));
        assertFalse("Set should no longer contain 'one'", set.contains("one"));
        assertEquals("Set size should be 1", 1, set.size());
    }

    @Test
    public void testRemoveNonExistent() {
        set.add("one");
        assertFalse("Removing non-existent element 'two' should return false", set.remove("two"));
        assertEquals("Set size should remain 1", 1, set.size());
    }

    @Test
    public void testContains() {
        set.add("one");
        set.add("two");
        assertTrue("Set should contain 'one'", set.contains("one"));
        assertFalse("Set should not contain 'three'", set.contains("three"));
    }

    @Test
    public void testIterator() {
        set.add("one");
        set.add("two");
        set.add("three");

        Iterator<String> iterator = set.iterator();
        assertTrue("Iterator should have next element", iterator.hasNext());
        assertEquals("First element should be 'one'", "one", iterator.next());
        assertTrue("Iterator should have next element", iterator.hasNext());
        assertEquals("Second element should be 'two'", "two", iterator.next());
        assertTrue("Iterator should have next element", iterator.hasNext());
        assertEquals("Third element should be 'three'", "three", iterator.next());
        assertFalse("Iterator should have no more elements", iterator.hasNext());
    }

    @Test
    public void testIteratorRemove() {
        set.add("one");
        set.add("two");
        set.add("three");

        Iterator<String> iterator = set.iterator();
        assertEquals("First element should be 'one'", "one", iterator.next());
        iterator.remove();
        assertFalse("Set should no longer contain 'one' after removal", set.contains("one"));
        assertEquals("Set size should be 2", 2, set.size());

        // Attempting consecutive remove operations should throw IllegalStateException
        try {
            iterator.remove();
            fail("Consecutive remove should throw IllegalStateException");
        } catch (Throwable e) {
            // Expected exception
        }
    }

    @Test
    public void testClear() {
        set.add("one");
        set.add("two");
        set.clear();
        assertTrue("Set should be empty after clear", set.isEmpty());
        assertEquals("Set size should be 0 after clear", 0, set.size());
        assertFalse("Set should not contain 'one' after clear", set.contains("one"));
        assertFalse("Set should not contain 'two' after clear", set.contains("two"));
    }

    @Test
    public void testToArray() {
        set.add("one");
        set.add("two");
        Object[] array = set.toArray();
        assertArrayEquals("toArray should return an array with all elements", new Object[] {"one", "two"}, array);
    }

    @Test
    public void testToArrayWithType() {
        set.add("one");
        set.add("two");
        String[] array = set.toArray(new String[0]);
        assertArrayEquals("toArray(T[]) should return an array with all elements", new String[] {"one", "two"}, array);
    }

    @Test
    public void testClone() {
        set.add("one");
        set.add("two");
        MemoryCountableObjectArraySet<String> clonedSet = set.clone();
        assertNotSame("Cloned set should be a different instance", set, clonedSet);
        assertEquals("Cloned set should have the same size as the original", set.size(), clonedSet.size());
        assertTrue("Cloned set should contain 'one'", clonedSet.contains("one"));
        assertTrue("Cloned set should contain 'two'", clonedSet.contains("two"));
    }

    @Test
    public void testAddAll() {
        set.addAll(Arrays.asList("one", "two", "three"));
        assertEquals("Set size should be 3", 3, set.size());
        assertTrue("Set should contain 'one'", set.contains("one"));
        assertTrue("Set should contain 'two'", set.contains("two"));
        assertTrue("Set should contain 'three'", set.contains("three"));
    }

    @Test
    public void testAddAllWithDuplicates() {
        set.add("one");
        set.addAll(Arrays.asList("one", "two", "two", "three"));
        assertEquals("Set size should be 3, ignoring duplicates", 3, set.size());
        assertTrue("Set should contain 'one'", set.contains("one"));
        assertTrue("Set should contain 'two'", set.contains("two"));
        assertTrue("Set should contain 'three'", set.contains("three"));
    }

    @Test
    public void testIsEmpty() {
        assertTrue("Newly created set should be empty", set.isEmpty());
        set.add("one");
        assertFalse("Set should not be empty after adding an element", set.isEmpty());
        set.remove("one");
        assertTrue("Set should be empty after removing the element", set.isEmpty());
    }

    @Test
    public void testSize() {
        assertEquals("Initial set size should be 0", 0, set.size());
        set.add("one");
        assertEquals("Set size should be 1 after adding one element", 1, set.size());
        set.add("two");
        assertEquals("Set size should be 2 after adding second element", 2, set.size());
        set.remove("one");
        assertEquals("Set size should be 1 after removing one element", 1, set.size());
        set.clear();
        assertEquals("Set size should be 0 after clearing", 0, set.size());
    }

    @Test
    public void testIteratorNoSuchElementException() {
        set.add("one");
        Iterator<String> iterator = set.iterator();
        assertTrue("Iterator should have next element", iterator.hasNext());
        assertEquals("First element should be 'one'", "one", iterator.next());
        assertFalse("Iterator should have no more elements", iterator.hasNext());
        try {
            iterator.next();
            fail("Calling next() when no elements should throw NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected exception
        }
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
