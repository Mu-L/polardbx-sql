package it.unimi.dsi.fastutil.ints;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/**
 * Unit tests for Int2MemoryCountableArrayMap focusing on keySet and entrySet.
 */
public class Int2MemoryCountableArrayMapV2Test {

    private Int2MemoryCountableArrayMap<TestMemoryCountable> map;

    /**
     * A simple implementation of MemoryCountable for testing purposes.
     */
    private static class TestMemoryCountable implements MemoryCountable {
        private long memory;

        public TestMemoryCountable(long memory) {
            this.memory = memory;
        }

        @Override
        public long getMemoryUsage() {
            return memory;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof TestMemoryCountable)) {
                return false;
            }
            TestMemoryCountable other = (TestMemoryCountable) obj;
            return memory == other.memory;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(memory);
        }
    }

    @Before
    public void setUp() {
        map = new Int2MemoryCountableArrayMap<>();
        map.put(1, new TestMemoryCountable(100));
        map.put(2, new TestMemoryCountable(200));
        map.put(3, new TestMemoryCountable(300));
    }

    @Test
    public void testConstructor() {
        Int2MemoryCountableArrayMap<TestMemoryCountable> map = new Int2MemoryCountableArrayMap<>(16);
    }

    /**
     * Test the keySet for correct keys and size.
     */
    @Test
    public void testKeySetContainsKeys() {
        IntSet keySet = map.keySet();
        assertEquals(3, keySet.size());
        assertTrue(keySet.contains(1));
        assertTrue(keySet.contains(2));
        assertTrue(keySet.contains(3));
        assertFalse(keySet.contains(4));
    }

    /**
     * Test the keySet iterator for correct iteration.
     */
    @Test
    public void testKeySetIterator() {
        IntSet keySet = map.keySet();
        IntIterator iterator = keySet.iterator();

        int[] expectedKeys = {1, 2, 3};
        int index = 0;
        while (iterator.hasNext()) {
            assertEquals(expectedKeys[index++], iterator.nextInt());
        }
        assertEquals(3, index);
    }

    /**
     * Test the keySet removal functionality.
     */
    @Test
    public void testKeySetRemove() {
        IntSet keySet = map.keySet();
        assertTrue(keySet.remove(2));
        assertEquals(2, keySet.size());
        assertFalse(keySet.contains(2));
        assertNull(map.get(2));

        // Attempt to remove a non-existing key
        assertFalse(keySet.remove(4));
    }

    /**
     * Test the entrySet for correct entries and size.
     */
    @Test
    public void testEntrySetContainsEntries() {
        Int2ObjectMap.FastEntrySet<TestMemoryCountable> entrySet = map.int2ObjectEntrySet();
        assertEquals(3, entrySet.size());

        Map.Entry<Integer, TestMemoryCountable> entry1 =
            new AbstractInt2ObjectMap.BasicEntry<>(1, new TestMemoryCountable(100));
        Map.Entry<Integer, TestMemoryCountable> entry2 =
            new AbstractInt2ObjectMap.BasicEntry<>(2, new TestMemoryCountable(200));
        Map.Entry<Integer, TestMemoryCountable> entry3 =
            new AbstractInt2ObjectMap.BasicEntry<>(3, new TestMemoryCountable(300));

        assertTrue(entrySet.contains(entry1));
        assertTrue(entrySet.contains(entry2));
        assertTrue(entrySet.contains(entry3));

        Map.Entry<Integer, TestMemoryCountable> nonExistingEntry =
            new AbstractInt2ObjectMap.BasicEntry<>(4, new TestMemoryCountable(400));
        assertFalse(entrySet.contains(nonExistingEntry));
    }

    /**
     * Test the entrySet iterator for correct iteration.
     */
    @Test
    public void testEntrySetIterator() {
        Int2ObjectMap.FastEntrySet<TestMemoryCountable> entrySet = map.int2ObjectEntrySet();
        ObjectIterator<Int2ObjectMap.Entry<TestMemoryCountable>> iterator = entrySet.iterator();

        int[] expectedKeys = {1, 2, 3};
        long[] expectedValues = {100, 200, 300};
        int index = 0;
        while (iterator.hasNext()) {
            Int2ObjectMap.Entry<TestMemoryCountable> entry = iterator.next();
            assertEquals(expectedKeys[index], entry.getIntKey());
            assertEquals(expectedValues[index], entry.getValue().getMemoryUsage());
            index++;
        }
        assertEquals(3, index);
    }

    /**
     * Test the entrySet removal functionality.
     */
    @Test
    public void testEntrySetRemove() {
        Int2ObjectMap.FastEntrySet<TestMemoryCountable> entrySet = map.int2ObjectEntrySet();
        Map.Entry<Integer, TestMemoryCountable> entryToRemove =
            new AbstractInt2ObjectMap.BasicEntry<>(2, new TestMemoryCountable(200));

        assertTrue(entrySet.remove(entryToRemove));
        assertEquals(2, entrySet.size());
        assertFalse(map.containsKey(2));

        // Attempt to remove a non-existing entry
        Map.Entry<Integer, TestMemoryCountable> nonExistingEntry =
            new AbstractInt2ObjectMap.BasicEntry<>(4, new TestMemoryCountable(400));
        assertFalse(entrySet.remove(nonExistingEntry));
    }

    /**
     * Test the keySet clear functionality.
     */
    @Test
    public void testKeySetClear() {
        IntSet keySet = map.keySet();
        keySet.clear();
        assertTrue(map.isEmpty());
        assertEquals(0, keySet.size());
    }

    /**
     * Test the entrySet clear functionality.
     */
    @Test
    public void testEntrySetClear() {
        Int2ObjectMap.FastEntrySet<TestMemoryCountable> entrySet = map.int2ObjectEntrySet();
        entrySet.clear();
        assertTrue(map.isEmpty());
        assertEquals(0, entrySet.size());
    }

    /**
     * Test for correct behavior when iterating beyond the last element in keySet.
     */
    @Test(expected = NoSuchElementException.class)
    public void testKeySetIteratorNoSuchElement() {
        IntSet keySet = map.keySet();
        IntIterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
            iterator.nextInt();
        }
        // This should throw NoSuchElementException
        iterator.nextInt();
    }

    /**
     * Test for correct behavior when iterating beyond the last element in entrySet.
     */
    @Test(expected = NoSuchElementException.class)
    public void testEntrySetIteratorNoSuchElement() {
        Int2ObjectMap.FastEntrySet<TestMemoryCountable> entrySet = map.int2ObjectEntrySet();
        ObjectIterator<Int2ObjectMap.Entry<TestMemoryCountable>> iterator = entrySet.iterator();
        while (iterator.hasNext()) {
            iterator.next();
        }
        // This should throw NoSuchElementException
        iterator.next();
    }

    /**
     * Test the size of keySet and entrySet after multiple operations.
     */
    @Test
    public void testSizeAfterOperations() {
        IntSet keySet = map.keySet();
        Int2ObjectMap.FastEntrySet<TestMemoryCountable> entrySet = map.int2ObjectEntrySet();

        // Initial size
        assertEquals(3, keySet.size());
        assertEquals(3, entrySet.size());

        // Remove a key
        keySet.remove(1);
        assertEquals(2, keySet.size());
        assertEquals(2, entrySet.size());

        // Add a new key-value pair
        map.put(4, new TestMemoryCountable(400));
        assertEquals(3, keySet.size());
        assertEquals(3, entrySet.size());

        // Clear keySet
        keySet.clear();
        assertEquals(0, keySet.size());
        assertEquals(0, entrySet.size());
        assertTrue(map.isEmpty());
    }
}
