package com.alibaba.polardbx.common.memory;

import org.junit.Before;
import org.junit.Test;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;
import org.openjdk.jol.util.VMSupport;

import java.util.Map;

import static com.alibaba.polardbx.common.memory.FastMemoryCounter.DEFAULT_MAX_DEPTH;
import static org.junit.Assert.*;

/**
 * Unit tests for the AnnotationBasedMemoryCounter class.
 */
public class AnnotationBasedMemoryCounterTest {

    private AnnotationBasedMemoryCounter memoryCounter;

    @Before
    public void setUp() {
        // Initialize the AnnotationBasedMemoryCounter with default parameters
        memoryCounter = new AnnotationBasedMemoryCounter(DEFAULT_MAX_DEPTH,
            true, true, true, null);
    }

    /**
     * Test memory usage of a simple object without any annotations.
     */
    @Test
    public void testSimpleObject() {
        SimpleObject obj = new SimpleObject();
        MemoryUsageReport report = memoryCounter.getMemoryUsage(obj);

        // Expected total size is instance size plus the size of obj's fields
        long expectedSize = GraphLayout.parseInstance(obj).totalSize();
        assertEquals("Total memory usage should match expected size", expectedSize, report.getTotalSize());
    }

    /**
     * Test memory usage of an object with @FieldMemoryCounter annotation.
     */
    @Test
    public void testFieldMemoryCounter() {
        AnnotatedObject obj = new AnnotatedObject();
        MemoryUsageReport report = memoryCounter.getMemoryUsage(obj);

        // Expected total size is instance size plus the size of annotated fields
        long expectedSize = VMSupport.sizeOf(obj) + GraphLayout.parseInstance(obj.field2).totalSize();
        assertEquals("Total memory usage should match expected size", expectedSize, report.getTotalSize());

        Map<String, Integer> fieldSizeMap = report.getFieldSizeMap();
        assertEquals("Field size map should contain one entry", 3, fieldSizeMap.size());
        assertTrue("Field size map should not contain 'field1'", !fieldSizeMap.containsKey("field1"));

        assertFalse("Memory usage tree should not be empty", report.getMemoryUsageTree().isEmpty());
    }

    /**
     * Test memory usage with shallow heap (@ShallowHeap annotation).
     */
    @Test
    public void testShallowHeap() {
        ShallowHeapObject obj = new ShallowHeapObject();
        MemoryUsageReport report = memoryCounter.getMemoryUsage(obj);

        // Expected total size is instance size plus the size of shallow fields
        long expectedSize =
            VMSupport.sizeOf(obj) + VMSupport.sizeOf(obj.field1) + GraphLayout.parseInstance(obj.field2).totalSize();
        assertEquals("Total memory usage should match expected size", expectedSize, report.getTotalSize());
    }

    /**
     * Test memory usage with conditional memory counter (@ConditionalMemoryCounter annotation).
     */
    @Test
    public void testConditionalMemoryCounterConditionMet() {
        memoryCounter = new AnnotationBasedMemoryCounter(DEFAULT_MAX_DEPTH,
            true, true, true, ConditionalMemoryType.PRODUCER);

        ConditionalObject obj = new ConditionalObject();

        MemoryUsageReport report = memoryCounter.getMemoryUsage(obj);

        // Since the object has PRODUCER, memory should be counted
        long expectedSize = VMSupport.sizeOf(obj) + GraphLayout.parseInstance(obj.field2).totalSize();
        assertEquals("Total memory usage should match expected size", expectedSize, report.getTotalSize());
    }

    /**
     * Test memory usage with conditional memory counter where condition is not met.
     */
    @Test
    public void testConditionalMemoryCounterConditionNotMet() {
        memoryCounter = new AnnotationBasedMemoryCounter(DEFAULT_MAX_DEPTH,
            true, true, true, ConditionalMemoryType.CONSUMER);

        ConditionalObject obj = new ConditionalObject();

        MemoryUsageReport report = memoryCounter.getMemoryUsage(obj);

        // Since the object has PRODUCER, memory should be counted
        long expectedSize = VMSupport.sizeOf(obj) + GraphLayout.parseInstance(obj.field1).totalSize();
        assertEquals("Total memory usage should match expected size", expectedSize, report.getTotalSize());
    }

    /**
     * Test memory usage with nested objects and maximum depth.
     */
    @Test
    public void testNestedObjectsWithMaxDepth() {
        // Create a nested object structure
        NestedObject obj = new NestedObject();
        obj.child = new NestedObject();
        obj.child.child = new NestedObject(); // Depth 2
        obj.child.child.child = new NestedObject(); // Depth 3

        // Initialize memory counter with maxDepth = 2
        AnnotationBasedMemoryCounter limitedDepthCounter =
            new AnnotationBasedMemoryCounter(2, true, false, false, null);
        MemoryUsageReport report = limitedDepthCounter.getMemoryUsage(obj);

        // Expected total size includes the root and two levels of children
        long expectedSize =
            VMSupport.sizeOf(obj)
                + VMSupport.sizeOf(obj.child)
                + VMSupport.sizeOf(obj.child.child);
        // The object at depth 3 should not be counted
        assertEquals("Total memory usage should count up to max depth", expectedSize, report.getTotalSize());

        assertTrue("Memory usage tree should be empty for nested objects without annotations",
            report.getMemoryUsageTree().isEmpty());
    }

    /**
     * Test memory usage with cyclic references.
     */
    @Test
    public void testCyclicReferences() {
        CyclicObject obj1 = new CyclicObject();
        CyclicObject obj2 = new CyclicObject();
        obj1.partner = obj2;
        obj2.partner = obj1; // Creates a cycle

        MemoryUsageReport report = memoryCounter.getMemoryUsage(obj1);

        // Expected total size includes both objects only once
        long expectedSize = GraphLayout.parseInstance(obj1).totalSize();
        assertEquals("Total memory usage should account for cyclic references without infinite loop", expectedSize,
            report.getTotalSize());
    }

    /**
     * Test memory usage with defined memory usage via @DefinedMemoryUsage annotation.
     */
    @Test
    public void testDefinedMemoryUsageAnnotation() {
        DefinedMemoryUsageObject obj = new DefinedMemoryUsageObject();
        MemoryUsageReport report = memoryCounter.getMemoryUsage(obj);

        // Expected total size is instance size + fields size + defined memory usage from DefinedMemoryUsageProvider
        long expectedSize = obj.getMemoryUsage();
        assertEquals("Total memory usage should include defined memory usage", expectedSize, report.getTotalSize());
    }

    /**
     * A simple object without any annotations.
     */
    private static class SimpleObject {
        int a = 10;
        String b = "test";
    }

    /**
     * An object with @FieldMemoryCounter annotation on one of its fields.
     */
    private static class AnnotatedObject {
        @FieldMemoryCounter(value = false)
        String field1 = "visibleField";

        String field2 = "invisibleField";
    }

    /**
     * An object with @ShallowHeap annotation on one of its fields.
     */
    private static class ShallowHeapObject {
        @ShallowHeap
        String[] field1 = {"shallowField", "test"};

        String field2 = "deepField";
    }

    /**
     * An object with @ConditionalMemoryCounter annotation.
     */
    private static class ConditionalObject {
        @ConditionalMemoryCounter(type = ConditionalMemoryType.CONSUMER)
        String field1 = "conditionalField_CONSUMER";

        @ConditionalMemoryCounter(type = ConditionalMemoryType.PRODUCER)
        String field2 = "conditionalField_PRODUCER";
    }

    /**
     * An object with nested references.
     */
    private static class NestedObject {
        NestedObject child;
    }

    /**
     * An object with cyclic references.
     */
    private static class CyclicObject {
        CyclicObject partner;
    }

    /**
     * An object with @DefinedMemoryUsage annotation.
     */
    @DefinedMemoryUsage
    private static class DefinedMemoryUsageObject implements MemoryCountable {
        String field1 = "definedField";

        /**
         * Provides the defined memory usage.
         *
         * @return memory usage in bytes
         */
        @Override
        public long getMemoryUsage() {
            return 50L; // Example memory usage
        }
    }
}
