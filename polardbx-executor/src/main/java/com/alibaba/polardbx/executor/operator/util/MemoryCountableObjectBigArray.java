package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.function.Consumer;

import static io.airlift.slice.SizeOf.sizeOfObjectArray;

public class MemoryCountableObjectBigArray<T extends MemoryCountable> implements MemoryCountable {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MemoryCountableObjectBigArray.class).instanceSize();
    /**
     * Initial number of segments to support in array.
     */
    public static final int INITIAL_SEGMENTS = 1024;

    /**
     * The shift used to compute the segment associated with an index (equivalently, the logarithm of the segment size).
     */
    public static final int SEGMENT_SHIFT = 10;

    /**
     * Size of a single segment of a BigArray
     */
    public static final int SEGMENT_SIZE = 1 << SEGMENT_SHIFT;

    /**
     * The mask used to compute the offset associated to an index.
     */
    public static final int SEGMENT_MASK = SEGMENT_SIZE - 1;

    private static final long SIZE_OF_SEGMENT = sizeOfObjectArray(SEGMENT_SIZE);

    private MemoryCountable[][] array;
    private int capacity;
    private int segments;
    private long accumulatedSize;

    /**
     * Creates a new big array containing one initial segment
     */
    public MemoryCountableObjectBigArray() {
        array = new MemoryCountable[INITIAL_SEGMENTS][];
        accumulatedSize = 0;
        allocateNewSegment();
    }

    @Override
    public long getMemoryUsage() {
        long memoryUsage = INSTANCE_SIZE + accumulatedSize;
        memoryUsage += FastMemoryCounter.sizeOfObjectArray(array);
        for (MemoryCountable[] memoryCountableArray : array) {
            memoryUsage += FastMemoryCounter.sizeOfObjectArray(memoryCountableArray);
        }

        return memoryUsage;
    }

    /**
     * Returns the size of this big array in bytes.
     */
    public long sizeOf() {
        return SizeOf.sizeOf(array) + (segments * SIZE_OF_SEGMENT);
    }

    /**
     * Returns the element of this big array at specified index.
     *
     * @param index a position in this big array.
     * @return the element of this big array at the specified position.
     */
    @SuppressWarnings("unchecked")
    public T get(long index) {
        return (T) array[segment(index)][offset(index)];
    }

    /**
     * Sets the element of this big array at specified index.
     *
     * @param index a position in this big array.
     */
    public void set(long index, T value) {
        accumulatedSize -= FastMemoryCounter.sizeOf(array[segment(index)][offset(index)]);
        accumulatedSize += FastMemoryCounter.sizeOf(value);
        array[segment(index)][offset(index)] = value;
    }

    public void resize(long index, Consumer<T> consumer) {
        MemoryCountable value = array[segment(index)][offset(index)];
        if (value == null) {
            return;
        }
        long oldMemoryUsage = FastMemoryCounter.sizeOf(value);
        consumer.accept((T) value);
        accumulatedSize -= oldMemoryUsage;
        accumulatedSize += FastMemoryCounter.sizeOf(value);
    }

    /**
     * Ensures this big array is at least the specified length.  If the array is smaller, segments
     * are added until the array is larger then the specified length.
     */
    public void ensureCapacity(long length) {
        if (capacity > length) {
            return;
        }

        grow(length);
    }

    private void grow(long length) {
        // how many segments are required to get to the length?
        int requiredSegments = segment(length) + 1;

        // grow base array if necessary
        if (array.length < requiredSegments) {
            array = Arrays.copyOf(array, requiredSegments);
        }

        // add new segments
        while (segments < requiredSegments) {
            allocateNewSegment();
        }
    }

    private void allocateNewSegment() {
        MemoryCountable[] newSegment = new MemoryCountable[SEGMENT_SIZE];
        array[segments] = newSegment;
        capacity += SEGMENT_SIZE;
        segments++;
    }

    public int segment(long index) {
        return (int) (index >>> SEGMENT_SHIFT);
    }

    public int offset(long index) {
        return (int) (index & SEGMENT_MASK);
    }
}
