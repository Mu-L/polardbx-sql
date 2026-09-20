package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class MemoryCountableAtomicReferenceArray<E extends MemoryCountable>
    implements java.io.Serializable, MemoryCountable {
    private static final long serialVersionUID = -6209656149925076980L;

    private static final int INSTANCE_SIZE =
        ClassLayout.parseClass(MemoryCountableAtomicReferenceArray.class).instanceSize();

    private static final Unsafe unsafe;
    private static final int base;
    private static final int shift;
    private static final long arrayFieldOffset;
    private final Object[] array; // must have exact type Object[]

    private AtomicLong elementMemoryUsage = new AtomicLong(0L);

    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }

            arrayFieldOffset = unsafe.objectFieldOffset
                (AtomicReferenceArray.class.getDeclaredField("array"));
            base = unsafe.arrayBaseOffset(Object[].class);
            int scale = unsafe.arrayIndexScale(Object[].class);
            if ((scale & (scale - 1)) != 0) {
                throw new Error("data type scale not a power of two");
            }
            shift = 31 - Integer.numberOfLeadingZeros(scale);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private long checkedByteOffset(int i) {
        if (i < 0 || i >= array.length) {
            throw new IndexOutOfBoundsException("index " + i);
        }

        return byteOffset(i);
    }

    private static long byteOffset(int i) {
        return ((long) i << shift) + base;
    }

    /**
     * Creates a new AtomicReferenceArray of the given length, with all
     * elements initially null.
     *
     * @param length the length of the array
     */
    public MemoryCountableAtomicReferenceArray(int length) {
        array = new Object[length];
    }

    /**
     * Creates a new AtomicReferenceArray with the same length as, and
     * all elements copied from, the given array.
     *
     * @param array the array to copy elements from
     * @throws NullPointerException if array is null
     */
    public MemoryCountableAtomicReferenceArray(E[] array) {
        // Visibility guaranteed by final field guarantees
        this.array = Arrays.copyOf(array, array.length, Object[].class);
    }

    /**
     * Returns the length of the array.
     *
     * @return the length of the array
     */
    public final int length() {
        return array.length;
    }

    /**
     * Gets the current value at position {@code i}.
     *
     * @param i the index
     * @return the current value
     */
    public final E get(int i) {
        return getRaw(checkedByteOffset(i));
    }

    @SuppressWarnings("unchecked")
    private E getRaw(long offset) {
        return (E) unsafe.getObjectVolatile(array, offset);
    }

    /**
     * Sets the element at position {@code i} to the given value.
     *
     * @param i the index
     * @param newValue the new value
     */
    public final void set(int i, E newValue) {
        E oldValue = (E) unsafe.getAndSetObject(array, checkedByteOffset(i), newValue);
        elementMemoryUsage.getAndAdd(-FastMemoryCounter.sizeOf(oldValue));
        elementMemoryUsage.getAndAdd(FastMemoryCounter.sizeOf(newValue));
    }

    /**
     * Atomically sets the element at position {@code i} to the given
     * value and returns the old value.
     *
     * @param i the index
     * @param newValue the new value
     * @return the previous value
     */
    @SuppressWarnings("unchecked")
    public final E getAndSet(int i, E newValue) {
        E oldValue = (E) unsafe.getAndSetObject(array, checkedByteOffset(i), newValue);
        elementMemoryUsage.getAndAdd(-FastMemoryCounter.sizeOf(oldValue));
        elementMemoryUsage.getAndAdd(FastMemoryCounter.sizeOf(newValue));
        return oldValue;
    }

    /**
     * Returns the String representation of the current values of array.
     *
     * @return the String representation of the current values of array
     */
    public String toString() {
        int iMax = array.length - 1;
        if (iMax == -1) {
            return "[]";
        }

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; ; i++) {
            b.append(getRaw(byteOffset(i)));
            if (i == iMax) {
                return b.append(']').toString();
            }
            b.append(',').append(' ');
        }
    }

    public void updateElementMemoryUsage(long delta) {
        elementMemoryUsage.addAndGet(delta);
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + VMSupport.align((int) SizeOf.sizeOf(array))
            + FastMemoryCounter.sizeOf(elementMemoryUsage)
            + elementMemoryUsage.get();
    }
}

