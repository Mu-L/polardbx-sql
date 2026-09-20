package com.alibaba.polardbx.common.collection;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceList;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Dynamically update the memory usage value of elements when adding or
 * removing elements. This avoids the excessive time consumption of
 * a full global calculation when there are too many elements.
 */
public class MemoryCountableObjectArrayList<T extends MemoryCountable> extends ReferenceArrayList<T>
    implements MemoryCountable {
    private static final int INSTANCE_SIZE =
        ClassLayout.parseClass(MemoryCountableObjectArrayList.class).instanceSize();

    protected long memoryUsageOfElements = 0L;

    public MemoryCountableObjectArrayList() {
        a = (T[]) new MemoryCountable[0];
    }

    public MemoryCountableObjectArrayList(Collection<? extends T> c) {
        super(c);
        if (c instanceof MemoryCountableObjectArrayList) {
            this.memoryUsageOfElements = ((MemoryCountableObjectArrayList) c).memoryUsageOfElements;
        }
    }

    public MemoryCountableObjectArrayList(int capacity) {
        super(capacity);
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a)) + memoryUsageOfElements;
    }

    @Override
    public int indexOf(final Object k) {
        final Object[] array = a;
        for (int i = 0; i < size; i++) {
            if (Objects.equals(k, array[i])) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int lastIndexOf(final Object k) {
        final Object[] array = a;
        for (int i = size; i-- != 0; ) {
            if (Objects.equals(k, array[i])) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public boolean add(T t) {
        boolean ret = super.add(t);
        memoryUsageOfElements += FastMemoryCounter.sizeOf(t);
        return ret;
    }

    @Override
    public T set(int index, T t) {
        T old = super.set(index, t);
        memoryUsageOfElements -= FastMemoryCounter.sizeOf(old);
        memoryUsageOfElements += FastMemoryCounter.sizeOf(t);
        return old;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return super.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        boolean ret = super.addAll(index, c);

        if (c instanceof MemoryCountableObjectArrayList) {
            memoryUsageOfElements += ((MemoryCountableObjectArrayList) c).memoryUsageOfElements;
        } else {
            for (T t : c) {
                memoryUsageOfElements += FastMemoryCounter.sizeOf(t);
            }
        }

        return ret;
    }

    @Override
    public void removeElements(final int from, final int to) {
        // remove elements of [from, to).
        for (int i = from; i < to; i++) {
            T memoryCountable = get(i);
            memoryUsageOfElements -= FastMemoryCounter.sizeOf(memoryCountable);
        }

        super.removeElements(from, to);
    }

    @Override
    public boolean addAll(ReferenceList<? extends T> l) {
        return super.addAll(l);
    }

    @Override
    public boolean addAll(int index, ReferenceList<? extends T> l) {
        boolean ret = super.addAll(index, l);
        if (l instanceof MemoryCountableObjectArrayList) {
            memoryUsageOfElements += ((MemoryCountableObjectArrayList) l).memoryUsageOfElements;
        } else {
            for (T t : l) {
                memoryUsageOfElements += FastMemoryCounter.sizeOf(t);
            }
        }
        return ret;
    }

    @Override
    public void clear() {
        super.clear();
        memoryUsageOfElements = 0L;
    }

    @Override
    public T remove(int index) {
        T old = super.remove(index);
        memoryUsageOfElements -= FastMemoryCounter.sizeOf(old);
        return old;
    }

    // forbidden functions.
    @Override
    public boolean remove(Object k) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void trim() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void trim(int n) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addElements(int index, T[] a, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setElements(int index, T[] a, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addElements(int index, T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void push(T o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T pop() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setElements(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setElements(int index, T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(UnaryOperator<T> operator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        throw new UnsupportedOperationException();
    }
}
