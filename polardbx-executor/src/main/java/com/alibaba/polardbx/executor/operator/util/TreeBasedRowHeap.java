package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.DefinedMemoryUsage;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.PriorityQueue;
import org.openjdk.jol.info.ClassLayout;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

@DefinedMemoryUsage
public class TreeBasedRowHeap<T> implements PriorityQueue<T>, MemoryCountable {
    protected static final int INSTANCE_SIZE = ClassLayout.parseClass(TreeBasedRowHeap.class).instanceSize();

    @FieldMemoryCounter(value = false)
    private TreeSet<T> treeSet;
    @FieldMemoryCounter(value = false)
    private Comparator<T> comparator;
    private int elementSize;
    private long accumulatedSize;

    public TreeBasedRowHeap(Comparator<T> comparator, int elementSize) {
        this.comparator = comparator;
        this.treeSet = new TreeSet<>(comparator);
        this.elementSize = elementSize;
        this.accumulatedSize = INSTANCE_SIZE;
    }

    @Override
    public long getMemoryUsage() {
        return accumulatedSize;
    }

    public long getEstimatedSizeInBytes() {
        return accumulatedSize;
    }

    public Iterator<T> descendingIterator() {
        return treeSet.descendingIterator();
    }

    /**
     * an iterator ranging from the x-th last element to the last element
     */
    public Iterator<T> lastDescendingIterator(int skip) {
        Preconditions.checkArgument(skip >= 0);

        Iterator<T> descendingIterator = treeSet.descendingIterator();
        for (int i = 0; i < skip; i++) {
            if (descendingIterator.hasNext()) {
                descendingIterator.next();
            }
        }
        return descendingIterator;
    }

    @Override
    public void enqueue(T x) {
        treeSet.add(x);
        accumulatedSize += elementSize;
    }

    @Override
    public T dequeue() {
        T result = treeSet.pollFirst();
        accumulatedSize -= elementSize;
        return result;
    }

    @Override
    public boolean isEmpty() {
        return treeSet.isEmpty();
    }

    @Override
    public T first() {
        if (treeSet.isEmpty()) {
            // to prevent from NoSuchElementException.
            return null;
        }
        return treeSet.first();
    }

    @Override
    public int size() {
        return treeSet.size();
    }

    @Override
    public void clear() {
        treeSet.clear();
    }

    @Override
    public T last() {
        return treeSet.last();
    }

    @Override
    public void changed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Comparator<T> comparator() {
        return comparator;
    }
}
