package com.alibaba.polardbx.executor.operator.util;

import it.unimi.dsi.fastutil.BidirectionalIterator;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectRBTreeSet;
import org.openjdk.jol.info.ClassLayout;

import java.util.Comparator;
import java.util.Iterator;

public class RedBlackTreeBasedRowHeap<T> implements PriorityQueue<T> {
    protected static final long INSTANCE_SIZE = ClassLayout.parseClass(TreeBasedRowHeap.class).instanceSize();
    protected static final long TREE_SET_SIZE = ClassLayout.parseClass(ObjectRBTreeSet.class).instanceSize();

    private ObjectRBTreeSet<T> treeSet;
    private Comparator<T> comparator;
    private int elementSize;

    public RedBlackTreeBasedRowHeap(Comparator<T> comparator, int elementSize) {
        this.comparator = comparator;
        this.treeSet = new ObjectRBTreeSet(comparator);
        this.elementSize = elementSize;
    }

    public long getEstimatedSizeInBytes() {
        return INSTANCE_SIZE + TREE_SET_SIZE + size() * elementSize;
    }

    private static class DescendingIterator<T> implements Iterator<T> {
        private final BidirectionalIterator<T> bidirectionalIterator;
        private final T lastValue;
        private boolean lastValueIsPeeked;

        public DescendingIterator(BidirectionalIterator<T> bidirectionalIterator, T lastValue) {
            this.bidirectionalIterator = bidirectionalIterator;
            this.lastValue = lastValue;
            this.lastValueIsPeeked = false;
        }

        @Override
        public boolean hasNext() {
            return bidirectionalIterator.hasPrevious();
        }

        @Override
        public T next() {
            return bidirectionalIterator.previous();
        }
    }

    public Iterator<T> descendingIterator() {
        BidirectionalIterator<T> bidirectionalIterator = treeSet.iterator(treeSet.last());
        T last = treeSet.last();
        DescendingIterator<T> descendingIterator = new DescendingIterator<>(bidirectionalIterator, last);
        return descendingIterator;
    }

    @Override
    public void enqueue(T x) {
        treeSet.add(x);
    }

    @Override
    public T dequeue() {
        T first = treeSet.first();
        treeSet.remove(first);
        return first;
    }

    @Override
    public boolean isEmpty() {
        return treeSet.isEmpty();
    }

    @Override
    public T first() {
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

