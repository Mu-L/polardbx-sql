package com.alibaba.polardbx.executor.operator.util;

import org.junit.Test;

import java.util.Comparator;
import java.util.Iterator;

public class TreeBasedRowHeapTest {

    private static class HeapElement {
        final long value;

        HeapElement(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "HeapElement{" +
                "value=" + value +
                '}';
        }
    }

    @Test
    public void test() {
        Comparator<HeapElement> heapComparator = (l1, l2) -> {
            if (l1 == l2) {
                return 0;
            }
            int cmp = Long.compare(l1.value, l2.value);
            // never equal
            return cmp == 0 ? -1 : (cmp < 0 ? 1 : -1);
        };

        RedBlackTreeBasedRowHeap<HeapElement> heap = new RedBlackTreeBasedRowHeap<>(heapComparator, Long.BYTES);

        heap.enqueue(new HeapElement(4L));
        heap.enqueue(new HeapElement(0L));
        heap.enqueue(new HeapElement(2L));
        heap.enqueue(new HeapElement(3L));
        heap.enqueue(new HeapElement(8L));
        heap.enqueue(new HeapElement(1L));

        // remove 4L
        // heap.dequeue();

        heap.enqueue(new HeapElement(5L));
        heap.enqueue(new HeapElement(7L));
        heap.enqueue(new HeapElement(6L));

        HeapElement[] expected = new HeapElement[] {
            new HeapElement(1),
            new HeapElement(1),
            new HeapElement(2),
            new HeapElement(3),
            new HeapElement(3),
            new HeapElement(4),
            new HeapElement(5),
            new HeapElement(5),
            new HeapElement(6)
        };
        int expectedIndex = 0;

        Iterator<HeapElement> descendingIterator = heap.descendingIterator();
        while (descendingIterator.hasNext()) {
            HeapElement next = descendingIterator.next();
            System.out.println(next);

            // Assert.assertTrue(next.value == expected[expectedIndex++].value);
        }
    }
}
