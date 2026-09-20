package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import org.openjdk.jol.info.ClassLayout;

import java.util.concurrent.atomic.AtomicLong;

public class MemoryCountableRingBuffer<T extends MemoryCountable> implements MemoryCountable {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MemoryCountableRingBuffer.class).instanceSize();

    // Inner cell class
    private static class Cell<T extends MemoryCountable> implements MemoryCountable {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(Cell.class).instanceSize();
        final AtomicLong sequence = new AtomicLong(0);
        volatile T value;

        Cell(long seq) {
            sequence.set(seq);
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(sequence)
                + FastMemoryCounter.sizeOf(value);
        }
    }

    private final int capacity;
    private final int mask;
    private final MemoryCountableAtomicReferenceArray<Cell<T>> buffer;
    private final AtomicLong tail = new AtomicLong(0);
    private final AtomicLong head = new AtomicLong(0);

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(buffer)
            + FastMemoryCounter.sizeOf(tail)
            + FastMemoryCounter.sizeOf(head);
    }

    public MemoryCountableRingBuffer(int capacity) {
        if (Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("Capacity must be a power of 2");
        }
        this.capacity = capacity;
        this.mask = capacity - 1;
        buffer = new MemoryCountableAtomicReferenceArray<>(capacity);
        for (int i = 0; i < capacity; i++) {
            buffer.set(i, new Cell<>(i));
        }
    }

    // Method called by producer
    public boolean offer(T item) {
        long currentTail;
        Cell<T> cell;
        while (true) {
            currentTail = tail.get();
            cell = buffer.get((int) (currentTail & mask));
            long seq = cell.sequence.get();

            long dif = seq - currentTail;
            if (dif == 0) {
                if (tail.compareAndSet(currentTail, currentTail + 1)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Buffer is full
            }

            // In other cases, retry
        }

        // should update atomic array size.
        long oldCellSize = FastMemoryCounter.sizeOf(cell);

        cell.value = item;
        cell.sequence.set(currentTail + 1);

        long newCellSize = FastMemoryCounter.sizeOf(cell);
        buffer.updateElementMemoryUsage(newCellSize - oldCellSize);

        return true;
    }

    // Method called by consumer
    public T poll() {
        long currentHead;
        Cell<T> cell;
        while (true) {
            currentHead = head.get();
            cell = buffer.get((int) (currentHead & mask));
            long seq = cell.sequence.get();
            long dif = seq - (currentHead + 1);

            if (dif == 0) {
                if (head.compareAndSet(currentHead, currentHead + 1)) {
                    break;
                }
            } else if (dif < 0) {
                return null; // Buffer is empty
            }

            // In other cases, retry
        }

        T value = cell.value;

        long oldCellSize = FastMemoryCounter.sizeOf(cell);

        cell.value = null;
        cell.sequence.set(currentHead + capacity);

        long newCellSize = FastMemoryCounter.sizeOf(cell);
        buffer.updateElementMemoryUsage(newCellSize - oldCellSize);

        return value;
    }

    public boolean isEmpty() {
        return head.get() == tail.get();
    }

    public boolean isFull() {
        return (tail.get() - head.get()) == capacity;
    }
}


