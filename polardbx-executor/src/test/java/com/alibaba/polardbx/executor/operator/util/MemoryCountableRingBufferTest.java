package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import org.junit.Test;
import org.openjdk.jol.info.ClassLayout;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MemoryCountableRingBufferTest {

    private static class MemoryCountableInteger implements MemoryCountable {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(MemoryCountableInteger.class).instanceSize();
        final int value;

        private MemoryCountableInteger(int value) {
            this.value = value;
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE;
        }
    }

    @Test(timeout = 60000) // Timeout after 60 seconds to prevent hanging tests
    public void testConcurrentSafety() throws InterruptedException {
        final int capacity = 1024;               // Capacity of the ring buffer (must be a power of 2)
        final int numProducers = 4;              // Number of producer threads
        final int numConsumers = 4;              // Number of consumer threads
        final int itemsPerProducer = 100_000;    // Number of items each producer will insert

        final MemoryCountableRingBuffer<MemoryCountableInteger> ringBuffer = new MemoryCountableRingBuffer<>(capacity);
        final AtomicInteger idGenerator = new AtomicInteger(0); // Generates unique item IDs

        // Thread-safe set to store consumed items
        final Set<Integer> consumedItems = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

        // ExecutorService to manage producer and consumer threads
        final ExecutorService executor = Executors.newFixedThreadPool(numProducers + numConsumers);

        // Latches to synchronize the completion of producers and consumers
        final CountDownLatch producersLatch = new CountDownLatch(numProducers);
        final CountDownLatch consumersLatch = new CountDownLatch(numConsumers);

        // Start producer threads
        for (int i = 0; i < numProducers; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < itemsPerProducer; j++) {
                        MemoryCountableInteger id = new MemoryCountableInteger(idGenerator.getAndIncrement());
                        // Attempt to offer the item to the ring buffer, retrying if the buffer is full
                        while (!ringBuffer.offer(id)) {
                            Thread.yield(); // Yield to allow consumers to consume items
                        }
                    }
                } finally {
                    producersLatch.countDown(); // Signal that this producer has finished
                }
            });
        }

        // Start consumer threads
        for (int i = 0; i < numConsumers; i++) {
            executor.submit(() -> {
                try {
                    while (true) {
                        MemoryCountableInteger item = ringBuffer.poll();
                        if (item != null) {
                            consumedItems.add(item.value); // Collect the consumed item
                        } else {
                            // If all producers have finished and the buffer is empty, exit
                            if (producersLatch.getCount() == 0 && ringBuffer.isEmpty()) {
                                break;
                            }
                            Thread.yield(); // Yield to allow producers to insert items
                        }
                    }
                } finally {
                    consumersLatch.countDown(); // Signal that this consumer has finished
                }
            });
        }

        // Wait for all producers to finish
        boolean producersFinished = producersLatch.await(60, TimeUnit.SECONDS);
        assertTrue("Producers did not finish in time", producersFinished);

        // Wait for all consumers to finish
        boolean consumersFinished = consumersLatch.await(60, TimeUnit.SECONDS);
        assertTrue("Consumers did not finish in time", consumersFinished);

        // Shutdown the executor service
        executor.shutdownNow();

        // Calculate the expected number of consumed items
        int expectedCount = numProducers * itemsPerProducer;

        // Verify that the number of consumed items matches the expected count
        assertEquals("Number of consumed items does not match", expectedCount, consumedItems.size());

        // Optionally, verify that all expected items are present
        for (int i = 0; i < expectedCount; i++) {
            assertTrue("Missing item: " + i, consumedItems.contains(i));
        }
    }
}