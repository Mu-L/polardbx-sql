package com.alibaba.polardbx.common.memory;

import io.airlift.slice.SizeOf;
import org.junit.Before;
import org.junit.Test;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MemoryTrackerSingleOperatorTest {
    long globalQueryQuota = 1024L * 1024 * 1024 * 1024;

    // task info
    String queryId = Long.toHexString(1234567890L) + "-100";
    int stageId = 2;
    int pipelineId = 0;
    int driverId = 3;
    int operatorId = 13;

    final int allocateTimes = 256;
    final int taskNum = 2;
    final int maxMemorySize = 1024 * 1024 * 128;
    final int minMemorySize = 1024 * 1024 * 2;
    List<List<Integer>> allocateList = new ArrayList<>();

    List<Integer> finalAllocatedSize = new ArrayList<>();
    private OperatorMemoryOwnerId operatorMemoryOwnerId;

    private CountDownLatch latch = new CountDownLatch(taskNum);

    @Before
    public void setup() {

        // resize global query memory usage
        MemoryTrackerManager.getGlobalMemoryTrackerManager().resize(globalQueryQuota);

        operatorMemoryOwnerId = MemoryTrackerManager.getGlobalMemoryTrackerManager()
            .createQueryMemoryOwnerId(queryId)
            .createChild(stageId, pipelineId)
            .createChild(driverId)
            .createChild(operatorId, "");

        Random random = new Random();
        for (int i = 0; i < taskNum; i++) {
            List<Integer> allocateForEachTask = new ArrayList<>();

            for (int j = 0; j < allocateTimes; j++) {
                allocateForEachTask.add(random.nextInt(maxMemorySize - minMemorySize) + minMemorySize);
            }

            allocateList.add(allocateForEachTask);
            finalAllocatedSize.add(allocateForEachTask.get(allocateForEachTask.size() - 1));
        }
    }

    @Test
    public void testSameOperator() throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(taskNum);

        for (int taskId = 0; taskId < taskNum; taskId++) {
            AllocateTask allocateTask = new AllocateTask(taskId);
            executorService.submit(allocateTask);
        }

        latch.await();

        System.out.println("memory watermark: " + MemoryTrackerManager.memoryWatermark(operatorMemoryOwnerId));
        System.out.println("memory quota: " + MemoryTrackerManager.memoryQuota(operatorMemoryOwnerId));
    }

    private class AllocateTask implements Runnable {
        private int taskId;
        AllocateTask(int taskId) {
            this.taskId = taskId;
        }

        @Override
        public void run() {
            try {
                MemoryCountableItem memoryCountableItem = new  MemoryCountableItem(operatorMemoryOwnerId);

                List<Integer> list = allocateList.get(taskId);
                for (int i = 0; i < list.size(); i++) {
                    memoryCountableItem.resize(list.get(i));

                    if (i % 16 == 0) {
//                        System.out.println("task = " + taskId + ", "
//                            + "memory watermark: " + MemoryTrackerManager.memoryWatermark(operatorMemoryOwnerId)
//                            + ", memory quota: " + MemoryTrackerManager.memoryQuota(operatorMemoryOwnerId));

                        System.out.println(MemoryTrackerManager.getGlobalMemoryTrackerManager().dump());
                    }
                }

                // release all.
                memoryCountableItem.resize(0);

                latch.countDown();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private static class MemoryCountableItem implements MemoryCountable {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(MemoryCountableItem.class).instanceSize();
        // MOCK: byte[] memoryRegion
        private int memoryRegionSize;

        @FieldMemoryCounter(value = false)
        private OperatorMemoryOwnerId operatorMemoryOwnerId;

        public MemoryCountableItem(OperatorMemoryOwnerId operatorMemoryOwnerId) {
            this.operatorMemoryOwnerId = operatorMemoryOwnerId;
        }

        public int resize(int size) {
            int oldSize = VMSupport.align((int) SizeOf.sizeOfByteArray(memoryRegionSize));
            int newSize = VMSupport.align((int) SizeOf.sizeOfByteArray(size));

            // try allocate
            MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId, newSize);

            // MOCK: memoryRegion = new byte[size];

            // release memory count
            MemoryTrackerManager.releaseReference(operatorMemoryOwnerId, oldSize);
            memoryRegionSize = size;
            return oldSize;
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOfByteArray(memoryRegionSize));
        }
    }
}
