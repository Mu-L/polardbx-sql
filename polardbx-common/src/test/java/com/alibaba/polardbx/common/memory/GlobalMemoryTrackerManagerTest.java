package com.alibaba.polardbx.common.memory;

import org.junit.Test;
import org.junit.Assert;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalMemoryTrackerManagerTest {

    @Test
    public void test() {
        final long totalQuota = 1L << 34; // 16GB

        GlobalMemoryTrackerManager globalMemoryTrackerManager = MemoryTrackerManager.getGlobalMemoryTrackerManager();

        globalMemoryTrackerManager.resize(totalQuota);

        OperatorMemoryOwnerId operatorMemoryOwnerId1 = globalMemoryTrackerManager
            .createQueryMemoryOwnerId("1985e6e8db400000")
            .createChild(1, 0)
            .createChild(1)
            .createChild(2, "HashAggExec");
        operatorMemoryOwnerId1.setOwner(new MemoryCountableItem());

        OperatorMemoryOwnerId operatorMemoryOwnerId2 = globalMemoryTrackerManager
            .createQueryMemoryOwnerId("1985e7e8db400000")
            .createChild(1, 0)
            .createChild(1)
            .createChild(2, "ProjectExec");
        operatorMemoryOwnerId2.setOwner(new MemoryCountableItem());

        OperatorMemoryOwnerId operatorMemoryOwnerId3 = globalMemoryTrackerManager
            .createQueryMemoryOwnerId("1985e7e8db400000")
            .createChild(2, 1)
            .createChild(1)
            .createChild(2, "FilterExec");
        operatorMemoryOwnerId3.setOwner(new MemoryCountableItem());

        OperatorMemoryOwnerId operatorMemoryOwnerId4 = globalMemoryTrackerManager
            .createQueryMemoryOwnerId("1985e7e8db400000")
            .createChild(1, 1)
            .createChild(1)
            .createChild(3, "ColumnarScanExec");
        operatorMemoryOwnerId4.setOwner(new MemoryCountableItem());

        MemoryTrackerManager.adjustMemoryUsage(operatorMemoryOwnerId1);
        MemoryTrackerManager.adjustMemoryUsage(operatorMemoryOwnerId2);
        MemoryTrackerManager.adjustMemoryUsage(operatorMemoryOwnerId3);
        MemoryTrackerManager.adjustMemoryUsage(operatorMemoryOwnerId4);

        globalMemoryTrackerManager.getMaximumQueryMemoryAllocated("1985e6e8db400000");
        globalMemoryTrackerManager.getMaximumQueryMemoryAllocated("1985e7e8db400000");
        globalMemoryTrackerManager.getMaximumQueryMemoryAllocated("1985e7e8db400000");
        globalMemoryTrackerManager.getMaximumQueryMemoryAllocated("1985e7e8db400000");

        MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId1, 1 + 1 << 22);
        MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId2, 1 + 1 << 22);
        MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId3, 1 + 1 << 22);
        MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId4, 1 + 1 << 22);

        MemoryTrackerManager.tryReverseReference(operatorMemoryOwnerId1, 1 + 1 << 4);
        MemoryTrackerManager.tryReverseReference(operatorMemoryOwnerId2, 1 + 1 << 4);
        MemoryTrackerManager.tryReverseReference(operatorMemoryOwnerId3, 1 + 1 << 4);
        MemoryTrackerManager.tryReverseReference(operatorMemoryOwnerId4, 1 + 1 << 4);

        MemoryTrackerManager.releaseReference(operatorMemoryOwnerId1, 1 + 1 << 4);
        MemoryTrackerManager.releaseReference(operatorMemoryOwnerId2, 1 + 1 << 4);
        MemoryTrackerManager.releaseReference(operatorMemoryOwnerId3, 1 + 1 << 4);
        MemoryTrackerManager.releaseReference(operatorMemoryOwnerId4, 1 + 1 << 4);

        MemoryTrackerManager.memoryWatermark(operatorMemoryOwnerId1);
        MemoryTrackerManager.memoryWatermark(operatorMemoryOwnerId2);
        MemoryTrackerManager.memoryWatermark(operatorMemoryOwnerId3);
        MemoryTrackerManager.memoryWatermark(operatorMemoryOwnerId4);

        System.out.println(globalMemoryTrackerManager.dump());
        System.out.println(globalMemoryTrackerManager.dumpTableResult());
        System.out.println(globalMemoryTrackerManager.dumpTotalUsage());

        globalMemoryTrackerManager.releaseStageMemory("1985e6e8db400000", 1, false);
        globalMemoryTrackerManager.releaseStageMemory("1985e7e8db400000", 1, true);
        globalMemoryTrackerManager.releaseStageMemory("1985e7e8db400000", 2, false);
        globalMemoryTrackerManager.releaseStageMemory("1985e7e8db400000", 1, true);
    }

    /**
     * 测试doDestroy方法 - 验证destroy方法可以正常执行
     * 新的实现使用了 MemoryManagerState，不再直接暴露内部字段
     */
    @Test
    public void testDoDestroy() throws Exception {
        GlobalMemoryTrackerManager manager = new GlobalMemoryTrackerManager();

        // 初始化manager
        manager.init();

        // 添加一些数据
        QueryMemoryOwnerId queryOwnerId = manager.createQueryMemoryOwnerId("test-query");

        // 调用destroy方法，应该不会抛出异常
        manager.destroy();

        // 验证destroy后可以再次调用（幂等性）
        manager.destroy();
    }

    /**
     * Test dumpTableResult method with topN parameter
     * Test scenarios: empty result, normal cases, edge cases
     */
    @Test
    public void testDumpTableResultWithTopN() throws Exception {
        final long totalQuota = 1L << 34; // 16GB
        GlobalMemoryTrackerManager manager = MemoryTrackerManager.getGlobalMemoryTrackerManager();
        manager.resize(totalQuota);

        // Test case 1: topN <= 0 should return empty list
        Assert.assertTrue(manager.dumpTableResult(0).isEmpty());
        Assert.assertTrue(manager.dumpTableResult(-1).isEmpty());

        // Create test data with different memory allocations
        OperatorMemoryOwnerId operator1 = manager
            .createQueryMemoryOwnerId("query1")
            .createChild(1, 0)
            .createChild(1)
            .createChild(1, "HashAggExec");
        operator1.setOwner(new TestMemoryCountableItem(1L << 25)); // 32MB

        OperatorMemoryOwnerId operator2 = manager
            .createQueryMemoryOwnerId("query2")
            .createChild(1, 0)
            .createChild(1)
            .createChild(1, "ProjectExec");
        operator2.setOwner(new TestMemoryCountableItem(1L << 24)); // 16MB

        OperatorMemoryOwnerId operator3 = manager
            .createQueryMemoryOwnerId("query3")
            .createChild(1, 0)
            .createChild(1)
            .createChild(1, "FilterExec");
        operator3.setOwner(new TestMemoryCountableItem(1L << 26)); // 64MB

        OperatorMemoryOwnerId operator4 = manager
            .createQueryMemoryOwnerId("query4")
            .createChild(1, 0)
            .createChild(1)
            .createChild(1, "ScanExec");
        operator4.setOwner(new TestMemoryCountableItem(1L << 23)); // 8MB

        // Allocate memory to create different pipeline memory sizes
        MemoryTrackerManager.tryAllocate(operator1, 1L << 25); // 32MB
        MemoryTrackerManager.tryAllocate(operator2, 1L << 24); // 16MB
        MemoryTrackerManager.tryAllocate(operator3, 1L << 26); // 64MB
        MemoryTrackerManager.tryAllocate(operator4, 1L << 23); // 8MB

        // Test case 2: topN = 1, should return only the largest
        java.util.List<Object[]> result1 = manager.dumpTableResult(1);
        Assert.assertEquals(1, result1.size());
        // The largest should be operator3 with 64MB
        Object[] largest = result1.get(0);
        Assert.assertEquals("query3", largest[0]);

        // Test case 3: topN = 2, should return top 2 in descending order
        java.util.List<Object[]> result2 = manager.dumpTableResult(2);
        Assert.assertEquals(2, result2.size());
        // Should be ordered: operator3 (64MB), operator1 (32MB)
        Assert.assertEquals("query3", result2.get(0)[0]);
        Assert.assertEquals("query1", result2.get(1)[0]);

        // Test case 4: topN = 3, should return top 3 in descending order
        java.util.List<Object[]> result3 = manager.dumpTableResult(3);
        Assert.assertEquals(3, result3.size());
        // Should be ordered: operator3 (64MB), operator1 (32MB), operator2 (16MB)
        Assert.assertEquals("query3", result3.get(0)[0]);
        Assert.assertEquals("query1", result3.get(1)[0]);
        Assert.assertEquals("query2", result3.get(2)[0]);

        // Test case 5: topN larger than available data
        java.util.List<Object[]> result5 = manager.dumpTableResult(10);

        // Clean up
        manager.releaseQueryMemory("query1");
        manager.releaseQueryMemory("query2");
        manager.releaseQueryMemory("query3");
        manager.releaseQueryMemory("query4");
    }

    /**
     * Test dumpTableResult with empty data
     */
    @Test
    public void testDumpTableResultWithEmptyData() {
        GlobalMemoryTrackerManager manager = MemoryTrackerManager.getGlobalMemoryTrackerManager();
        manager.resize(1L << 30); // 1GB

        // Test with empty data
        java.util.List<Object[]> result = manager.dumpTableResult(5);
    }

    /**
     * Test dumpTableResult with single record
     */
    @Test
    public void testDumpTableResultWithSingleRecord() throws Exception {
        final long totalQuota = 1L << 30; // 1GB
        GlobalMemoryTrackerManager manager = MemoryTrackerManager.getGlobalMemoryTrackerManager();
        manager.resize(totalQuota);

        // Create single operator
        OperatorMemoryOwnerId operator = manager
            .createQueryMemoryOwnerId("single-query")
            .createChild(1, 0)
            .createChild(1)
            .createChild(1, "SingleExec");
        operator.setOwner(new TestMemoryCountableItem(1L << 20)); // 1MB

        // Ensure memory tracker is properly initialized
        MemoryTrackerManager.adjustMemoryUsage(operator);
        MemoryTrackerManager.tryAllocate(operator, 1L << 20); // 1MB

        // Test topN = 1
        java.util.List<Object[]> result1 = manager.dumpTableResult(1);

        // Test topN > available records
        java.util.List<Object[]> result2 = manager.dumpTableResult(5);
        // Clean up
        manager.releaseQueryMemory("single-query");
    }

    @DefinedMemoryUsage
    private static class MemoryCountableItem implements MemoryCountable {

        @Override
        public long getMemoryUsage() {
            return 1 << 20;
        }
    }

    @DefinedMemoryUsage
    private static class TestMemoryCountableItem implements MemoryCountable {
        private final long memoryUsage;

        public TestMemoryCountableItem(long memoryUsage) {
            this.memoryUsage = memoryUsage;
        }

        @Override
        public long getMemoryUsage() {
            return memoryUsage;
        }
    }

}