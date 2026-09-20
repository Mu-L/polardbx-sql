package com.alibaba.polardbx.common.cdc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class BinlogDumpMetricsManagerTest {

    private BinlogDumpMetricsManager manager;

    @Before
    public void setUp() {
        manager = BinlogDumpMetricsManager.getInstance();
    }

    @After
    public void tearDown() {
        // Clean up any registered metrics to avoid test pollution
        manager.unregister(1L);
        manager.unregister(2L);
        manager.unregister(3L);
        manager.unregister(100L);
        manager.unregister(200L);
    }

    @Test
    public void testSingleton() {
        BinlogDumpMetricsManager instance1 = BinlogDumpMetricsManager.getInstance();
        BinlogDumpMetricsManager instance2 = BinlogDumpMetricsManager.getInstance();
        Assert.assertSame(instance1, instance2);
    }

    @Test
    public void testRegisterAndGetAll() {
        BinlogDumpMetrics metrics = new BinlogDumpMetrics("trace-1");
        manager.register(1L, metrics);

        List<BinlogDumpMetrics> all = manager.getAllMetrics();
        Assert.assertTrue(all.contains(metrics));
    }

    @Test
    public void testUnregister() {
        BinlogDumpMetrics metrics = new BinlogDumpMetrics("trace-2");
        manager.register(2L, metrics);
        Assert.assertTrue(manager.getAllMetrics().contains(metrics));

        manager.unregister(2L);
        Assert.assertFalse(manager.getAllMetrics().contains(metrics));
    }

    @Test
    public void testUnregisterNonExistent() {
        // Should not throw
        manager.unregister(888L);
    }

    @Test
    public void testGetAllMetrics() {
        BinlogDumpMetrics m1 = new BinlogDumpMetrics("t1");
        BinlogDumpMetrics m2 = new BinlogDumpMetrics("t2");
        manager.register(100L, m1);
        manager.register(200L, m2);

        List<BinlogDumpMetrics> all = manager.getAllMetrics();
        Assert.assertTrue(all.size() >= 2);
        Assert.assertTrue(all.contains(m1));
        Assert.assertTrue(all.contains(m2));
    }

    @Test
    public void testRegisterOverwritesExisting() {
        BinlogDumpMetrics m1 = new BinlogDumpMetrics("t1");
        BinlogDumpMetrics m2 = new BinlogDumpMetrics("t2");
        manager.register(3L, m1);
        Assert.assertTrue(manager.getAllMetrics().contains(m1));

        manager.register(3L, m2);
        Assert.assertTrue(manager.getAllMetrics().contains(m2));
        Assert.assertFalse(manager.getAllMetrics().contains(m1));
    }

    @Test
    public void testGetAllMetricsReturnsNewList() {
        BinlogDumpMetrics m = new BinlogDumpMetrics("t");
        manager.register(1L, m);
        List<BinlogDumpMetrics> list1 = manager.getAllMetrics();
        List<BinlogDumpMetrics> list2 = manager.getAllMetrics();
        Assert.assertNotSame(list1, list2);
    }
}
