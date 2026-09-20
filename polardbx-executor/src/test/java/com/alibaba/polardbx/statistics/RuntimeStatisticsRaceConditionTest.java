package com.alibaba.polardbx.statistics;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for race condition fixes in RuntimeStatistics:
 * 1. toSketch() null safety when relationIdToNode has missing entries
 * 2. OperatorStatisticsGroup.toSketch() synchronized with concurrent add()
 * 3. OperatorStatisticsGroup.toSketchExt() synchronized with concurrent add()
 */
public class RuntimeStatisticsRaceConditionTest {

    private RuntimeStatistics runtimeStatistics;
    private ExecutionContext executionContext;

    @Before
    public void setUp() {
        executionContext = mock(ExecutionContext.class);
        runtimeStatistics = new RuntimeStatistics("test-schema", executionContext);
    }

    /**
     * Test: toSketch() skips entries where relationIdToNode.get() returns null.
     * This can happen when relationToStatistics (ConcurrentHashMap) has an entry
     * but relationIdToNode (HashMap) does not, due to lack of happens-before.
     */
    @Test
    public void testToSketch_nullNodeSkipped() {
        // Directly manipulate the internal maps via the ConcurrentHashMap
        ConcurrentHashMap<Integer, RuntimeStatistics.OperatorStatisticsGroup> relToStats =
            (ConcurrentHashMap<Integer, RuntimeStatistics.OperatorStatisticsGroup>)
                runtimeStatistics.getRelationToStatistics();

        // Add an entry to relationToStatistics with key=42
        RuntimeStatistics.OperatorStatisticsGroup group =
            new RuntimeStatistics.OperatorStatisticsGroup(runtimeStatistics);
        OperatorStatistics opStats = new OperatorStatistics();
        group.add(opStats);
        relToStats.put(42, group);

        // Do NOT add key=42 to relationIdToNode → .get(42) returns null

        // toSketch() should not throw NPE, just skip the null entry
        Map<RelNode, RuntimeStatisticsSketch> result = runtimeStatistics.toSketch();
        Assert.assertNotNull(result);
        Assert.assertTrue("Entry with null node should be skipped", result.isEmpty());
    }

    /**
     * Test: toSketch() correctly maps entries when relationIdToNode has the node.
     */
    @Test
    public void testToSketch_withValidNode() {
        ConcurrentHashMap<Integer, RuntimeStatistics.OperatorStatisticsGroup> relToStats =
            (ConcurrentHashMap<Integer, RuntimeStatistics.OperatorStatisticsGroup>)
                runtimeStatistics.getRelationToStatistics();

        // Create a mock RelNode with a specific relatedId
        RelNode mockNode = mock(AbstractRelNode.class);
        when(mockNode.getRelatedId()).thenReturn(100);

        // Add to both maps (simulating normal registration)
        RuntimeStatistics.OperatorStatisticsGroup group =
            new RuntimeStatistics.OperatorStatisticsGroup(runtimeStatistics);
        OperatorStatistics opStats = new OperatorStatistics();
        group.add(opStats);
        relToStats.put(100, group);

        // Access the internal relationIdToNode
        Map<Integer, RelNode> relationIdToNode = getRelationIdToNode();
        relationIdToNode.put(100, mockNode);

        Map<RelNode, RuntimeStatisticsSketch> result = runtimeStatistics.toSketch();
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsKey(mockNode));
    }

    /**
     * Test: toSketch() with mix of valid and null entries.
     */
    @Test
    public void testToSketch_mixedValidAndNull() {
        ConcurrentHashMap<Integer, RuntimeStatistics.OperatorStatisticsGroup> relToStats =
            (ConcurrentHashMap<Integer, RuntimeStatistics.OperatorStatisticsGroup>)
                runtimeStatistics.getRelationToStatistics();
        Map<Integer, RelNode> relationIdToNode = getRelationIdToNode();

        // Entry 1: has node
        RelNode node1 = mock(AbstractRelNode.class);
        when(node1.getRelatedId()).thenReturn(1);
        RuntimeStatistics.OperatorStatisticsGroup group1 =
            new RuntimeStatistics.OperatorStatisticsGroup(runtimeStatistics);
        group1.add(new OperatorStatistics());
        relToStats.put(1, group1);
        relationIdToNode.put(1, node1);

        // Entry 2: missing node (null)
        RuntimeStatistics.OperatorStatisticsGroup group2 =
            new RuntimeStatistics.OperatorStatisticsGroup(runtimeStatistics);
        group2.add(new OperatorStatistics());
        relToStats.put(2, group2);
        // Deliberately NOT adding key=2 to relationIdToNode

        // Entry 3: has node
        RelNode node3 = mock(AbstractRelNode.class);
        when(node3.getRelatedId()).thenReturn(3);
        RuntimeStatistics.OperatorStatisticsGroup group3 =
            new RuntimeStatistics.OperatorStatisticsGroup(runtimeStatistics);
        group3.add(new OperatorStatistics());
        relToStats.put(3, group3);
        relationIdToNode.put(3, node3);

        Map<RelNode, RuntimeStatisticsSketch> result = runtimeStatistics.toSketch();
        Assert.assertNotNull(result);
        // Only entries 1 and 3 should be present; entry 2 (null node) skipped
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsKey(node1));
        Assert.assertTrue(result.containsKey(node3));
    }

    /**
     * Test: OperatorStatisticsGroup.toSketch() is synchronized with add().
     * Concurrent add + toSketch should not throw ConcurrentModificationException.
     */
    @Test
    public void testOperatorStatisticsGroup_concurrentAddAndToSketch() throws InterruptedException {
        final int NUM_ITERATIONS = 100;
        final AtomicBoolean failed = new AtomicBoolean(false);
        final AtomicInteger successCount = new AtomicInteger(0);

        for (int iter = 0; iter < NUM_ITERATIONS && !failed.get(); iter++) {
            RuntimeStatistics.OperatorStatisticsGroup group =
                new RuntimeStatistics.OperatorStatisticsGroup(runtimeStatistics);

            // Pre-populate
            for (int i = 0; i < 50; i++) {
                group.add(new OperatorStatistics());
            }

            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(2);

            // Thread 1: toSketch() reader
            Thread reader = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 10; i++) {
                        group.toSketch();
                    }
                } catch (Throwable e) {
                    failed.set(true);
                } finally {
                    doneLatch.countDown();
                }
            });

            // Thread 2: add() writer
            Thread writer = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 50; i++) {
                        group.add(new OperatorStatistics());
                    }
                } catch (Throwable e) {
                    failed.set(true);
                } finally {
                    doneLatch.countDown();
                }
            });

            reader.start();
            writer.start();
            startLatch.countDown();
            doneLatch.await(5, TimeUnit.SECONDS);

            if (!failed.get()) {
                successCount.incrementAndGet();
            }
        }

        Assert.assertFalse("ConcurrentModificationException in toSketch() + add()", failed.get());
        Assert.assertEquals(NUM_ITERATIONS, successCount.get());
    }

    /**
     * Test: OperatorStatisticsGroup.toSketchExt() is synchronized with add().
     * Concurrent add + toSketchExt should not throw ConcurrentModificationException.
     */
    @Test
    public void testOperatorStatisticsGroup_concurrentAddAndToSketchExt() throws InterruptedException {
        final int NUM_ITERATIONS = 100;
        final AtomicBoolean failed = new AtomicBoolean(false);
        final AtomicInteger successCount = new AtomicInteger(0);

        for (int iter = 0; iter < NUM_ITERATIONS && !failed.get(); iter++) {
            RuntimeStatistics rs = new RuntimeStatistics("test", executionContext);
            RuntimeStatistics.OperatorStatisticsGroup group =
                new RuntimeStatistics.OperatorStatisticsGroup(rs);

            // Pre-populate
            for (int i = 0; i < 50; i++) {
                group.add(new OperatorStatistics());
            }

            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(2);

            // Thread 1: toSketchExt() reader
            Thread reader = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 10; i++) {
                        group.toSketchExt();
                    }
                } catch (Throwable e) {
                    failed.set(true);
                } finally {
                    doneLatch.countDown();
                }
            });

            // Thread 2: add() writer
            Thread writer = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 50; i++) {
                        group.add(new OperatorStatistics());
                    }
                } catch (Throwable e) {
                    failed.set(true);
                } finally {
                    doneLatch.countDown();
                }
            });

            reader.start();
            writer.start();
            startLatch.countDown();
            doneLatch.await(5, TimeUnit.SECONDS);

            if (!failed.get()) {
                successCount.incrementAndGet();
            }
        }

        Assert.assertFalse("ConcurrentModificationException in toSketchExt() + add()", failed.get());
        Assert.assertEquals(NUM_ITERATIONS, successCount.get());
    }

    /**
     * Test: toSketch() returns correct aggregated values.
     */
    @Test
    public void testOperatorStatisticsGroup_toSketch_correctValues() {
        RuntimeStatistics.OperatorStatisticsGroup group =
            new RuntimeStatistics.OperatorStatisticsGroup(runtimeStatistics);

        // rowCount, runtimeFilteredCount, ioReadBytes, memory, startupDuration, processDuration, closeDuration, spillCnt
        OperatorStatistics stats1 = new OperatorStatistics(
            100L, 0L, 0L, 0L, 0L, 1000000000L, 0L, 0);
        OperatorStatistics stats2 = new OperatorStatistics(
            200L, 0L, 0L, 0L, 0L, 2000000000L, 0L, 0);

        group.add(stats1);
        group.add(stats2);

        RuntimeStatisticsSketch sketch = group.toSketch();
        Assert.assertNotNull(sketch);
        // rowCount = 100 + 200 = 300
        Assert.assertEquals(300L, sketch.getRowCount());
    }

    // --- Utility ---

    @SuppressWarnings("unchecked")
    private Map<Integer, RelNode> getRelationIdToNode() {
        try {
            java.lang.reflect.Field field =
                RuntimeStatistics.class.getDeclaredField("relationIdToNode");
            field.setAccessible(true);
            return (Map<Integer, RelNode>) field.get(runtimeStatistics);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
