package com.alibaba.polardbx.common.columnar;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class VersionStorageStatisticsTest {

    private VersionStorageStatistics statistics;

    @Before
    public void setUp() {
        statistics = new VersionStorageStatistics();
        // Clean up thread local before each test
        VersionStorageStatistics.removeThreadLocalStatistics();
    }

    @After
    public void tearDown() {
        // Clean up thread local after each test
        VersionStorageStatistics.removeThreadLocalStatistics();
    }

    @Test
    public void testDefaultConstructor() {
        VersionStorageStatistics stats = new VersionStorageStatistics();

        // Test initial values for GMS
        assertEquals(0, stats.getMaxGMSRt());
        assertEquals(0, stats.getMinGMSRt());
        assertEquals(0, stats.getSumRtm());
        assertEquals(0, stats.getGmsRtCount());

        // Test initial values for CSV
        assertEquals(0, stats.getMaxCSVRt());
        assertEquals(0, stats.getMinCSVRt());
        assertEquals(0, stats.getSumCSVRt());
        assertEquals(0, stats.getCsvRtCount());

        // Test initial values for ORC
        assertEquals(0, stats.getMaxORCRt());
        assertEquals(0, stats.getMinORCRt());
        assertEquals(0, stats.getSumORCRt());
        assertEquals(0, stats.getOrcRtCount());

        // Test initial values for DEL
        assertEquals(0, stats.getMaxDELRt());
        assertEquals(0, stats.getMinDELRt());
        assertEquals(0, stats.getSumDELRt());
        assertEquals(0, stats.getDelRtCount());

        // Test hash is set
        assertTrue(stats.getHash() != 0);
    }

    @Test
    public void testJsonConstructor() {
        int hash = 12345;
        long maxGMSRt = 1000;
        long minGMSRt = 100;
        long sumGMSRt = 5000;
        long gmsRtCount = 5;
        long maxCSVRt = 2000;
        long minCSVRt = 200;
        long sumCSVRt = 10000;
        long csvRtCount = 10;
        long maxORCRt = 3000;
        long minORCRt = 300;
        long sumORCRt = 15000;
        long orcRtCount = 15;
        long maxDELRt = 4000;
        long minDELRt = 400;
        long sumDELRt = 20000;
        long delRtCount = 20;
        long maxPreheatRt = 5000;
        long minPreheatRt = 500;
        long sumPreheatRt = 25000;
        long preheatRtCount = 25;

        VersionStorageStatistics stats = new VersionStorageStatistics(
            hash, maxGMSRt, minGMSRt, sumGMSRt, gmsRtCount,
            maxCSVRt, minCSVRt, sumCSVRt, csvRtCount,
            maxORCRt, minORCRt, sumORCRt, orcRtCount,
            maxDELRt, minDELRt, sumDELRt, delRtCount,
            maxPreheatRt, minPreheatRt, sumPreheatRt, preheatRtCount
        );

        assertEquals(hash, stats.getHash());
        assertEquals(maxGMSRt, stats.getMaxGMSRt());
        assertEquals(minGMSRt, stats.getMinGMSRt());
        assertEquals(sumGMSRt, stats.getSumRtm());
        assertEquals(gmsRtCount, stats.getGmsRtCount());
        assertEquals(maxCSVRt, stats.getMaxCSVRt());
        assertEquals(minCSVRt, stats.getMinCSVRt());
        assertEquals(sumCSVRt, stats.getSumCSVRt());
        assertEquals(csvRtCount, stats.getCsvRtCount());
        assertEquals(maxORCRt, stats.getMaxORCRt());
        assertEquals(minORCRt, stats.getMinORCRt());
        assertEquals(sumORCRt, stats.getSumORCRt());
        assertEquals(orcRtCount, stats.getOrcRtCount());
        assertEquals(maxDELRt, stats.getMaxDELRt());
        assertEquals(minDELRt, stats.getMinDELRt());
        assertEquals(sumDELRt, stats.getSumDELRt());
        assertEquals(delRtCount, stats.getDelRtCount());
    }

    @Test
    public void testThreadLocalStatistics() {
        // Test initial state
        assertNull(VersionStorageStatistics.getThreadLocalStatistics());

        // Test set and get
        VersionStorageStatistics stats = new VersionStorageStatistics();
        VersionStorageStatistics.setThreadLocalStatistics(stats);
        assertSame(stats, VersionStorageStatistics.getThreadLocalStatistics());

        // Test remove
        VersionStorageStatistics.removeThreadLocalStatistics();
        assertNull(VersionStorageStatistics.getThreadLocalStatistics());
    }

    @Test
    public void testUpdateGmsStatistics() {
        // Test single update
        statistics.updateGmsStatistics(500);
        assertEquals(500, statistics.getMaxGMSRt());
        assertEquals(500, statistics.getMinGMSRt());
        assertEquals(500, statistics.getSumRtm());
        assertEquals(1, statistics.getGmsRtCount());

        // Test multiple updates
        statistics.updateGmsStatistics(300);
        statistics.updateGmsStatistics(700);
        assertEquals(700, statistics.getMaxGMSRt());
        assertEquals(300, statistics.getMinGMSRt());
        assertEquals(1500, statistics.getSumRtm());
        assertEquals(3, statistics.getGmsRtCount());
    }

    @Test
    public void testUpdateCsvStatistics() {
        // Test single update
        statistics.updateCsvStatistics(400);
        assertEquals(400, statistics.getMaxCSVRt());
        assertEquals(400, statistics.getMinCSVRt());
        assertEquals(400, statistics.getSumCSVRt());
        assertEquals(1, statistics.getCsvRtCount());

        // Test multiple updates
        statistics.updateCsvStatistics(200);
        statistics.updateCsvStatistics(600);
        assertEquals(600, statistics.getMaxCSVRt());
        assertEquals(200, statistics.getMinCSVRt());
        assertEquals(1200, statistics.getSumCSVRt());
        assertEquals(3, statistics.getCsvRtCount());
    }

    @Test
    public void testUpdateOrcStatistics() {
        // Test single update
        statistics.updateOrcStatistics(800);
        assertEquals(800, statistics.getMaxORCRt());
        assertEquals(800, statistics.getMinORCRt());
        assertEquals(800, statistics.getSumORCRt());
        assertEquals(1, statistics.getOrcRtCount());

        // Test multiple updates
        statistics.updateOrcStatistics(600);
        statistics.updateOrcStatistics(1000);
        assertEquals(1000, statistics.getMaxORCRt());
        assertEquals(600, statistics.getMinORCRt());
        assertEquals(2400, statistics.getSumORCRt());
        assertEquals(3, statistics.getOrcRtCount());
    }

    @Test
    public void testUpdateDelStatistics() {
        // Test single update
        statistics.updateDelStatistics(350);
        assertEquals(350, statistics.getMaxDELRt());
        assertEquals(350, statistics.getMinDELRt());
        assertEquals(350, statistics.getSumDELRt());
        assertEquals(1, statistics.getDelRtCount());

        // Test multiple updates
        statistics.updateDelStatistics(250);
        statistics.updateDelStatistics(450);
        assertEquals(450, statistics.getMaxDELRt());
        assertEquals(250, statistics.getMinDELRt());
        assertEquals(1050, statistics.getSumDELRt());
        assertEquals(3, statistics.getDelRtCount());
    }

    @Test
    public void testConcurrentUpdates() throws InterruptedException {
        final int threadCount = 10;
        final int updatesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < updatesPerThread; j++) {
                        long rt = threadId * 100 + j;
                        statistics.updateGmsStatistics(rt);
                        statistics.updateCsvStatistics(rt);
                        statistics.updateOrcStatistics(rt);
                        statistics.updateDelStatistics(rt);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // Verify counts
        assertEquals(threadCount * updatesPerThread, statistics.getGmsRtCount());
        assertEquals(threadCount * updatesPerThread, statistics.getCsvRtCount());
        assertEquals(threadCount * updatesPerThread, statistics.getOrcRtCount());
        assertEquals(threadCount * updatesPerThread, statistics.getDelRtCount());

        // Verify that statistics were updated (exact values depend on thread execution order)
        assertTrue(statistics.getSumRtm() > 0);
        assertTrue(statistics.getSumCSVRt() > 0);
        assertTrue(statistics.getSumORCRt() > 0);
        assertTrue(statistics.getSumDELRt() > 0);
    }

    @Test
    public void testFromMethod() {
        // Create multiple statistics objects
        VersionStorageStatistics stats1 = new VersionStorageStatistics();
        stats1.updateGmsStatistics(100);
        stats1.updateCsvStatistics(200);
        stats1.updateOrcStatistics(300);
        stats1.updateDelStatistics(400);

        VersionStorageStatistics stats2 = new VersionStorageStatistics();
        stats2.updateGmsStatistics(150);
        stats2.updateCsvStatistics(250);
        stats2.updateOrcStatistics(350);
        stats2.updateDelStatistics(450);

        VersionStorageStatistics stats3 = new VersionStorageStatistics();
        stats3.updateGmsStatistics(120);
        stats3.updateCsvStatistics(220);
        stats3.updateOrcStatistics(320);
        stats3.updateDelStatistics(420);

        Collection<VersionStorageStatistics> collection = new ArrayList<>();
        collection.add(stats1);
        collection.add(stats2);
        collection.add(stats3);

        VersionStorageStatistics merged = VersionStorageStatistics.from(collection);

        // Verify merged counts
        assertEquals(3, merged.getGmsRtCount());
        assertEquals(3, merged.getCsvRtCount());
        assertEquals(3, merged.getOrcRtCount());
        assertEquals(3, merged.getDelRtCount());

        // Verify merged sums
        assertEquals(370, merged.getSumRtm()); // 100 + 150 + 120
        assertEquals(670, merged.getSumCSVRt()); // 200 + 250 + 220
        assertEquals(970, merged.getSumORCRt()); // 300 + 350 + 320
        assertEquals(1270, merged.getSumDELRt()); // 400 + 450 + 420

        // Verify merged max values
        assertEquals(150, merged.getMaxGMSRt());
        assertEquals(250, merged.getMaxCSVRt());
        assertEquals(350, merged.getMaxORCRt());
        assertEquals(450, merged.getMaxDELRt());

        // Verify merged min values
        assertEquals(100, merged.getMinGMSRt());
        assertEquals(200, merged.getMinCSVRt());
        assertEquals(300, merged.getMinORCRt());
        assertEquals(400, merged.getMinDELRt());
    }

    @Test
    public void testFromMethodWithEmptyCollection() {
        Collection<VersionStorageStatistics> emptyCollection = new ArrayList<>();
        VersionStorageStatistics merged = VersionStorageStatistics.from(emptyCollection);

        assertEquals(0, merged.getGmsRtCount());
        assertEquals(0, merged.getCsvRtCount());
        assertEquals(0, merged.getOrcRtCount());
        assertEquals(0, merged.getDelRtCount());
        assertEquals(0, merged.getSumRtm());
        assertEquals(0, merged.getSumCSVRt());
        assertEquals(0, merged.getSumORCRt());
        assertEquals(0, merged.getSumDELRt());
        assertEquals(0, merged.getMaxGMSRt());
        assertEquals(0, merged.getMaxCSVRt());
        assertEquals(0, merged.getMaxORCRt());
        assertEquals(0, merged.getMaxDELRt());
        assertEquals(0, merged.getMinGMSRt());
        assertEquals(0, merged.getMinCSVRt());
        assertEquals(0, merged.getMinORCRt());
        assertEquals(0, merged.getMinDELRt());
    }

    @Test
    public void testToString() {
        statistics.updateGmsStatistics(100);
        statistics.updateCsvStatistics(200);
        statistics.updateOrcStatistics(300);
        statistics.updateDelStatistics(400);

        String result = statistics.toString();

        assertTrue(result.contains("VersionStorageStatistics{"));
        assertTrue(result.contains("hash="));
        assertTrue(result.contains("delRtCount=1"));
        assertTrue(result.contains("sumDELRt=400"));
        assertTrue(result.contains("minDELRt=400"));
        assertTrue(result.contains("maxDELRt=400"));
        assertTrue(result.contains("orcRtCount=1"));
        assertTrue(result.contains("sumORCRt=300"));
        assertTrue(result.contains("minORCRt=300"));
        assertTrue(result.contains("maxORCRt=300"));
        assertTrue(result.contains("csvRtCount=1"));
        assertTrue(result.contains("sumCSVRt=200"));
        assertTrue(result.contains("minCSVRt=200"));
        assertTrue(result.contains("maxCSVRt=200"));
        assertTrue(result.contains("gmsRtCount=1"));
        assertTrue(result.contains("sumGMSRt=100"));
        assertTrue(result.contains("minGMSRt=100"));
        assertTrue(result.contains("maxGMSRt=100"));
        assertTrue(result.endsWith("}"));
    }

    @Test
    public void testToPrintable() {
        statistics.updateGmsStatistics(100);
        statistics.updateCsvStatistics(200);
        statistics.updateOrcStatistics(300);
        statistics.updateDelStatistics(400);

        String result = statistics.toPrintable();

        assertTrue(result.contains("orcRtCount-1"));
        assertTrue(result.contains("sumORCRt-300"));
        assertTrue(result.contains("minORCRt-300"));
        assertTrue(result.contains("maxORCRt-300"));
        assertTrue(result.contains("avgORCRt-300"));
        assertTrue(result.contains("gmsRtCount-1"));
        assertTrue(result.contains("sumGMSRt-100"));
        assertTrue(result.contains("minGMSRt-100"));
        assertTrue(result.contains("maxGMSRt-100"));
        assertTrue(result.contains("avgGMSRt-100"));
        assertTrue(result.contains("csvRtCount-1"));
        assertTrue(result.contains("sumCSVRt-200"));
        assertTrue(result.contains("minCSVRt-200"));
        assertTrue(result.contains("maxCSVRt-200"));
        assertTrue(result.contains("avgCSVRt-200"));
        assertTrue(result.contains("delRtCount-1"));
        assertTrue(result.contains("sumDELRt-400"));
        assertTrue(result.contains("minDELRt-400"));
        assertTrue(result.contains("maxDELRt-400"));
        assertTrue(result.contains("avgDELRt-400"));
    }

    @Test
    public void testToPrintableWithZeroCounts() {
        String result = statistics.toPrintable();

        assertTrue(result.contains("orcRtCount-0"));
        assertTrue(result.contains("avgORCRt-0"));
        assertTrue(result.contains("gmsRtCount-0"));
        assertTrue(result.contains("avgGMSRt-0"));
        assertTrue(result.contains("csvRtCount-0"));
        assertTrue(result.contains("avgCSVRt-0"));
        assertTrue(result.contains("delRtCount-0"));
        assertTrue(result.contains("avgDELRt-0"));
    }

    @Test
    public void testGettersWithMinMaxValues() {
        // Test with Long.MIN_VALUE and Long.MAX_VALUE
        VersionStorageStatistics stats = new VersionStorageStatistics(
            12345,
            Long.MIN_VALUE, Long.MAX_VALUE, 0, 0,
            Long.MIN_VALUE, Long.MAX_VALUE, 0, 0,
            Long.MIN_VALUE, Long.MAX_VALUE, 0, 0,
            Long.MIN_VALUE, Long.MAX_VALUE, 0, 0,
            Long.MIN_VALUE, Long.MAX_VALUE, 0, 0
        );

        // All getters should return 0 when values are at their initial extremes
        assertEquals(0, stats.getMaxGMSRt());
        assertEquals(0, stats.getMinGMSRt());
        assertEquals(0, stats.getMaxCSVRt());
        assertEquals(0, stats.getMinCSVRt());
        assertEquals(0, stats.getMaxORCRt());
        assertEquals(0, stats.getMinORCRt());
        assertEquals(0, stats.getMaxDELRt());
        assertEquals(0, stats.getMinDELRt());
    }

    @Test
    public void testMultipleThreadLocalOperations() {
        VersionStorageStatistics stats1 = new VersionStorageStatistics();
        VersionStorageStatistics stats2 = new VersionStorageStatistics();

        // Test multiple set operations
        VersionStorageStatistics.setThreadLocalStatistics(stats1);
        assertSame(stats1, VersionStorageStatistics.getThreadLocalStatistics());

        VersionStorageStatistics.setThreadLocalStatistics(stats2);
        assertSame(stats2, VersionStorageStatistics.getThreadLocalStatistics());

        // Test multiple remove operations
        VersionStorageStatistics.removeThreadLocalStatistics();
        assertNull(VersionStorageStatistics.getThreadLocalStatistics());

        VersionStorageStatistics.removeThreadLocalStatistics();
        assertNull(VersionStorageStatistics.getThreadLocalStatistics());
    }
}