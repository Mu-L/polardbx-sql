package com.alibaba.polardbx.common.columnar;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ColumnarScanMetricsTest {

    private ColumnarScanMetrics metrics;

    @Before
    public void setUp() {
        metrics = new ColumnarScanMetrics();
    }

    @Test
    public void testDefaultConstructor() {
        ColumnarScanMetrics metrics = new ColumnarScanMetrics();

        assertEquals(0, metrics.getTotalScanRows());
        assertEquals(0, metrics.getTotalScanBytes());
        assertEquals(0, metrics.getTotalFilteredRows());
    }

    @Test
    public void testJsonConstructor() {
        long totalScanRows = 1000;
        long totalScanBytes = 2048;
        long totalFilteredRows = 500;

        ColumnarScanMetrics metrics = new ColumnarScanMetrics(totalScanRows, totalScanBytes, totalFilteredRows);

        assertEquals(totalScanRows, metrics.getTotalScanRows());
        assertEquals(totalScanBytes, metrics.getTotalScanBytes());
        assertEquals(totalFilteredRows, metrics.getTotalFilteredRows());
    }

    @Test
    public void testGetters() {
        long totalScanRows = 12345;
        long totalScanBytes = 67890;
        long totalFilteredRows = 54321;

        ColumnarScanMetrics metrics = new ColumnarScanMetrics(totalScanRows, totalScanBytes, totalFilteredRows);

        assertEquals(totalScanRows, metrics.getTotalScanRows());
        assertEquals(totalScanBytes, metrics.getTotalScanBytes());
        assertEquals(totalFilteredRows, metrics.getTotalFilteredRows());
    }

    @Test
    public void testUpdateTotalScanRows() {
        // Test single update
        metrics.updateTotalScanRows(100, 1024, 50);
        assertEquals(100, metrics.getTotalScanRows());
        assertEquals(1024, metrics.getTotalScanBytes());
        assertEquals(50, metrics.getTotalFilteredRows());

        // Test multiple updates
        metrics.updateTotalScanRows(200, 2048, 75);
        assertEquals(300, metrics.getTotalScanRows());
        assertEquals(3072, metrics.getTotalScanBytes());
        assertEquals(125, metrics.getTotalFilteredRows());

        // Test with zero values
        metrics.updateTotalScanRows(0, 0, 0);
        assertEquals(300, metrics.getTotalScanRows());
        assertEquals(3072, metrics.getTotalScanBytes());
        assertEquals(125, metrics.getTotalFilteredRows());
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
                        metrics.updateTotalScanRows(1, 10, 1);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // Verify final counts
        assertEquals(threadCount * updatesPerThread, metrics.getTotalScanRows());
        assertEquals(threadCount * updatesPerThread * 10, metrics.getTotalScanBytes());
        assertEquals(threadCount * updatesPerThread, metrics.getTotalFilteredRows());
    }

    @Test
    public void testFromMethod() {
        // Create multiple metrics objects
        ColumnarScanMetrics metrics1 = new ColumnarScanMetrics(100, 1024, 50);
        ColumnarScanMetrics metrics2 = new ColumnarScanMetrics(200, 2048, 75);
        ColumnarScanMetrics metrics3 = new ColumnarScanMetrics(300, 4096, 125);

        Collection<ColumnarScanMetrics> collection = new ArrayList<>();
        collection.add(metrics1);
        collection.add(metrics2);
        collection.add(metrics3);

        ColumnarScanMetrics merged = ColumnarScanMetrics.from(collection);

        // Verify merged values
        assertEquals(600, merged.getTotalScanRows()); // 100 + 200 + 300
        assertEquals(7168, merged.getTotalScanBytes()); // 1024 + 2048 + 4096
        assertEquals(250, merged.getTotalFilteredRows()); // 50 + 75 + 125
    }

    @Test
    public void testFromMethodWithEmptyCollection() {
        Collection<ColumnarScanMetrics> emptyCollection = new ArrayList<>();
        ColumnarScanMetrics merged = ColumnarScanMetrics.from(emptyCollection);

        assertEquals(0, merged.getTotalScanRows());
        assertEquals(0, merged.getTotalScanBytes());
        assertEquals(0, merged.getTotalFilteredRows());
    }

    @Test
    public void testFromMethodWithSingleElement() {
        ColumnarScanMetrics metrics1 = new ColumnarScanMetrics(500, 8192, 200);
        Collection<ColumnarScanMetrics> collection = new ArrayList<>();
        collection.add(metrics1);

        ColumnarScanMetrics merged = ColumnarScanMetrics.from(collection);

        assertEquals(500, merged.getTotalScanRows());
        assertEquals(8192, merged.getTotalScanBytes());
        assertEquals(200, merged.getTotalFilteredRows());
    }

    @Test
    public void testToPrintableWithZeroRows() {
        ColumnarScanMetrics metrics = new ColumnarScanMetrics(0, 1024, 0);
        String result = metrics.toPrintable();

        assertTrue(result.contains("totalScanRows-0"));
        assertTrue(result.contains("totalScanBytes-1.0KB"));
        assertTrue(result.contains("totalFilteredRows-0"));
        assertTrue(result.contains("predicateSelectivity-0"));
    }

    @Test
    public void testToPrintableWithNormalValues() {
        ColumnarScanMetrics metrics = new ColumnarScanMetrics(1000, 2048, 250);
        String result = metrics.toPrintable();

        assertTrue(result.contains("totalScanRows-1000"));
        assertTrue(result.contains("totalScanBytes-2.0KB"));
        assertTrue(result.contains("totalFilteredRows-250"));
        assertTrue(result.contains("predicateSelectivity-0.2500"));
    }

    @Test
    public void testToPrintableWithDifferentSelectivity() {
        // Test different selectivity values
        ColumnarScanMetrics metrics1 = new ColumnarScanMetrics(1000, 1024, 500);
        String result1 = metrics1.toPrintable();
        assertTrue(result1.contains("predicateSelectivity-0.5000"));

        ColumnarScanMetrics metrics2 = new ColumnarScanMetrics(1000, 1024, 1000);
        String result2 = metrics2.toPrintable();
        assertTrue(result2.contains("predicateSelectivity-1.0000"));

        ColumnarScanMetrics metrics3 = new ColumnarScanMetrics(1000, 1024, 1);
        String result3 = metrics3.toPrintable();
        assertTrue(result3.contains("predicateSelectivity-0.0010"));
    }

    @Test
    public void testMemorySizeFormattingBytes() {
        // Test bytes formatting (< 1024)
        ColumnarScanMetrics metrics = new ColumnarScanMetrics(100, 512, 50);
        String result = metrics.toPrintable();
        assertTrue(result.contains("totalScanBytes-512.0B"));

        // Test edge case: exactly 1023 bytes
        metrics = new ColumnarScanMetrics(100, 1023, 50);
        result = metrics.toPrintable();
        assertTrue(result.contains("totalScanBytes-1023.0B"));
    }

    @Test
    public void testMemorySizeFormattingKB() {
        // Test KB formatting (>= 1024, < 1024*1024)
        ColumnarScanMetrics metrics = new ColumnarScanMetrics(100, 1024, 50);
        String result = metrics.toPrintable();
        assertTrue(result.contains("totalScanBytes-1.0KB"));

        // Test larger KB value
        metrics = new ColumnarScanMetrics(100, 2560, 50);
        result = metrics.toPrintable();
        assertTrue(result.contains("totalScanBytes-2.5KB"));

        // Test edge case: exactly 1024*1024 - 1 bytes
        metrics = new ColumnarScanMetrics(100, 1024 * 1024 - 1, 50);
        result = metrics.toPrintable();
        assertTrue(result.contains("KB"));
    }

    @Test
    public void testMemorySizeFormattingMB() {
        // Test MB formatting (>= 1024*1024, < 1024*1024*1024)
        ColumnarScanMetrics metrics = new ColumnarScanMetrics(100, 1024 * 1024, 50);
        String result = metrics.toPrintable();
        assertTrue(result.contains("totalScanBytes-1.0MB"));

        // Test larger MB value
        metrics = new ColumnarScanMetrics(100, 5 * 1024 * 1024, 50);
        result = metrics.toPrintable();
        assertTrue(result.contains("totalScanBytes-5.0MB"));

        // Test edge case: exactly 1024*1024*1024 - 1 bytes
        metrics = new ColumnarScanMetrics(100, 1024 * 1024 * 1024 - 1, 50);
        result = metrics.toPrintable();
        assertTrue(result.contains("MB"));
    }

    @Test
    public void testMemorySizeFormattingGB() {
        // Test GB formatting (>= 1024*1024*1024)
        long oneGB = 1024L * 1024 * 1024;
        ColumnarScanMetrics metrics = new ColumnarScanMetrics(100, oneGB, 50);
        String result = metrics.toPrintable();
        assertTrue(result.contains("totalScanBytes-1.0GB"));

        // Test larger GB value
        metrics = new ColumnarScanMetrics(100, 3L * oneGB, 50);
        result = metrics.toPrintable();
        assertTrue(result.contains("totalScanBytes-3.0GB"));

        // Test fractional GB
        metrics = new ColumnarScanMetrics(100, oneGB + 512L * 1024 * 1024, 50);
        result = metrics.toPrintable();
        assertTrue(result.contains("totalScanBytes-1.5GB"));
    }

    @Test
    public void testMemorySizeFormattingZero() {
        ColumnarScanMetrics metrics = new ColumnarScanMetrics(100, 0, 50);
        String result = metrics.toPrintable();
        assertTrue(result.contains("totalScanBytes-0.0B"));
    }

    @Test
    public void testToPrintableFormat() {
        ColumnarScanMetrics metrics = new ColumnarScanMetrics(2000, 4096, 800);
        String result = metrics.toPrintable();

        // Verify the format structure
        assertTrue(result.startsWith("totalScanRows-"));
        assertTrue(result.contains("/totalScanBytes-"));
        assertTrue(result.contains("/totalFilteredRows-"));
        assertTrue(result.contains("/predicateSelectivity-"));

        // Verify specific values
        assertTrue(result.contains("totalScanRows-2000"));
        assertTrue(result.contains("totalScanBytes-4.0KB"));
        assertTrue(result.contains("totalFilteredRows-800"));
        assertTrue(result.contains("predicateSelectivity-0.4000"));
    }

    @Test
    public void testUpdateWithLargeValues() {
        long largeRows = Long.MAX_VALUE / 2;
        long largeBytes = Long.MAX_VALUE / 3;
        long largeFiltered = Long.MAX_VALUE / 4;

        metrics.updateTotalScanRows(largeRows, largeBytes, largeFiltered);

        assertEquals(largeRows, metrics.getTotalScanRows());
        assertEquals(largeBytes, metrics.getTotalScanBytes());
        assertEquals(largeFiltered, metrics.getTotalFilteredRows());
    }

    @Test
    public void testFromMethodWithZeroValues() {
        ColumnarScanMetrics metrics1 = new ColumnarScanMetrics(0, 0, 0);
        ColumnarScanMetrics metrics2 = new ColumnarScanMetrics(100, 1024, 50);
        ColumnarScanMetrics metrics3 = new ColumnarScanMetrics(0, 0, 0);

        Collection<ColumnarScanMetrics> collection = new ArrayList<>();
        collection.add(metrics1);
        collection.add(metrics2);
        collection.add(metrics3);

        ColumnarScanMetrics merged = ColumnarScanMetrics.from(collection);

        assertEquals(100, merged.getTotalScanRows());
        assertEquals(1024, merged.getTotalScanBytes());
        assertEquals(50, merged.getTotalFilteredRows());
    }

    @Test
    public void testPredicateSelectivityPrecision() {
        // Test precision of predicate selectivity calculation
        ColumnarScanMetrics metrics = new ColumnarScanMetrics(3, 1024, 1);
        String result = metrics.toPrintable();
        assertTrue(result.contains("predicateSelectivity-0.3333"));

        metrics = new ColumnarScanMetrics(7, 1024, 2);
        result = metrics.toPrintable();
        assertTrue(result.contains("predicateSelectivity-0.2857"));
    }
}