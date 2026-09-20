package com.alibaba.polardbx.executor.operator.scan.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link AdaptiveColumnarScanMonitor} and {@link AdaptiveColumnarScanStatus}.
 */
public class AdaptiveColumnarScanMonitorTest {

    private AdaptiveColumnarScanMonitor monitor;

    private ScanWorkId.WorkId createWorkId(String queryId, int stripeId, int workNumber) {
        return new ScanWorkId.WorkId(queryId, "test_schema", "test_table",
            "/path/to/file.orc", stripeId, workNumber);
    }

    @Before
    public void setUp() {
        monitor = new AdaptiveColumnarScanMonitor();
    }

    // ========== AdaptiveColumnarScanStatus Tests ==========

    @Test
    public void testStatusConstruction() {
        ScanWorkId workId = ScanWorkId.of(createWorkId("q1", 0, 0), 0);
        AdaptiveColumnarScanStatus status = new AdaptiveColumnarScanStatus(workId, true);

        Assert.assertEquals(workId, status.getWorkId());
        Assert.assertTrue(status.isSplitTask());
    }

    @Test
    public void testStatusFieldsSetAndGet() {
        ScanWorkId workId = ScanWorkId.of(createWorkId("q1", 0, 0), 0);
        AdaptiveColumnarScanStatus status = new AdaptiveColumnarScanStatus(workId, false);

        status.setStatus(AdaptiveColumnarScanStatus.Status.RUNNING);
        status.setErrorMsg("some error");
        status.setStartTime(1000L);
        status.setScheduleTime(2000L);
        status.setEndTime(5000L);
        status.setStartRowGroupId(10);
        status.setGranularity(128);
        status.setThreadLimit(4);
        status.setAcquiredFilterIOPermits(100L);
        status.setAcquiredFilterPermits(200L);
        status.setAcquiredProjectIOPermits(300L);
        status.setAcquiredProjectPermits(400L);
        status.setRemainingPermits(50L);
        status.setTotalScanRows(10000L);
        status.setTotalScanBytes(20000L);
        status.setTotalFilteredRows(5000L);

        Assert.assertEquals(AdaptiveColumnarScanStatus.Status.RUNNING, status.getStatus());
        Assert.assertEquals("some error", status.getErrorMsg());
        Assert.assertEquals(1000L, status.getStartTime());
        Assert.assertEquals(2000L, status.getScheduleTime());
        Assert.assertEquals(5000L, status.getEndTime());
        Assert.assertEquals(10, status.getStartRowGroupId());
        Assert.assertEquals(128, status.getGranularity());
        Assert.assertEquals(4, status.getThreadLimit());
        Assert.assertEquals(100L, status.getAcquiredFilterIOPermits());
        Assert.assertEquals(200L, status.getAcquiredFilterPermits());
        Assert.assertEquals(300L, status.getAcquiredProjectIOPermits());
        Assert.assertEquals(400L, status.getAcquiredProjectPermits());
        Assert.assertEquals(50L, status.getRemainingPermits());
        Assert.assertEquals(10000L, status.getTotalScanRows());
        Assert.assertEquals(20000L, status.getTotalScanBytes());
        Assert.assertEquals(5000L, status.getTotalFilteredRows());
        Assert.assertFalse(status.isSplitTask());
    }

    @Test
    public void testStatusAllStatusValues() {
        ScanWorkId workId = ScanWorkId.of(createWorkId("q1", 0, 0), 0);
        AdaptiveColumnarScanStatus status = new AdaptiveColumnarScanStatus(workId, false);

        for (AdaptiveColumnarScanStatus.Status statusValue : AdaptiveColumnarScanStatus.Status.values()) {
            status.setStatus(statusValue);
            Assert.assertEquals(statusValue, status.getStatus());
        }
    }

    // ========== AdaptiveColumnarScanMonitor Tests ==========

    @Test
    public void testCreateStatus() {
        ScanWorkId.WorkId workId = createWorkId("q1", 0, 0);
        AtomicInteger sequenceNumber = new AtomicInteger(0);

        AdaptiveColumnarScanStatus status = monitor.create(workId, sequenceNumber, true);

        Assert.assertNotNull(status);
        Assert.assertTrue(status.isSplitTask());
        Assert.assertEquals(0, status.getWorkId().getSequence());
        Assert.assertEquals(1, sequenceNumber.get());
    }

    @Test
    public void testCreateMultipleStatusesIncrementsSequence() {
        ScanWorkId.WorkId workId = createWorkId("q1", 0, 0);
        AtomicInteger sequenceNumber = new AtomicInteger(0);

        AdaptiveColumnarScanStatus status1 = monitor.create(workId, sequenceNumber, false);
        AdaptiveColumnarScanStatus status2 = monitor.create(workId, sequenceNumber, true);
        AdaptiveColumnarScanStatus status3 = monitor.create(workId, sequenceNumber, false);

        Assert.assertEquals(0, status1.getWorkId().getSequence());
        Assert.assertEquals(1, status2.getWorkId().getSequence());
        Assert.assertEquals(2, status3.getWorkId().getSequence());
        Assert.assertEquals(3, sequenceNumber.get());
    }

    @Test
    public void testAddStatusAndGetIterator() {
        ScanWorkId workId = ScanWorkId.of(createWorkId("q1", 0, 0), 0);
        AdaptiveColumnarScanStatus status = new AdaptiveColumnarScanStatus(workId, false);
        status.setStatus(AdaptiveColumnarScanStatus.Status.SUCCESS);

        monitor.addStatus(status);

        Iterator<AdaptiveColumnarScanStatus> iterator = monitor.getStatusIterator();
        Assert.assertTrue(iterator.hasNext());
        AdaptiveColumnarScanStatus retrieved = iterator.next();
        Assert.assertEquals(AdaptiveColumnarScanStatus.Status.SUCCESS, retrieved.getStatus());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testGetStatusIteratorEmpty() {
        Iterator<AdaptiveColumnarScanStatus> iterator = monitor.getStatusIterator();
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testResizeWithPositiveValue() {
        monitor.resize(100);

        // After resize, should still be able to add and retrieve statuses
        ScanWorkId.WorkId workId = createWorkId("q1", 0, 0);
        AtomicInteger sequenceNumber = new AtomicInteger(0);
        monitor.create(workId, sequenceNumber, false);

        Iterator<AdaptiveColumnarScanStatus> iterator = monitor.getStatusIterator();
        Assert.assertTrue(iterator.hasNext());
    }

    @Test
    public void testResizeWithZero() {
        monitor.resize(0);
        // Should not throw exception
    }

    @Test
    public void testResizeWithNegativeValue() {
        // Negative value should be ignored, no exception
        monitor.resize(-1);

        // Monitor should still work normally
        ScanWorkId.WorkId workId = createWorkId("q1", 0, 0);
        AtomicInteger sequenceNumber = new AtomicInteger(0);
        monitor.create(workId, sequenceNumber, false);

        Iterator<AdaptiveColumnarScanStatus> iterator = monitor.getStatusIterator();
        Assert.assertTrue(iterator.hasNext());
    }

    @Test
    public void testGeneratePacketsEmpty() {
        List<Object[]> packets = monitor.generatePackets();
        Assert.assertNotNull(packets);
        Assert.assertTrue(packets.isEmpty());
    }

    @Test
    public void testGeneratePacketsSingleStatus() {
        ScanWorkId.WorkId workId = createWorkId("query-123", 5, 10);
        AtomicInteger sequenceNumber = new AtomicInteger(0);

        AdaptiveColumnarScanStatus status = monitor.create(workId, sequenceNumber, true);
        status.setStatus(AdaptiveColumnarScanStatus.Status.SUCCESS);
        status.setErrorMsg(null);
        status.setStartTime(1000L);
        status.setScheduleTime(2000L);
        status.setEndTime(5000L);
        status.setStartRowGroupId(10);
        status.setGranularity(128);
        status.setThreadLimit(4);
        status.setAcquiredFilterIOPermits(100L);
        status.setAcquiredFilterPermits(200L);
        status.setAcquiredProjectIOPermits(300L);
        status.setAcquiredProjectPermits(400L);
        status.setRemainingPermits(50L);
        status.setTotalScanRows(10000L);
        status.setTotalScanBytes(20000L);
        status.setTotalFilteredRows(5000L);

        List<Object[]> packets = monitor.generatePackets();

        Assert.assertEquals(1, packets.size());
        Object[] row = packets.get(0);
        Assert.assertEquals(26, row.length);

        // Verify fields in order
        int index = 0;
        Assert.assertEquals("query-123", row[index++]);           // QUERY_ID
        Assert.assertEquals("test_schema", row[index++]);         // LOGICAL_SCHEMA
        Assert.assertEquals("test_table", row[index++]);          // LOGICAL_TABLE
        Assert.assertEquals("/path/to/file.orc", row[index++]);  // FILE_PATH
        Assert.assertEquals(5, row[index++]);                     // STRIPE_ID
        Assert.assertEquals(10, row[index++]);                    // WORK_NUMBER
        Assert.assertEquals(0, row[index++]);                     // SEQUENCE
        Assert.assertEquals(true, row[index++]);                  // SPLIT_TASK
        Assert.assertEquals(AdaptiveColumnarScanStatus.Status.SUCCESS, row[index++]); // STATUS
        Assert.assertNull(row[index++]);                          // ERROR_MSG

        Assert.assertTrue(row[index++] instanceof Timestamp);     // START_TIME
        Assert.assertTrue(row[index++] instanceof Timestamp);     // SCHEDULE_TIME
        Assert.assertTrue(row[index++] instanceof Timestamp);     // END_TIME

        Assert.assertEquals(1000L, row[index++]);                 // QUEUE_TIME_COST (2000 - 1000)
        Assert.assertEquals(4000L, row[index++]);                 // RUNNING_TIME_COST (5000 - 1000)

        Assert.assertEquals(10, row[index++]);                    // START_ROW_GROUP_ID
        Assert.assertEquals(128, row[index++]);                   // GRANULARITY
        Assert.assertEquals(4, row[index++]);                     // THREAD_LIMIT
        Assert.assertEquals(100L, row[index++]);                  // ACQUIRED_FILTER_IO_PERMITS
        Assert.assertEquals(200L, row[index++]);                  // ACQUIRED_FILTER_PERMITS
        Assert.assertEquals(300L, row[index++]);                  // ACQUIRED_PROJECT_IO_PERMITS
        Assert.assertEquals(400L, row[index++]);                  // ACQUIRED_PROJECT_PERMITS
        Assert.assertEquals(50L, row[index++]);                   // REMAINING_PERMITS
        Assert.assertEquals(10000L, row[index++]);                // TOTAL_SCAN_ROWS
        Assert.assertEquals(20000L, row[index++]);                // TOTAL_SCAN_BYTES
        Assert.assertEquals(5000L, row[index++]);                 // TOTAL_FILTERED_ROWS
    }

    @Test
    public void testGeneratePacketsMultipleStatuses() {
        ScanWorkId.WorkId workId1 = createWorkId("q1", 0, 0);
        ScanWorkId.WorkId workId2 = createWorkId("q2", 1, 1);
        AtomicInteger seq1 = new AtomicInteger(0);
        AtomicInteger seq2 = new AtomicInteger(0);

        monitor.create(workId1, seq1, false);
        monitor.create(workId2, seq2, true);

        List<Object[]> packets = monitor.generatePackets();

        Assert.assertEquals(2, packets.size());
        for (Object[] row : packets) {
            Assert.assertEquals(26, row.length);
        }
    }

    @Test
    public void testGeneratePacketsTimeCostCalculation() {
        ScanWorkId.WorkId workId = createWorkId("q1", 0, 0);
        AtomicInteger sequenceNumber = new AtomicInteger(0);

        AdaptiveColumnarScanStatus status = monitor.create(workId, sequenceNumber, false);
        status.setStartTime(100L);
        status.setScheduleTime(300L);
        status.setEndTime(1000L);

        List<Object[]> packets = monitor.generatePackets();
        Object[] row = packets.get(0);

        // QUEUE_TIME_COST = scheduleTime - startTime = 300 - 100 = 200
        Assert.assertEquals(200L, row[13]);
        // RUNNING_TIME_COST = endTime - startTime = 1000 - 100 = 900
        Assert.assertEquals(900L, row[14]);
    }

    @Test
    public void testGeneratePacketsWithErrorMsg() {
        ScanWorkId.WorkId workId = createWorkId("q1", 0, 0);
        AtomicInteger sequenceNumber = new AtomicInteger(0);

        AdaptiveColumnarScanStatus status = monitor.create(workId, sequenceNumber, false);
        status.setStatus(AdaptiveColumnarScanStatus.Status.FAILED);
        status.setErrorMsg("OOM error");

        List<Object[]> packets = monitor.generatePackets();
        Object[] row = packets.get(0);

        Assert.assertEquals(AdaptiveColumnarScanStatus.Status.FAILED, row[8]);
        Assert.assertEquals("OOM error", row[9]);
    }

    @Test
    public void testConstants() {
        Assert.assertEquals(4096, AdaptiveColumnarScanMonitor.MAXIMUM_SIZE);
        Assert.assertEquals(7, AdaptiveColumnarScanMonitor.DURATION);
    }
}
