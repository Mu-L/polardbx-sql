package com.alibaba.polardbx.common.cdc;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BinlogDumpMetricsTest {

    private BinlogDumpMetrics metrics;

    @Before
    public void setUp() {
        metrics = new BinlogDumpMetrics("trace-001");
    }

    // ==================== Constructor & Getters ====================

    @Test
    public void testConstructorSetsTraceId() {
        Assert.assertEquals("trace-001", metrics.getTraceId());
    }

    @Test
    public void testInitialStateIsZero() {
        Assert.assertEquals(0, metrics.getLastOnNextEndNanos());
        Assert.assertEquals(0.0, metrics.getFetchWaitRatio(), 0.0);
        Assert.assertEquals(0.0, metrics.getProcessRatio(), 0.0);
        Assert.assertEquals(0.0, metrics.getWriteRatio(), 0.0);
        Assert.assertEquals(0.0, metrics.getIdleRatio(), 0.0);
        Assert.assertEquals(0.0, metrics.getRecentAvgFetchWaitMs(), 0.0);
        Assert.assertEquals(0.0, metrics.getRecentAvgProcessMs(), 0.0);
        Assert.assertEquals(0.0, metrics.getRecentAvgWriteMs(), 0.0);
    }

    // ==================== markDumpStart ====================

    @Test
    public void testMarkDumpStart() {
        Assert.assertEquals(0, metrics.getLastOnNextEndNanos());
        metrics.markDumpStart();
        Assert.assertTrue(metrics.getLastOnNextEndNanos() > 0);
    }

    // ==================== recordOnNext ====================

    @Test
    public void testRecordOnNextAccumulatesTotals() {
        metrics.recordOnNext(100_000, 200_000, 300_000, false);
        // total = 600_000, fetch ratio = 100/600
        Assert.assertEquals(100_000.0 / 600_000.0, metrics.getFetchWaitRatio(), 0.0001);
        Assert.assertEquals(200_000.0 / 600_000.0, metrics.getProcessRatio(), 0.0001);
        Assert.assertEquals(300_000.0 / 600_000.0, metrics.getWriteRatio(), 0.0001);

        metrics.recordOnNext(150_000, 250_000, 350_000, false);
        // totals: fetch=250_000, process=450_000, write=650_000, total=1_350_000
        Assert.assertEquals(250_000.0 / 1_350_000.0, metrics.getFetchWaitRatio(), 0.0001);
        Assert.assertEquals(450_000.0 / 1_350_000.0, metrics.getProcessRatio(), 0.0001);
        Assert.assertEquals(650_000.0 / 1_350_000.0, metrics.getWriteRatio(), 0.0001);
    }

    @Test
    public void testRecordOnNextUpdatesLastOnNextEndNanos() {
        long before = System.nanoTime();
        metrics.recordOnNext(100_000, 200_000, 300_000, false);
        long after = System.nanoTime();
        Assert.assertTrue(metrics.getLastOnNextEndNanos() >= before);
        Assert.assertTrue(metrics.getLastOnNextEndNanos() <= after);
    }

    @Test
    public void testRecordOnNextHeartbeatAccumulatesIdleTime() {
        metrics.recordOnNext(500_000, 10_000, 20_000, true);
        metrics.recordOnNext(200_000, 10_000, 20_000, false);

        // idle ratio = 500000 / 700000
        double expectedIdle = 500_000.0 / 700_000.0;
        Assert.assertEquals(expectedIdle, metrics.getIdleRatio(), 0.0001);
    }

    // ==================== Ratio Calculations ====================

    @Test
    public void testRatiosZeroWhenNoData() {
        Assert.assertEquals(0.0, metrics.getFetchWaitRatio(), 0.0);
        Assert.assertEquals(0.0, metrics.getProcessRatio(), 0.0);
        Assert.assertEquals(0.0, metrics.getWriteRatio(), 0.0);
        Assert.assertEquals(0.0, metrics.getIdleRatio(), 0.0);
    }

    @Test
    public void testRatiosSumToOne() {
        metrics.recordOnNext(1_000_000, 2_000_000, 3_000_000, false);
        double sum = metrics.getFetchWaitRatio() + metrics.getProcessRatio() + metrics.getWriteRatio();
        Assert.assertEquals(1.0, sum, 0.0001);
    }

    @Test
    public void testRatiosCorrectValues() {
        // total = 6_000_000
        metrics.recordOnNext(1_000_000, 2_000_000, 3_000_000, false);
        Assert.assertEquals(1.0 / 6.0, metrics.getFetchWaitRatio(), 0.0001);
        Assert.assertEquals(2.0 / 6.0, metrics.getProcessRatio(), 0.0001);
        Assert.assertEquals(3.0 / 6.0, metrics.getWriteRatio(), 0.0001);
    }

    @Test
    public void testRatiosWithMultipleRecords() {
        metrics.recordOnNext(100, 200, 300, false);
        metrics.recordOnNext(400, 100, 100, false);
        // totals: fetch=500, process=300, write=400, total=1200
        Assert.assertEquals(500.0 / 1200.0, metrics.getFetchWaitRatio(), 0.0001);
        Assert.assertEquals(300.0 / 1200.0, metrics.getProcessRatio(), 0.0001);
        Assert.assertEquals(400.0 / 1200.0, metrics.getWriteRatio(), 0.0001);
    }

    @Test
    public void testIdleRatioAllHeartbeats() {
        metrics.recordOnNext(1_000_000, 10_000, 10_000, true);
        metrics.recordOnNext(2_000_000, 10_000, 10_000, true);
        // idle = 3_000_000 / 3_000_000 = 1.0
        Assert.assertEquals(1.0, metrics.getIdleRatio(), 0.0001);
    }

    @Test
    public void testIdleRatioNoHeartbeats() {
        metrics.recordOnNext(1_000_000, 10_000, 10_000, false);
        metrics.recordOnNext(2_000_000, 10_000, 10_000, false);
        Assert.assertEquals(0.0, metrics.getIdleRatio(), 0.0001);
    }

    @Test
    public void testIdleRatioMixed() {
        metrics.recordOnNext(3_000_000, 10_000, 10_000, true);  // idle
        metrics.recordOnNext(1_000_000, 10_000, 10_000, false);  // data
        // idle = 3_000_000 / (3_000_000 + 1_000_000) = 0.75
        Assert.assertEquals(0.75, metrics.getIdleRatio(), 0.0001);
    }

    // ==================== Recent Avg (Sliding Window) ====================

    @Test
    public void testRecentAvgZeroWhenNoData() {
        Assert.assertEquals(0.0, metrics.getRecentAvgFetchWaitMs(), 0.0);
        Assert.assertEquals(0.0, metrics.getRecentAvgProcessMs(), 0.0);
        Assert.assertEquals(0.0, metrics.getRecentAvgWriteMs(), 0.0);
    }

    @Test
    public void testRecentAvgSingleRecord() {
        // 1_000_000 nanos = 1.0 ms
        metrics.recordOnNext(1_000_000, 2_000_000, 3_000_000, false);
        Assert.assertEquals(1.0, metrics.getRecentAvgFetchWaitMs(), 0.0001);
        Assert.assertEquals(2.0, metrics.getRecentAvgProcessMs(), 0.0001);
        Assert.assertEquals(3.0, metrics.getRecentAvgWriteMs(), 0.0001);
    }

    @Test
    public void testRecentAvgMultipleRecords() {
        metrics.recordOnNext(1_000_000, 2_000_000, 4_000_000, false);
        metrics.recordOnNext(3_000_000, 4_000_000, 6_000_000, false);
        // avg fetch = (1+3)/2 = 2ms, avg process = (2+4)/2 = 3ms, avg write = (4+6)/2 = 5ms
        Assert.assertEquals(2.0, metrics.getRecentAvgFetchWaitMs(), 0.0001);
        Assert.assertEquals(3.0, metrics.getRecentAvgProcessMs(), 0.0001);
        Assert.assertEquals(5.0, metrics.getRecentAvgWriteMs(), 0.0001);
    }

    @Test
    public void testRecentAvgWindowWraparound() {
        int windowSize = 10000;
        // Fill with value 1_000_000 (1ms)
        for (int i = 0; i < windowSize; i++) {
            metrics.recordOnNext(1_000_000, 1_000_000, 1_000_000, false);
        }
        Assert.assertEquals(1.0, metrics.getRecentAvgFetchWaitMs(), 0.0001);

        // Now overwrite half the window with 3_000_000 (3ms)
        for (int i = 0; i < windowSize / 2; i++) {
            metrics.recordOnNext(3_000_000, 3_000_000, 3_000_000, false);
        }
        // Window has 5000 entries of 1ms and 5000 entries of 3ms -> avg = 2ms
        Assert.assertEquals(2.0, metrics.getRecentAvgFetchWaitMs(), 0.0001);

        // Overwrite entire window with 5_000_000 (5ms)
        for (int i = 0; i < windowSize; i++) {
            metrics.recordOnNext(5_000_000, 5_000_000, 5_000_000, false);
        }
        Assert.assertEquals(5.0, metrics.getRecentAvgFetchWaitMs(), 0.0001);
    }

    // ==================== isHeartbeatPacket ====================

    @Test
    public void testIsHeartbeatPacketNull() {
        Assert.assertFalse(BinlogDumpMetrics.isHeartbeatPacket(null));
    }

    @Test
    public void testIsHeartbeatPacketEmptyArray() {
        Assert.assertFalse(BinlogDumpMetrics.isHeartbeatPacket(new byte[0]));
    }

    @Test
    public void testIsHeartbeatPacketTooShort() {
        // payload shorter than EVENT_TYPE_OFFSET + 1
        byte[] shortPayload = new byte[9];
        Assert.assertFalse(BinlogDumpMetrics.isHeartbeatPacket(shortPayload));
    }

    @Test
    public void testIsHeartbeatPacketExactMinLength() {
        // payload[9] = 27 (exactly length 10)
        byte[] payload = new byte[10];
        payload[9] = 27;
        Assert.assertTrue(BinlogDumpMetrics.isHeartbeatPacket(payload));
    }

    @Test
    public void testIsHeartbeatPacketTrue() {
        // Simulate heartbeat packet structure:
        // [packet_len(3) + seq(1) + status(1) + timestamp(4) + event_type(1) + ...]
        byte[] payload = new byte[30];
        payload[0] = 0x19;  // packet length low byte
        payload[1] = 0x00;
        payload[2] = 0x00;
        payload[3] = 0x01;  // sequence
        payload[4] = 0x00;  // status OK
        // timestamp = 0 (4 bytes)
        payload[5] = 0x00;
        payload[6] = 0x00;
        payload[7] = 0x00;
        payload[8] = 0x00;
        // event_type = 27 (HEARTBEAT_LOG_EVENT)
        payload[9] = 27;
        Assert.assertTrue(BinlogDumpMetrics.isHeartbeatPacket(payload));
    }

    @Test
    public void testIsHeartbeatPacketFalseWithDataEvent() {
        // Simulate a WRITE_ROWS event (type = 30)
        byte[] payload = new byte[30];
        payload[9] = 30;  // WRITE_ROWS_EVENT
        Assert.assertFalse(BinlogDumpMetrics.isHeartbeatPacket(payload));
    }

    @Test
    public void testIsHeartbeatPacketFalseWithQueryEvent() {
        // Simulate a QUERY event (type = 2)
        byte[] payload = new byte[30];
        payload[9] = 2;  // QUERY_EVENT
        Assert.assertFalse(BinlogDumpMetrics.isHeartbeatPacket(payload));
    }

    @Test
    public void testIsHeartbeatPacketFalseWithRotateEvent() {
        // Simulate a ROTATE event (type = 4)
        byte[] payload = new byte[30];
        payload[9] = 4;  // ROTATE_EVENT
        Assert.assertFalse(BinlogDumpMetrics.isHeartbeatPacket(payload));
    }

    @Test
    public void testIsHeartbeatPacketUnsignedByteHandling() {
        // Ensure unsigned byte comparison works correctly
        // Test with value > 127 to verify & 0xFF logic
        byte[] payload = new byte[30];
        payload[9] = (byte) 0xFF;  // 255 unsigned, should NOT match 27
        Assert.assertFalse(BinlogDumpMetrics.isHeartbeatPacket(payload));

        payload[9] = (byte) 0x1B;  // 27 = 0x1B
        Assert.assertTrue(BinlogDumpMetrics.isHeartbeatPacket(payload));
    }

    // ==================== Constants ====================

    @Test
    public void testConstants() {
        Assert.assertEquals(27, BinlogDumpMetrics.HEARTBEAT_LOG_EVENT);
        Assert.assertEquals(9, BinlogDumpMetrics.EVENT_TYPE_OFFSET);
    }
}
