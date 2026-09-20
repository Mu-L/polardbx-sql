package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.common.cdc.BinlogDumpMetrics;
import com.alibaba.polardbx.common.cdc.BinlogDumpMetricsManager;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ShowBinlogDumpMetricsSyncActionTest {

    private BinlogDumpMetricsManager manager;

    @Before
    public void setUp() {
        manager = BinlogDumpMetricsManager.getInstance();
    }

    @After
    public void tearDown() {
        manager.unregister(1L);
        manager.unregister(2L);
        manager.unregister(3L);
    }

    @Test
    public void testSyncReturnsEmptyWhenNoMetrics() {
        ShowBinlogDumpMetricsSyncAction action = new ShowBinlogDumpMetricsSyncAction();
        ResultCursor result = action.sync();

        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof ArrayResultCursor);
        ArrayResultCursor cursor = (ArrayResultCursor) result;
        Assert.assertEquals(8, cursor.getReturnColumns().size());
        Assert.assertTrue(cursor.getRows().isEmpty());
    }

    @Test
    public void testSyncReturnsMetricsWithCorrectColumns() {
        BinlogDumpMetrics metrics = new BinlogDumpMetrics("trace-abc");
        metrics.markDumpStart();
        metrics.recordOnNext(1_000_000, 2_000_000, 3_000_000, false);
        manager.register(1L, metrics);

        ShowBinlogDumpMetricsSyncAction action = new ShowBinlogDumpMetricsSyncAction();
        ResultCursor result = action.sync();

        ArrayResultCursor cursor = (ArrayResultCursor) result;
        Assert.assertEquals(8, cursor.getReturnColumns().size());
        Assert.assertEquals("Trace_Id", cursor.getReturnColumns().get(0).getName());
        Assert.assertEquals("Recent_Avg_Fetch_Wait_Ms", cursor.getReturnColumns().get(1).getName());
        Assert.assertEquals("Recent_Avg_Process_Ms", cursor.getReturnColumns().get(2).getName());
        Assert.assertEquals("Recent_Avg_Write_Ms", cursor.getReturnColumns().get(3).getName());
        Assert.assertEquals("Idle_Ratio", cursor.getReturnColumns().get(4).getName());
        Assert.assertEquals("Fetch_Wait_Ratio", cursor.getReturnColumns().get(5).getName());
        Assert.assertEquals("Process_Ratio", cursor.getReturnColumns().get(6).getName());
        Assert.assertEquals("Write_Ratio", cursor.getReturnColumns().get(7).getName());
    }

    @Test
    public void testSyncReturnsCorrectMetricValues() {
        BinlogDumpMetrics metrics = new BinlogDumpMetrics("trace-123");
        metrics.markDumpStart();
        // fetchWait=1ms, process=2ms, write=3ms, not heartbeat
        metrics.recordOnNext(1_000_000, 2_000_000, 3_000_000, false);
        manager.register(1L, metrics);

        ShowBinlogDumpMetricsSyncAction action = new ShowBinlogDumpMetricsSyncAction();
        ArrayResultCursor cursor = (ArrayResultCursor) action.sync();

        List<Row> rows = cursor.getRows();
        Assert.assertEquals(1, rows.size());

        Row row = rows.get(0);
        Assert.assertEquals("trace-123", row.getString(0));

        // Recent avg fetch wait: 1ms
        double avgFetchWait = ((Number) row.getObject(1)).doubleValue();
        Assert.assertEquals(1.0, avgFetchWait, 0.01);

        // Recent avg process: 2ms
        double avgProcess = ((Number) row.getObject(2)).doubleValue();
        Assert.assertEquals(2.0, avgProcess, 0.01);

        // Recent avg write: 3ms
        double avgWrite = ((Number) row.getObject(3)).doubleValue();
        Assert.assertEquals(3.0, avgWrite, 0.01);

        // Idle ratio: 0 (no heartbeat)
        double idleRatio = ((Number) row.getObject(4)).doubleValue();
        Assert.assertEquals(0.0, idleRatio, 0.001);

        // Fetch wait ratio: 1/(1+2+3) = 1/6
        double fetchWaitRatio = ((Number) row.getObject(5)).doubleValue();
        Assert.assertEquals(1.0 / 6.0, fetchWaitRatio, 0.001);

        // Process ratio: 2/(1+2+3) = 2/6
        double processRatio = ((Number) row.getObject(6)).doubleValue();
        Assert.assertEquals(2.0 / 6.0, processRatio, 0.001);

        // Write ratio: 3/(1+2+3) = 3/6
        double writeRatio = ((Number) row.getObject(7)).doubleValue();
        Assert.assertEquals(3.0 / 6.0, writeRatio, 0.001);
    }

    @Test
    public void testSyncReturnsMultipleMetrics() {
        BinlogDumpMetrics m1 = new BinlogDumpMetrics("trace-a");
        m1.markDumpStart();
        m1.recordOnNext(1_000_000, 1_000_000, 1_000_000, false);
        manager.register(1L, m1);

        BinlogDumpMetrics m2 = new BinlogDumpMetrics("trace-b");
        m2.markDumpStart();
        m2.recordOnNext(2_000_000, 2_000_000, 2_000_000, true);
        manager.register(2L, m2);

        ShowBinlogDumpMetricsSyncAction action = new ShowBinlogDumpMetricsSyncAction();
        ArrayResultCursor cursor = (ArrayResultCursor) action.sync();

        List<Row> rows = cursor.getRows();
        Assert.assertEquals(2, rows.size());

        // Verify both trace IDs are present
        boolean foundA = false, foundB = false;
        for (Row row : rows) {
            String traceId = row.getString(0);
            if ("trace-a".equals(traceId)) {
                foundA = true;
            } else if ("trace-b".equals(traceId)) {
                foundB = true;
                // m2 recorded a heartbeat, so idle ratio should be 1.0
                double idleRatio = ((Number) row.getObject(4)).doubleValue();
                Assert.assertEquals(1.0, idleRatio, 0.001);
            }
        }
        Assert.assertTrue("trace-a should be present", foundA);
        Assert.assertTrue("trace-b should be present", foundB);
    }

    @Test
    public void testSyncSkipsNullTraceId() {
        BinlogDumpMetrics metrics = new BinlogDumpMetrics(null);
        metrics.markDumpStart();
        metrics.recordOnNext(1_000_000, 1_000_000, 1_000_000, false);
        manager.register(3L, metrics);

        ShowBinlogDumpMetricsSyncAction action = new ShowBinlogDumpMetricsSyncAction();
        ArrayResultCursor cursor = (ArrayResultCursor) action.sync();

        // Should not include the null-traceId metrics
        List<Row> rows = cursor.getRows();
        for (Row row : rows) {
            Assert.assertNotNull(row.getString(0));
        }
    }

    @Test
    public void testSyncWithNoRecordedData() {
        // Register metrics that has markDumpStart but no recordOnNext
        BinlogDumpMetrics metrics = new BinlogDumpMetrics("trace-empty");
        metrics.markDumpStart();
        manager.register(1L, metrics);

        ShowBinlogDumpMetricsSyncAction action = new ShowBinlogDumpMetricsSyncAction();
        ArrayResultCursor cursor = (ArrayResultCursor) action.sync();

        List<Row> rows = cursor.getRows();
        Assert.assertEquals(1, rows.size());

        Row row = rows.get(0);
        Assert.assertEquals("trace-empty", row.getString(0));
        // All averages should be 0
        Assert.assertEquals(0.0, ((Number) row.getObject(1)).doubleValue(), 0.001);
        Assert.assertEquals(0.0, ((Number) row.getObject(2)).doubleValue(), 0.001);
        Assert.assertEquals(0.0, ((Number) row.getObject(3)).doubleValue(), 0.001);
    }
}
