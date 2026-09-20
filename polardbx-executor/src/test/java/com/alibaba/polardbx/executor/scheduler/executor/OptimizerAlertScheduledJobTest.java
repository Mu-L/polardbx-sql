package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

public class OptimizerAlertScheduledJobTest {
    private MockedStatic<EventLogger> mockedEventLogger;

    @Before
    public void setUp() {
        mockedEventLogger = mockStatic(EventLogger.class);
    }

    @After
    public void tearDown() {
        mockedEventLogger.close();
    }

    private OptimizerAlertScheduledJob createJob() {
        return new OptimizerAlertScheduledJob(null); // 不需要实际的 job 实例
    }

    @Test
    public void testGenRemark_EmptyResults() {
        OptimizerAlertScheduledJob job = createJob();
        List<List<Map<String, Object>>> results = new ArrayList<>();
        String result = job.genRemark(results);
        assertEquals(OptimizerAlertScheduledJob.NO_ALERT, result);
    }

    @Test
    public void testGenRemark_EmptyMapResults() {
        OptimizerAlertScheduledJob job = createJob();
        List<List<Map<String, Object>>> results = new ArrayList<>();
        results.add(new ArrayList<>());
        String result = job.genRemark(results);
        assertEquals(OptimizerAlertScheduledJob.NO_ALERT, result);
    }

    @Test
    public void testGenRemark_OnlyStatisticAlert() {

        OptimizerAlertScheduledJob job = createJob();

        List<Map<String, Object>> nodeRows = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("COUNT", 1L);
        row.put("ALERT_TYPE", "STATISTIC_MISS");
        row.put("COMPUTE_NODE", "node1");
        nodeRows.add(row);

        List<List<Map<String, Object>>> results = Collections.singletonList(nodeRows);

        String result = job.genRemark(results);

        mockedEventLogger.verify(() -> EventLogger.log(eq(EventType.STATISTIC_ALERT), anyString()), times(1));
        assertEquals("1 statistic alerts found:STATISTIC_MISS", result);
    }

    @Test
    public void testGenRemark_OnlySpmAlert() {

        OptimizerAlertScheduledJob job = createJob();

        List<Map<String, Object>> nodeRows = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("COUNT", 1L);
        row.put("ALERT_TYPE", "SPM_PLAN_BUILD_ERR");
        row.put("COMPUTE_NODE", "node1");
        nodeRows.add(row);

        List<List<Map<String, Object>>> results = Collections.singletonList(nodeRows);

        String result = job.genRemark(results);

        mockedEventLogger.verify(() -> EventLogger.log(eq(EventType.SPM_ALERT), anyString()), times(1));
        assertEquals("1 spm alerts found:SPM_PLAN_BUILD_ERR", result);
    }

    @Test
    public void testGenRemark_OnlyOptimizerAlert() {
        OptimizerAlertScheduledJob job = createJob();

        List<Map<String, Object>> nodeRows = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("COUNT", 1L);
        row.put("ALERT_TYPE", "OPTIMIZER_SLOW");
        row.put("COMPUTE_NODE", "node1");
        nodeRows.add(row);

        List<List<Map<String, Object>>> results = Collections.singletonList(nodeRows);

        String result = job.genRemark(results);

        mockedEventLogger.verify(() -> EventLogger.log(eq(EventType.OPTIMIZER_ALERT), anyString()), times(1));
        assertEquals("1 optimizer alerts found:OPTIMIZER_SLOW", result);
    }

    @Test
    public void testGenRemark_AllTypes() {

        OptimizerAlertScheduledJob job = createJob();

        List<Map<String, Object>> nodeRows = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("COUNT", 1L);
        row1.put("ALERT_TYPE", "STATISTIC_MISS");
        row1.put("COMPUTE_NODE", "node1");

        Map<String, Object> row2 = new HashMap<>();
        row2.put("COUNT", 1L);
        row2.put("ALERT_TYPE", "SPM_PLAN_BUILD_ERR");
        row2.put("COMPUTE_NODE", "node1");

        Map<String, Object> row3 = new HashMap<>();
        row3.put("COUNT", 1L);
        row3.put("ALERT_TYPE", "OPTIMIZER_SLOW");
        row3.put("COMPUTE_NODE", "node2");

        Map<String, Object> row4 = new HashMap<>();
        row4.put("COUNT", 1L);
        row4.put("ALERT_TYPE", "SPM_PLAN_COST_ERR");
        row4.put("COMPUTE_NODE", "node1");
        nodeRows.add(row1);
        nodeRows.add(row2);
        nodeRows.add(row3);
        nodeRows.add(row4);

        List<List<Map<String, Object>>> results = Collections.singletonList(nodeRows);

        mockedEventLogger.when(() -> EventLogger.log(eq(EventType.STATISTIC_ALERT), anyString())).
            thenAnswer(invocation -> {
                String msg = invocation.getArgument(1);
                assertEquals("node1{STATISTIC_MISS:1,},", msg);
                return null;
            });
        mockedEventLogger.when(() -> EventLogger.log(eq(EventType.SPM_ALERT), anyString())).
            thenAnswer(invocation -> {
                String msg = invocation.getArgument(1);
                assertEquals("node1{SPM_PLAN_BUILD_ERR:1,SPM_PLAN_COST_ERR:1,},", msg);
                return null;
            });

        mockedEventLogger.when(() -> EventLogger.log(eq(EventType.OPTIMIZER_ALERT), anyString())).
            thenAnswer(invocation -> {
                String msg = invocation.getArgument(1);
                assertEquals("node2{OPTIMIZER_SLOW:1,},", msg);
                return null;
            });
        String result = job.genRemark(results);

        assertEquals(
            "1 statistic alerts found:STATISTIC_MISS,2 spm alerts found:SPM_PLAN_BUILD_ERR,SPM_PLAN_COST_ERR,1 optimizer alerts found:OPTIMIZER_SLOW",
            result);
    }
}