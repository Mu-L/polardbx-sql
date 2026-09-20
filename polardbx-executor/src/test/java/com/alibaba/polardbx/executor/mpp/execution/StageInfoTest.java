package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.columnar.ColumnarScanMetrics;
import com.alibaba.polardbx.common.columnar.VersionStorageStatistics;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.mpp.operator.DriverStats;
import com.alibaba.polardbx.executor.mpp.operator.TaskStats;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StageInfoTest {
    @Test
    public void test() {
        Map<String, List<Object[]>> driverStatistics = new HashMap<>();

        StageInfo stageInfo = Mockito.mock(StageInfo.class);

        StageId stageId = new StageId("mock_query_id", 0);
        Mockito.when(stageInfo.getStageId()).thenReturn(stageId);

        Mockito.when(stageInfo.isCompleteInfo()).thenReturn(true);

        TaskInfo taskInfo = Mockito.mock(TaskInfo.class);
        Mockito.when(stageInfo.getTasks()).thenReturn(ImmutableList.of(taskInfo));

        TaskStats taskStats = Mockito.mock(TaskStats.class);
        Mockito.when(taskInfo.getTaskStats()).thenReturn(taskStats);

        DriverStats driverStats = Mockito.mock(DriverStats.class);
        Mockito.when(taskStats.getDriverStats()).thenReturn(ImmutableList.of(driverStats));
        Mockito.when(driverStats.getDriverId()).thenReturn("12434d414.1.2.3.4");

        DriverContext.DriverRuntimeStatistics driverRuntimeStatistics = new DriverContext.DriverRuntimeStatistics(
            10000, 10000, 10000, 10000, 40000,
            10, 10, 10,
            "{ORC:1}",
            5000, 5000, 5000,
            "WAIT_DRIVER_CONSUMER_FINISHED:3{1/2/3}"
        );
        Mockito.when(driverStats.getDriverRuntimeStatistics()).thenReturn(driverRuntimeStatistics);

        StageInfo.collectStats(stageInfo, driverStatistics);

        Assert.assertTrue(driverStatistics.containsKey("mock_query_id.0"));

        List<Object[]> list = driverStatistics.get("mock_query_id.0");
        Object[] objects = list.get(0);

        Assert.assertTrue(objects[0].equals("12434d414"));
        Assert.assertTrue(objects[1].equals("1-3"));
        Assert.assertTrue(objects[2].equals("2"));
        Assert.assertTrue(objects[3].equals("4"));
        Assert.assertTrue(objects[4].equals(10000L));
        Assert.assertTrue(objects[5].equals(10000L));
        Assert.assertTrue(objects[6].equals(10000L));
        Assert.assertTrue(objects[7].equals(10000L));
        Assert.assertTrue(objects[8].equals(40000L));
        Assert.assertTrue(objects[9].equals(10));
        Assert.assertTrue(objects[10].equals(10));
        Assert.assertTrue(objects[11].equals(10));
        Assert.assertTrue(objects[12].equals("{ORC:1}"));
        Assert.assertTrue(objects[13].equals(5000L));
        Assert.assertTrue(objects[14].equals(5000L));
        Assert.assertTrue(objects[15].equals(5000L));
        Assert.assertTrue(objects[16].equals("WAIT_DRIVER_CONSUMER_FINISHED:3{1/2/3}"));
    }

    @Test
    public void testCollectTaskStatisticsWithAllStatistics() {
        // Test case: StageInfo with tasks that have all types of statistics
        StageInfo rootStage = Mockito.mock(StageInfo.class);

        // Create mock TaskInfo with all statistics
        TaskInfo taskInfo1 = Mockito.mock(TaskInfo.class);
        TaskInfo taskInfo2 = Mockito.mock(TaskInfo.class);
        Mockito.when(rootStage.getTasks()).thenReturn(ImmutableList.of(taskInfo1, taskInfo2));

        // Mock TaskId
        TaskId taskId1 = new TaskId("query1", 1, 1);
        TaskId taskId2 = new TaskId("query2", 2, 2);

        // Mock VersionStorageStatistics
        VersionStorageStatistics versionStats1 = new VersionStorageStatistics();
        VersionStorageStatistics versionStats2 = new VersionStorageStatistics();
        Pair<TaskId, VersionStorageStatistics> versionPair1 = Pair.of(taskId1, versionStats1);
        Pair<TaskId, VersionStorageStatistics> versionPair2 = Pair.of(taskId2, versionStats2);
        Mockito.when(taskInfo1.getVersionStorageStatisticsPair()).thenReturn(versionPair1);
        Mockito.when(taskInfo2.getVersionStorageStatisticsPair()).thenReturn(versionPair2);

        // Mock MaximumQueryMemoryUsage
        Pair<String, Long> memoryPair1 = Pair.of("node1", 1000L);
        Pair<String, Long> memoryPair2 = Pair.of("node2", 2000L);
        Mockito.when(taskInfo1.getMaximumQueryMemoryUsagePair()).thenReturn(memoryPair1);
        Mockito.when(taskInfo2.getMaximumQueryMemoryUsagePair()).thenReturn(memoryPair2);

        // Mock ColumnarScanMetrics
        ColumnarScanMetrics scanMetrics1 = new ColumnarScanMetrics(100, 1000, 10);
        ColumnarScanMetrics scanMetrics2 = new ColumnarScanMetrics(200, 2000, 20);
        Pair<TaskId, ColumnarScanMetrics> scanPair1 = Pair.of(taskId1, scanMetrics1);
        Pair<TaskId, ColumnarScanMetrics> scanPair2 = Pair.of(taskId2, scanMetrics2);
        Mockito.when(taskInfo1.getColumnarScanMetricsPair()).thenReturn(scanPair1);
        Mockito.when(taskInfo2.getColumnarScanMetricsPair()).thenReturn(scanPair2);

        // Mock no sub stages
        Mockito.when(rootStage.getSubStages()).thenReturn(ImmutableList.of());

        // Prepare result maps
        Map<TaskId, VersionStorageStatistics> versionStorageStatisticsMap = new HashMap<>();
        Map<String, Long> maximumQueryMemoryUsageMap = new HashMap<>();
        Map<TaskId, ColumnarScanMetrics> columnarScanMetricsMap = new HashMap<>();

        // Execute the method
        StageInfo.collectTaskStatistics(rootStage, versionStorageStatisticsMap,
            maximumQueryMemoryUsageMap, columnarScanMetricsMap);

        // Verify results
        Assert.assertEquals(2, versionStorageStatisticsMap.size());
        Assert.assertEquals(versionStats1, versionStorageStatisticsMap.get(taskId1));
        Assert.assertEquals(versionStats2, versionStorageStatisticsMap.get(taskId2));

        Assert.assertEquals(2, maximumQueryMemoryUsageMap.size());
        Assert.assertEquals(Long.valueOf(1000L), maximumQueryMemoryUsageMap.get("node1"));
        Assert.assertEquals(Long.valueOf(2000L), maximumQueryMemoryUsageMap.get("node2"));

        Assert.assertEquals(2, columnarScanMetricsMap.size());
        Assert.assertEquals(scanMetrics1, columnarScanMetricsMap.get(taskId1));
        Assert.assertEquals(scanMetrics2, columnarScanMetricsMap.get(taskId2));
    }

    @Test
    public void testCollectTaskStatisticsWithNullStatistics() {
        // Test case: StageInfo with tasks that have null statistics
        StageInfo rootStage = Mockito.mock(StageInfo.class);

        // Create mock TaskInfo with null statistics
        TaskInfo taskInfo = Mockito.mock(TaskInfo.class);
        Mockito.when(rootStage.getTasks()).thenReturn(ImmutableList.of(taskInfo));

        // Mock all statistics as null
        Mockito.when(taskInfo.getVersionStorageStatisticsPair()).thenReturn(null);
        Mockito.when(taskInfo.getMaximumQueryMemoryUsagePair()).thenReturn(null);
        Mockito.when(taskInfo.getColumnarScanMetricsPair()).thenReturn(null);

        // Mock no sub stages
        Mockito.when(rootStage.getSubStages()).thenReturn(ImmutableList.of());

        // Prepare result maps
        Map<TaskId, VersionStorageStatistics> versionStorageStatisticsMap = new HashMap<>();
        Map<String, Long> maximumQueryMemoryUsageMap = new HashMap<>();
        Map<TaskId, ColumnarScanMetrics> columnarScanMetricsMap = new HashMap<>();

        // Execute the method
        StageInfo.collectTaskStatistics(rootStage, versionStorageStatisticsMap,
            maximumQueryMemoryUsageMap, columnarScanMetricsMap);

        // Verify results - all maps should be empty since statistics are null
        Assert.assertEquals(0, versionStorageStatisticsMap.size());
        Assert.assertEquals(0, maximumQueryMemoryUsageMap.size());
        Assert.assertEquals(0, columnarScanMetricsMap.size());
    }

    @Test
    public void testCollectTaskStatisticsWithEmptyTasks() {
        // Test case: StageInfo with empty tasks list
        StageInfo rootStage = Mockito.mock(StageInfo.class);

        // Mock empty tasks list
        Mockito.when(rootStage.getTasks()).thenReturn(ImmutableList.of());

        // Mock no sub stages
        Mockito.when(rootStage.getSubStages()).thenReturn(ImmutableList.of());

        // Prepare result maps
        Map<TaskId, VersionStorageStatistics> versionStorageStatisticsMap = new HashMap<>();
        Map<String, Long> maximumQueryMemoryUsageMap = new HashMap<>();
        Map<TaskId, ColumnarScanMetrics> columnarScanMetricsMap = new HashMap<>();

        // Execute the method
        StageInfo.collectTaskStatistics(rootStage, versionStorageStatisticsMap,
            maximumQueryMemoryUsageMap, columnarScanMetricsMap);

        // Verify results - all maps should be empty since there are no tasks
        Assert.assertEquals(0, versionStorageStatisticsMap.size());
        Assert.assertEquals(0, maximumQueryMemoryUsageMap.size());
        Assert.assertEquals(0, columnarScanMetricsMap.size());
    }

    @Test
    public void testCollectTaskStatisticsWithSubStages() {
        // Test case: StageInfo with sub stages (recursive call)
        StageInfo rootStage = Mockito.mock(StageInfo.class);
        StageInfo subStage1 = Mockito.mock(StageInfo.class);
        StageInfo subStage2 = Mockito.mock(StageInfo.class);

        // Mock root stage with empty tasks but has sub stages
        Mockito.when(rootStage.getTasks()).thenReturn(ImmutableList.of());
        Mockito.when(rootStage.getSubStages()).thenReturn(ImmutableList.of(subStage1, subStage2));

        // Mock sub stage 1 with one task
        TaskInfo taskInfo1 = Mockito.mock(TaskInfo.class);
        Mockito.when(subStage1.getTasks()).thenReturn(ImmutableList.of(taskInfo1));
        Mockito.when(subStage1.getSubStages()).thenReturn(ImmutableList.of());

        TaskId taskId1 = new TaskId("query1", 1, 1);
        VersionStorageStatistics versionStats1 = new VersionStorageStatistics();
        Pair<TaskId, VersionStorageStatistics> versionPair1 = Pair.of(taskId1, versionStats1);
        Mockito.when(taskInfo1.getVersionStorageStatisticsPair()).thenReturn(versionPair1);
        Mockito.when(taskInfo1.getMaximumQueryMemoryUsagePair()).thenReturn(null);
        Mockito.when(taskInfo1.getColumnarScanMetricsPair()).thenReturn(null);

        // Mock sub stage 2 with one task
        TaskInfo taskInfo2 = Mockito.mock(TaskInfo.class);
        Mockito.when(subStage2.getTasks()).thenReturn(ImmutableList.of(taskInfo2));
        Mockito.when(subStage2.getSubStages()).thenReturn(ImmutableList.of());

        TaskId taskId2 = new TaskId("query2", 2, 2);
        Pair<String, Long> memoryPair2 = Pair.of("node2", 2000L);
        Mockito.when(taskInfo2.getVersionStorageStatisticsPair()).thenReturn(null);
        Mockito.when(taskInfo2.getMaximumQueryMemoryUsagePair()).thenReturn(memoryPair2);
        Mockito.when(taskInfo2.getColumnarScanMetricsPair()).thenReturn(null);

        // Prepare result maps
        Map<TaskId, VersionStorageStatistics> versionStorageStatisticsMap = new HashMap<>();
        Map<String, Long> maximumQueryMemoryUsageMap = new HashMap<>();
        Map<TaskId, ColumnarScanMetrics> columnarScanMetricsMap = new HashMap<>();

        // Execute the method
        StageInfo.collectTaskStatistics(rootStage, versionStorageStatisticsMap,
            maximumQueryMemoryUsageMap, columnarScanMetricsMap);

        // Verify results from sub stages
        Assert.assertEquals(1, versionStorageStatisticsMap.size());
        Assert.assertEquals(versionStats1, versionStorageStatisticsMap.get(taskId1));

        Assert.assertEquals(1, maximumQueryMemoryUsageMap.size());
        Assert.assertEquals(Long.valueOf(2000L), maximumQueryMemoryUsageMap.get("node2"));

        Assert.assertEquals(0, columnarScanMetricsMap.size());
    }

    @Test
    public void testCollectTaskStatisticsWithNullSubStages() {
        // Test case: StageInfo with null sub stages
        StageInfo rootStage = Mockito.mock(StageInfo.class);

        // Create mock TaskInfo
        TaskInfo taskInfo = Mockito.mock(TaskInfo.class);
        Mockito.when(rootStage.getTasks()).thenReturn(ImmutableList.of(taskInfo));

        // Mock statistics
        TaskId taskId = new TaskId("query1", 1, 1);
        VersionStorageStatistics versionStats = new VersionStorageStatistics();
        Pair<TaskId, VersionStorageStatistics> versionPair = Pair.of(taskId, versionStats);
        Mockito.when(taskInfo.getVersionStorageStatisticsPair()).thenReturn(versionPair);
        Mockito.when(taskInfo.getMaximumQueryMemoryUsagePair()).thenReturn(null);
        Mockito.when(taskInfo.getColumnarScanMetricsPair()).thenReturn(null);

        // Mock null sub stages
        Mockito.when(rootStage.getSubStages()).thenReturn(null);

        // Prepare result maps
        Map<TaskId, VersionStorageStatistics> versionStorageStatisticsMap = new HashMap<>();
        Map<String, Long> maximumQueryMemoryUsageMap = new HashMap<>();
        Map<TaskId, ColumnarScanMetrics> columnarScanMetricsMap = new HashMap<>();

        // Execute the method
        StageInfo.collectTaskStatistics(rootStage, versionStorageStatisticsMap,
            maximumQueryMemoryUsageMap, columnarScanMetricsMap);

        // Verify results
        Assert.assertEquals(1, versionStorageStatisticsMap.size());
        Assert.assertEquals(versionStats, versionStorageStatisticsMap.get(taskId));
        Assert.assertEquals(0, maximumQueryMemoryUsageMap.size());
        Assert.assertEquals(0, columnarScanMetricsMap.size());
    }

    @Test
    public void testCollectTaskStatisticsWithMixedStatistics() {
        // Test case: StageInfo with tasks having mixed statistics (some null, some not)
        StageInfo rootStage = Mockito.mock(StageInfo.class);

        // Create mock TaskInfos
        TaskInfo taskInfo1 = Mockito.mock(TaskInfo.class);
        TaskInfo taskInfo2 = Mockito.mock(TaskInfo.class);
        TaskInfo taskInfo3 = Mockito.mock(TaskInfo.class);
        Mockito.when(rootStage.getTasks()).thenReturn(ImmutableList.of(taskInfo1, taskInfo2, taskInfo3));

        // Task 1: only version statistics
        TaskId taskId1 = new TaskId("query1", 1, 1);
        VersionStorageStatistics versionStats1 = new VersionStorageStatistics();
        Pair<TaskId, VersionStorageStatistics> versionPair1 = Pair.of(taskId1, versionStats1);
        Mockito.when(taskInfo1.getVersionStorageStatisticsPair()).thenReturn(versionPair1);
        Mockito.when(taskInfo1.getMaximumQueryMemoryUsagePair()).thenReturn(null);
        Mockito.when(taskInfo1.getColumnarScanMetricsPair()).thenReturn(null);

        // Task 2: only memory statistics
        Pair<String, Long> memoryPair2 = Pair.of("node2", 2000L);
        Mockito.when(taskInfo2.getVersionStorageStatisticsPair()).thenReturn(null);
        Mockito.when(taskInfo2.getMaximumQueryMemoryUsagePair()).thenReturn(memoryPair2);
        Mockito.when(taskInfo2.getColumnarScanMetricsPair()).thenReturn(null);

        // Task 3: only scan metrics
        TaskId taskId3 = new TaskId("query3", 3, 3);
        ColumnarScanMetrics scanMetrics3 = new ColumnarScanMetrics(300, 3000, 30);
        Pair<TaskId, ColumnarScanMetrics> scanPair3 = Pair.of(taskId3, scanMetrics3);
        Mockito.when(taskInfo3.getVersionStorageStatisticsPair()).thenReturn(null);
        Mockito.when(taskInfo3.getMaximumQueryMemoryUsagePair()).thenReturn(null);
        Mockito.when(taskInfo3.getColumnarScanMetricsPair()).thenReturn(scanPair3);

        // Mock no sub stages
        Mockito.when(rootStage.getSubStages()).thenReturn(ImmutableList.of());

        // Prepare result maps
        Map<TaskId, VersionStorageStatistics> versionStorageStatisticsMap = new HashMap<>();
        Map<String, Long> maximumQueryMemoryUsageMap = new HashMap<>();
        Map<TaskId, ColumnarScanMetrics> columnarScanMetricsMap = new HashMap<>();

        // Execute the method
        StageInfo.collectTaskStatistics(rootStage, versionStorageStatisticsMap,
            maximumQueryMemoryUsageMap, columnarScanMetricsMap);

        // Verify results
        Assert.assertEquals(1, versionStorageStatisticsMap.size());
        Assert.assertEquals(versionStats1, versionStorageStatisticsMap.get(taskId1));

        Assert.assertEquals(1, maximumQueryMemoryUsageMap.size());
        Assert.assertEquals(Long.valueOf(2000L), maximumQueryMemoryUsageMap.get("node2"));

        Assert.assertEquals(1, columnarScanMetricsMap.size());
        Assert.assertEquals(scanMetrics3, columnarScanMetricsMap.get(taskId3));
    }
}