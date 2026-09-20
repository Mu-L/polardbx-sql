package com.alibaba.polardbx.statistics;

import com.alibaba.polardbx.common.columnar.ColumnarScanMetrics;
import com.alibaba.polardbx.common.columnar.VersionStorageStatistics;
import com.alibaba.polardbx.executor.mpp.execution.TaskId;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RuntimeStatistics class covering selected methods
 */
public class RuntimeStatisticsTest {

    private RuntimeStatistics runtimeStatistics;

    @Mock
    private ExecutionContext executionContext;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        runtimeStatistics = new RuntimeStatistics("test-schema", executionContext);
    }

    @Test
    public void testUpdateSqlMemoryMaxUsageInfo_withNullInput() {
        // Test with null input
        runtimeStatistics.updateSqlMemoryMaxUsageInfo(null);

        Map<String, Long> result = runtimeStatistics.getMaxMemoryUsageForEachNode();
        assertTrue("Should be empty when input is null", result.isEmpty());
    }

    @Test
    public void testUpdateSqlMemoryMaxUsageInfo_withValidInput() {
        // Test with valid input
        Map<String, Long> maxQueryMemoryUsage = new HashMap<>();
        maxQueryMemoryUsage.put("host1:3306", 1024L);
        maxQueryMemoryUsage.put("host2:3306", 2048L);

        runtimeStatistics.updateSqlMemoryMaxUsageInfo(maxQueryMemoryUsage);

        Map<String, Long> result = runtimeStatistics.getMaxMemoryUsageForEachNode();
        assertEquals("Should have 2 entries", 2, result.size());
        assertEquals("host1:3306 should have correct value", Long.valueOf(1024L), result.get("host1:3306"));
        assertEquals("host2:3306 should have correct value", Long.valueOf(2048L), result.get("host2:3306"));
    }

    @Test
    public void testUpdateSqlMemoryMaxUsageInfo_withUpdatedValues() {
        // Test updating with higher values
        Map<String, Long> initialUsage = new HashMap<>();
        initialUsage.put("host1:3306", 1024L);
        runtimeStatistics.updateSqlMemoryMaxUsageInfo(initialUsage);

        Map<String, Long> updatedUsage = new HashMap<>();
        updatedUsage.put("host1:3306", 2048L); // Higher value
        runtimeStatistics.updateSqlMemoryMaxUsageInfo(updatedUsage);

        Map<String, Long> result = runtimeStatistics.getMaxMemoryUsageForEachNode();
        assertEquals("Should keep the maximum value", Long.valueOf(2048L), result.get("host1:3306"));
    }

    @Test
    public void testUpdateSqlMemoryMaxUsageInfo_withLowerValues() {
        // Test updating with lower values (should keep the higher one)
        Map<String, Long> initialUsage = new HashMap<>();
        initialUsage.put("host1:3306", 2048L);
        runtimeStatistics.updateSqlMemoryMaxUsageInfo(initialUsage);

        Map<String, Long> updatedUsage = new HashMap<>();
        updatedUsage.put("host1:3306", 1024L); // Lower value
        runtimeStatistics.updateSqlMemoryMaxUsageInfo(updatedUsage);

        Map<String, Long> result = runtimeStatistics.getMaxMemoryUsageForEachNode();
        assertEquals("Should keep the maximum value", Long.valueOf(2048L), result.get("host1:3306"));
    }

    @Test
    public void testGetMaxMemoryUsageForEachNode() {
        Map<String, Long> maxQueryMemoryUsage = new HashMap<>();
        maxQueryMemoryUsage.put("host1:3306", 1024L);
        maxQueryMemoryUsage.put("host2:3306", 2048L);

        runtimeStatistics.updateSqlMemoryMaxUsageInfo(maxQueryMemoryUsage);

        Map<String, Long> result = runtimeStatistics.getMaxMemoryUsageForEachNode();
        assertNotNull("Result should not be null", result);
        assertEquals("Should have correct size", 2, result.size());
        assertTrue("Should contain host1:3306", result.containsKey("host1:3306"));
        assertTrue("Should contain host2:3306", result.containsKey("host2:3306"));
    }

    @Test
    public void testGetMppMaxMemoryUsageInfo_withEmptyMap() {
        String result = runtimeStatistics.getMppMaxMemoryUsageInfo();
        assertEquals("Should return '0' for empty map", "0", result);
    }

    @Test
    public void testGetMppMaxMemoryUsageInfo_withSingleNode() {
        Map<String, Long> maxQueryMemoryUsage = new HashMap<>();
        maxQueryMemoryUsage.put("host1:3306", 1024L);

        runtimeStatistics.updateSqlMemoryMaxUsageInfo(maxQueryMemoryUsage);

        String result = runtimeStatistics.getMppMaxMemoryUsageInfo();
        assertTrue("Should contain count", result.contains("cnt-1"));
        assertTrue("Should contain max", result.contains("max-1.0KB"));
        assertTrue("Should contain min", result.contains("min-1.0KB"));
        assertTrue("Should contain avg", result.contains("avg-1.0KB"));
        assertTrue("Should contain sum", result.contains("sum-1.0KB"));
    }

    @Test
    public void testGetMppMaxMemoryUsageInfo_withMultipleNodes() {
        Map<String, Long> maxQueryMemoryUsage = new HashMap<>();
        maxQueryMemoryUsage.put("host1:3306", 1024L);
        maxQueryMemoryUsage.put("host2:3306", 2048L);
        maxQueryMemoryUsage.put("host3:3306", 3072L);

        runtimeStatistics.updateSqlMemoryMaxUsageInfo(maxQueryMemoryUsage);

        String result = runtimeStatistics.getMppMaxMemoryUsageInfo();
        assertTrue("Should contain count", result.contains("cnt-3"));
        assertTrue("Should contain max", result.contains("max-3.0KB"));
        assertTrue("Should contain min", result.contains("min-1.0KB"));
        assertTrue("Should contain avg", result.contains("avg-2.0KB"));
        assertTrue("Should contain sum", result.contains("sum-6.0KB"));
    }

    @Test
    public void testToMemorySizeString_bytes() throws Exception {
        Method method = RuntimeStatistics.class.getDeclaredMethod("toMemorySizeString", long.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, 512L);
        assertEquals("Should format bytes correctly", "512.0B", result);
    }

    @Test
    public void testToMemorySizeString_kilobytes() throws Exception {
        Method method = RuntimeStatistics.class.getDeclaredMethod("toMemorySizeString", long.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, 1536L); // 1.5KB
        assertEquals("Should format KB correctly", "1.5KB", result);
    }

    @Test
    public void testToMemorySizeString_megabytes() throws Exception {
        Method method = RuntimeStatistics.class.getDeclaredMethod("toMemorySizeString", long.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, 1572864L); // 1.5MB
        assertEquals("Should format MB correctly", "1.5MB", result);
    }

    @Test
    public void testToMemorySizeString_gigabytes() throws Exception {
        Method method = RuntimeStatistics.class.getDeclaredMethod("toMemorySizeString", long.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, 1610612736L); // 1.5GB
        assertEquals("Should format GB correctly", "1.5GB", result);
    }

    @Test
    public void testUpdateVersionStorageStatistics() {
        Map<TaskId, VersionStorageStatistics> versionStorageStatistics = new HashMap<>();
        TaskId taskId1 = new TaskId("query1", 1, 1);
        TaskId taskId2 = new TaskId("query1", 1, 2);

        VersionStorageStatistics stats1 = new VersionStorageStatistics();
        VersionStorageStatistics stats2 = new VersionStorageStatistics();

        versionStorageStatistics.put(taskId1, stats1);
        versionStorageStatistics.put(taskId2, stats2);

        runtimeStatistics.updateVersionStorageStatistics(versionStorageStatistics);

        // Verify the statistics were updated
        String result = runtimeStatistics.getVersionStorageStatisticsInfo();
        assertNotNull("Version storage statistics info should not be null", result);
        assertTrue("Should contain statistics format", result.contains("orcRtCount"));
    }

    @Test
    public void testGetVersionStorageStatisticsInfo_withEmptyMap() {
        String result = runtimeStatistics.getVersionStorageStatisticsInfo();
        assertNotNull("Should not return null", result);
        assertTrue("Should contain default statistics", result.contains("orcRtCount-0"));
    }

    @Test
    public void testUpdateColumnarScanMetrics() {
        Map<TaskId, ColumnarScanMetrics> columnarScanMetrics = new HashMap<>();
        TaskId taskId1 = new TaskId("query1", 1, 1);
        TaskId taskId2 = new TaskId("query1", 1, 2);

        ColumnarScanMetrics metrics1 = new ColumnarScanMetrics(100L, 1024L, 50L);
        ColumnarScanMetrics metrics2 = new ColumnarScanMetrics(200L, 2048L, 100L);

        columnarScanMetrics.put(taskId1, metrics1);
        columnarScanMetrics.put(taskId2, metrics2);

        runtimeStatistics.updateColumnarScanMetrics(columnarScanMetrics);

        // Verify the metrics were updated
        String result = runtimeStatistics.getColumnarScanMetricsInfo();
        assertNotNull("Columnar scan metrics info should not be null", result);
        assertTrue("Should contain scan statistics", result.contains("totalScanRows"));
    }

    @Test
    public void testGetColumnarScanMetricsInfo_withEmptyMap() {
        String result = runtimeStatistics.getColumnarScanMetricsInfo();
        assertNotNull("Should not return null", result);
        assertTrue("Should contain default metrics", result.contains("totalScanRows-0"));
    }

    @Test
    public void testUpdateVersionStorageStatistics_overwrite() {
        // First update
        Map<TaskId, VersionStorageStatistics> firstUpdate = new HashMap<>();
        TaskId taskId = new TaskId("query1", 1, 1);
        VersionStorageStatistics stats1 = new VersionStorageStatistics();
        firstUpdate.put(taskId, stats1);

        runtimeStatistics.updateVersionStorageStatistics(firstUpdate);

        // Second update with same TaskId (should overwrite)
        Map<TaskId, VersionStorageStatistics> secondUpdate = new HashMap<>();
        VersionStorageStatistics stats2 = new VersionStorageStatistics();
        secondUpdate.put(taskId, stats2);

        runtimeStatistics.updateVersionStorageStatistics(secondUpdate);

        String result = runtimeStatistics.getVersionStorageStatisticsInfo();
        assertNotNull("Should not be null after overwrite", result);
    }

    @Test
    public void testUpdateColumnarScanMetrics_overwrite() {
        // First update
        Map<TaskId, ColumnarScanMetrics> firstUpdate = new HashMap<>();
        TaskId taskId = new TaskId("query1", 1, 1);
        ColumnarScanMetrics metrics1 = new ColumnarScanMetrics(100L, 1024L, 50L);
        firstUpdate.put(taskId, metrics1);

        runtimeStatistics.updateColumnarScanMetrics(firstUpdate);

        // Second update with same TaskId (should overwrite)
        Map<TaskId, ColumnarScanMetrics> secondUpdate = new HashMap<>();
        ColumnarScanMetrics metrics2 = new ColumnarScanMetrics(200L, 2048L, 100L);
        secondUpdate.put(taskId, metrics2);

        runtimeStatistics.updateColumnarScanMetrics(secondUpdate);

        String result = runtimeStatistics.getColumnarScanMetricsInfo();
        assertNotNull("Should not be null after overwrite", result);
        assertTrue("Should reflect updated metrics", result.contains("totalScanRows-200"));
    }
}