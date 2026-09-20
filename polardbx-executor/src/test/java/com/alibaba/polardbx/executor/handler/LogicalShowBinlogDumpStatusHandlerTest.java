package com.alibaba.polardbx.executor.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.cdc.CdcConstants;
import com.alibaba.polardbx.common.cdc.ResultCode;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.PooledHttpHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowBinlogDumpStatus;
import org.apache.http.entity.ContentType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LogicalShowBinlogDumpStatusHandlerTest {

    @Mock
    private IRepository repo;

    @Mock
    private LogicalShow logicalPlan;

    @Mock
    private SqlNode with;

    @Mock
    private ExecutionContext executionContext;

    @Mock
    private SqlShowBinlogDumpStatus sqlShowBinlogDumpStatus;
    MockedStatic<PooledHttpHelper> pooledHttpHelperMockedStatic;
    MockedStatic<CdcTargetUtil> cdcTargetUtilMockedStatic;

    @InjectMocks
    private LogicalShowBinlogDumpStatusHandler handler;
    private Map<String, String> params = new HashMap<>(1);
    private String url = "http://127.0.0.1:1201/dumper/showBinlogDumpStatus";

    @Before
    public void setUp() {
        // 设置模拟对象
        when(logicalPlan.getNativeSqlNode()).thenReturn(sqlShowBinlogDumpStatus);
        when(sqlShowBinlogDumpStatus.getWith()).thenReturn(with);
        when(with.toString()).thenReturn("");
        pooledHttpHelperMockedStatic = mockStatic(PooledHttpHelper.class);
        cdcTargetUtilMockedStatic = mockStatic(CdcTargetUtil.class);
        cdcTargetUtilMockedStatic.when(CdcTargetUtil::getDaemonMasterTarget).thenReturn("127.0.0.1:1201");
        handler = new LogicalShowBinlogDumpStatusHandler(repo);

        params.put("instId", InstIdUtil.getInstId());
    }

    @After
    public void clear() {
        pooledHttpHelperMockedStatic.close();
        cdcTargetUtilMockedStatic.close();
    }

    @Test
    public void handleSuccessfulResponseReturnsCursor() {
        // 模拟 HTTP 响应
        String paramJson = JSON.toJSONString(params);
        String jsonResponse =
            "[{\"Process_Id\":\"1\",\"Trace_Id\":\"2\",\"Dumper_Address\":\"3\",\"Client_Ip\":\"4\",\"Client_Port\":\"5\",\"Filename\":\"6\",\"Position\":\"7\",\"Delay\":\"8\",\"Bps\":\"9\",\"Last_Sync_Timestamp\":\"10\",\"Alive_Second\":\"11\"}]";
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.SUCCESS_CODE, "success");
        resultCode.setData(jsonResponse);
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 4000))
            .thenReturn(responseJson);

        Cursor cursor = handler.handle(logicalPlan, executionContext);

        Assert.assertNotNull(cursor);
        Assert.assertTrue(cursor instanceof ArrayResultCursor);
        ArrayResultCursor arrayResultCursor = (ArrayResultCursor) cursor;
        List<Row> rows = arrayResultCursor.getRows();
        for (int i = 1; i <= 11; i++) {
            int actualVal = Integer.parseInt(rows.get(0).getString(i - 1));
            Assert.assertEquals(i, actualVal);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handleFailedHttpResponseThrowsException() {
        String paramJson = JSON.toJSONString(params);
        // 模拟 HTTP 响应失败
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 4000))
            .thenThrow(
                new RuntimeException("HTTP error"));

        handler.handle(logicalPlan, executionContext);
    }

    @Test(expected = TddlRuntimeException.class)
    public void handleErrorCodeInResponseThrowsException() {
        // 模拟错误响应
        String paramJson = JSON.toJSONString(params);
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.FAILURE_CODE, "Error message");
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 4000))
            .thenReturn(responseJson);

        handler.handle(logicalPlan, executionContext);
    }

    @Test
    public void handleEmptyResponseReturnsEmptyCursor() {
        // 模拟空响应
        String paramJson = JSON.toJSONString(params);
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.SUCCESS_CODE, "[]");
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 4000))
            .thenReturn(responseJson);

        Cursor cursor = handler.handle(logicalPlan, executionContext);

        Assert.assertNotNull(cursor);
        Assert.assertTrue(cursor instanceof ArrayResultCursor);
        ArrayResultCursor arrayResultCursor = (ArrayResultCursor) cursor;
        List<ColumnMeta> columns = arrayResultCursor.getReturnColumns();
        Assert.assertEquals(18, columns.size());
        Assert.assertEquals("Process_Id", columns.get(0).getName());
        Assert.assertEquals("Trace_Id", columns.get(1).getName());
        Assert.assertEquals("Dumper_Address", columns.get(2).getName());
        Assert.assertEquals("Client_Ip", columns.get(3).getName());
        Assert.assertEquals("Client_Port", columns.get(4).getName());
        Assert.assertEquals("Filename", columns.get(5).getName());
        Assert.assertEquals("Position", columns.get(6).getName());
        Assert.assertEquals("Delay", columns.get(7).getName());
        Assert.assertEquals("Bps", columns.get(8).getName());
        Assert.assertEquals("Last_Sync_Timestamp", columns.get(9).getName());
        Assert.assertEquals("Alive_Second", columns.get(10).getName());
        Assert.assertEquals("Recent_Avg_Fetch_Wait_Ms", columns.get(11).getName());
        Assert.assertEquals("Recent_Avg_Process_Ms", columns.get(12).getName());
        Assert.assertEquals("Recent_Avg_Write_Ms", columns.get(13).getName());
        Assert.assertEquals("Idle_Ratio", columns.get(14).getName());
        Assert.assertEquals("Fetch_Wait_Ratio", columns.get(15).getName());
        Assert.assertEquals("Process_Ratio", columns.get(16).getName());
        Assert.assertEquals("Write_Ratio", columns.get(17).getName());
    }

    @Test
    public void handleResponseWithCnMetricsEmpty() {
        // 当 SyncAction 不可用时（测试环境），CN 指标列应为空字符串
        String paramJson = JSON.toJSONString(params);
        String jsonResponse =
            "[{\"Process_Id\":\"1\",\"Trace_Id\":\"abc123\",\"Dumper_Address\":\"3\",\"Client_Ip\":\"4\",\"Client_Port\":\"5\",\"Filename\":\"6\",\"Position\":\"7\",\"Delay\":\"8\",\"Bps\":\"9\",\"Last_Sync_Timestamp\":\"10\",\"Alive_Second\":\"11\"}]";
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.SUCCESS_CODE, "success");
        resultCode.setData(jsonResponse);
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 4000))
            .thenReturn(responseJson);

        Cursor cursor = handler.handle(logicalPlan, executionContext);

        ArrayResultCursor arrayResultCursor = (ArrayResultCursor) cursor;
        List<Row> rows = arrayResultCursor.getRows();
        Assert.assertEquals(1, rows.size());
        Row row = rows.get(0);
        // CDC 列正确填充
        Assert.assertEquals("1", row.getString(0));
        Assert.assertEquals("abc123", row.getString(1));
        // CN 指标列应为空字符串（SyncAction class 在测试环境不可用）
        for (int i = 11; i < 18; i++) {
            Assert.assertEquals("", row.getString(i));
        }
    }

    @Test
    public void handleResponseWithMultipleRows() {
        // 多行响应，模拟多个 dump 连接
        String paramJson = JSON.toJSONString(params);
        String jsonResponse =
            "[{\"Process_Id\":\"1\",\"Trace_Id\":\"trace-a\",\"Dumper_Address\":\"addr1\",\"Client_Ip\":\"10.0.0.1\",\"Client_Port\":\"3306\",\"Filename\":\"binlog.001\",\"Position\":\"100\",\"Delay\":\"0\",\"Bps\":\"1024\",\"Last_Sync_Timestamp\":\"2026-01-01\",\"Alive_Second\":\"3600\"},"
                + "{\"Process_Id\":\"2\",\"Trace_Id\":\"trace-b\",\"Dumper_Address\":\"addr2\",\"Client_Ip\":\"10.0.0.2\",\"Client_Port\":\"3307\",\"Filename\":\"binlog.002\",\"Position\":\"200\",\"Delay\":\"5\",\"Bps\":\"2048\",\"Last_Sync_Timestamp\":\"2026-01-02\",\"Alive_Second\":\"7200\"}]";
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.SUCCESS_CODE, "success");
        resultCode.setData(jsonResponse);
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 4000))
            .thenReturn(responseJson);

        Cursor cursor = handler.handle(logicalPlan, executionContext);

        ArrayResultCursor arrayResultCursor = (ArrayResultCursor) cursor;
        List<Row> rows = arrayResultCursor.getRows();
        Assert.assertEquals(2, rows.size());
        Assert.assertEquals("trace-a", rows.get(0).getString(1));
        Assert.assertEquals("trace-b", rows.get(1).getString(1));
    }

    @Test
    public void testConvertToDouble() throws Exception {
        Method method = LogicalShowBinlogDumpStatusHandler.class.getDeclaredMethod("convertToDouble", Object.class);
        method.setAccessible(true);

        // null -> 0
        Assert.assertEquals(0.0, (double) method.invoke(null, (Object) null), 0.001);

        // Number types
        Assert.assertEquals(3.14, (double) method.invoke(null, 3.14), 0.001);
        Assert.assertEquals(42.0, (double) method.invoke(null, 42), 0.001);
        Assert.assertEquals(100.0, (double) method.invoke(null, 100L), 0.001);
        Assert.assertEquals(1.5, (double) method.invoke(null, 1.5f), 0.01);

        // String parseable
        Assert.assertEquals(2.718, (double) method.invoke(null, "2.718"), 0.001);

        // String not parseable -> 0
        Assert.assertEquals(0.0, (double) method.invoke(null, "not_a_number"), 0.001);
        Assert.assertEquals(0.0, (double) method.invoke(null, ""), 0.001);
    }

    @Test
    public void testGatherMetricsFromAllCnReturnsEmptyWhenClassUnavailable() throws Exception {
        // 测试 getSyncActionClass 返回 null 时的行为
        Method method = LogicalShowBinlogDumpStatusHandler.class.getDeclaredMethod(
            "gatherMetricsFromAllCn", ExecutionContext.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, double[]> result = (Map<String, double[]>) method.invoke(handler, executionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetSyncActionClassReturnsNullInTestEnv() throws Exception {
        // 在测试环境中，ShowBinlogDumpMetricsSyncAction 不在 classpath，
        // getSyncActionClass() 应优雅返回 null 而非抛异常
        Method method = LogicalShowBinlogDumpStatusHandler.class.getDeclaredMethod("getSyncActionClass");
        method.setAccessible(true);
        Object result = method.invoke(null);
        Assert.assertNull(result);
    }

    @Test
    public void testCnMetricsColumnsAreEmptyStringsWhenNoMatch() {
        // 验证当 Trace_Id 在 metrics map 中无匹配时，CN 列全部为空字符串
        String paramJson = JSON.toJSONString(params);
        String jsonResponse =
            "[{\"Process_Id\":\"100\",\"Trace_Id\":\"trace-no-match\",\"Dumper_Address\":\"addr\",\"Client_Ip\":\"10.1.1.1\",\"Client_Port\":\"3306\",\"Filename\":\"binlog.000001\",\"Position\":\"4\",\"Delay\":\"0\",\"Bps\":\"512\",\"Last_Sync_Timestamp\":\"2026-05-08\",\"Alive_Second\":\"1000\"}]";
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.SUCCESS_CODE, "success");
        resultCode.setData(jsonResponse);
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 4000))
            .thenReturn(responseJson);

        Cursor cursor = handler.handle(logicalPlan, executionContext);

        ArrayResultCursor arrayResultCursor = (ArrayResultCursor) cursor;
        List<Row> rows = arrayResultCursor.getRows();
        Assert.assertEquals(1, rows.size());
        Row row = rows.get(0);
        // CDC 列填充正确
        Assert.assertEquals("100", row.getString(0));
        Assert.assertEquals("trace-no-match", row.getString(1));
        // 所有 CN 指标列为空字符串
        Assert.assertEquals("", row.getString(11));  // Recent_Avg_Fetch_Wait_Ms
        Assert.assertEquals("", row.getString(12));  // Recent_Avg_Process_Ms
        Assert.assertEquals("", row.getString(13));  // Recent_Avg_Write_Ms
        Assert.assertEquals("", row.getString(14));  // Idle_Ratio
        Assert.assertEquals("", row.getString(15));  // Fetch_Wait_Ratio
        Assert.assertEquals("", row.getString(16));  // Process_Ratio
        Assert.assertEquals("", row.getString(17));  // Write_Ratio
    }

    @Test
    public void testCnMetricsColumnsTotalCount() {
        // 验证总列数 = CDC 列数(11) + CN 指标列数(7) = 18
        String paramJson = JSON.toJSONString(params);
        String jsonResponse =
            "[{\"Process_Id\":\"1\",\"Trace_Id\":\"t\",\"Dumper_Address\":\"d\",\"Client_Ip\":\"i\",\"Client_Port\":\"p\",\"Filename\":\"f\",\"Position\":\"p\",\"Delay\":\"d\",\"Bps\":\"b\",\"Last_Sync_Timestamp\":\"s\",\"Alive_Second\":\"a\"}]";
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.SUCCESS_CODE, "success");
        resultCode.setData(jsonResponse);
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 4000))
            .thenReturn(responseJson);

        Cursor cursor = handler.handle(logicalPlan, executionContext);

        ArrayResultCursor arrayResultCursor = (ArrayResultCursor) cursor;
        List<ColumnMeta> columns = arrayResultCursor.getReturnColumns();
        Assert.assertEquals(18, columns.size());
        // CN 指标列在最后 7 列
        Assert.assertEquals("Recent_Avg_Fetch_Wait_Ms", columns.get(11).getName());
        Assert.assertEquals("Recent_Avg_Process_Ms", columns.get(12).getName());
        Assert.assertEquals("Recent_Avg_Write_Ms", columns.get(13).getName());
        Assert.assertEquals("Idle_Ratio", columns.get(14).getName());
        Assert.assertEquals("Fetch_Wait_Ratio", columns.get(15).getName());
        Assert.assertEquals("Process_Ratio", columns.get(16).getName());
        Assert.assertEquals("Write_Ratio", columns.get(17).getName());
    }

    @Test
    public void testNullTraceIdInResponse() {
        // 当 CDC 返回的行没有 Trace_Id 字段或为 null 时，CN 指标列为空
        String paramJson = JSON.toJSONString(params);
        String jsonResponse =
            "[{\"Process_Id\":\"1\",\"Trace_Id\":null,\"Dumper_Address\":\"d\",\"Client_Ip\":\"i\",\"Client_Port\":\"p\",\"Filename\":\"f\",\"Position\":\"p\",\"Delay\":\"d\",\"Bps\":\"b\",\"Last_Sync_Timestamp\":\"s\",\"Alive_Second\":\"a\"}]";
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.SUCCESS_CODE, "success");
        resultCode.setData(jsonResponse);
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 4000))
            .thenReturn(responseJson);

        Cursor cursor = handler.handle(logicalPlan, executionContext);

        ArrayResultCursor arrayResultCursor = (ArrayResultCursor) cursor;
        List<Row> rows = arrayResultCursor.getRows();
        Assert.assertEquals(1, rows.size());
        // CN 指标列全为空
        for (int i = 11; i < 18; i++) {
            Assert.assertEquals("", rows.get(0).getString(i));
        }
    }

    @Test
    public void testProcessMetricsResultsWithValidData() {
        // 测试 processMetricsResults 正常处理有效数据
        List<List<Map<String, Object>>> results = new ArrayList<>();
        List<Map<String, Object>> nodeRows = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("Trace_Id", "trace-001");
        row.put("Recent_Avg_Fetch_Wait_Ms", 1.5);
        row.put("Recent_Avg_Process_Ms", 2.5);
        row.put("Recent_Avg_Write_Ms", 3.5);
        row.put("Idle_Ratio", 0.1);
        row.put("Fetch_Wait_Ratio", 0.2);
        row.put("Process_Ratio", 0.3);
        row.put("Write_Ratio", 0.4);
        nodeRows.add(row);
        results.add(nodeRows);

        Map<String, double[]> metricsMap = LogicalShowBinlogDumpStatusHandler.processMetricsResults(results);

        Assert.assertEquals(1, metricsMap.size());
        double[] values = metricsMap.get("trace-001");
        Assert.assertNotNull(values);
        Assert.assertEquals(1.5, values[0], 0.001);
        Assert.assertEquals(2.5, values[1], 0.001);
        Assert.assertEquals(3.5, values[2], 0.001);
        Assert.assertEquals(0.1, values[3], 0.001);
        Assert.assertEquals(0.2, values[4], 0.001);
        Assert.assertEquals(0.3, values[5], 0.001);
        Assert.assertEquals(0.4, values[6], 0.001);
    }

    @Test
    public void testProcessMetricsResultsWithNullNodeRows() {
        // 测试 processMetricsResults 跳过 null nodeRows
        List<List<Map<String, Object>>> results = new ArrayList<>();
        results.add(null);
        List<Map<String, Object>> validRows = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("Trace_Id", "trace-002");
        row.put("Recent_Avg_Fetch_Wait_Ms", 5.0);
        row.put("Recent_Avg_Process_Ms", 6.0);
        row.put("Recent_Avg_Write_Ms", 7.0);
        row.put("Idle_Ratio", 0.5);
        row.put("Fetch_Wait_Ratio", 0.15);
        row.put("Process_Ratio", 0.25);
        row.put("Write_Ratio", 0.1);
        validRows.add(row);
        results.add(validRows);

        Map<String, double[]> metricsMap = LogicalShowBinlogDumpStatusHandler.processMetricsResults(results);

        Assert.assertEquals(1, metricsMap.size());
        Assert.assertNotNull(metricsMap.get("trace-002"));
    }

    @Test
    public void testProcessMetricsResultsWithNullTraceId() {
        // 测试 processMetricsResults 跳过 traceId 为 null 的行
        List<List<Map<String, Object>>> results = new ArrayList<>();
        List<Map<String, Object>> nodeRows = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("Trace_Id", null);
        row.put("Recent_Avg_Fetch_Wait_Ms", 1.0);
        nodeRows.add(row);
        results.add(nodeRows);

        Map<String, double[]> metricsMap = LogicalShowBinlogDumpStatusHandler.processMetricsResults(results);

        Assert.assertTrue(metricsMap.isEmpty());
    }

    @Test
    public void testProcessMetricsResultsWithMultipleNodes() {
        // 测试 processMetricsResults 处理来自多个节点的数据
        List<List<Map<String, Object>>> results = new ArrayList<>();

        List<Map<String, Object>> node1Rows = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("Trace_Id", "trace-a");
        row1.put("Recent_Avg_Fetch_Wait_Ms", 1.0);
        row1.put("Recent_Avg_Process_Ms", 2.0);
        row1.put("Recent_Avg_Write_Ms", 3.0);
        row1.put("Idle_Ratio", 0.8);
        row1.put("Fetch_Wait_Ratio", 0.05);
        row1.put("Process_Ratio", 0.1);
        row1.put("Write_Ratio", 0.05);
        node1Rows.add(row1);
        results.add(node1Rows);

        List<Map<String, Object>> node2Rows = new ArrayList<>();
        Map<String, Object> row2 = new HashMap<>();
        row2.put("Trace_Id", "trace-b");
        row2.put("Recent_Avg_Fetch_Wait_Ms", 10.0);
        row2.put("Recent_Avg_Process_Ms", 20.0);
        row2.put("Recent_Avg_Write_Ms", 30.0);
        row2.put("Idle_Ratio", 0.6);
        row2.put("Fetch_Wait_Ratio", 0.1);
        row2.put("Process_Ratio", 0.2);
        row2.put("Write_Ratio", 0.1);
        node2Rows.add(row2);
        results.add(node2Rows);

        Map<String, double[]> metricsMap = LogicalShowBinlogDumpStatusHandler.processMetricsResults(results);

        Assert.assertEquals(2, metricsMap.size());
        Assert.assertEquals(1.0, metricsMap.get("trace-a")[0], 0.001);
        Assert.assertEquals(10.0, metricsMap.get("trace-b")[0], 0.001);
    }

    @Test
    public void testHandleWithMetricsFormatting() {
        // 使用 spy 测试有 metrics 匹配时的格式化分支
        LogicalShowBinlogDumpStatusHandler spyHandler = spy(new LogicalShowBinlogDumpStatusHandler(repo));

        Map<String, double[]> metricsMap = new HashMap<>();
        metricsMap.put("trace-fmt", new double[] {1.234, 5.678, 9.012, 0.75, 0.10, 0.10, 0.05});
        doReturn(metricsMap).when(spyHandler).gatherMetricsFromAllCn(any());

        String paramJson = JSON.toJSONString(params);
        String jsonResponse =
            "[{\"Process_Id\":\"1\",\"Trace_Id\":\"trace-fmt\",\"Dumper_Address\":\"d\",\"Client_Ip\":\"i\",\"Client_Port\":\"p\",\"Filename\":\"f\",\"Position\":\"p\",\"Delay\":\"d\",\"Bps\":\"b\",\"Last_Sync_Timestamp\":\"s\",\"Alive_Second\":\"a\"}]";
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.SUCCESS_CODE, "success");
        resultCode.setData(jsonResponse);
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 4000))
            .thenReturn(responseJson);

        Cursor cursor = spyHandler.handle(logicalPlan, executionContext);

        ArrayResultCursor arrayResultCursor = (ArrayResultCursor) cursor;
        List<Row> rows = arrayResultCursor.getRows();
        Assert.assertEquals(1, rows.size());
        Row row = rows.get(0);
        // 验证 CN 指标列已格式化
        Assert.assertEquals("1.234", row.getString(11));   // Recent_Avg_Fetch_Wait_Ms
        Assert.assertEquals("5.678", row.getString(12));   // Recent_Avg_Process_Ms
        Assert.assertEquals("9.012", row.getString(13));   // Recent_Avg_Write_Ms
        Assert.assertEquals("75.0%", row.getString(14));   // Idle_Ratio
        Assert.assertEquals("10.0%", row.getString(15));   // Fetch_Wait_Ratio
        Assert.assertEquals("10.0%", row.getString(16));   // Process_Ratio
        Assert.assertEquals("5.0%", row.getString(17));    // Write_Ratio
    }

    @Test
    public void testHandleWithPartialMetricsMatch() {
        // 多行中部分匹配 metrics，部分不匹配
        LogicalShowBinlogDumpStatusHandler spyHandler = spy(new LogicalShowBinlogDumpStatusHandler(repo));

        Map<String, double[]> metricsMap = new HashMap<>();
        metricsMap.put("trace-matched", new double[] {2.0, 3.0, 4.0, 0.5, 0.2, 0.2, 0.1});
        doReturn(metricsMap).when(spyHandler).gatherMetricsFromAllCn(any());

        String paramJson = JSON.toJSONString(params);
        String jsonResponse =
            "[{\"Process_Id\":\"1\",\"Trace_Id\":\"trace-matched\",\"Dumper_Address\":\"d\",\"Client_Ip\":\"i\",\"Client_Port\":\"p\",\"Filename\":\"f\",\"Position\":\"p\",\"Delay\":\"d\",\"Bps\":\"b\",\"Last_Sync_Timestamp\":\"s\",\"Alive_Second\":\"a\"},"
                + "{\"Process_Id\":\"2\",\"Trace_Id\":\"trace-unmatched\",\"Dumper_Address\":\"d\",\"Client_Ip\":\"i\",\"Client_Port\":\"p\",\"Filename\":\"f\",\"Position\":\"p\",\"Delay\":\"d\",\"Bps\":\"b\",\"Last_Sync_Timestamp\":\"s\",\"Alive_Second\":\"a\"}]";
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.SUCCESS_CODE, "success");
        resultCode.setData(jsonResponse);
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 4000))
            .thenReturn(responseJson);

        Cursor cursor = spyHandler.handle(logicalPlan, executionContext);

        ArrayResultCursor arrayResultCursor = (ArrayResultCursor) cursor;
        List<Row> rows = arrayResultCursor.getRows();
        Assert.assertEquals(2, rows.size());
        // 第一行有 metrics
        Assert.assertEquals("2.000", rows.get(0).getString(11));
        Assert.assertEquals("50.0%", rows.get(0).getString(14));
        // 第二行无 metrics，全部为空
        Assert.assertEquals("", rows.get(1).getString(11));
        Assert.assertEquals("", rows.get(1).getString(14));
    }
}
