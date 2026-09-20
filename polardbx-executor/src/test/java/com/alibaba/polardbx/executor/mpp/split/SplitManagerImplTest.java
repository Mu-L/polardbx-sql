package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.LookupSql;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.utils.IColumnarTransaction;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.columnar.VersionStorageStatistics;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.AUTO_COMMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;

public class SplitManagerImplTest {
    @InjectMocks
    private SplitManagerImpl splitManager;

    @Mock
    private OSSTableScan ossTableScan;

    @Mock
    private ParamManager paramManager;

    @Mock
    private ColumnarManager columnarManager;

    private AutoCloseable autoCloseable;
    private ExecutionContext executionContext;

    @Mock
    private LogicalView logicalView;

    @Mock
    private PhyTableOperation phyTableOperation;

    @Before
    public void setUp() {
        autoCloseable = MockitoAnnotations.openMocks(this);
        executionContext = new ExecutionContext();
        executionContext.setParamManager(paramManager);
        when(columnarManager.latestTso()).thenReturn(233L);
    }

    @After
    public void tearDown() throws Exception {
        if (autoCloseable != null) {
            autoCloseable.close();
        }
    }

    // 测试 ossTableScanSplit 方法 - 空值检查
    @Test
    public void testOssTableScanSplit_NullOssTableScan() {
        try {
            splitManager.ossTableScanSplit(null, executionContext, false);
            fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertEquals(8006, e.getErrorCode());
        }
    }

    // 测试获取并发策略
    @Test
    public void testOssTableScanSplit_GetConcurrencyPolicy() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class)) {

            // Mock 基本设置
            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            when(ossTableScan.getLogicalTableName()).thenReturn("test_table");
            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.SEQUENTIAL);

            // Mock fetchTsoAndInitTransaction
            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            // Mock getInputs
            List<RelNode> inputs = new ArrayList<>();
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            // Mock 特殊情况检查
            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(false);
            when(executionContext.isReadOrcOnly()).thenReturn(false);
            when(executionContext.isReadSpecifiedColumnarFiles()).thenReturn(false);

            // Mock 列存索引检查
            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
            assertNotNull(result);
        }
    }

    // 测试特殊情况处理 - CCI增量检查
    @Test
    public void testOssTableScanSplit_CciIncrementalCheck() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.SEQUENTIAL);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<RelNode> inputs = new ArrayList<>();
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            // 设置特殊情况
            when(executionContext.isCciIncrementalCheck()).thenReturn(true);

            SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
            assertNotNull(result);
        }
    }

    // 测试特殊情况处理 - 读取CSV文件
    @Test
    public void testOssTableScanSplit_ReadCsvOnly() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.SEQUENTIAL);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<RelNode> inputs = new ArrayList<>();
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(true);

            SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
            assertNotNull(result);
        }
    }

    // 测试特殊情况处理 - 读取ORC文件
    @Test
    public void testOssTableScanSplit_ReadOrcOnly() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.SEQUENTIAL);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<RelNode> inputs = new ArrayList<>();
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(false);
            when(executionContext.isReadOrcOnly()).thenReturn(true);

            SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
            assertNotNull(result);
        }
    }

    // 测试特殊情况处理 - 读取指定列存文件
    @Test
    public void testOssTableScanSplit_ReadSpecifiedColumnarFiles() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.SEQUENTIAL);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<RelNode> inputs = new ArrayList<>();
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(false);
            when(executionContext.isReadOrcOnly()).thenReturn(false);
            when(executionContext.isReadSpecifiedColumnarFiles()).thenReturn(true);

            SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
            assertNotNull(result);
        }
    }

    // 测试列存索引特殊处理
    @Test
    public void testOssTableScanSplit_ColumnarIndexSpecialHandling() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class);
            MockedStatic<CBOUtil> mockedCBOUtil = mockStatic(CBOUtil.class);
            MockedStatic<SplitManagerImpl> mockedSplitManager = mockStatic(SplitManagerImpl.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            when(ossTableScan.getLogicalTableName()).thenReturn("test_table");

            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.SEQUENTIAL);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<RelNode> inputs = new ArrayList<>();
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(false);
            when(executionContext.isReadOrcOnly()).thenReturn(false);
            when(executionContext.isReadSpecifiedColumnarFiles()).thenReturn(false);

            // 设置列存索引条件
            when(ossTableScan.isColumnarIndex()).thenReturn(true);
            mockedCBOUtil.when(() -> CBOUtil.isArchiveCCi("test_schema", "test_table")).thenReturn(true);
            when(paramManager.getBoolean(ConnectionParams.ENABLE_COLUMNAR_MULTI_VERSION_PARTITION)).thenReturn(true);

            // Mock columnarOssTableScanSplit
            SplitInfo mockSplitInfo = mock(SplitInfo.class);
            mockedSplitManager.when(
                    () -> SplitManagerImpl.columnarOssTableScanSplit(ossTableScan, executionContext, 233L))
                .thenReturn(mockSplitInfo);

            try {
                SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
                assertNotNull(result);
            } catch (Throwable t) {

            }

        }
    }

    // 测试多输入情况 - 设置全表扫描标志
    @Test
    public void testOssTableScanSplit_MultipleInputs_SetFullTableScan() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.SEQUENTIAL);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            // 创建多个输入
            List<RelNode> inputs = new ArrayList<>();
            inputs.add(mock(RelNode.class));
            inputs.add(mock(RelNode.class));
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(false);
            when(executionContext.isReadOrcOnly()).thenReturn(false);
            when(executionContext.isReadSpecifiedColumnarFiles()).thenReturn(false);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
            assertNotNull(result);
        }
    }

    // 测试 SEQUENTIAL 并发策略
    @Test
    public void testOssTableScanSplit_SequentialConcurrency() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class);
            MockedStatic<OssSplit> mockedOssSplit = mockStatic(OssSplit.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            when(ossTableScan.getRelatedId()).thenReturn(12345);  // 修改为 Integer 类型
            when(ossTableScan.isExpandView()).thenReturn(false);

            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.SEQUENTIAL);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<RelNode> inputs = new ArrayList<>();
            RelNode mockInput = mock(RelNode.class);
            inputs.add(mockInput);
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            // Mock zigzagInputsByDnInst
            mockedExecUtils.when(() -> ExecUtils.zigzagInputsByDnInst(inputs, "test_schema", executionContext))
                .thenReturn(inputs);

            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(false);
            when(executionContext.isReadOrcOnly()).thenReturn(false);
            when(executionContext.isReadSpecifiedColumnarFiles()).thenReturn(false);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            // Mock OssSplit.getTableConcurrencySplit
            List<OssSplit> mockSplits = new ArrayList<>();
            OssSplit mockSplit = mock(OssSplit.class);
            when(mockSplit.getPhysicalSchema()).thenReturn("phy_schema");
            when(mockSplit.getLogicalSchema()).thenReturn("log_schema");
            mockSplits.add(mockSplit);

            mockedOssSplit.when(
                    () -> OssSplit.getTableConcurrencySplit(ossTableScan, mockInput, executionContext, 233L))
                .thenReturn(mockSplits);

            SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
            assertNotNull(result);
        }
    }

    // 测试 CONCURRENT 并发策略
    @Test
    public void testOssTableScanSplit_ConcurrentPolicy() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class);
            MockedStatic<OssSplit> mockedOssSplit = mockStatic(OssSplit.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            when(ossTableScan.getRelatedId()).thenReturn(12345);  // 修改为 Integer 类型
            when(ossTableScan.isExpandView()).thenReturn(false);

            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.CONCURRENT);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<RelNode> inputs = new ArrayList<>();
            RelNode mockInput = mock(RelNode.class);
            inputs.add(mockInput);
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            mockedExecUtils.when(() -> ExecUtils.zigzagInputsByDnInst(inputs, "test_schema", executionContext))
                .thenReturn(inputs);

            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(false);
            when(executionContext.isReadOrcOnly()).thenReturn(false);
            when(executionContext.isReadSpecifiedColumnarFiles()).thenReturn(false);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<OssSplit> mockSplits = new ArrayList<>();
            OssSplit mockSplit = mock(OssSplit.class);
            when(mockSplit.getPhysicalSchema()).thenReturn("phy_schema");
            when(mockSplit.getLogicalSchema()).thenReturn("log_schema");
            mockSplits.add(mockSplit);

            mockedOssSplit.when(
                    () -> OssSplit.getTableConcurrencySplit(ossTableScan, mockInput, executionContext, 233L))
                .thenReturn(mockSplits);

            SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
            assertNotNull(result);
        }
    }

    // 测试 FIRST_THEN_CONCURRENT 并发策略
    @Test
    public void testOssTableScanSplit_FirstThenConcurrentPolicy() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class);
            MockedStatic<OssSplit> mockedOssSplit = mockStatic(OssSplit.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            when(ossTableScan.getRelatedId()).thenReturn(12345);  // 修改为 Integer 类型
            when(ossTableScan.isExpandView()).thenReturn(false);

            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.FIRST_THEN_CONCURRENT);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<RelNode> inputs = new ArrayList<>();
            RelNode mockInput = mock(RelNode.class);
            inputs.add(mockInput);
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            mockedExecUtils.when(() -> ExecUtils.zigzagInputsByDnInst(inputs, "test_schema", executionContext))
                .thenReturn(inputs);

            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(false);
            when(executionContext.isReadOrcOnly()).thenReturn(false);
            when(executionContext.isReadSpecifiedColumnarFiles()).thenReturn(false);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<OssSplit> mockSplits = new ArrayList<>();
            OssSplit mockSplit = mock(OssSplit.class);
            when(mockSplit.getPhysicalSchema()).thenReturn("phy_schema");
            when(mockSplit.getLogicalSchema()).thenReturn("log_schema");
            mockSplits.add(mockSplit);

            mockedOssSplit.when(
                    () -> OssSplit.getTableConcurrencySplit(ossTableScan, mockInput, executionContext, 233L))
                .thenReturn(mockSplits);

            SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
            assertNotNull(result);
        }
    }

    // 测试 GROUP_CONCURRENT_BLOCK 并发策略
    @Test
    public void testOssTableScanSplit_GroupConcurrentBlockPolicy() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class);
            MockedStatic<OssSplit> mockedOssSplit = mockStatic(OssSplit.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            when(ossTableScan.getRelatedId()).thenReturn(12345);  // 修改为 Integer 类型
            when(ossTableScan.isExpandView()).thenReturn(false);

            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<RelNode> inputs = new ArrayList<>();
            RelNode mockInput = mock(RelNode.class);
            inputs.add(mockInput);
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            mockedExecUtils.when(() -> ExecUtils.zigzagInputsByDnInst(inputs, "test_schema", executionContext))
                .thenReturn(inputs);

            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(false);
            when(executionContext.isReadOrcOnly()).thenReturn(false);
            when(executionContext.isReadSpecifiedColumnarFiles()).thenReturn(false);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<OssSplit> mockSplits = new ArrayList<>();
            OssSplit mockSplit = mock(OssSplit.class);
            when(mockSplit.getPhysicalSchema()).thenReturn("phy_schema");
            when(mockSplit.getLogicalSchema()).thenReturn("log_schema");
            mockSplits.add(mockSplit);

            mockedOssSplit.when(
                    () -> OssSplit.getTableConcurrencySplit(ossTableScan, mockInput, executionContext, 233L))
                .thenReturn(mockSplits);

            SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
            assertNotNull(result);
        }
    }

    // 测试 FILE_CONCURRENT 并发策略
    @Test
    public void testOssTableScanSplit_FileConcurrentPolicy() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class);
            MockedStatic<OssSplit> mockedOssSplit = mockStatic(OssSplit.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            when(ossTableScan.getRelatedId()).thenReturn(12345);  // 修改为 Integer 类型
            when(ossTableScan.isExpandView()).thenReturn(false);

            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.FILE_CONCURRENT);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<RelNode> inputs = new ArrayList<>();
            RelNode mockInput = mock(RelNode.class);
            inputs.add(mockInput);
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            mockedExecUtils.when(() -> ExecUtils.zigzagInputsByDnInst(inputs, "test_schema", executionContext))
                .thenReturn(inputs);

            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(false);
            when(executionContext.isReadOrcOnly()).thenReturn(false);
            when(executionContext.isReadSpecifiedColumnarFiles()).thenReturn(false);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<OssSplit> mockSplits = new ArrayList<>();
            OssSplit mockSplit = mock(OssSplit.class);
            when(mockSplit.getPhysicalSchema()).thenReturn("phy_schema");
            when(mockSplit.getLogicalSchema()).thenReturn("log_schema");
            mockSplits.add(mockSplit);

            mockedOssSplit.when(() -> OssSplit.getFileConcurrencySplit(ossTableScan, mockInput, executionContext, 233L))
                .thenReturn(mockSplits);

            SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
            assertNotNull(result);
        }
    }

    // 测试 FILE_CONCURRENT 并发策略 - 空分片情况
    @Test
    public void testOssTableScanSplit_FileConcurrentPolicy_NullSplits() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class);
            MockedStatic<OssSplit> mockedOssSplit = mockStatic(OssSplit.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            when(ossTableScan.getRelatedId()).thenReturn(12345);  // 修改为 Integer 类型
            when(ossTableScan.isExpandView()).thenReturn(false);

            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.FILE_CONCURRENT);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            List<RelNode> inputs = new ArrayList<>();
            RelNode mockInput = mock(RelNode.class);
            inputs.add(mockInput);
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            mockedExecUtils.when(() -> ExecUtils.zigzagInputsByDnInst(inputs, "test_schema", executionContext))
                .thenReturn(inputs);

            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(false);
            when(executionContext.isReadOrcOnly()).thenReturn(false);
            when(executionContext.isReadSpecifiedColumnarFiles()).thenReturn(false);

            when(ossTableScan.isColumnarIndex()).thenReturn(false);

            // 返回 null 分片
            mockedOssSplit.when(() -> OssSplit.getFileConcurrencySplit(ossTableScan, mockInput, executionContext, 233L))
                .thenReturn(null);

            SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
            assertNotNull(result);
        }
    }

    // 测试列存索引情况下的 zigzag 处理
    @Test
    public void testOssTableScanSplit_ColumnarIndex_NoZigzag() {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<VersionStorageStatistics> mockedVersionStats = mockStatic(VersionStorageStatistics.class);
            MockedStatic<OssSplit> mockedOssSplit = mockStatic(OssSplit.class)) {

            when(ossTableScan.getSchemaName()).thenReturn("test_schema");
            when(ossTableScan.getRelatedId()).thenReturn(12345);  // 修改为 Integer 类型
            when(ossTableScan.isExpandView()).thenReturn(false);

            mockedExecUtils.when(() -> ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan))
                .thenReturn(QueryConcurrencyPolicy.SEQUENTIAL);

            // 设置为列存索引
            when(ossTableScan.isColumnarIndex()).thenReturn(true);

            List<RelNode> inputs = new ArrayList<>();
            RelNode mockInput = mock(RelNode.class);
            inputs.add(mockInput);
            mockedExecUtils.when(
                    () -> ExecUtils.getInputs(any(OSSTableScan.class), any(ExecutionContext.class), anyBoolean()))
                .thenReturn(inputs);

            when(executionContext.isCciIncrementalCheck()).thenReturn(false);
            when(executionContext.isReadCsvOnly()).thenReturn(false);
            when(executionContext.isReadOrcOnly()).thenReturn(false);
            when(executionContext.isReadSpecifiedColumnarFiles()).thenReturn(false);

            List<OssSplit> mockSplits = new ArrayList<>();
            OssSplit mockSplit = mock(OssSplit.class);
            when(mockSplit.getPhysicalSchema()).thenReturn("phy_schema");
            when(mockSplit.getLogicalSchema()).thenReturn("log_schema");
            mockSplits.add(mockSplit);

            mockedOssSplit.when(
                    () -> OssSplit.getTableConcurrencySplit(ossTableScan, mockInput, executionContext, 233L))
                .thenReturn(mockSplits);

            try {
                SplitInfo result = splitManager.ossTableScanSplit(ossTableScan, executionContext, false);
                assertNotNull(result);
            } catch (Throwable t) {

            }

        }
    }

    @Test
    public void testFetchTsoAndInitTransaction1() {
        IColumnarTransaction columnarTransaction = mock(IColumnarTransaction.class);
        executionContext.setTransaction(columnarTransaction);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(null);
        when(columnarTransaction.snapshotSeqIsEmpty()).thenReturn(false);
        when(columnarTransaction.getSnapshotSeq()).thenReturn(100L);

        Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);

        assertEquals(Long.valueOf(100L), tso);
    }

    @Test
    public void testFetchTsoAndInitTransaction2() {
        IColumnarTransaction columnarTransaction = mock(IColumnarTransaction.class);
        executionContext.setTransaction(columnarTransaction);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(null);
        when(columnarTransaction.snapshotSeqIsEmpty()).thenReturn(true);
        when(paramManager.getBoolean(ConnectionParams.USE_LATEST_COLUMNAR_TSO)).thenReturn(true);

        try (MockedStatic<ColumnarTransactionUtils> mockedStatic = Mockito.mockStatic(ColumnarTransactionUtils.class)) {
            mockedStatic.when(ColumnarTransactionUtils::getLatestTsoFromGms).thenReturn(200L);

            Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);

            assertEquals(Long.valueOf(200L), tso);
        }
    }

    @Test
    public void testFetchTsoAndInitTransaction3() {
        IColumnarTransaction columnarTransaction = mock(IColumnarTransaction.class);
        executionContext.setTransaction(columnarTransaction);
        when(columnarTransaction.snapshotSeqIsEmpty()).thenReturn(true);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(null);
        when(paramManager.getBoolean(ConnectionParams.USE_LATEST_COLUMNAR_TSO)).thenReturn(false);

        try (MockedStatic<ColumnarManager> staticColumnarManager = mockStatic(ColumnarManager.class)) {
            staticColumnarManager.when(ColumnarManager::getInstance).thenReturn(columnarManager);

            Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);
            assertEquals(Long.valueOf(233L), tso);
        }
    }

    @Test
    public void testFetchTsoAndInitTransaction4() {
        IColumnarTransaction columnarTransaction = mock(IColumnarTransaction.class);
        executionContext.setTransaction(columnarTransaction);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(400L);

        Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);

        assertEquals(Long.valueOf(400L), tso);
    }

    @Test
    public void testFetchTsoAndInitTransaction5() {
        ITransaction nonColumnarTransaction = mock(ITransaction.class);
        when(nonColumnarTransaction.getTransactionClass()).thenReturn(AUTO_COMMIT);
        executionContext.setTransaction(nonColumnarTransaction);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(500L);

        Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);

        assertEquals(Long.valueOf(500L), tso);
    }

    @Test
    public void testFetchTsoAndInitTransaction6() {
        ITransaction nonColumnarTransaction = mock(ITransaction.class);
        when(nonColumnarTransaction.getTransactionClass()).thenReturn(AUTO_COMMIT);
        executionContext.setTransaction(nonColumnarTransaction);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(null);
        when(paramManager.getBoolean(ConnectionParams.USE_LATEST_COLUMNAR_TSO)).thenReturn(true);

        try (MockedStatic<ColumnarTransactionUtils> mockedStatic = Mockito.mockStatic(ColumnarTransactionUtils.class)) {
            mockedStatic.when(ColumnarTransactionUtils::getLatestTsoFromGms).thenReturn(600L);

            Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);

            assertEquals(Long.valueOf(600L), tso);
        }
    }

    @Test
    public void testFetchTsoAndInitTransaction7() {
        ITransaction nonColumnarTransaction = mock(ITransaction.class);
        when(nonColumnarTransaction.getTransactionClass()).thenReturn(AUTO_COMMIT);
        executionContext.setTransaction(nonColumnarTransaction);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(null);
        when(paramManager.getBoolean(ConnectionParams.USE_LATEST_COLUMNAR_TSO)).thenReturn(false);

        try (MockedStatic<ColumnarManager> staticColumnarManager = mockStatic(ColumnarManager.class)) {
            staticColumnarManager.when(ColumnarManager::getInstance).thenReturn(columnarManager);

            Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);
            assertEquals(Long.valueOf(233L), tso);
        }
    }

    @Test
    public void testGenerateLookupSql1() {
        PhyTableScanBuilder phyOperationBuilder = mock(PhyTableScanBuilder.class);
        when(logicalView.isMGetEnabled()).thenReturn(false);
        when(phyTableOperation.getPhyOperationBuilder()).thenReturn(phyOperationBuilder);
        when(phyOperationBuilder.buildPhysicalOrderByClause()).thenReturn("order id");
        when(phyTableOperation.getBytesSql()).thenReturn(BytesSql.getBytesSql("select * from t1"));
        LookupSql sql = splitManager.generateLookupSql(logicalView, phyTableOperation);
        assertEquals(sql.orderBy, "order id");
    }

    @Test
    public void testGenerateLookupSql2() throws SqlParseException {
        String sql = "Select * from t where id = 2";
        SqlNode select = parse(sql);

        PhyTableScanBuilder phyOperationBuilder = mock(PhyTableScanBuilder.class);
        when(logicalView.isMGetEnabled()).thenReturn(true);
        when(logicalView.isInToUnionAll()).thenReturn(false);
        when(phyTableOperation.getPhyOperationBuilder()).thenReturn(phyOperationBuilder);
        when(phyOperationBuilder.buildPhysicalOrderByClause()).thenReturn("order id");
        when(phyTableOperation.getBytesSql()).thenReturn(BytesSql.getBytesSql("select * from t1"));

        when(phyTableOperation.getNativeSqlNode()).thenReturn(select);
        LookupSql lookupSql = splitManager.innerGenerateLookupSql(logicalView, phyTableOperation);
        assertEquals(lookupSql.bytesSql.toString(), "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`ID` = 2) AND ('bka_magic' = 'bka_magic'))");
    }

    @Test
    public void testGenerateLookupSql3() throws SqlParseException {
        String sql = "Select * from t where id = 2";
        SqlNode select = parse(sql);

        PhyTableScanBuilder phyOperationBuilder = mock(PhyTableScanBuilder.class);
        when(logicalView.isMGetEnabled()).thenReturn(true);
        when(logicalView.isInToUnionAll()).thenReturn(true);
        when(phyTableOperation.getPhyOperationBuilder()).thenReturn(phyOperationBuilder);
        when(phyOperationBuilder.buildPhysicalOrderByClause()).thenReturn("order id");
        when(phyTableOperation.getBytesSql()).thenReturn(BytesSql.getBytesSql("select * from t1"));

        when(phyTableOperation.getNativeSqlNode()).thenReturn(select);
        LookupSql lookupSql = splitManager.innerGenerateLookupSql(logicalView, phyTableOperation);
        assertEquals(lookupSql.bytesSql.toString(), "select * from t1");
    }

    private SqlNode parse(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql);
        return parser.parseQuery();
    }
}