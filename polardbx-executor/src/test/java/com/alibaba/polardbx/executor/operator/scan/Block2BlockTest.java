package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.TddlGroupExecutor;
import com.alibaba.polardbx.executor.chunk.BlackHoleBlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.operator.TableScanClient;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.rpc.result.chunk.BlockDecoder;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class Block2BlockTest {

    @Test
    public void testBlock2BlockBlackHoleBlockBuilder() throws Exception {
        // Arrange
        DataType type = mock(DataType.class);
        when(type.getDataClass()).thenReturn(Decimal.class);

        PolarxResultset.ColumnMetaData metaData = mock(PolarxResultset.ColumnMetaData.class);

        BlockDecoder src = mock(BlockDecoder.class);
        when(src.isNull()).thenReturn(false); // 模拟非空值
        when(src.getDecimal()).thenReturn(new com.alibaba.polardbx.rpc.result.chunk.Decimal(12345L, 2)); // 模拟 Decimal 值

        BlackHoleBlockBuilder dst = new BlackHoleBlockBuilder();

        int rowCount = 1; // 最小行数


        // 创建 TableScanClient 实例
        ExecutionContext context = mock(ExecutionContext.class);
        ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
        TopologyHandler topologyHandler = Mockito.mock(TopologyHandler.class);
        TddlGroupExecutor mockTddlGroupExecutor = Mockito.mock(TddlGroupExecutor.class);
        TGroupDataSource dataSource = Mockito.mock(TGroupDataSource.class);

        when(executorContext.getTopologyHandler()).thenReturn(topologyHandler);
        when(topologyHandler.get(anyString())).thenReturn(mockTddlGroupExecutor);
        when(mockTddlGroupExecutor.getDataSource()).thenReturn(dataSource);
        when(dataSource.isXDataSource()).thenReturn(false);

        try (MockedStatic<ExecutorContext> executorContextMockedStatic = Mockito.mockStatic(ExecutorContext.class)) {
            when(ExecutorContext.getContext(Mockito.anyString())).thenReturn(executorContext);

            CursorMeta meta = mock(CursorMeta.class);
//        TableScanClient tableScanClient = new TableScanClient(context, meta, false, 1);
            ParamManager paramManager = mock(ParamManager.class);
            when(context.getParamManager()).thenReturn(paramManager);
            when(paramManager.getLong(any())).thenReturn(5000L); // 模拟 socketTimeout 的值

            // 创建 TableScanClient 实例
            TableScanClient tableScanClient = new TableScanClient(context, meta, false, 1);

            // 创建 SplitResultSet 实例
            JdbcSplit jdbcSplit = mock(JdbcSplit.class);
            when(jdbcSplit.getSchemaName()).thenReturn("test_schema");
            when(jdbcSplit.getDbIndex()).thenReturn("0");

            TableScanClient.SplitResultSet splitResultSet = tableScanClient.newSplitResultSet(jdbcSplit, 0);

            Method block2blockMethod = TableScanClient.SplitResultSet.class.getDeclaredMethod(
                "block2block", DataType.class, PolarxResultset.ColumnMetaData.class, BlockDecoder.class,
                BlockBuilder.class, int.class);
            block2blockMethod.setAccessible(true); // 设置方法可访问

            // Act
            block2blockMethod.invoke(splitResultSet, type, metaData, src, dst, rowCount);

            // Assert
            verify(src, times(1)).next(); // 验证 src.next() 被调用一次
            verify(src, times(1)).isNull(); // 验证 src.isNull() 被调用一次
            verify(src, times(1)).getDecimal(); // 验证 src.getDecimal() 被调用一次
        }
    }

    @Test
    public void testBlock2BlockDecimalBuilder() throws Exception {
        // Arrange
        DataType type = mock(DataType.class);
        when(type.getDataClass()).thenReturn(Decimal.class);

        PolarxResultset.ColumnMetaData metaData = mock(PolarxResultset.ColumnMetaData.class);

        BlockDecoder src = mock(BlockDecoder.class);
        when(src.isNull()).thenReturn(false); // 模拟非空值
        when(src.getDecimal()).thenReturn(new com.alibaba.polardbx.rpc.result.chunk.Decimal(12345L, 2)); // 模拟 Decimal 值

        DecimalBlockBuilder dst = new DecimalBlockBuilder(10);

        int rowCount = 1; // 最小行数


        // 创建 TableScanClient 实例
        ExecutionContext context = mock(ExecutionContext.class);
        ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
        TopologyHandler topologyHandler = Mockito.mock(TopologyHandler.class);
        TddlGroupExecutor mockTddlGroupExecutor = Mockito.mock(TddlGroupExecutor.class);
        TGroupDataSource dataSource = Mockito.mock(TGroupDataSource.class);

        when(executorContext.getTopologyHandler()).thenReturn(topologyHandler);
        when(topologyHandler.get(anyString())).thenReturn(mockTddlGroupExecutor);
        when(mockTddlGroupExecutor.getDataSource()).thenReturn(dataSource);
        when(dataSource.isXDataSource()).thenReturn(false);

        try (MockedStatic<ExecutorContext> executorContextMockedStatic = Mockito.mockStatic(ExecutorContext.class)) {
            when(ExecutorContext.getContext(Mockito.anyString())).thenReturn(executorContext);

            CursorMeta meta = mock(CursorMeta.class);
//        TableScanClient tableScanClient = new TableScanClient(context, meta, false, 1);
            ParamManager paramManager = mock(ParamManager.class);
            when(context.getParamManager()).thenReturn(paramManager);
            when(paramManager.getLong(any())).thenReturn(5000L); // 模拟 socketTimeout 的值

            // 创建 TableScanClient 实例
            TableScanClient tableScanClient = new TableScanClient(context, meta, false, 1);

            // 创建 SplitResultSet 实例
            JdbcSplit jdbcSplit = mock(JdbcSplit.class);
            when(jdbcSplit.getSchemaName()).thenReturn("test_schema");
            when(jdbcSplit.getDbIndex()).thenReturn("0");

            TableScanClient.SplitResultSet splitResultSet = tableScanClient.newSplitResultSet(jdbcSplit, 0);

            Method block2blockMethod = TableScanClient.SplitResultSet.class.getDeclaredMethod(
                "block2block", DataType.class, PolarxResultset.ColumnMetaData.class, BlockDecoder.class,
                BlockBuilder.class, int.class);
            block2blockMethod.setAccessible(true); // 设置方法可访问

            // Act
            block2blockMethod.invoke(splitResultSet, type, metaData, src, dst, rowCount);

            // Assert
            verify(src, times(1)).next(); // 验证 src.next() 被调用一次
            verify(src, times(1)).isNull(); // 验证 src.isNull() 被调用一次
            verify(src, times(1)).getDecimal(); // 验证 src.getDecimal() 被调用一次
        }
    }



}
