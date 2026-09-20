package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.scheduler.ColumnarWarmupRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class InformationSchemaColumnarWarmupHandlerTest {

    ExecutionContext executionContext;
    private ArrayResultCursor resultCursor;
    private MockedStatic<MetaDbUtil> mockMetaDbUtil;
    private MockedStatic<InstIdUtil> instIdUtilMock;

    @Before
    public void setupExecutionContext() {
        executionContext = new ExecutionContext();
        executionContext.setSchemaName("test_db");
    }

    @Before
    public void setupCluster() {
        instIdUtilMock = Mockito.mockStatic(InstIdUtil.class);
        instIdUtilMock.when(() -> InstIdUtil.getInstId()).thenReturn("pxc-xxxxxxx");
    }

    @Before
    public void setUpConnection() throws SQLException {
        // Mock connection and accessor.
        Connection conn = mock(Connection.class);
        mockMetaDbUtil = mockStatic(MetaDbUtil.class);
        ResultSet queryResumedByInstId = mock(ResultSet.class);

        final AtomicBoolean getConnectionFailed = new AtomicBoolean(false);
        final AtomicBoolean queryFailed = new AtomicBoolean(false);

        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenAnswer(invocation -> {
            if (getConnectionFailed.get()) {
                throw new RuntimeException("Mock get connection failed");
            } else {
                return conn;
            }
        });

        final ColumnarWarmupRecord columnarWarmupRecord = new ColumnarWarmupRecord();
        when(queryResumedByInstId.getLong(matches("task_id"))).thenReturn(999L);
        when(queryResumedByInstId.getString(matches("create_time"))).thenReturn("2024-12-01 00:00:00");
        when(queryResumedByInstId.getString(matches("update_time"))).thenReturn("2024-12-01 01:00:00");
        when(queryResumedByInstId.getString(matches("instance_id"))).thenReturn("pxc-xxxxxxx");
        when(queryResumedByInstId.getString(matches("schema_name"))).thenReturn("test_db");
        when(queryResumedByInstId.getString(matches("cron_expression"))).thenReturn("*/1 * * * *");
        when(queryResumedByInstId.getString(matches("sql_def"))).thenReturn("select * from test_db");
        when(queryResumedByInstId.getInt(matches("status"))).thenReturn(0);
        columnarWarmupRecord.fill(queryResumedByInstId);

        final List<ColumnarWarmupRecord> records = ImmutableList.of(columnarWarmupRecord);
        mockMetaDbUtil.when(
            () -> MetaDbUtil.query(anyString(), anyMap(), eq(ColumnarWarmupRecord.class), eq(conn))
        ).thenAnswer(invocation -> {
            if (queryFailed.get()) {
                throw new RuntimeException("Mock query failed");
            } else {
                return records;
            }
        });

        mockMetaDbUtil.when(
            () -> MetaDbUtil.query(anyString(), eq(ColumnarWarmupRecord.class), eq(conn))
        ).thenAnswer(invocation -> {
            if (queryFailed.get()) {
                throw new RuntimeException("Mock query failed");
            } else {
                return records;
            }
        });
    }

    @Before
    public void setupCursor() {
        resultCursor = new ArrayResultCursor("WARMUP_EXECUTION_LOGS");

        resultCursor.addColumn("task_id", DataTypes.LongType);
        resultCursor.addColumn("create_time", DataTypes.VarcharType);
        resultCursor.addColumn("update_time", DataTypes.VarcharType);
        resultCursor.addColumn("instance_id", DataTypes.VarcharType);
        resultCursor.addColumn("schema_name", DataTypes.VarcharType);
        resultCursor.addColumn("cron_expression", DataTypes.VarcharType);
        resultCursor.addColumn("sql_def", DataTypes.VarcharType);
        resultCursor.addColumn("status", DataTypes.IntegerType);

        resultCursor.initMeta();
    }

    @Test
    public void test() {
        InformationSchemaColumnarWarmupHandler handler =
            new InformationSchemaColumnarWarmupHandler(Mockito.mock(VirtualViewHandler.class));

        Cursor cursor = handler.handle(Mockito.mock(VirtualView.class), executionContext, resultCursor);

        Row row;
        while ((row = cursor.next()) != null) {
            System.out.println(row);
        }

    }

    @After
    public void tearDown() {
        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }

        if (instIdUtilMock != null) {
            instIdUtilMock.close();
        }
    }

}