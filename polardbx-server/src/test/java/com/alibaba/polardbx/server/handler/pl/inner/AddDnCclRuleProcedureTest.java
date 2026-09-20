package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.server.ServerConnection;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class AddDnCclRuleProcedureTest {

    @Test
    public void testExecuteComprehensive() throws Exception {
        // 创建被测试对象
        AddDnCclRuleProcedure procedure = new AddDnCclRuleProcedure();

        // Mock dependencies
        ServerConnection mockConnection = mock(ServerConnection.class);
        SQLCallStatement mockStatement = mock(SQLCallStatement.class);
        ArrayResultCursor mockCursor = mock(ArrayResultCursor.class);

        // Test Case 1: 参数数量不匹配 - 应该抛异常
        List<SQLExpr> incorrectParams = Arrays.asList(
            new SQLCharExpr("storageInst1"),
            new SQLCharExpr("SELECT"),
            new SQLCharExpr("dbName"),
            new SQLCharExpr("tableName"),
            new SQLIntegerExpr(BigInteger.valueOf(10))
            // 缺少第6个参数
        );
        when(mockStatement.getParameters()).thenReturn(incorrectParams);

        try {
            procedure.execute(mockConnection, mockStatement, mockCursor);
            fail("Should throw TddlRuntimeException for incorrect parameter count");
        } catch (TddlRuntimeException e) {
            assertTrue("Should contain parameter mismatch message",
                e.getMessage().contains("AddDnCclRuleProcedure expects 6 parameters"));
        }

        // Test Case 2: 参数类型错误 - 字符串参数传入非字符串
        List<SQLExpr> wrongTypeParams = Arrays.asList(
            new SQLIntegerExpr(BigInteger.valueOf(123)), // storageInstId应该是字符串
            new SQLCharExpr("SELECT"),
            new SQLCharExpr("dbName"),
            new SQLCharExpr("tableName"),
            new SQLIntegerExpr(BigInteger.valueOf(10)),
            new SQLCharExpr("keywords")
        );
        when(mockStatement.getParameters()).thenReturn(wrongTypeParams);

        try {
            procedure.execute(mockConnection, mockStatement, mockCursor);
            fail("Should throw TddlRuntimeException for wrong parameter type");
        } catch (TddlRuntimeException e) {
            assertTrue("Should contain integer type error message",
                e.getMessage().contains("ERR_CCL"));
        }

        // Test Case 3: concurrency参数类型错误 - 整数参数传入非整数
        List<SQLExpr> wrongConcurrencyTypeParams = Arrays.asList(
            new SQLCharExpr("storageInst1"),
            new SQLCharExpr("SELECT"),
            new SQLCharExpr("dbName"),
            new SQLCharExpr("tableName"),
            new SQLCharExpr("notAnInteger"), // concurrency应该是整数
            new SQLCharExpr("keywords")
        );
        when(mockStatement.getParameters()).thenReturn(wrongConcurrencyTypeParams);

        try {
            procedure.execute(mockConnection, mockStatement, mockCursor);
            fail("Should throw TddlRuntimeException for wrong concurrency type");
        } catch (TddlRuntimeException e) {
            assertTrue("Should contain integer type error message",
                e.getMessage().contains("ERR_CCL"));
        }

        // Test Case 4: 正常流程 - 成功场景
        List<SQLExpr> validParams = Arrays.asList(
            new SQLCharExpr("storageInst1"),
            new SQLCharExpr("SELECT"),
            new SQLCharExpr("testDb"),
            new SQLCharExpr("testTable"),
            new SQLIntegerExpr(BigInteger.valueOf(10)),
            new SQLCharExpr("test_keywords")
        );
        when(mockStatement.getParameters()).thenReturn(validParams);

        // Mock SyncManagerHelper返回成功结果
        List<Map<String, Object>> successResultRow = new ArrayList<>();
        Map<String, Object> successRow = new HashMap<>();
        successRow.put("COMPUTE_NODE", "node1:8080");
        successRow.put("STATUS", "SUCCESS");
        successRow.put("STORAGE_INST_ID", "storageInst1");
        successRow.put("GENERATED_SQL", "call dbms_ccl.add_ccl_rule('SELECT', '', '', 10, 'test_keywords')");
        successRow.put("MESSAGE", "Successfully created CCL rule with ID: 12345");
        successResultRow.add(successRow);

        List<List<Map<String, Object>>> successResults = Arrays.asList(successResultRow);

        try (MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class)) {
            syncMock.when(
                    () -> SyncManagerHelper.syncIgnoreExceptions(any(), eq(SystemDbHelper.DEFAULT_DB_NAME), any()))
                .thenReturn(successResults);

            // 执行测试
            procedure.execute(mockConnection, mockStatement, mockCursor);

            // 验证cursor.addColumn被调用3次
            verify(mockCursor, times(3)).addColumn(any(), any());
            // 验证cursor.addRow被调用1次，且传入成功状态
            verify(mockCursor, times(1)).addRow(argThat(row -> {
                Object[] objects = (Object[]) row;
                return "Success".equals(objects[0]) &&
                    "storageInst1".equals(objects[1]) &&
                    objects[2].toString().contains("call dbms_ccl.add_ccl_rule");
            }));
        }

        // Test Case 5: 部分失败场景
        List<Map<String, Object>> mixedResultRow = new ArrayList<>();
        Map<String, Object> failRow = new HashMap<>();
        failRow.put("COMPUTE_NODE", "node2:8080");
        failRow.put("STATUS", "FAILED");
        failRow.put("STORAGE_INST_ID", "storageInst1");
        failRow.put("GENERATED_SQL", "");
        failRow.put("MESSAGE", "Connection failed to storage instance");
        mixedResultRow.add(failRow);

        Map<String, Object> successRow2 = new HashMap<>();
        successRow2.put("COMPUTE_NODE", "node3:8080");
        successRow2.put("STATUS", "SUCCESS");
        successRow2.put("STORAGE_INST_ID", "storageInst1");
        successRow2.put("GENERATED_SQL", "call dbms_ccl.add_ccl_rule('SELECT', '', '', 10, 'test_keywords')");
        successRow2.put("MESSAGE", "Successfully created CCL rule");
        mixedResultRow.add(successRow2);

        List<List<Map<String, Object>>> mixedResults = Arrays.asList(mixedResultRow);

        // 重置mock
        reset(mockCursor);

        try (MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class)) {
            syncMock.when(
                    () -> SyncManagerHelper.syncIgnoreExceptions(any(), eq(SystemDbHelper.DEFAULT_DB_NAME), any()))
                .thenReturn(mixedResults);

            // 执行测试
            procedure.execute(mockConnection, mockStatement, mockCursor);

            // 验证cursor.addColumn被调用3次
            verify(mockCursor, times(3)).addColumn(any(), any());
            // 验证cursor.addRow被调用1次，且传入失败状态
            verify(mockCursor, times(1)).addRow(argThat(row -> {
                Object[] objects = (Object[]) row;
                return objects[0].toString().startsWith("Fail, Please Check Log") &&
                    "storageInst1".equals(objects[1]);
            }));
        }

        // Test Case 6: 空结果场景
        List<List<Map<String, Object>>> emptyResults = Arrays.asList();

        // 重置mock
        reset(mockCursor);

        try (MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class)) {
            syncMock.when(
                    () -> SyncManagerHelper.syncIgnoreExceptions(any(), eq(SystemDbHelper.DEFAULT_DB_NAME), any()))
                .thenReturn(emptyResults);

            // 执行测试
            procedure.execute(mockConnection, mockStatement, mockCursor);

            // 验证cursor.addColumn被调用3次
            verify(mockCursor, times(3)).addColumn(any(), any());
            // 验证cursor.addRow被调用1次，且传入成功状态（因为没有失败的节点）
            verify(mockCursor, times(1)).addRow(argThat(row -> {
                Object[] objects = (Object[]) row;
                return "Success".equals(objects[0]) &&
                    "storageInst1".equals(objects[1]) &&
                    "".equals(objects[2]); // 没有生成SQL
            }));
        }

        // Test Case 7: null结果场景
        List<List<Map<String, Object>>> nullResults = Arrays.asList((List<Map<String, Object>>) null);

        // 重置mock
        reset(mockCursor);

        try (MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class)) {
            syncMock.when(
                    () -> SyncManagerHelper.syncIgnoreExceptions(any(), eq(SystemDbHelper.DEFAULT_DB_NAME), any()))
                .thenReturn(nullResults);

            // 执行测试
            procedure.execute(mockConnection, mockStatement, mockCursor);

            // 验证cursor.addColumn被调用3次
            verify(mockCursor, times(3)).addColumn(any(), any());
            // 验证cursor.addRow被调用1次，且传入成功状态（因为null被跳过）
            verify(mockCursor, times(1)).addRow(argThat(row -> {
                Object[] objects = (Object[]) row;
                return "Success".equals(objects[0]);
            }));
        }
    }
}
