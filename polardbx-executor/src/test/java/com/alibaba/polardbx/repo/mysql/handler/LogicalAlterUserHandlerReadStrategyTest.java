package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAlterUser;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlUserName;
import org.apache.calcite.util.NlsString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * LogicalAlterUserHandler的单元测试类
 * 用于测试handlerAlterUserReadStrategy方法的各种场景
 */
public class LogicalAlterUserHandlerReadStrategyTest {

    private LogicalAlterUserHandler handler;
    private SqlAlterUser mockSqlAlterUser;
    private ExecutionContext mockExecutionContext;
    private ExecutionPlan mockExecutionPlan;
    private Cursor mockCursor;
    private MockedStatic<Planner> mockedPlanner;
    private MockedStatic<ExecutorHelper> mockedExecutorHelper;
    private Planner mockPlanner;

    @Before
    public void setUp() {
        handler = new LogicalAlterUserHandler(null);
        mockSqlAlterUser = mock(SqlAlterUser.class);
        mockExecutionContext = mock(ExecutionContext.class);
        mockExecutionPlan = mock(ExecutionPlan.class);
        mockCursor = mock(Cursor.class);
        mockPlanner = mock(Planner.class);
        mockedPlanner = Mockito.mockStatic(Planner.class);
        mockedExecutorHelper = Mockito.mockStatic(ExecutorHelper.class);
    }

    @After
    public void tearDown() {
        mockedPlanner.close();
        mockedExecutorHelper.close();
    }

    /**
     * 测试用例1：测试FOLLOWER策略成功创建路由规则
     * 设计思路：验证当readStrategy为FOLLOWER时，能够正确生成create routing_rule SQL并执行
     * 测试重要性：这是核心功能之一，确保用户可以设置为FOLLOWER读取策略
     */
    @Test
    public void testHandlerAlterUserReadStrategy_Follower_Success() {
        SqlUserName mockUserName = mock(SqlUserName.class);
        SqlCharStringLiteral mockReadStrategy = mock(SqlCharStringLiteral.class);
        NlsString mockNlsString = mock(NlsString.class);

        when(mockSqlAlterUser.getReadStrategy()).thenReturn(mockReadStrategy);
        when(mockReadStrategy.getNlsString()).thenReturn(mockNlsString);
        when(mockNlsString.getValue()).thenReturn("FOLLOWER");
        when(mockSqlAlterUser.getUser()).thenReturn(mockUserName);
        when(mockUserName.getUser()).thenReturn("testuser");

        when(mockExecutionContext.copy()).thenReturn(mockExecutionContext);

        mockedPlanner.when(Planner::getInstance).thenReturn(mockPlanner);
        when(mockPlanner.plan(anyString(), any(ExecutionContext.class))).thenReturn(mockExecutionPlan);
        when(mockExecutionPlan.getPlan()).thenReturn(mock(RelNode.class));
        mockedExecutorHelper.when(() -> ExecutorHelper.execute(any(RelNode.class), any(ExecutionContext.class))).
            thenReturn(mockCursor);

        Cursor result = handler.handlerAlterUserReadStrategy(mockSqlAlterUser, mockExecutionContext);

        assertNotNull("返回的Cursor不应为null", result);
        assertEquals("应返回mock的Cursor", mockCursor, result);

        verify(mockExecutionContext).copy();
        verify(mockExecutionContext).newStatement();
        verify(Planner.getInstance()).plan(contains("create routing_rule"), any(ExecutionContext.class));

        LogicalDal mockLogicalDal = mock(LogicalDal.class);
        when(mockLogicalDal.getNativeSqlNode()).thenReturn(mockSqlAlterUser);
        when(mockSqlAlterUser.getUser()).thenReturn(mock(SqlUserName.class));
        when(mockSqlAlterUser.getLock()).thenReturn(null);
        handler.handle(mockLogicalDal, mockExecutionContext);
    }

    @Test
    public void testHandlerAlterUserReadStrategy_Stable_Success() {
        SqlUserName mockUserName = mock(SqlUserName.class);
        SqlCharStringLiteral mockReadStrategy = mock(SqlCharStringLiteral.class);
        NlsString mockNlsString = mock(NlsString.class);

        when(mockSqlAlterUser.getReadStrategy()).thenReturn(mockReadStrategy);
        when(mockReadStrategy.getNlsString()).thenReturn(mockNlsString);
        when(mockNlsString.getValue()).thenReturn("Stale");
        when(mockSqlAlterUser.getUser()).thenReturn(mockUserName);
        when(mockUserName.getUser()).thenReturn("testuser");

        when(mockExecutionContext.copy()).thenReturn(mockExecutionContext);

        mockedPlanner.when(Planner::getInstance).thenReturn(mockPlanner);
        when(mockPlanner.plan(anyString(), any(ExecutionContext.class))).thenReturn(mockExecutionPlan);
        when(mockExecutionPlan.getPlan()).thenReturn(mock(RelNode.class));
        mockedExecutorHelper.when(() -> ExecutorHelper.execute(any(RelNode.class), any(ExecutionContext.class))).
            thenReturn(mockCursor);

        Cursor result = handler.handlerAlterUserReadStrategy(mockSqlAlterUser, mockExecutionContext);

        assertNotNull("返回的Cursor不应为null", result);
        assertEquals("应返回mock的Cursor", mockCursor, result);

        verify(mockExecutionContext).copy();
        verify(mockExecutionContext).newStatement();
        verify(Planner.getInstance()).plan(contains("create routing_rule"), any(ExecutionContext.class));
    }

    /**
     * 测试用例2：测试NONE策略成功删除路由规则
     * 设计思路：验证当readStrategy为NONE时，能够正确生成drop routing_rule SQL并执行
     * 测试重要性：确保用户可以取消FOLLOWER读取策略，恢复默认行为
     */
    @Test
    public void testHandlerAlterUserReadStrategy_None_Success() {
        SqlUserName mockUserName = mock(SqlUserName.class);
        SqlCharStringLiteral mockReadStrategy = mock(SqlCharStringLiteral.class);
        NlsString mockNlsString = mock(NlsString.class);

        when(mockSqlAlterUser.getReadStrategy()).thenReturn(mockReadStrategy);
        when(mockReadStrategy.getNlsString()).thenReturn(mockNlsString);
        when(mockNlsString.getValue()).thenReturn("NONE");
        when(mockSqlAlterUser.getUser()).thenReturn(mockUserName);
        when(mockUserName.getUser()).thenReturn("testuser");

        when(mockExecutionContext.copy()).thenReturn(mockExecutionContext);
        mockedPlanner.when(Planner::getInstance).thenReturn(mockPlanner);
        when(mockPlanner.plan(anyString(), any(ExecutionContext.class))).thenReturn(mockExecutionPlan);
        when(mockExecutionPlan.getPlan()).thenReturn(mock(RelNode.class));
        mockedExecutorHelper.when(() -> ExecutorHelper.execute(any(RelNode.class), any(ExecutionContext.class))).
            thenReturn(mockCursor);

        Cursor result = handler.handlerAlterUserReadStrategy(mockSqlAlterUser, mockExecutionContext);

        assertNotNull("返回的Cursor不应为null", result);
        assertEquals("应返回mock的Cursor", mockCursor, result);

        verify(Planner.getInstance()).plan(contains("drop routing_rule if exists"), any(ExecutionContext.class));
    }

    /**
     * 测试用例3：测试不支持的readStrategy抛出异常
     * 设计思路：验证当传入不支持的readStrategy时，能够抛出正确的异常
     * 测试重要性：确保代码的健壮性，防止非法输入导致未定义行为
     */
    @Test(expected = TddlRuntimeException.class)
    public void testHandlerAlterUserReadStrategy_UnsupportedStrategy_ThrowsException() {
        SqlUserName mockUserName = mock(SqlUserName.class);
        SqlCharStringLiteral mockReadStrategy = mock(SqlCharStringLiteral.class);
        NlsString mockNlsString = mock(NlsString.class);

        when(mockSqlAlterUser.getReadStrategy()).thenReturn(mockReadStrategy);
        when(mockReadStrategy.getNlsString()).thenReturn(mockNlsString);
        when(mockNlsString.getValue()).thenReturn("INVALID");
        when(mockSqlAlterUser.getUser()).thenReturn(mockUserName);
        when(mockUserName.getUser()).thenReturn("testuser");

        try {
            handler.handlerAlterUserReadStrategy(mockSqlAlterUser, mockExecutionContext);
            fail("应该抛出TddlRuntimeException异常");
        } catch (TddlRuntimeException e) {
            assertTrue("错误信息应包含not support", e.getMessage().contains("not support"));
            throw e;
        }
    }

    /**
     * 测试用例4：测试路由规则已存在时抛出特定异常
     * 设计思路：验证当执行create routing_rule时，如果规则已存在，能够捕获并转换为特定异常
     * 测试重要性：提供友好的错误提示，帮助用户理解问题原因
     */
    @Test(expected = TddlRuntimeException.class)
    public void testHandlerAlterUserReadStrategy_AlreadyExists_ThrowsSpecificException() {
        SqlUserName mockUserName = mock(SqlUserName.class);
        SqlCharStringLiteral mockReadStrategy = mock(SqlCharStringLiteral.class);
        NlsString mockNlsString = mock(NlsString.class);

        when(mockSqlAlterUser.getReadStrategy()).thenReturn(mockReadStrategy);
        when(mockReadStrategy.getNlsString()).thenReturn(mockNlsString);
        when(mockNlsString.getValue()).thenReturn("FOLLOWER");
        when(mockSqlAlterUser.getUser()).thenReturn(mockUserName);
        when(mockUserName.getUser()).thenReturn("testuser");

        when(mockExecutionContext.copy()).thenReturn(mockExecutionContext);
        mockedPlanner.when(Planner::getInstance).thenReturn(mockPlanner);
        when(mockPlanner.plan(anyString(), any(ExecutionContext.class))).thenReturn(mockExecutionPlan);
        when(mockExecutionPlan.getPlan()).thenReturn(mock(RelNode.class));
        when(ExecutorHelper.execute(any(RelNode.class), any(ExecutionContext.class)))
            .thenThrow(new RuntimeException("routing rule already exists"));

        try {
            handler.handlerAlterUserReadStrategy(mockSqlAlterUser, mockExecutionContext);
            fail("应该抛出TddlRuntimeException异常");
        } catch (TddlRuntimeException e) {
            assertTrue("错误信息应包含read strategy already set", e.getMessage().contains("read strategy already set"));
            throw e;
        }
    }

    /**
     * 测试用例5：测试执行过程中抛出其他异常时正常传播
     * 设计思路：验证当执行过程中出现非"already exists"异常时，能够正常传播异常
     * 测试重要性：确保不会意外吞掉其他类型的异常，便于问题排查
     */
    @Test(expected = RuntimeException.class)
    public void testHandlerAlterUserReadStrategy_OtherException_Propagates() {
        SqlUserName mockUserName = mock(SqlUserName.class);
        SqlCharStringLiteral mockReadStrategy = mock(SqlCharStringLiteral.class);
        NlsString mockNlsString = mock(NlsString.class);

        when(mockSqlAlterUser.getReadStrategy()).thenReturn(mockReadStrategy);
        when(mockReadStrategy.getNlsString()).thenReturn(mockNlsString);
        when(mockNlsString.getValue()).thenReturn("FOLLOWER");
        when(mockSqlAlterUser.getUser()).thenReturn(mockUserName);
        when(mockUserName.getUser()).thenReturn("testuser");

        when(mockExecutionContext.copy()).thenReturn(mockExecutionContext);
        mockedPlanner.when(Planner::getInstance).thenReturn(mockPlanner);
        when(mockPlanner.plan(anyString(), any(ExecutionContext.class))).thenReturn(mockExecutionPlan);
        when(mockExecutionPlan.getPlan()).thenReturn(mock(RelNode.class));
        when(ExecutorHelper.execute(any(RelNode.class), any(ExecutionContext.class)))
            .thenThrow(new RuntimeException("database connection failed"));

        try {
            handler.handlerAlterUserReadStrategy(mockSqlAlterUser, mockExecutionContext);
            fail("应该抛出RuntimeException异常");
        } catch (RuntimeException e) {
            assertTrue("错误信息应包含database connection failed",
                e.getMessage().contains("database connection failed"));
            throw e;
        }
    }

    /**
     * 测试用例6：测试空用户名场景
     * 设计思路：验证当用户名为空字符串时，能够正确处理
     * 测试重要性：边界条件测试，确保代码对空值的处理
     */
    @Test
    public void testHandlerAlterUserReadStrategy_EmptyUsername_Success() {
        SqlUserName mockUserName = mock(SqlUserName.class);
        SqlCharStringLiteral mockReadStrategy = mock(SqlCharStringLiteral.class);
        NlsString mockNlsString = mock(NlsString.class);

        when(mockSqlAlterUser.getReadStrategy()).thenReturn(mockReadStrategy);
        when(mockReadStrategy.getNlsString()).thenReturn(mockNlsString);
        when(mockNlsString.getValue()).thenReturn("NONE");
        when(mockSqlAlterUser.getUser()).thenReturn(mockUserName);
        when(mockUserName.getUser()).thenReturn("");

        when(mockExecutionContext.copy()).thenReturn(mockExecutionContext);
        mockedPlanner.when(Planner::getInstance).thenReturn(mockPlanner);
        when(mockPlanner.plan(anyString(), any(ExecutionContext.class))).thenReturn(mockExecutionPlan);
        when(mockExecutionPlan.getPlan()).thenReturn(mock(RelNode.class));
        mockedExecutorHelper.when(() -> ExecutorHelper.execute(any(RelNode.class), any(ExecutionContext.class))).
            thenReturn(mockCursor);

        Cursor result = handler.handlerAlterUserReadStrategy(mockSqlAlterUser, mockExecutionContext);

        assertNotNull("返回的Cursor不应为null", result);
        verify(Planner.getInstance()).plan(contains("$inner$_"), any(ExecutionContext.class));
    }

    /**
     * 测试用例7：测试特殊字符用户名场景
     * 设计思路：验证当用户名包含特殊字符时，能够正确处理
     * 测试重要性：确保SQL注入防护和特殊字符处理
     */
    @Test
    public void testHandlerAlterUserReadStrategy_SpecialCharacterUsername_Success() {
        SqlUserName mockUserName = mock(SqlUserName.class);
        SqlCharStringLiteral mockReadStrategy = mock(SqlCharStringLiteral.class);
        NlsString mockNlsString = mock(NlsString.class);

        when(mockSqlAlterUser.getReadStrategy()).thenReturn(mockReadStrategy);
        when(mockReadStrategy.getNlsString()).thenReturn(mockNlsString);
        when(mockNlsString.getValue()).thenReturn("FOLLOWER");
        when(mockSqlAlterUser.getUser()).thenReturn(mockUserName);
        when(mockUserName.getUser()).thenReturn("`u_ser@test.com`");

        when(mockExecutionContext.copy()).thenReturn(mockExecutionContext);
        mockedPlanner.when(Planner::getInstance).thenReturn(mockPlanner);
        when(mockPlanner.plan(anyString(), any(ExecutionContext.class))).thenReturn(mockExecutionPlan);
        when(mockExecutionPlan.getPlan()).thenReturn(mock(RelNode.class));
        mockedExecutorHelper.when(() -> ExecutorHelper.execute(any(RelNode.class), any(ExecutionContext.class))).
            thenReturn(mockCursor);

        try {
            handler.handlerAlterUserReadStrategy(mockSqlAlterUser, mockExecutionContext);
            fail("应该抛出RuntimeException异常");
        } catch (RuntimeException e) {
            assertTrue("错误信息应包含must be composed solely of letters, digits, underscores, and dollar signs",
                e.getMessage().contains("must be composed solely of letters, digits, underscores, and dollar signs"));
        }
    }

    /**
     * 测试用例12：测试ExecutionContext.newStatement()被正确调用
     * 设计思路：验证ExecutionContext.newStatement()方法被正确调用
     * 测试重要性：确保执行上下文的正确初始化
     */
    @Test
    public void testHandlerAlterUserReadStrategy_NewStatementCalled() {
        SqlUserName mockUserName = mock(SqlUserName.class);
        SqlCharStringLiteral mockReadStrategy = mock(SqlCharStringLiteral.class);
        NlsString mockNlsString = mock(NlsString.class);

        when(mockSqlAlterUser.getReadStrategy()).thenReturn(mockReadStrategy);
        when(mockReadStrategy.getNlsString()).thenReturn(mockNlsString);
        when(mockNlsString.getValue()).thenReturn("NONE");
        when(mockSqlAlterUser.getUser()).thenReturn(mockUserName);
        when(mockUserName.getUser()).thenReturn("testuser");

        when(mockExecutionContext.copy()).thenReturn(mockExecutionContext);
        mockedPlanner.when(Planner::getInstance).thenReturn(mockPlanner);
        when(mockPlanner.plan(anyString(), any(ExecutionContext.class))).thenReturn(mockExecutionPlan);
        when(mockExecutionPlan.getPlan()).thenReturn(mock(RelNode.class));
        mockedExecutorHelper.when(() -> ExecutorHelper.execute(any(RelNode.class), any(ExecutionContext.class))).
            thenReturn(mockCursor);

        handler.handlerAlterUserReadStrategy(mockSqlAlterUser, mockExecutionContext);

        verify(mockExecutionContext, times(1)).newStatement();
    }

    /**
     * 测试用例13：测试SQL生成的正确性-FOLLOWER策略
     * 设计思路：验证生成的SQL语句格式正确
     * 测试重要性：确保SQL语句的正确性，避免语法错误
     */
    @Test
    public void testHandlerAlterUserReadStrategy_SqlGeneration_Follower() {
        SqlUserName mockUserName = mock(SqlUserName.class);
        SqlCharStringLiteral mockReadStrategy = mock(SqlCharStringLiteral.class);
        NlsString mockNlsString = mock(NlsString.class);

        when(mockSqlAlterUser.getReadStrategy()).thenReturn(mockReadStrategy);
        when(mockReadStrategy.getNlsString()).thenReturn(mockNlsString);
        when(mockNlsString.getValue()).thenReturn("FOLLOWER");
        when(mockSqlAlterUser.getUser()).thenReturn(mockUserName);
        when(mockUserName.getUser()).thenReturn("myuser");

        when(mockExecutionContext.copy()).thenReturn(mockExecutionContext);
        mockedPlanner.when(Planner::getInstance).thenReturn(mockPlanner);
        when(mockPlanner.plan(anyString(), any(ExecutionContext.class))).thenReturn(mockExecutionPlan);
        when(mockExecutionPlan.getPlan()).thenReturn(mock(RelNode.class));
        mockedExecutorHelper.when(() -> ExecutorHelper.execute(any(RelNode.class), any(ExecutionContext.class))).
            thenReturn(mockCursor);

        handler.handlerAlterUserReadStrategy(mockSqlAlterUser, mockExecutionContext);

        verify(Planner.getInstance()).plan(argThat((String sql) -> sql.contains("create routing_rule") &&
            sql.contains("$inner$_myuser") &&
            sql.contains("to 'myuser'") &&
            sql.contains("type=FOLLOWER")), any(ExecutionContext.class));
    }

    /**
     * 测试用例14：测试SQL生成的正确性-NONE策略
     * 设计思路：验证生成的SQL语句格式正确
     * 测试重要性：确保SQL语句的正确性，避免语法错误
     */
    @Test
    public void testHandlerAlterUserReadStrategy_SqlGeneration_None() {
        SqlUserName mockUserName = mock(SqlUserName.class);
        SqlCharStringLiteral mockReadStrategy = mock(SqlCharStringLiteral.class);
        NlsString mockNlsString = mock(NlsString.class);

        when(mockSqlAlterUser.getReadStrategy()).thenReturn(mockReadStrategy);
        when(mockReadStrategy.getNlsString()).thenReturn(mockNlsString);
        when(mockNlsString.getValue()).thenReturn("NONE");
        when(mockSqlAlterUser.getUser()).thenReturn(mockUserName);
        when(mockUserName.getUser()).thenReturn("myuser");

        when(mockExecutionContext.copy()).thenReturn(mockExecutionContext);
        mockedPlanner.when(Planner::getInstance).thenReturn(mockPlanner);
        when(mockPlanner.plan(anyString(), any(ExecutionContext.class))).thenReturn(mockExecutionPlan);
        when(mockExecutionPlan.getPlan()).thenReturn(mock(RelNode.class));
        mockedExecutorHelper.when(() -> ExecutorHelper.execute(any(RelNode.class), any(ExecutionContext.class))).
            thenReturn(mockCursor);

        handler.handlerAlterUserReadStrategy(mockSqlAlterUser, mockExecutionContext);

        verify(Planner.getInstance()).plan(argThat((String sql) -> {
            return sql.contains("drop routing_rule if exists") &&
                sql.contains("$inner$_myuser");
        }), any(ExecutionContext.class));
    }

    @Test
    public void testHandlerAlterUserReadStrategy_InvalidUser_SqlGeneration_Follower() {
        SqlUserName mockUserName = mock(SqlUserName.class);
        SqlCharStringLiteral mockReadStrategy = mock(SqlCharStringLiteral.class);
        NlsString mockNlsString = mock(NlsString.class);

        when(mockSqlAlterUser.getReadStrategy()).thenReturn(mockReadStrategy);
        when(mockReadStrategy.getNlsString()).thenReturn(mockNlsString);
        when(mockNlsString.getValue()).thenReturn("FOLLOWER");
        when(mockSqlAlterUser.getUser()).thenReturn(mockUserName);
        when(mockUserName.getUser()).thenReturn("s;drop\\table");

        try {
            handler.handlerAlterUserReadStrategy(mockSqlAlterUser, mockExecutionContext);
            fail("应该抛出RuntimeException异常");
        } catch (RuntimeException e) {
            assertTrue("错误信息应包含must be composed solely of letters, digits, underscores, and dollar signs",
                e.getMessage().contains("must be composed solely of letters, digits, underscores, and dollar signs"));
        }
    }
}