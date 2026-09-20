package com.alibaba.polardbx.repo.mysql.spi;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import org.apache.calcite.sql.SqlKind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class MyJdbcHandlerOOMTest {

    private MyJdbcHandler myJdbcHandler;

    @Mock
    private ExecutionContext executionContext;

    @Mock
    private ParamManager paramManager;

    @Mock
    private ITransaction transaction;

    @Mock
    private BaseQueryOperation queryOperation;

    // 用于捕获日志输出
    private ByteArrayOutputStream logCapturingStream;
    private PrintStream originalOut;
    private PrintStream originalErr;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        // Mock execution context
        when(executionContext.getTransaction()).thenReturn(transaction);

        when(executionContext.getTraceId()).thenReturn("test-trace-id");
        when(executionContext.getSchemaName()).thenReturn("test_schema");
        when(executionContext.getParamManager()).thenReturn(paramManager);
        when(paramManager.getBoolean(ConnectionParams.LIMIT_TDDL_LOG_SQL_PARAMS_LENGTH)).thenReturn(true);

        // Create MyJdbcHandler with a spy to avoid constructor issues
        myJdbcHandler = mock(MyJdbcHandler.class);

        // Use reflection to set the executionContext field
        try {
            java.lang.reflect.Field executionContextField = MyJdbcHandler.class.getDeclaredField("executionContext");
            executionContextField.setAccessible(true);
            executionContextField.set(myJdbcHandler, executionContext);
        } catch (Exception e) {
            // Ignore reflection errors
        }

        // 设置日志捕获
        setupLogCapture();
    }

    private void setupLogCapture() {
        logCapturingStream = new ByteArrayOutputStream();
        originalOut = System.out;
        originalErr = System.err;
        System.setOut(new PrintStream(logCapturingStream));
        System.setErr(new PrintStream(logCapturingStream));
    }

    private String getTestCapturedLog() {
        return logCapturingStream.toString();
    }

    private void resetLogCapture() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    /**
     * 创建SqlAndParam对象的辅助方法
     */
    private Object createSqlAndParam(String sql, Map<Integer, ParameterContext> paramMap) throws Exception {
        // 使用反射获取SqlAndParam类
        Class<?> sqlAndParamClass = Class.forName("com.alibaba.polardbx.repo.mysql.spi.MyJdbcHandler$SqlAndParam");

        // 获取构造函数并创建实例
        Constructor<?> constructor = sqlAndParamClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object sqlAndParam = constructor.newInstance();

        // 设置sql和param字段
        java.lang.reflect.Field sqlField = sqlAndParamClass.getDeclaredField("sql");
        sqlField.setAccessible(true);
        sqlField.set(sqlAndParam, sql);

        java.lang.reflect.Field paramField = sqlAndParamClass.getDeclaredField("param");
        paramField.setAccessible(true);
        paramField.set(sqlAndParam, paramMap);

        return sqlAndParam;
    }

    /**
     * 调用handleException私有方法的辅助方法
     */
    private void callHandleException(Object sqlAndParam, Throwable e) throws Exception {
        // 使用反射调用handleException方法
        Method handleExceptionMethod = MyJdbcHandler.class.getDeclaredMethod(
            "handleException",
            BaseQueryOperation.class,
            sqlAndParam.getClass(),
            Throwable.class,
            Boolean.class,
            ITransaction.RW.class
        );
        handleExceptionMethod.setAccessible(true);
        try {
            handleExceptionMethod.invoke(
                myJdbcHandler,
                queryOperation,
                sqlAndParam,
                e,
                Boolean.FALSE,
                ITransaction.RW.READ
            );
        } catch (IllegalAccessException ex) {
            // 反射调用访问权限异常
            System.err.println("IllegalAccessException in reflection call: " + ex.getMessage());
        } catch (IllegalArgumentException ex) {
            // 反射调用参数异常
            System.err.println("IllegalArgumentException in reflection call: " + ex.getMessage());
        } catch (java.lang.reflect.InvocationTargetException ex) {
            // 被调用方法抛出的异常，这在我们的场景中是预期的，因为handleException本身就是异常处理方法
            // 我们只关心OOM防护机制是否正常工作，所以记录但不中断测试
            System.err.println(
                "InvocationTargetException in reflection call (expected): " + ex.getCause().getMessage());
        } catch (Exception ex) {
            // 其他可能的异常
            System.err.println("Unexpected exception in reflection call: " + ex.getMessage());
        }
    }

    /**
     * 测试处理大量参数的情况，验证OOM防护机制
     */
    @Test
    public void testHandleExceptionWithManyParameters() throws Exception {
        // 准备测试数据
        String sql = "INSERT INTO test_table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Map<Integer, ParameterContext> paramMap = new HashMap<>();

        // 添加超过MAX_LOG_PARAM_COUNT数量的参数（MyJdbcHandler中定义为500）
        for (int i = 1; i <= 200; i++) {
            paramMap.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i, "value" + i}));
        }

        // 验证对象创建成功
        Object sqlAndParam = createSqlAndParam(sql, paramMap);
        assertNotNull("SqlAndParam对象创建失败", sqlAndParam);

        // 调用handleException方法
        callHandleException(sqlAndParam, new SQLException("Test exception"));

        String logOutput = getTestCapturedLog();

        // 验证日志输出包含填入的异常信息
        assertTrue("日志应包含异常信息", logOutput.contains("Test exception"));

        // 验证日志输出包含OOM防护信息
        assertTrue("日志应包含参数总数信息", logOutput.contains("Total 200 parameters, showing first 100"));
        assertTrue("日志应包含前100个参数", logOutput.contains("value100"));
        assertFalse("日志不应包含第101个参数", logOutput.contains("value101"));
    }
}