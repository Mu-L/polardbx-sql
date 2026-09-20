package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class KillSyncActionTest {

    private Boolean invokeRollbackOrContinueDdlJobIfNecessary(KillSyncAction action, TConnection conn)
        throws Exception {
        Method method = KillSyncAction.class.getDeclaredMethod(
            "rollbackOrContinueDdlJobIfNecessary", TConnection.class);
        method.setAccessible(true);
        return (Boolean) method.invoke(action, conn);
    }

    @Test
    public void testReturnsFalseWhenConnIsNull() throws Exception {
        KillSyncAction action = new KillSyncAction("test", 1L, true);
        Boolean result = invokeRollbackOrContinueDdlJobIfNecessary(action, null);
        assertFalse(result);
    }

    @Test
    public void testReturnsFalseWhenExecutionContextIsNull() throws Exception {
        KillSyncAction action = new KillSyncAction("test", 1L, true);
        TConnection conn = mock(TConnection.class);
        when(conn.getExecutionContext()).thenReturn(null);
        Boolean result = invokeRollbackOrContinueDdlJobIfNecessary(action, conn);
        assertFalse(result);
    }

    @Test
    public void testReturnsFalseWhenNotDdlStatement() throws Exception {
        KillSyncAction action = new KillSyncAction("test", 1L, true);
        TConnection conn = mock(TConnection.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(conn.getExecutionContext()).thenReturn(ec);
        when(conn.isDdlStatement()).thenReturn(false);
        Boolean result = invokeRollbackOrContinueDdlJobIfNecessary(action, conn);
        assertFalse(result);
    }

    @Test
    public void testReturnsTrueWhenDdlStatementAndJobIdIsNull() throws Exception {
        KillSyncAction action = new KillSyncAction("test", 1L, true);
        TConnection conn = mock(TConnection.class);
        ExecutionContext ec = new ExecutionContext();
        ec.setRunOnNewDdlEngine(true);
        ec.setTraceId("trace-123");
        when(conn.getExecutionContext()).thenReturn(ec);
        when(conn.isDdlStatement()).thenReturn(true);

        Boolean result = invokeRollbackOrContinueDdlJobIfNecessary(action, conn);
        assertTrue("Should return true for DDL with null jobId to skip cancelQuery", result);
    }

    @Test
    public void testReturnsTrueWhenDdlStatementAndJobIdIsNotNull() throws Exception {
        KillSyncAction action = new KillSyncAction("test", 1L, true);
        TConnection conn = mock(TConnection.class);
        ExecutionContext ec = new ExecutionContext();
        ec.setRunOnNewDdlEngine(true);
        DdlContext ddlContext = new DdlContext();
        ddlContext.setJobId(12345L);
        ec.setDdlContext(ddlContext);
        ec.setTraceId("trace-456");
        when(conn.getExecutionContext()).thenReturn(ec);
        when(conn.isDdlStatement()).thenReturn(true);

        try (MockedStatic<DdlEngineRequester> mocked = mockStatic(DdlEngineRequester.class)) {
            mocked.when(() -> DdlEngineRequester.tryRollbackOrContinueJob(anyLong(), any(ExecutionContext.class)))
                .thenReturn(true);

            Boolean result = invokeRollbackOrContinueDdlJobIfNecessary(action, conn);
            assertTrue("Should return true for DDL with valid jobId", result);

            mocked.verify(() -> DdlEngineRequester.tryRollbackOrContinueJob(12345L, ec));
        }
    }

    @Test
    public void testUsesInitialJobIdBeforeDdlJobId() throws Exception {
        KillSyncAction action = new KillSyncAction("test", 1L, true);
        TConnection conn = mock(TConnection.class);
        ExecutionContext ec = new ExecutionContext();
        ec.setRunOnNewDdlEngine(true);
        setDdlInitialJobId(ec, 11L);
        ec.setTraceId("trace-initial");
        when(conn.getExecutionContext()).thenReturn(ec);
        when(conn.isDdlStatement()).thenReturn(true);

        try (MockedStatic<DdlEngineRequester> mocked = mockStatic(DdlEngineRequester.class)) {
            mocked.when(() -> DdlEngineRequester.tryRollbackOrContinueJob(anyLong(), any(ExecutionContext.class)))
                .thenReturn(true);

            Boolean result = invokeRollbackOrContinueDdlJobIfNecessary(action, conn);
            assertTrue("Should return true for DDL with initial jobId", result);

            mocked.verify(() -> DdlEngineRequester.tryRollbackOrContinueJob(11L, ec));
        }
    }

    private void setDdlInitialJobId(ExecutionContext executionContext, Long jobId) throws Exception {
        Field field = ExecutionContext.class.getDeclaredField("ddlInitialJobId");
        field.setAccessible(true);
        field.set(executionContext, jobId);
    }
}
