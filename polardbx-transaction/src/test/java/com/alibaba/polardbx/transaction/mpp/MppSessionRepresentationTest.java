package com.alibaba.polardbx.transaction.mpp;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.QueryContext;
import com.alibaba.polardbx.executor.mpp.execution.SessionRepresentation;
import com.alibaba.polardbx.executor.mpp.execution.TaskId;
import com.alibaba.polardbx.executor.mpp.server.TaskResource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.htaprouting.PlanType;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadType;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPool;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.trx.AutoCommitTransaction;
import com.alibaba.polardbx.transaction.trx.ColumnarExplicitTransaction;
import com.alibaba.polardbx.transaction.trx.ColumnarTransaction;
import com.alibaba.polardbx.transaction.trx.MppReadOnlyTransaction;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class MppSessionRepresentationTest {

    private final TransactionManager transactionManager = new TransactionManager();

    @Mock
    private ExecutorContext executorContext;
    @Mock
    private StorageInfoManager storageInfoManager;

    private AutoCloseable closeable;
    private MockedStatic<ExecutorContext> executorContextMockedStatic;

    @Before
    public void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        transactionManager.prepare("schema", new HashMap<>(), storageInfoManager);
        ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        executorContextMockedStatic = mockStatic(ExecutorContext.class);
        executorContextMockedStatic.when(() -> ExecutorContext.getContext(anyString())).thenReturn(executorContext);
        when(executorContext.getTransactionManager()).thenReturn(transactionManager);

        TaskResource.setDrdsContextHandler((schemaName, hintCmds, txIsolation) -> {
            ExecutionContext ec = new ExecutionContext();
            ec.setSchemaName(schemaName);
            ec.setInternalSystemSql(false);
            ec.setUsingPhySqlCache(true);
            ec.setExecuteMode(ExecutorMode.MPP);
            ec.setTxIsolation(txIsolation);
            ec.putAllHintCmds(hintCmds);
            return ec;
        });
    }

    @After
    public void tearDown() throws Exception {
        if (closeable != null) {
            closeable.close();
        }
        if (executorContextMockedStatic != null) {
            executorContextMockedStatic.close();
        }
    }

    private static @NotNull SessionRepresentation getSessionRepresentation(long tsoTimeStamp, PlanType planType,
                                                                           boolean autoCommit) {
        Map<String, Object> serverVariables = new HashMap<>();
        Map<String, Object> userDefVariables = new HashMap<>();
        Map<String, Object> hintCmds = new HashMap<>();
        Parameters params = new Parameters(new HashMap<>());

        return new SessionRepresentation(
            "traceId",
            "catalog",
            "schema",
            "user",
            "host",
            "encoding",
            "mdcConnString",
            "sqlMode",
            1,
            1000,
            true,
            1619079085555L,
            serverVariables,
            userDefVariables,
            hintCmds,
            params,
            new HashSet<>(),
            new HashMap<>(),
            null,
            false,
            12345L,
            InternalTimeZone.defaultTimeZone,
            tsoTimeStamp,
            planType,
            "",
            false,
            new HashMap<>(),
            false,
            false,
            false,
            WorkloadType.AP,
            autoCommit,
            System.currentTimeMillis(),
            new HashMap<>());
    }

    @Test
    public void testNonAutoCommitMpp() {
        SessionRepresentation sessionRepresentation =
            getSessionRepresentation(22350L, PlanType.ROW, false);
        QueryContext queryContext = mock(QueryContext.class);
        TaskId taskId = new TaskId("queryId", 1, 1);

        when(queryContext.createQueryMemoryPool(anyString(), any(ExecutionContext.class)))
            .thenReturn(new QueryMemoryPool("test", 1000L, null));
        when(queryContext.createQuerySpillSpaceMonitor()).thenReturn(new QuerySpillSpaceMonitor("test"));

        Session session = sessionRepresentation.toSession(taskId, queryContext, 1L, null);
        assertTrue(session.getClientContext().getTransaction() instanceof MppReadOnlyTransaction);
        assertEquals(22350L,
            ((MppReadOnlyTransaction) session.getClientContext().getTransaction()).getSnapshotSeq());
        assertTrue(session.getClientContext().isAutoCommit());
    }

    @Test
    public void testAutoCommitMpp() {
        SessionRepresentation sessionRepresentation =
            getSessionRepresentation(67890L, PlanType.ROW, true);

        QueryContext queryContext = mock(QueryContext.class);
        ColumnarTracer columnarTracer = new ColumnarTracer();
        TaskId taskId = new TaskId("queryId", 1, 1);

        when(queryContext.createQueryMemoryPool(anyString(), any(ExecutionContext.class)))
            .thenReturn(new QueryMemoryPool("test", 1000L, null));
        when(queryContext.createQuerySpillSpaceMonitor()).thenReturn(new QuerySpillSpaceMonitor("test"));

        Session session = sessionRepresentation.toSession(taskId, queryContext, 1L, columnarTracer);

        assertTrue(session.getClientContext().getTransaction() instanceof MppReadOnlyTransaction);
        assertEquals(67890L, ((MppReadOnlyTransaction) session.getClientContext().getTransaction()).getSnapshotSeq());
        assertTrue(session.getClientContext().isAutoCommit());
    }

    @Test
    public void testAutoCommitColumnar() {
        SessionRepresentation sessionRepresentation =
            getSessionRepresentation(98760L, PlanType.COLUMNAR, true);

        QueryContext queryContext = mock(QueryContext.class);
        ColumnarTracer columnarTracer = new ColumnarTracer();
        TaskId taskId = new TaskId("queryId", 1, 1);

        when(queryContext.createQueryMemoryPool(anyString(), any(ExecutionContext.class)))
            .thenReturn(new QueryMemoryPool("test", 1000L, null));
        when(queryContext.createQuerySpillSpaceMonitor()).thenReturn(new QuerySpillSpaceMonitor("test"));

        Session session = sessionRepresentation.toSession(taskId, queryContext, 1L, columnarTracer);

        assertTrue(session.getClientContext().getTransaction() instanceof ColumnarTransaction);
        assertEquals(98760L, ((ColumnarTransaction) session.getClientContext().getTransaction()).getSnapshotSeq());
        assertTrue(session.getClientContext().isAutoCommit());
        session.generateTsoInfo();
        assertEquals(98760L, session.getTsoTime());
    }

    @Test
    public void testAutoCommitAutoCommit() {
        SessionRepresentation sessionRepresentation =
            getSessionRepresentation(0L, PlanType.ROW_COLUMNAR, true);

        QueryContext queryContext = mock(QueryContext.class);
        ColumnarTracer columnarTracer = new ColumnarTracer();
        TaskId taskId = new TaskId("queryId", 1, 1);

        when(queryContext.createQueryMemoryPool(anyString(), any(ExecutionContext.class)))
            .thenReturn(new QueryMemoryPool("test", 1000L, null));
        when(queryContext.createQuerySpillSpaceMonitor()).thenReturn(new QuerySpillSpaceMonitor("test"));

        Session session = sessionRepresentation.toSession(taskId, queryContext, 1L, columnarTracer);

        assertTrue(session.getClientContext().getTransaction() instanceof AutoCommitTransaction);
        assertTrue(session.getClientContext().isAutoCommit());

    }

    @Test
    public void testExplicitColumnar() {
        SessionRepresentation sessionRepresentation =
            getSessionRepresentation(12350L, PlanType.COLUMNAR, false);

        QueryContext queryContext = mock(QueryContext.class);
        ColumnarTracer columnarTracer = new ColumnarTracer();
        TaskId taskId = new TaskId("queryId", 1, 1);

        when(queryContext.createQueryMemoryPool(anyString(), any(ExecutionContext.class)))
            .thenReturn(new QueryMemoryPool("test", 1000L, null));
        when(queryContext.createQuerySpillSpaceMonitor()).thenReturn(new QuerySpillSpaceMonitor("test"));

        Session session;
        try (MockedStatic<ConfigDataMode> mockedStatic = Mockito.mockStatic(ConfigDataMode.class)) {
            mockedStatic.when(ConfigDataMode::isColumnarMode).thenReturn(true);
            session = sessionRepresentation.toSession(taskId, queryContext, 1L, columnarTracer);
        }

        assertTrue(session.getClientContext().getTransaction() instanceof ColumnarExplicitTransaction);
        assertEquals(12350L,
            ((ColumnarExplicitTransaction) session.getClientContext().getTransaction()).getSnapshotSeq());
        assertFalse(session.getClientContext().isAutoCommit());
        session.generateTsoInfo();
        assertEquals(12350L, session.getTsoTime());
    }

}
