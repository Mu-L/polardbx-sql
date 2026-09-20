package com.alibaba.polardbx.executor.pl;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class RuntimeFunctionTest {

    // inner EC keeps its own monitor; outer monitor stays untouched
    @Test
    public void prepareExecutionContextUsesIndependentQuerySpillMonitor() {
        ExecutionContext source = new ExecutionContext();
        QuerySpillSpaceMonitor outerMonitor = Mockito.mock(QuerySpillSpaceMonitor.class);
        source.setQuerySpillSpaceMonitor(outerMonitor);

        SpParameterizedStmt stmt = Mockito.mock(SpParameterizedStmt.class);
        Mockito.when(stmt.getParamsForPlan()).thenReturn(new HashMap<Integer, ParameterContext>());

        MemoryManager memoryManager = Mockito.mock(MemoryManager.class);
        MemoryPool pool = Mockito.mock(MemoryPool.class);
        Mockito.when(memoryManager.createQueryMemoryPool(
            Mockito.anyBoolean(), Mockito.anyString(), Mockito.<Map<String, Object>>any())).thenReturn(pool);

        try (MockedStatic<MemoryManager> mocked = Mockito.mockStatic(MemoryManager.class)) {
            mocked.when(MemoryManager::getInstance).thenReturn(memoryManager);

            ExecutionContext inner = RuntimeFunction.prepareExecutionContext(source, "trace-udf-1", stmt);

            Assert.assertNull(inner.getQuerySpillSpaceMonitor());
            Assert.assertSame(outerMonitor, source.getQuerySpillSpaceMonitor());
            Assert.assertEquals("trace-udf-1", inner.getTraceId());
        }
    }

    // else branch: null paramManager only logs, no exception
    @Test
    public void prepareExecutionContextLogsWhenParamManagerMissing() {
        ExecutionContext source = Mockito.spy(new ExecutionContext());
        Mockito.doReturn(null).when(source).getParamManager();

        SpParameterizedStmt stmt = Mockito.mock(SpParameterizedStmt.class);
        Mockito.when(stmt.getParamsForPlan()).thenReturn(new HashMap<Integer, ParameterContext>());

        MemoryManager memoryManager = Mockito.mock(MemoryManager.class);
        MemoryPool pool = Mockito.mock(MemoryPool.class);
        Mockito.when(memoryManager.createQueryMemoryPool(
            Mockito.anyBoolean(), Mockito.anyString(), Mockito.<Map<String, Object>>any())).thenReturn(pool);

        try (MockedStatic<MemoryManager> mocked = Mockito.mockStatic(MemoryManager.class)) {
            mocked.when(MemoryManager::getInstance).thenReturn(memoryManager);

            ExecutionContext inner = RuntimeFunction.prepareExecutionContext(source, "trace-udf-2", stmt);

            // source paramManager is null, so inner keeps its own copy-default one
            Assert.assertNotNull(inner.getParamManager());
            Assert.assertEquals("trace-udf-2", inner.getTraceId());
        }
    }
}
