package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for LogicalKillHandler's columnar kill-all branch.
 */
public class LogicalKillHandlerTest {

    /**
     * A stub ISyncAction that simulates KillAllColumnarSyncAction behavior.
     * This class is used in tests to replace the real KillAllColumnarSyncAction
     * (which lives in polardbx-server and is not on executor's classpath).
     */
    public static class StubKillAllSyncAction implements ISyncAction {
        private long currentConnId;

        public StubKillAllSyncAction() {
        }

        public StubKillAllSyncAction(long currentConnId) {
            this.currentConnId = currentConnId;
        }

        public long getCurrentConnId() {
            return currentConnId;
        }

        public void setCurrentConnId(long currentConnId) {
            this.currentConnId = currentConnId;
        }

        @Override
        public ResultCursor sync() {
            ArrayResultCursor result = new ArrayResultCursor("KILL_ALL");
            result.addColumn(ResultCursor.AFFECT_ROW, DataTypes.IntegerType);
            result.initMeta();
            result.addRow(new Object[] {5});
            return result;
        }
    }

    private Cursor invokeKillallColumnar(LogicalKillHandler handler, String schemaName,
                                         ExecutionContext executionContext) throws Exception {
        Method method = LogicalKillHandler.class.getDeclaredMethod(
            "killallColumnar", String.class, ExecutionContext.class);
        method.setAccessible(true);
        return (Cursor) method.invoke(handler, schemaName, executionContext);
    }

    private void setKillAllColumnarSyncActionClass(Class<?> clazz) throws Exception {
        Field field = LogicalKillHandler.class.getDeclaredField("killAllColumnarSyncActionClass");
        field.setAccessible(true);
        field.set(null, clazz);
    }

    @Test
    public void testKillallColumnarNormalPath() throws Exception {
        // Set the static field to our stub class
        Class<?> originalClass = getOriginalClass();
        try {
            setKillAllColumnarSyncActionClass(StubKillAllSyncAction.class);

            LogicalKillHandler handler = new LogicalKillHandler(mock(IRepository.class));
            ExecutionContext ec = mock(ExecutionContext.class);
            when(ec.getConnId()).thenReturn(42L);

            // Mock SyncManagerHelper.sync() to return result with affectRow = 3
            List<List<Map<String, Object>>> syncResult = new ArrayList<>();
            List<Map<String, Object>> nodeResult = new ArrayList<>();
            Map<String, Object> row = new HashMap<>();
            row.put(ResultCursor.AFFECT_ROW, 3);
            nodeResult.add(row);
            syncResult.add(nodeResult);

            try (MockedStatic<ExtensionLoader> extMock = mockStatic(ExtensionLoader.class);
                MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class,
                    invocation -> {
                        if (invocation.getMethod().getName().equals("sync")
                            && invocation.getArguments().length == 4) {
                            return syncResult;
                        }
                        return null;
                    })) {

                Cursor cursor = invokeKillallColumnar(handler, "test_schema", ec);

                assertNotNull(cursor);
                assertTrue(cursor instanceof AffectRowCursor);
                int[] affectRows = getAffectRows((AffectRowCursor) cursor);
                assertEquals(3, affectRows[0]);
            }
        } finally {
            setKillAllColumnarSyncActionClass(originalClass);
        }
    }

    @Test
    public void testKillallColumnarMultipleNodes() throws Exception {
        Class<?> originalClass = getOriginalClass();
        try {
            setKillAllColumnarSyncActionClass(StubKillAllSyncAction.class);

            LogicalKillHandler handler = new LogicalKillHandler(mock(IRepository.class));
            ExecutionContext ec = mock(ExecutionContext.class);
            when(ec.getConnId()).thenReturn(1L);

            // Multiple nodes returning results
            List<List<Map<String, Object>>> syncResult = new ArrayList<>();
            List<Map<String, Object>> node1 = new ArrayList<>();
            Map<String, Object> row1 = new HashMap<>();
            row1.put(ResultCursor.AFFECT_ROW, 2);
            node1.add(row1);
            syncResult.add(node1);
            List<Map<String, Object>> node2 = new ArrayList<>();
            Map<String, Object> row2 = new HashMap<>();
            row2.put(ResultCursor.AFFECT_ROW, 5);
            node2.add(row2);
            syncResult.add(node2);

            try (MockedStatic<ExtensionLoader> extMock = mockStatic(ExtensionLoader.class);
                MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class,
                    invocation -> {
                        if (invocation.getMethod().getName().equals("sync")
                            && invocation.getArguments().length == 4) {
                            return syncResult;
                        }
                        return null;
                    })) {

                Cursor cursor = invokeKillallColumnar(handler, "db1", ec);
                int[] affectRows = getAffectRows((AffectRowCursor) cursor);
                assertEquals(7, affectRows[0]); // 2 + 5
            }
        } finally {
            setKillAllColumnarSyncActionClass(originalClass);
        }
    }

    @Test
    public void testKillallColumnarNullResults() throws Exception {
        Class<?> originalClass = getOriginalClass();
        try {
            setKillAllColumnarSyncActionClass(StubKillAllSyncAction.class);

            LogicalKillHandler handler = new LogicalKillHandler(mock(IRepository.class));
            ExecutionContext ec = mock(ExecutionContext.class);
            when(ec.getConnId()).thenReturn(1L);

            try (MockedStatic<ExtensionLoader> extMock = mockStatic(ExtensionLoader.class);
                MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class,
                    invocation -> null)) {

                Cursor cursor = invokeKillallColumnar(handler, "db1", ec);
                int[] affectRows = getAffectRows((AffectRowCursor) cursor);
                assertEquals(0, affectRows[0]);
            }
        } finally {
            setKillAllColumnarSyncActionClass(originalClass);
        }
    }

    @Test
    public void testKillallColumnarClassNotFound() throws Exception {
        Class<?> originalClass = getOriginalClass();
        try {
            // Set to null to simulate class not found
            setKillAllColumnarSyncActionClass(null);

            LogicalKillHandler handler = new LogicalKillHandler(mock(IRepository.class));
            ExecutionContext ec = mock(ExecutionContext.class);
            when(ec.getConnId()).thenReturn(1L);

            try {
                invokeKillallColumnar(handler, "db1", ec);
                fail("Should have thrown TddlRuntimeException");
            } catch (Exception e) {
                // The reflection invoke wraps it in InvocationTargetException
                Throwable cause = e.getCause();
                assertTrue(cause instanceof TddlRuntimeException);
                assertTrue(cause.getMessage().contains("KillAllColumnarSyncAction class not found"));
            }
        } finally {
            setKillAllColumnarSyncActionClass(originalClass);
        }
    }

    @Test
    public void testKillallColumnarWithNullNodeResult() throws Exception {
        Class<?> originalClass = getOriginalClass();
        try {
            setKillAllColumnarSyncActionClass(StubKillAllSyncAction.class);

            LogicalKillHandler handler = new LogicalKillHandler(mock(IRepository.class));
            ExecutionContext ec = mock(ExecutionContext.class);
            when(ec.getConnId()).thenReturn(1L);

            List<List<Map<String, Object>>> syncResult = new ArrayList<>();
            syncResult.add(null); // null node result
            List<Map<String, Object>> validNode = new ArrayList<>();
            Map<String, Object> row = new HashMap<>();
            row.put(ResultCursor.AFFECT_ROW, 4);
            validNode.add(row);
            syncResult.add(validNode);

            try (MockedStatic<ExtensionLoader> extMock = mockStatic(ExtensionLoader.class);
                MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class,
                    invocation -> {
                        if (invocation.getMethod().getName().equals("sync")
                            && invocation.getArguments().length == 4) {
                            return syncResult;
                        }
                        return null;
                    })) {

                Cursor cursor = invokeKillallColumnar(handler, "db1", ec);
                int[] affectRows = getAffectRows((AffectRowCursor) cursor);
                assertEquals(4, affectRows[0]); // only valid node counted
            }
        } finally {
            setKillAllColumnarSyncActionClass(originalClass);
        }
    }

    private Class<?> getOriginalClass() throws Exception {
        Field field = LogicalKillHandler.class.getDeclaredField("killAllColumnarSyncActionClass");
        field.setAccessible(true);
        return (Class<?>) field.get(null);
    }

    private int[] getAffectRows(AffectRowCursor cursor) throws Exception {
        Field field = AffectRowCursor.class.getDeclaredField("affectRows");
        field.setAccessible(true);
        return (int[]) field.get(cursor);
    }
}
