package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link KillAllColumnarSyncAction}.
 */
public class KillAllColumnarSyncActionTest {

    private static ConfigDataMode.Mode originalMode;

    @BeforeClass
    public static void setUp() {
        originalMode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
    }

    @AfterClass
    public static void tearDown() {
        ConfigDataMode.setMode(originalMode);
    }

    @Test
    public void testSyncClosesAllConnectionsExceptCurrent() {
        try (MockedStatic<CobarServer> cobarServerMock = Mockito.mockStatic(CobarServer.class)) {
            CobarServer mockServer = mock(CobarServer.class);
            cobarServerMock.when(CobarServer::getInstance).thenReturn(mockServer);

            NIOProcessor mockProcessor = mock(NIOProcessor.class);
            when(mockServer.getProcessors()).thenReturn(new NIOProcessor[] {mockProcessor});

            // Create 3 connections: id=100 (current), id=200, id=300
            FrontendConnection fc1 = mock(FrontendConnection.class);
            when(fc1.getId()).thenReturn(100L);
            FrontendConnection fc2 = mock(FrontendConnection.class);
            when(fc2.getId()).thenReturn(200L);
            FrontendConnection fc3 = mock(FrontendConnection.class);
            when(fc3.getId()).thenReturn(300L);

            ConcurrentHashMap<Long, FrontendConnection> frontends = new ConcurrentHashMap<>();
            frontends.put(100L, fc1);
            frontends.put(200L, fc2);
            frontends.put(300L, fc3);
            when(mockProcessor.getFrontends()).thenReturn(frontends);

            KillAllColumnarSyncAction action = new KillAllColumnarSyncAction(100L);
            ResultCursor cursor = action.sync();

            // fc1 (current) should NOT be closed
            verify(fc1, never()).close();
            // fc2, fc3 should be closed
            verify(fc2).close();
            verify(fc3).close();

            // Verify result cursor returns affectRow = 2
            assertNotNull(cursor);
            Row row = cursor.next();
            assertNotNull(row);
            assertEquals(2, row.getObject(0));
        }
    }

    @Test
    public void testSyncWithAllConnectionsBeingCurrent() {
        try (MockedStatic<CobarServer> cobarServerMock = Mockito.mockStatic(CobarServer.class)) {
            CobarServer mockServer = mock(CobarServer.class);
            cobarServerMock.when(CobarServer::getInstance).thenReturn(mockServer);

            NIOProcessor mockProcessor = mock(NIOProcessor.class);
            when(mockServer.getProcessors()).thenReturn(new NIOProcessor[] {mockProcessor});

            // Only the current connection exists
            FrontendConnection fc1 = mock(FrontendConnection.class);
            when(fc1.getId()).thenReturn(50L);

            ConcurrentHashMap<Long, FrontendConnection> frontends = new ConcurrentHashMap<>();
            frontends.put(50L, fc1);
            when(mockProcessor.getFrontends()).thenReturn(frontends);

            KillAllColumnarSyncAction action = new KillAllColumnarSyncAction(50L);
            ResultCursor cursor = action.sync();

            verify(fc1, never()).close();

            Row row = cursor.next();
            assertEquals(0, row.getObject(0));
        }
    }

    @Test
    public void testSyncHandlesExceptionGracefully() {
        try (MockedStatic<CobarServer> cobarServerMock = Mockito.mockStatic(CobarServer.class)) {
            CobarServer mockServer = mock(CobarServer.class);
            cobarServerMock.when(CobarServer::getInstance).thenReturn(mockServer);

            NIOProcessor mockProcessor = mock(NIOProcessor.class);
            when(mockServer.getProcessors()).thenReturn(new NIOProcessor[] {mockProcessor});

            // fc2 throws exception on close, fc3 should still be closed
            FrontendConnection fc1 = mock(FrontendConnection.class);
            when(fc1.getId()).thenReturn(1L);
            FrontendConnection fc2 = mock(FrontendConnection.class);
            when(fc2.getId()).thenReturn(2L);
            when(fc2.close()).thenThrow(new RuntimeException("close failed"));
            FrontendConnection fc3 = mock(FrontendConnection.class);
            when(fc3.getId()).thenReturn(3L);

            ConcurrentHashMap<Long, FrontendConnection> frontends = new ConcurrentHashMap<>();
            frontends.put(1L, fc1);
            frontends.put(2L, fc2);
            frontends.put(3L, fc3);
            when(mockProcessor.getFrontends()).thenReturn(frontends);

            KillAllColumnarSyncAction action = new KillAllColumnarSyncAction(1L);
            ResultCursor cursor = action.sync();

            verify(fc1, never()).close();
            verify(fc2).close();
            verify(fc3).close();

            // fc3 closed successfully, fc2 failed but still counted? No - exception means count not incremented
            // The actual count depends on iteration order of ConcurrentHashMap, but:
            // - fc2 throws → not counted
            // - fc3 closes OK → counted
            // Result: at least 1, at most 1 (since fc2 failed)
            Row row = cursor.next();
            int affectRow = (int) row.getObject(0);
            // fc3 should succeed (count=1), fc2 failed (not counted)
            assertEquals(1, affectRow);
        }
    }

    @Test
    public void testSyncWithEmptyProcessors() {
        try (MockedStatic<CobarServer> cobarServerMock = Mockito.mockStatic(CobarServer.class)) {
            CobarServer mockServer = mock(CobarServer.class);
            cobarServerMock.when(CobarServer::getInstance).thenReturn(mockServer);

            when(mockServer.getProcessors()).thenReturn(new NIOProcessor[] {});

            KillAllColumnarSyncAction action = new KillAllColumnarSyncAction(1L);
            ResultCursor cursor = action.sync();

            Row row = cursor.next();
            assertEquals(0, row.getObject(0));
        }
    }

    @Test
    public void testSyncWithMultipleProcessors() {
        try (MockedStatic<CobarServer> cobarServerMock = Mockito.mockStatic(CobarServer.class)) {
            CobarServer mockServer = mock(CobarServer.class);
            cobarServerMock.when(CobarServer::getInstance).thenReturn(mockServer);

            NIOProcessor proc1 = mock(NIOProcessor.class);
            NIOProcessor proc2 = mock(NIOProcessor.class);
            when(mockServer.getProcessors()).thenReturn(new NIOProcessor[] {proc1, proc2});

            FrontendConnection fc1 = mock(FrontendConnection.class);
            when(fc1.getId()).thenReturn(10L);
            FrontendConnection fc2 = mock(FrontendConnection.class);
            when(fc2.getId()).thenReturn(20L);

            ConcurrentHashMap<Long, FrontendConnection> frontends1 = new ConcurrentHashMap<>();
            frontends1.put(10L, fc1);
            when(proc1.getFrontends()).thenReturn(frontends1);

            ConcurrentHashMap<Long, FrontendConnection> frontends2 = new ConcurrentHashMap<>();
            frontends2.put(20L, fc2);
            when(proc2.getFrontends()).thenReturn(frontends2);

            KillAllColumnarSyncAction action = new KillAllColumnarSyncAction(999L);
            ResultCursor cursor = action.sync();

            verify(fc1).close();
            verify(fc2).close();

            Row row = cursor.next();
            assertEquals(2, row.getObject(0));
        }
    }

    @Test
    public void testGetterSetter() {
        KillAllColumnarSyncAction action = new KillAllColumnarSyncAction();
        assertEquals(0L, action.getCurrentConnId());

        action.setCurrentConnId(12345L);
        assertEquals(12345L, action.getCurrentConnId());

        KillAllColumnarSyncAction action2 = new KillAllColumnarSyncAction(99L);
        assertEquals(99L, action2.getCurrentConnId());
    }
}
