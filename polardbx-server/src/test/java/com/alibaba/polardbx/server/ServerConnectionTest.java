/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.server;

import com.alibaba.polardbx.CobarConfig;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.net.AbstractConnection;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.handler.NIOHandler;
import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.net.packet.ErrorPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.optimizer.ccl.common.RescheduleTask;
import com.alibaba.polardbx.optimizer.ccl.exception.CclRescheduleException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.gms.sqlaudit.SqlAuditInterceptor;
import com.alibaba.polardbx.server.util.AuditUtil;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.alibaba.polardbx.server.util.MockUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTE_ON_MYSQL;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_SQL_EXCEED_CCL_EXECUTION_TIME;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_DEADLOCK;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServerConnectionTest {

    private ServerConnection serverConnection;
    private Logger logger;

    @Before
    public void setUp() throws IOException {
        serverConnection = Mockito.spy(Mockito.mock(ServerConnection.class));
        logger = Mockito.mock(Logger.class);
    }

    @AfterClass
    public static void cleanUp() {
        DynamicConfig.getInstance().loadValue(null, "MAPPING_TO_MYSQL_ERROR_CODE", "");
    }

    @Test
    public void testLogError() {
        // check EOFException
        ErrorCode errCode = ErrorCode.ERR_HANDLE_DATA;
        Throwable t = new EOFException();
        String sql = "SELECT * FROM table";
        when(logger.isInfoEnabled()).thenReturn(true);
        when(logger.isDebugEnabled()).thenReturn(true);
        when(logger.isWarnEnabled()).thenReturn(true);

        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.info
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).info((Throwable) any());

        // clear invocation
        clearInvocations(serverConnection);
        clearInvocations(logger);

        // check ClosedChannelException
        t = new ClosedChannelException();
        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.info
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).info((Throwable) any());

        // clear invocation
        clearInvocations(serverConnection);
        clearInvocations(logger);

        // check isConnectionReset
        t = new IOException("Connection reset by peer");
        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.info
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).info((Throwable) any());

        // clear invocation
        clearInvocations(serverConnection);
        clearInvocations(logger);

        // check Table doesn't exist
        t = new Exception("Table xx doesn't exist");
        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.debug
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).debug((Throwable) any());

        // clear invocation
        clearInvocations(serverConnection);
        clearInvocations(logger);

        // check Column not found
        t = new Exception("Column xx not found in any table");
        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.debug
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).debug((Throwable) any());

        // clear invocation
        clearInvocations(serverConnection);
        clearInvocations(logger);

        // check isMySQLIntegrityConstraintViolationException
        t = new MySQLIntegrityConstraintViolationException();
        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.debug
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).debug((Throwable) any());

        // clear invocation
        clearInvocations(serverConnection);
        clearInvocations(logger);

        // check warning log
        t = new TddlRuntimeException(ErrorCode.ERR_READ);
        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.debug
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).warn(anyString(), any());

        // clear invocation
        clearInvocations(serverConnection);
        clearInvocations(logger);
    }

    @Test
    public void testHandleError() throws NoSuchFieldException, IllegalAccessException {
        final ExecutionContext ec = new ExecutionContext();
        try (final MockedConstruction<ServerConnection> serverConnectionMockedConstruction =
            mockConstruction(ServerConnection.class, (mock, context) -> {
                doCallRealMethod().when(mock).handleError(any(), any(), anyString(), anyBoolean());
                doNothing().when(mock).writeErrMessage(anyInt(), any(), anyString());
                doReturn(FrontendConnection.PacketOutputState.NONE).when(mock).getPacketOutputState();
            });
        ) {
            final ServerConnection serverConnection = new ServerConnection(null);
            // ServerConnection.conn=null
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);

            final TConnection mockTConnection = mock(TConnection.class);
            when(mockTConnection.getExecutionContext()).thenReturn(ec);

            final Field connField = ServerConnection.class.getDeclaredField("conn");
            connField.setAccessible(true);
            connField.set(serverConnection, mockTConnection);

            // ServerConnection.conn.getExecutionContext().getParamManager()=null
            final ParamManager paramManager = ec.getParamManager();
            ec.setParamManager(null);
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);
            ec.setParamManager(paramManager);

            // OUTPUT_MYSQL_ERROR_CODE=false
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);

            // OUTPUT_MYSQL_ERROR_CODE=true
            ParamManager.setBooleanVal(ec.getParamManager().getProps(),
                ConnectionParams.OUTPUT_MYSQL_ERROR_CODE,
                true,
                false);
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);

            // DynamicConfig.getInstance().getErrorCodeMapping is not empty
            DynamicConfig.getInstance().loadValue(null, "MAPPING_TO_MYSQL_ERROR_CODE", "{4007:1146}");
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);

            // check mapping result
            DynamicConfig.getInstance().loadValue(null, "MAPPING_TO_MYSQL_ERROR_CODE", "{4006:1146}");
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);

            verify(serverConnection, atMost(5)).writeErrMessage(eq(4006), isNull(), anyString());
            verify(serverConnection).writeErrMessage(eq(1146), isNull(), anyString());

            clearInvocations(serverConnection);

            DynamicConfig.getInstance().loadValue(null, "MAPPING_TO_MYSQL_ERROR_CODE", "");

            // check ENABLE_CONSISTENT_ERRORCODE
            DynamicConfig.getInstance().loadValue(null, "ENABLE_CONSISTENT_ERRORCODE", "true");
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);
            DynamicConfig.getInstance().loadValue(null, "ENABLE_CONSISTENT_ERRORCODE", "false");

            verify(serverConnection, times(1)).writeErrMessage(eq(1146), eq("42S02"), anyString());
        } finally {
            DynamicConfig.getInstance().loadValue(null, "ENABLE_CONSISTENT_ERRORCODE", "false");
        }
    }

    @Test
    public void testCleanPartialResultSendsErrBeforeClosingWhenSwitchIsDisabled() {
        DynamicConfig.getInstance().loadValue(null,
            ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "false");
        try {
            ServerConnection target = mockPartialResultConnection(FrontendConnection.PacketOutputState.CLEAN, true);

            target.handleError(ErrorCode.ERR_HANDLE_DATA, new RuntimeException("partial result failed"), "select 1",
                true);

            InOrder inOrder = Mockito.inOrder(target);
            inOrder.verify(target).flushActivePacketOutputProxy();
            inOrder.verify(target).writeErrMessage(anyInt(), isNull(), anyString());
            inOrder.verify(target).close();
        } finally {
            DynamicConfig.getInstance().loadValue(null,
                ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "false");
        }
    }

    @Test
    public void testCleanPartialResultSendsErrBeforeClosingForUnsafeClient() {
        DynamicConfig.getInstance().loadValue(null,
            ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "true");
        try {
            ServerConnection target = mockPartialResultConnection(FrontendConnection.PacketOutputState.CLEAN, false);

            target.handleError(ErrorCode.ERR_HANDLE_DATA, new RuntimeException("partial result failed"), "select 1",
                true);

            InOrder inOrder = Mockito.inOrder(target);
            inOrder.verify(target).flushActivePacketOutputProxy();
            inOrder.verify(target).writeErrMessage(anyInt(), isNull(), anyString());
            inOrder.verify(target).close();
        } finally {
            DynamicConfig.getInstance().loadValue(null,
                ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "false");
        }
    }

    @Test
    public void testCleanPartialResultKeepsConnectionForSafeClient() {
        DynamicConfig.getInstance().loadValue(null,
            ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "true");
        try {
            ServerConnection target = mockPartialResultConnection(FrontendConnection.PacketOutputState.CLEAN, true);

            target.handleError(ErrorCode.ERR_HANDLE_DATA, new RuntimeException("partial result failed"), "select 1",
                true);

            InOrder inOrder = Mockito.inOrder(target);
            inOrder.verify(target).flushActivePacketOutputProxy();
            inOrder.verify(target).writeErrMessage(anyInt(), isNull(), anyString());
            verify(target, Mockito.never()).close();
        } finally {
            DynamicConfig.getInstance().loadValue(null,
                ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "false");
        }
    }

    @Test
    public void testDeadlockCodeAndSqlStateArePreservedForCleanPartialResult() {
        DynamicConfig.getInstance().loadValue(null,
            ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "true");
        try {
            ServerConnection target = mockPartialResultConnection(FrontendConnection.PacketOutputState.CLEAN, true);
            TddlRuntimeException deadlock = new TddlRuntimeException(ER_LOCK_DEADLOCK);
            deadlock.setSQLState("40001");

            target.handleError(ErrorCode.ERR_HANDLE_DATA, deadlock, "select 1", true);

            InOrder inOrder = Mockito.inOrder(target);
            inOrder.verify(target).flushActivePacketOutputProxy();
            inOrder.verify(target).writeErrMessage(eq(1213), eq("40001"), anyString());
            verify(target, Mockito.never()).close();
        } finally {
            DynamicConfig.getInstance().loadValue(null,
                ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "false");
        }
    }

    @Test
    public void testDirtyPartialResultClosesWithoutSendingErr() {
        DynamicConfig.getInstance().loadValue(null,
            ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "true");
        try {
            ServerConnection target = mockPartialResultConnection(FrontendConnection.PacketOutputState.DIRTY, true);

            target.handleError(ErrorCode.ERR_HANDLE_DATA, new RuntimeException("partial result failed"), "select 1",
                true);

            verify(target).close();
            verify(target, Mockito.never()).flushActivePacketOutputProxy();
            verify(target, Mockito.never()).writeErrMessage(anyInt(), isNull(), anyString());
        } finally {
            DynamicConfig.getInstance().loadValue(null,
                ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "false");
        }
    }

    @Test
    public void testPartialResultClosesWithoutErrWhenPendingBufferFlushFails() {
        DynamicConfig.getInstance().loadValue(null,
            ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "true");
        try {
            ServerConnection target = mockPartialResultConnection(FrontendConnection.PacketOutputState.CLEAN, true);
            doThrow(new RuntimeException("flush failed")).when(target).flushActivePacketOutputProxy();

            target.handleError(ErrorCode.ERR_HANDLE_DATA, new RuntimeException("partial result failed"), "select 1",
                true);

            InOrder inOrder = Mockito.inOrder(target);
            inOrder.verify(target).flushActivePacketOutputProxy();
            inOrder.verify(target).close();
            verify(target, Mockito.never()).writeErrMessage(anyInt(), isNull(), anyString());
        } finally {
            DynamicConfig.getInstance().loadValue(null,
                ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "false");
        }
    }

    @Test
    public void testCleanOutputWithoutRowsKeepsConnection() {
        DynamicConfig.getInstance().loadValue(null,
            ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "false");
        try {
            ServerConnection target = mockPartialResultConnection(FrontendConnection.PacketOutputState.CLEAN, false);

            target.handleError(ErrorCode.ERR_HANDLE_DATA, new RuntimeException("query failed"), "select 1", false);

            InOrder inOrder = Mockito.inOrder(target);
            inOrder.verify(target).flushActivePacketOutputProxy();
            inOrder.verify(target).writeErrMessage(anyInt(), isNull(), anyString());
            verify(target, Mockito.never()).close();
        } finally {
            DynamicConfig.getInstance().loadValue(null,
                ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "false");
        }
    }

    @Test
    public void testDirtyOutputWithoutRowsClosesWithoutErr() {
        ServerConnection target =
            mockPartialResultConnection(FrontendConnection.PacketOutputState.DIRTY, true);

        target.handleError(ErrorCode.ERR_HANDLE_DATA, new RuntimeException("query failed"), "select 1", false);

        verify(target).close();
        verify(target, Mockito.never()).flushActivePacketOutputProxy();
        verify(target, Mockito.never()).writeErrMessage(anyInt(), isNull(), anyString());
    }

    @Test
    public void testFatalErrorWithoutPacketBoundaryClosesWithoutErr() {
        ServerConnection target =
            mockPartialResultConnection(FrontendConnection.PacketOutputState.NONE, true);

        target.handleError(ErrorCode.ERR_HANDLE_DATA, new RuntimeException("query failed"), "select 1", true);

        verify(target).close();
        verify(target, Mockito.never()).flushActivePacketOutputProxy();
        verify(target, Mockito.never()).writeErrMessage(anyInt(), isNull(), anyString());
    }

    @Test
    public void testErrorWithoutPacketOutputFlushesBeforeErr() {
        ServerConnection target =
            mockPartialResultConnection(FrontendConnection.PacketOutputState.NONE, false);

        target.handleError(ErrorCode.ERR_HANDLE_DATA, new RuntimeException("query failed"), "select 1", false);

        // The central cleanup always releases a possibly registered active proxy before the ERR packet.
        InOrder inOrder = Mockito.inOrder(target);
        inOrder.verify(target).flushActivePacketOutputProxy();
        inOrder.verify(target).writeErrMessage(anyInt(), isNull(), anyString());
        verify(target, Mockito.never()).close();
    }

    @Test
    public void testErrorWithoutPacketOutputClosesWhenFlushFails() {
        ServerConnection target =
            mockPartialResultConnection(FrontendConnection.PacketOutputState.NONE, false);
        Mockito.doThrow(new RuntimeException("flush failed")).when(target).flushActivePacketOutputProxy();

        target.handleError(ErrorCode.ERR_HANDLE_DATA, new RuntimeException("query failed"), "select 1", false);

        InOrder inOrder = Mockito.inOrder(target);
        inOrder.verify(target).flushActivePacketOutputProxy();
        inOrder.verify(target).close();
        verify(target, Mockito.never()).writeErrMessage(anyInt(), isNull(), anyString());
    }

    private ServerConnection mockPartialResultConnection(FrontendConnection.PacketOutputState state,
                                                         boolean clientSafe) {
        ServerConnection target = mock(ServerConnection.class);
        doCallRealMethod().when(target).handleError(any(), any(), anyString(), anyBoolean());
        when(target.getPacketOutputState()).thenReturn(state);
        when(target.isClientSafeForErrAfterPartialResult()).thenReturn(clientSafe);
        return target;
    }

    @Test
    public void testSet() {
        serverConnection.setUser("polardbx_root");
        Assert.assertTrue(serverConnection.isPolardbxRoot());
    }

    @Test
    public void testSwitchDbSameSchemaDoesNotCloseConnection() throws Exception {
        ServerConnection connection = mock(ServerConnection.class);
        TConnection currentConnection = mock(TConnection.class);
        SchemaConfig currentSchema = new SchemaConfig("same_schema");
        doCallRealMethod().when(connection).switchDb(anyString());

        Field connField = ServerConnection.class.getDeclaredField("conn");
        connField.setAccessible(true);
        connField.set(connection, currentConnection);

        Field schemaConfigField = ServerConnection.class.getDeclaredField("schemaConfig");
        schemaConfigField.setAccessible(true);
        schemaConfigField.set(connection, currentSchema);

        boolean originalSwitchValue = DynamicConfig.getInstance().isEnableSameDbSwitchNoop();
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_SAME_DB_SWITCH_NOOP, "true");
        try {
            connection.switchDb("SAME_SCHEMA");
        } finally {
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_SAME_DB_SWITCH_NOOP,
                Boolean.toString(originalSwitchValue));
        }

        verify(currentConnection, never()).close();
        org.junit.Assert.assertSame(currentSchema, schemaConfigField.get(connection));
    }

    @Test
    public void testLock() {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (MockedStatic<CobarServer> mockedStaticServer = Mockito.mockStatic(CobarServer.class);
            MockedStatic<PacketUtil> mockedUtil = Mockito.mockStatic(PacketUtil.class);
            MockedStatic<PacketOutputProxyFactory> mockedStaticPacketFactory = Mockito.mockStatic(
                PacketOutputProxyFactory.class)) {

            CobarServer mockedServer = mock(CobarServer.class);
            PacketOutputProxyFactory mockedPacketFactory = mock(PacketOutputProxyFactory.class);
            CobarConfig mockedConfig = mock(CobarConfig.class);

            mockedStaticServer.when(CobarServer::getInstance).thenReturn(mockedServer);
            mockedStaticPacketFactory.when(PacketOutputProxyFactory::getInstance).thenReturn(mockedPacketFactory);
            when(mockedServer.getConfig()).thenReturn(mockedConfig);
            IPacketOutputProxy proxy = mock(IPacketOutputProxy.class);
            when(mockedConfig.isLock()).thenReturn(true);

            ErrorPacket mockedPacket = mock(ErrorPacket.class);
            final AtomicInteger errCount = new AtomicInteger(0);

            doAnswer((invocation) -> {
                errCount.incrementAndGet();
                return proxy;
            }).when(mockedPacket).write(proxy);
            mockedUtil.when(PacketUtil::getLock).thenReturn(mockedPacket);

            doReturn(true).when(serverConnection).isClosed();

            serverConnection.setUser("testUser");
            Assert.assertTrue(!serverConnection.isPolardbxRoot());

            ServerQueryHandler queryHandler = new ServerQueryHandler(serverConnection);
            serverConnection.setConnectionCharset("utf8");
            serverConnection.setQueryHandler(queryHandler);

            try {
                serverConnection.query(new byte[] {
                    33, 0, 0, 0, 3, 115, 101, 108, 101, 99, 116, 32, 64, 64, 118, 101, 114, 115, 105,
                    111, 110, 95, 99, 111, 109, 109, 101, 110, 116, 32, 108, 105, 109, 105, 116, 32, 49});
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        } finally {
            ConfigDataMode.setMode(mode);
        }
    }

    @Test
    public void testReschedule() throws Exception {
        Field field = FrontendConnection.class.getDeclaredField("executingFuture");
        field.setAccessible(true); // Make the private field accessible
        final AtomicReference<Future<?>> objectAtomicReference = new AtomicReference<>();
        field.set(serverConnection, objectAtomicReference);
        serverConnection.setRescheduled(true,
            new RescheduleTask(null, 0, 0, null, false, true, false, new AtomicBoolean(false)));
        final ServerThreadPool mockedPool = spy(new ServerThreadPool("test", 1, 1000));
        final NIOProcessor mockedProcessor = Mockito.mock(NIOProcessor.class);
        when(mockedProcessor.getHandler()).thenReturn(mockedPool);
        field = AbstractConnection.class.getDeclaredField("processor");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, mockedProcessor);

        field = AbstractConnection.class.getDeclaredField("isClosed");
        field.setAccessible(true); // Make the private field accessible
        final AtomicBoolean isClosed = new AtomicBoolean(false);
        field.set(serverConnection, isClosed);

        final ServerConnection.RescheduleParam param =
            new ServerConnection.RescheduleParam(new ByteString("xx".getBytes(), Charset.defaultCharset()), null, null,
                null);
        field = ServerConnection.class.getDeclaredField("rescheduleParam");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, param);

        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        final CobarConfig cobarConfig = Mockito.mock(CobarConfig.class);
        field = CobarServer.class.getDeclaredField("config");
        field.setAccessible(true); // Make the private field accessible
        field.set(CobarServer.getInstance(), cobarConfig);
        final SystemConfig systemConfig = Mockito.mock(SystemConfig.class);
        when(cobarConfig.getSystem()).thenAnswer(invocation -> systemConfig);
        when(systemConfig.getSqlSimpleMaxLen()).thenAnswer(invocation -> 4000);
        doAnswer(invocation -> CompletableFuture.completedFuture(true)).when(serverConnection)
            .innerExecute(any(), any(), any(), any());

        final ExecutionContext ec = mock(ExecutionContext.class);
        when(serverConnection.getExecutionContext()).thenReturn(ec);
        when(ec.getRescheduleStmtDone()).thenReturn(new AtomicBoolean());

        serverConnection.reschedule(null);
        objectAtomicReference.get().get();

        serverConnection.setRescheduled(true,
            new RescheduleTask(null, 0, 0, null, false, true, false, new AtomicBoolean(false)));
        isClosed.set(true);

        serverConnection.reschedule(null);
        try {
            objectAtomicReference.get().get();
        } catch (Throwable ignore) {
        }
    }

    @Test
    public void testCancel() throws Exception {
        Field field = ServerConnection.class.getDeclaredField("statementExecuting");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, new AtomicBoolean());
        doNothing().when(serverConnection).handleError(any(ErrorCode.class), any(Throwable.class));
        serverConnection.setRescheduled(true,
            new RescheduleTask(null, 0, 0, null, false, true, false, new AtomicBoolean(false)));
        serverConnection.cancelQuery(ErrorCode.ERR_HANDLE_DATA);
    }

    @Test
    public void testHandleData() throws Exception {
        Field field = FrontendConnection.class.getDeclaredField("executingFuture");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, new AtomicReference<>());

        final NIOProcessor processor = mock(NIOProcessor.class);
        final ServerThreadPool pool = mock(ServerThreadPool.class);
        when(pool.submit(anyString(), anyString(), anyInt(), any(Runnable.class))).thenAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(3);
            runnable.run();
            return null;
        });
        when(processor.getHandler()).thenReturn(pool);
        field = AbstractConnection.class.getDeclaredField("processor");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, processor);

        final NIOHandler handler = mock(NIOHandler.class);
        field = FrontendConnection.class.getDeclaredField("handler");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, handler);

        final byte[] data = new byte[] {1, 0, 0, 0, 1};
        serverConnection.handleData(data);
    }

    @Test
    public void testHandleDataBinlog() throws Exception {
        Field field = FrontendConnection.class.getDeclaredField("executingFuture");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, new AtomicReference<>(CompletableFuture.completedFuture(true)));

        final NIOProcessor processor = mock(NIOProcessor.class);
        final ServerThreadPool pool = mock(ServerThreadPool.class);
        when(pool.submit(anyString(), anyString(), anyInt(), any(Runnable.class))).thenAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(3);
            runnable.run();
            return null;
        });
        when(processor.getHandler()).thenReturn(pool);
        field = AbstractConnection.class.getDeclaredField("processor");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, processor);

        final NIOHandler handler = mock(NIOHandler.class);
        field = FrontendConnection.class.getDeclaredField("handler");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, handler);

        field = FrontendConnection.class.getDeclaredField("isBinlogDumpConn");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, true);

        final byte[] data = new byte[] {1, 0, 0, 0, 1};
        serverConnection.handleData(data);
    }

    @Test
    public void testHandleDataThrow() throws Exception {
        Field field = FrontendConnection.class.getDeclaredField("executingFuture");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, new AtomicReference<>(CompletableFuture.completedFuture(true)));

        final NIOProcessor processor = mock(NIOProcessor.class);
        final ServerThreadPool mockedPool = spy(new ServerThreadPool("test", 1, 1000, 1));
        when(processor.getHandler()).thenReturn(mockedPool);
        field = AbstractConnection.class.getDeclaredField("processor");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, processor);

        final NIOHandler handler = mock(NIOHandler.class);
        field = FrontendConnection.class.getDeclaredField("handler");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, handler);

        field = FrontendConnection.class.getDeclaredField("rescheduled");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, true);

        field = FrontendConnection.class.getDeclaredField("packageLimit");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, 16777215);

        field = AbstractConnection.class.getDeclaredField("isClosed");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, new AtomicBoolean(true));

        final byte[] data0 = new byte[] {1, 0, 0, 0, 1};
        long cnt = mockedPool.getCompletedTaskCount();
        serverConnection.handleData(data0);
        while (mockedPool.getCompletedTaskCount() < cnt + 1) {
            Thread.sleep(1);
        }

        field = FrontendConnection.class.getDeclaredField("isBinlogDumpConn");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, true);

        cnt = mockedPool.getCompletedTaskCount();
        serverConnection.handleData(data0);
        while (mockedPool.getCompletedTaskCount() < cnt + 1) {
            Thread.sleep(1);
        }

        field.set(serverConnection, false);

        final byte[] data1 = new byte[] {1, 0, 0, 0, 2};
        cnt = mockedPool.getCompletedTaskCount();
        serverConnection.handleData(data1);
        while (mockedPool.getCompletedTaskCount() < cnt + 1) {
            Thread.sleep(1);
        }

        field = FrontendConnection.class.getDeclaredField("rescheduled");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, false);

        cnt = mockedPool.getCompletedTaskCount();
        serverConnection.handleData(data0);
        while (mockedPool.getCompletedTaskCount() < cnt + 1) {
            Thread.sleep(1);
        }

        field = FrontendConnection.class.getDeclaredField("executingFuture");
        field.setAccessible(true); // Make the private field accessible
        final Future<?> future = spy(CompletableFuture.completedFuture(true));
        field.set(serverConnection, new AtomicReference<>(future));

        when(future.get()).thenThrow(new RuntimeException("test"));

        cnt = mockedPool.getCompletedTaskCount();
        serverConnection.handleData(data0);
        while (mockedPool.getCompletedTaskCount() < cnt + 1) {
            Thread.sleep(1);
        }
    }

    @Test
    public void testExecute() {
        doNothing().when(serverConnection).writeErrMessage(any(), anyString());
        serverConnection.execute(null, true, true, null, -1, MySQLPacket.CURSOR_TYPE_NO_CURSOR, null, null);

        doAnswer(invocation -> CompletableFuture.completedFuture(true))
            .when(serverConnection)
            .execute(any(), anyBoolean(), anyBoolean(), any(), anyInt(), anyByte(), any(), any());
        serverConnection.execute(null, true, true, null, null, null);
        serverConnection.execute(null, null, -1, (byte) 1, null);
        serverConnection.execute(new ByteString("test".getBytes(), Charset.defaultCharset()), false);
        serverConnection.executeFuture(new ByteString("test".getBytes(), Charset.defaultCharset()), false);

        doAnswer(invocation -> new FutureTask<>(() -> false))
            .when(serverConnection)
            .execute(any(), anyBoolean(), anyBoolean(), any(), anyInt(), anyByte(), any(), any());
        serverConnection.execute(null, true, true, null, null, null);
        serverConnection.execute(null, null, -1, (byte) 1, null);
        serverConnection.execute(new ByteString("test".getBytes(), Charset.defaultCharset()), false);

        doThrow(new RuntimeException("test")).when(serverConnection)
            .execute(any(), anyBoolean(), anyBoolean(), any(), anyInt(), anyByte(), any(), any());
        try {
            serverConnection.execute(null, true, true, null, null, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof TddlRuntimeException);
        }
        try {
            serverConnection.execute(null, null, -1, (byte) 1, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof TddlRuntimeException);
        }
    }

    @Test
    public void testAfterExecution() throws Exception {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_EXTREME_PERFORMANCE, "true");

        final TConnection tConnection = mock(TConnection.class);
        Field field = ServerConnection.class.getDeclaredField("conn");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, tConnection);

        serverConnection.afterExecution(null, null, null, null, 0, null, 0, null, 1);
        serverConnection.afterExecution(null, null, null, null, 0, null, 0, new CclRescheduleException("xx", null), 1);
    }

    @Test
    @Ignore
    public void testInnerExecute() throws Exception {
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (final MockedStatic<CobarServer> cobarServerMockedStatic = Mockito.mockStatic(CobarServer.class);
            final MockedStatic<PacketOutputProxyFactory> packetOutputProxyFactoryMockedStatic = mockStatic(
                PacketOutputProxyFactory.class)) {
            final CobarServer cobarServer = mock(CobarServer.class);
            cobarServerMockedStatic.when(CobarServer::getInstance).thenReturn(cobarServer);
            when(cobarServer.isOnline()).thenReturn(false);

            final PacketOutputProxyFactory packetOutputProxyFactory = mock(PacketOutputProxyFactory.class);
            packetOutputProxyFactoryMockedStatic.when(PacketOutputProxyFactory::getInstance)
                .thenReturn(packetOutputProxyFactory);
            when(packetOutputProxyFactory.createProxy(any(FrontendConnection.class))).thenAnswer(invocation -> null);

            Field field = AbstractConnection.class.getDeclaredField("isClosed");
            field.setAccessible(true); // Make the private field accessible
            final AtomicBoolean isClosed = new AtomicBoolean(true);
            field.set(serverConnection, isClosed);

            field = ServerConnection.class.getDeclaredField("autocommit");
            field.setAccessible(true); // Make the private field accessible
            field.set(serverConnection, true);

            final ErrorPacket errorPacket = mock(ErrorPacket.class);
            field = ServerConnection.class.getDeclaredField("shutDownError");
            field.setAccessible(true); // Make the private field accessible
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(serverConnection, errorPacket);

            serverConnection.innerExecute(null, null, null, null);

            when(cobarServer.isOnline()).thenReturn(true);

            serverConnection.innerExecute(null, null, null, null);

            isClosed.set(false);
            doNothing().when(serverConnection).writeErrMessage(any(), anyString());

            serverConnection.innerExecute(null, null, null, null);

            final SchemaConfig schemaConfig = mock(SchemaConfig.class);
            doAnswer(invocation -> schemaConfig).when(serverConnection).getSchemaConfig();

            field = ServerConnection.class.getDeclaredField("statementExecuting");
            field.setAccessible(true); // Make the private field accessible
            field.set(serverConnection, new AtomicBoolean());

            field = ServerConnection.class.getDeclaredField("sqlMock");
            field.setAccessible(true); // Make the private field accessible
            field.set(serverConnection, true);

            doNothing().when(serverConnection).processMock(any());

            serverConnection.innerExecute(null, null, null, null);

            field.set(serverConnection, false);
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);

            try (final MockedStatic<MockUtil> mockUtilMockedStatic = mockStatic(MockUtil.class)) {
                mockUtilMockedStatic.when(() -> MockUtil.mockSchema(anyString())).thenAnswer(invocation -> null);

                final QueryResultHandler handler = mock(QueryResultHandler.class);
                serverConnection.innerExecute(new ByteString("drop database aa".getBytes(), Charset.defaultCharset()),
                    null, handler, null);
            }

            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
            doNothing().when(serverConnection).getConnection(any());

            field = ServerConnection.class.getDeclaredField("mdlContextLock");
            field.setAccessible(true); // Make the private field accessible
            field.set(serverConnection, new Object());

            serverConnection.innerExecute(null, null, null, null);

            doThrow(new CclRescheduleException("xx", reschedulable -> {
            })).when(serverConnection).getConnection(any());

            serverConnection.innerExecute(null, null, null, null);

            doThrow(new RuntimeException()).when(serverConnection).getConnection(any());

            serverConnection.innerExecute(null, null, null, null);
        }
    }

    @Test
    public void testWaitReschedule() throws Exception {
        final ExecutionContext ec = mock(ExecutionContext.class);
        doReturn(ec).when(serverConnection).getExecutionContext();

        final FutureTask<Boolean> booleanFutureTask = spy(new FutureTask<>(() -> true));
        booleanFutureTask.run();
        when(ec.getRescheduleStmtFuture()).thenReturn(booleanFutureTask);
        serverConnection.waitReschedule();

        when(booleanFutureTask.get()).thenThrow(new RuntimeException());
        serverConnection.waitReschedule();
    }

    @Test
    public void testPrepareBinlogDumpExtConfig() {
        Map<String, Object> variables = new HashMap<>();
        Map<String, Object> connectionVariables = new HashMap<>();
        connectionVariables.put(ConnectionParams.BINLOG_DUMP_ARCHIVE_IGNORE_ENABLED.getName(), "true");
        MockedStatic<InstConfUtil> instConfUtilMockedStatic = Mockito.mockStatic(InstConfUtil.class);
        instConfUtilMockedStatic.when(
            () -> InstConfUtil.getValNoDefault(ConnectionParams.BINLOG_DUMP_FILTER_USER_CONFIG)
        ).thenReturn("{\"polardbx_root\":{\"BINLOG_DUMP_ROWS_QUERY_IGNORE_ENABLED\":\"false\"}}");
        instConfUtilMockedStatic.when(
            () -> InstConfUtil.getValNoDefault(ConnectionParams.BINLOG_DUMP_IGNORE_TABLE)
        ).thenReturn("zm.ignore_tb");
        instConfUtilMockedStatic.when(
            () -> InstConfUtil.getOriginVal(ConnectionParams.BINLOG_DUMP_DO_TABLE)
        ).thenReturn("zm.test_tb");
        when(serverConnection.getUserDefVariables()).thenReturn(variables);
        when(serverConnection.getConnectionVariables()).thenReturn(connectionVariables);
        serverConnection.setUser("polardbx_root");
        Map<String, Object> ext = serverConnection.prepareBinlogDumpExtConfig();
        Assert.assertEqual((boolean) ext.get("archive_ignore"), true);
        Assert.assertEqual((boolean) ext.get("rows_query_ignore"), false);
        Assert.assertEqual((String) ext.get("table_ignore"), "zm.ignore_tb");
        instConfUtilMockedStatic.close();
    }

    @Test
    public void testException() {
        Throwable e = new TddlRuntimeException(ERR_EXECUTE_ON_MYSQL, "test");
        Throwable real = ServerConnection.getRealException(e, null);
        Assert.assertTrue(real == e);

        real = ServerConnection.getRealException(e, ER_LOCK_DEADLOCK);
        Assert.assertTrue(((TddlRuntimeException) real).getErrorCode() == ER_LOCK_DEADLOCK.getCode());
        Assert.assertTrue(((TddlRuntimeException) real).getSQLState().equalsIgnoreCase("40001"));
    }

    @Test
    public void testErrorCode() {
        Throwable e = new TddlRuntimeException(ERR_SQL_EXCEED_CCL_EXECUTION_TIME, "test");
        int code = ServerConnection.getErrorCode(e);
        Assert.assertEqual(code, ERR_SQL_EXCEED_CCL_EXECUTION_TIME.getCode());
        Assert.assertEqual(false, ServerConnection.isDeadLockException(e, "test"));

        e = new TddlRuntimeException(ER_LOCK_DEADLOCK, "test");
        code = ServerConnection.getErrorCode(e);
        Assert.assertEqual(code, ER_LOCK_DEADLOCK.getCode());
        Assert.assertEqual(true, ServerConnection.isDeadLockException(e, "test"));

        e = new TddlRuntimeException(ER_LOCK_DEADLOCK,
            new SQLException("Deadlock found", "", ER_LOCK_DEADLOCK.getCode()));
        code = ServerConnection.getErrorCode(e);
        Assert.assertEqual(code, ER_LOCK_DEADLOCK.getCode());
        Assert.assertEqual(true, ServerConnection.isDeadLockException(e, "test"));

        e = new SQLException("Deadlock found", "", ER_LOCK_DEADLOCK.getCode());
        code = ServerConnection.getErrorCode(e);
        Assert.assertEqual(code, -1);
    }

    @Test
    public void testLogout() {
        try (MockedStatic<SqlAuditInterceptor> sqlAuditInterceptorMockedStatic =
            mockStatic(SqlAuditInterceptor.class);
            MockedStatic<AuditUtil> auditUtilMockedStatic = mockStatic(AuditUtil.class)) {

            // Mock SqlAuditInterceptor.hasLogoutPerm to return false, so sqlAuditLogout won't be called
            sqlAuditInterceptorMockedStatic.when(() -> SqlAuditInterceptor.hasLogoutPerm(anyString()))
                .thenReturn(false);

            // Mock AuditUtil.logAuditInfo to do nothing
            auditUtilMockedStatic.when(() -> AuditUtil.logAuditInfo(
                    any(ServerConnection.class), anyString(), anyString(), anyString(), anyString(), anyInt(), any()))
                .then(invocation -> null);

            doNothing().when(serverConnection).setLastActiveTime(any(Long.class));
            doReturn("testUser").when(serverConnection).getUser();
            doReturn("testInstance").when(serverConnection).getInstanceId();
            doReturn("testSchema").when(serverConnection).getSchema();
            doReturn("localhost").when(serverConnection).getHost();
            doReturn(3306).when(serverConnection).getPort();
            doCallRealMethod().when(serverConnection).logout();

            // Call logout and ensure no exception is thrown
            serverConnection.logout();

            // Test with hasLogoutPerm returning true
            sqlAuditInterceptorMockedStatic.when(() -> SqlAuditInterceptor.hasLogoutPerm(anyString()))
                .thenReturn(true);
            doNothing().when(serverConnection).sqlAuditLogout();

            serverConnection.logout();
        }
    }

    @Test
    public void testLogoutRtNotCorruptedByStaleLastActiveTime() throws Exception {
        // AONE-84826254: Logout rt in sql.log must reflect only the Logout processing time.
        // It must NOT include a stale lastActiveTime left over by a concurrent afterExecution()
        // call that finished handling a slow SQL right before this Logout is audited.
        ch.qos.logback.classic.Logger sqlLoggerImpl =
            (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("sql");
        ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> listAppender =
            new ch.qos.logback.core.read.ListAppender<>();
        listAppender.setContext((ch.qos.logback.classic.LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory());
        listAppender.start();
        ch.qos.logback.classic.Level originalLevel = sqlLoggerImpl.getLevel();
        sqlLoggerImpl.addAppender(listAppender);
        sqlLoggerImpl.setLevel(ch.qos.logback.classic.Level.INFO);

        try {
            com.alibaba.polardbx.config.SchemaConfig schemaConfig =
                new com.alibaba.polardbx.config.SchemaConfig("testSchema");
            com.alibaba.polardbx.matrix.jdbc.TDataSource mockDs =
                mock(com.alibaba.polardbx.matrix.jdbc.TDataSource.class);
            when(mockDs.getConnectionProperties()).thenReturn(new HashMap<>());
            schemaConfig.setDataSource(mockDs);

            doReturn("testUser").when(serverConnection).getUser();
            doReturn("testInstance").when(serverConnection).getInstanceId();
            doReturn("testSchema").when(serverConnection).getSchema();
            doReturn("localhost").when(serverConnection).getHost();
            doReturn(3306).when(serverConnection).getPort();
            doReturn(1L).when(serverConnection).getId();
            doReturn("traceId-1").when(serverConnection).getTraceId();
            doReturn(true).when(serverConnection).isAutocommit();
            doReturn(schemaConfig).when(serverConnection).getSchemaConfig();
            doReturn(null).when(serverConnection).getDroppedSchemaConfigIfExists();
            doReturn(null).when(serverConnection).getExecutionContext();

            // Simulate afterExecution() having overwritten lastActiveTime with a completion
            // timestamp of an unrelated, already-finished long-running SQL, 5 seconds ago.
            long staleLastActiveTime = System.nanoTime() - 5_000_000_000L;
            doReturn(staleLastActiveTime).when(serverConnection).getLastActiveTime();

            doCallRealMethod().when(serverConnection).sqlAuditLogout();

            serverConnection.sqlAuditLogout();

            org.junit.Assert.assertTrue("Logout should be recorded in sql.log",
                listAppender.list.size() > 0);
            String logLine = listAppender.list.get(listAppender.list.size() - 1).getFormattedMessage();

            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("rt=(-?\\d+)").matcher(logLine);
            org.junit.Assert.assertTrue("Logout log line must contain rt= field: " + logLine, matcher.find());
            long rtMicros = Long.parseLong(matcher.group(1));

            org.junit.Assert.assertTrue(
                "Logout rt should reflect only Logout processing time (<=10000us), but was " + rtMicros
                    + "us, indicating lastActiveTime was polluted by a stale timestamp from afterExecution()",
                rtMicros <= 10000);
        } finally {
            sqlLoggerImpl.detachAppender(listAppender);
            sqlLoggerImpl.setLevel(originalLevel);
            listAppender.stop();
        }
    }

    @Test
    public void testTransactionCommandsShouldNotIncrementRequestWhenSwitchOff() throws Exception {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (final MockedConstruction<ServerConnection> scConstruction =
            mockConstruction(ServerConnection.class, (mock, context) -> {
                doCallRealMethod().when(mock).commit(anyBoolean());
                doCallRealMethod().when(mock).rollback(anyBoolean());
                doCallRealMethod().when(mock).begin();
                doCallRealMethod().when(mock).begin(anyBoolean(), any());
                doCallRealMethod().when(mock).getStatistics();
                doNothing().when(mock).innerCommit();
                doNothing().when(mock).innerRollback();
                doNothing().when(mock).setAutocommit(anyBoolean(), anyBoolean());
                doNothing().when(mock).setReadOnly(anyBoolean());
                doNothing().when(mock).setStmtTxIsolation(anyInt());
                doReturn(mock(ByteBufferHolder.class)).when(mock).allocate();
                doNothing().when(mock).handleError(any(), any(), anyString(), anyBoolean());
            });
            MockedStatic<PacketOutputProxyFactory> packetFactoryMock = mockStatic(PacketOutputProxyFactory.class);
            MockedStatic<DynamicConfig> dynamicConfigMock = mockStatic(DynamicConfig.class)
        ) {
            PacketOutputProxyFactory mockFactory = mock(PacketOutputProxyFactory.class);
            packetFactoryMock.when(PacketOutputProxyFactory::getInstance).thenReturn(mockFactory);
            IPacketOutputProxy mockProxy = mock(IPacketOutputProxy.class);
            when(mockFactory.createProxy(any(FrontendConnection.class), any(ByteBufferHolder.class))).thenReturn(
                mockProxy);

            DynamicConfig mockConfig = mock(DynamicConfig.class);
            dynamicConfigMock.when(DynamicConfig::getInstance).thenReturn(mockConfig);

            // Switch OFF: should NOT increment request count
            when(mockConfig.isEnableTransactionQpsCount()).thenReturn(false);

            final ServerConnection sc = new ServerConnection(null);

            MatrixStatistics stats = new MatrixStatistics();
            Field statsField = ServerConnection.class.getDeclaredField("stats");
            statsField.setAccessible(true);
            statsField.set(sc, stats);

            TConnection mockConn = mock(TConnection.class);
            Field connField = ServerConnection.class.getDeclaredField("conn");
            connField.setAccessible(true);
            connField.set(sc, mockConn);

            long beforeRequest = stats.request;

            sc.commit(false);
            org.junit.Assert.assertEquals("COMMIT should NOT increment request count when switch off",
                beforeRequest, stats.request);

            sc.rollback(false);
            org.junit.Assert.assertEquals("ROLLBACK should NOT increment request count when switch off",
                beforeRequest, stats.request);

            sc.begin();
            org.junit.Assert.assertEquals("BEGIN should NOT increment request count when switch off",
                beforeRequest, stats.request);
        } finally {
            ConfigDataMode.setMode(mode);
        }
    }

    @Test
    public void testTransactionCommandsShouldIncrementRequestWhenSwitchOn() throws Exception {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (final MockedConstruction<ServerConnection> scConstruction =
            mockConstruction(ServerConnection.class, (mock, context) -> {
                doCallRealMethod().when(mock).commit(anyBoolean());
                doCallRealMethod().when(mock).rollback(anyBoolean());
                doCallRealMethod().when(mock).begin();
                doCallRealMethod().when(mock).begin(anyBoolean(), any());
                doCallRealMethod().when(mock).getStatistics();
                doNothing().when(mock).innerCommit();
                doNothing().when(mock).innerRollback();
                doNothing().when(mock).setAutocommit(anyBoolean(), anyBoolean());
                doNothing().when(mock).setReadOnly(anyBoolean());
                doNothing().when(mock).setStmtTxIsolation(anyInt());
                doReturn(mock(ByteBufferHolder.class)).when(mock).allocate();
                doNothing().when(mock).handleError(any(), any(), anyString(), anyBoolean());
            });
            MockedStatic<PacketOutputProxyFactory> packetFactoryMock = mockStatic(PacketOutputProxyFactory.class);
            MockedStatic<DynamicConfig> dynamicConfigMock = mockStatic(DynamicConfig.class)
        ) {
            PacketOutputProxyFactory mockFactory = mock(PacketOutputProxyFactory.class);
            packetFactoryMock.when(PacketOutputProxyFactory::getInstance).thenReturn(mockFactory);
            IPacketOutputProxy mockProxy = mock(IPacketOutputProxy.class);
            when(mockFactory.createProxy(any(FrontendConnection.class), any(ByteBufferHolder.class))).thenReturn(
                mockProxy);

            DynamicConfig mockConfig = mock(DynamicConfig.class);
            dynamicConfigMock.when(DynamicConfig::getInstance).thenReturn(mockConfig);

            // Switch ON: should increment request count
            when(mockConfig.isEnableTransactionQpsCount()).thenReturn(true);

            final ServerConnection sc = new ServerConnection(null);

            MatrixStatistics stats = new MatrixStatistics();
            Field statsField = ServerConnection.class.getDeclaredField("stats");
            statsField.setAccessible(true);
            statsField.set(sc, stats);

            TConnection mockConn = mock(TConnection.class);
            Field connField = ServerConnection.class.getDeclaredField("conn");
            connField.setAccessible(true);
            connField.set(sc, mockConn);

            long beforeRequest = stats.request;

            sc.commit(false);
            org.junit.Assert.assertEquals("COMMIT should increment request count when switch on",
                beforeRequest + 1, stats.request);

            sc.rollback(false);
            org.junit.Assert.assertEquals("ROLLBACK should increment request count when switch on",
                beforeRequest + 2, stats.request);

            sc.begin();
            org.junit.Assert.assertEquals("BEGIN should increment request count when switch on",
                beforeRequest + 3, stats.request);
        } finally {
            ConfigDataMode.setMode(mode);
        }
    }

    @Test
    public void testBeginWithReadOnlyAndIsolationLevel() throws Exception {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (final MockedConstruction<ServerConnection> scConstruction =
            mockConstruction(ServerConnection.class, (mock, context) -> {
                doCallRealMethod().when(mock).begin();
                doCallRealMethod().when(mock).begin(anyBoolean(), any());
                doCallRealMethod().when(mock).getStatistics();
                doNothing().when(mock).setAutocommit(anyBoolean(), anyBoolean());
                doNothing().when(mock).setReadOnly(anyBoolean());
                doNothing().when(mock).setStmtTxIsolation(anyInt());
                doReturn(mock(ByteBufferHolder.class)).when(mock).allocate();
                doNothing().when(mock).handleError(any(), any(), anyString(), anyBoolean());
            });
            MockedStatic<PacketOutputProxyFactory> packetFactoryMock = mockStatic(PacketOutputProxyFactory.class);
            MockedStatic<DynamicConfig> dynamicConfigMock = mockStatic(DynamicConfig.class)
        ) {
            PacketOutputProxyFactory mockFactory = mock(PacketOutputProxyFactory.class);
            packetFactoryMock.when(PacketOutputProxyFactory::getInstance).thenReturn(mockFactory);
            IPacketOutputProxy mockProxy = mock(IPacketOutputProxy.class);
            when(mockFactory.createProxy(any(FrontendConnection.class), any(ByteBufferHolder.class))).thenReturn(
                mockProxy);

            DynamicConfig mockConfig = mock(DynamicConfig.class);
            dynamicConfigMock.when(DynamicConfig::getInstance).thenReturn(mockConfig);
            when(mockConfig.isEnableTransactionQpsCount()).thenReturn(true);

            final ServerConnection sc = new ServerConnection(null);

            MatrixStatistics stats = new MatrixStatistics();
            Field statsField = ServerConnection.class.getDeclaredField("stats");
            statsField.setAccessible(true);
            statsField.set(sc, stats);

            // Test begin(readOnly=true, level=null)
            long beforeRequest = stats.request;
            sc.begin(true, null);
            org.junit.Assert.assertEquals("begin(readOnly=true) should increment request count",
                beforeRequest + 1, stats.request);

            // Test begin(readOnly=false, level=READ_COMMITTED)
            sc.begin(false, IsolationLevel.READ_COMMITTED);
            org.junit.Assert.assertEquals("begin with isolation level should increment request count",
                beforeRequest + 2, stats.request);

            // Verify setStmtTxIsolation was called when level is non-null
            org.mockito.Mockito.verify(sc, org.mockito.Mockito.atLeastOnce())
                .setStmtTxIsolation(IsolationLevel.READ_COMMITTED.getCode());
        } finally {
            ConfigDataMode.setMode(mode);
        }
    }

    @Test
    public void testCommitRollbackBeginIndividualSwitchOn() throws Exception {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (final MockedConstruction<ServerConnection> scConstruction =
            mockConstruction(ServerConnection.class, (mock, context) -> {
                doCallRealMethod().when(mock).commit(anyBoolean());
                doCallRealMethod().when(mock).rollback(anyBoolean());
                doCallRealMethod().when(mock).begin();
                doCallRealMethod().when(mock).begin(anyBoolean(), any());
                doCallRealMethod().when(mock).getStatistics();
                doNothing().when(mock).innerCommit();
                doNothing().when(mock).innerRollback();
                doNothing().when(mock).setAutocommit(anyBoolean(), anyBoolean());
                doNothing().when(mock).setReadOnly(anyBoolean());
                doNothing().when(mock).setStmtTxIsolation(anyInt());
                doReturn(mock(ByteBufferHolder.class)).when(mock).allocate();
                doNothing().when(mock).handleError(any(), any(), anyString(), anyBoolean());
            });
            MockedStatic<PacketOutputProxyFactory> packetFactoryMock = mockStatic(PacketOutputProxyFactory.class);
            MockedStatic<DynamicConfig> dynamicConfigMock = mockStatic(DynamicConfig.class)
        ) {
            PacketOutputProxyFactory mockFactory = mock(PacketOutputProxyFactory.class);
            packetFactoryMock.when(PacketOutputProxyFactory::getInstance).thenReturn(mockFactory);
            IPacketOutputProxy mockProxy = mock(IPacketOutputProxy.class);
            when(mockFactory.createProxy(any(FrontendConnection.class), any(ByteBufferHolder.class))).thenReturn(
                mockProxy);

            DynamicConfig mockConfig = mock(DynamicConfig.class);
            dynamicConfigMock.when(DynamicConfig::getInstance).thenReturn(mockConfig);

            final ServerConnection sc = new ServerConnection(null);

            MatrixStatistics stats = new MatrixStatistics();
            Field statsField = ServerConnection.class.getDeclaredField("stats");
            statsField.setAccessible(true);
            statsField.set(sc, stats);

            TConnection mockConn = mock(TConnection.class);
            Field connField = ServerConnection.class.getDeclaredField("conn");
            connField.setAccessible(true);
            connField.set(sc, mockConn);

            // Test COMMIT individually
            when(mockConfig.isEnableTransactionQpsCount()).thenReturn(true);
            long beforeCommit = stats.request;
            sc.commit(false);
            org.junit.Assert.assertEquals("COMMIT individually should increment request count",
                beforeCommit + 1, stats.request);

            // Test ROLLBACK individually
            long beforeRollback = stats.request;
            sc.rollback(false);
            org.junit.Assert.assertEquals("ROLLBACK individually should increment request count",
                beforeRollback + 1, stats.request);

            // Test BEGIN individually
            long beforeBegin = stats.request;
            sc.begin();
            org.junit.Assert.assertEquals("BEGIN individually should increment request count",
                beforeBegin + 1, stats.request);
        } finally {
            ConfigDataMode.setMode(mode);
        }
    }
}
