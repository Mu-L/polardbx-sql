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

package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.common.columnar.ColumnarOption;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.handler.ColumnarConfigHandler;
import com.alibaba.polardbx.server.handler.pl.inner.ColumnarRollbackProcedure;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ColumnarRollbackProcedureTest {

    /**
     * Test checkParameters: missing parameters
     */
    @Test
    public void testCheckParameters_MissingParams() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser(Mockito.anyString())).thenReturn(true);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback()",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail("Should throw IllegalArgumentException for missing parameters");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("parameters is not match 1 parameters"));
            }
        }
    }

    /**
     * Test checkParameters: string parameter instead of integer
     */
    @Test
    public void testCheckParameters_StringParam() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser(Mockito.anyString())).thenReturn(true);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(\"123\")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail("Should throw IllegalArgumentException for string parameter");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("parameters need Long number"));
            }
        }
    }

    /**
     * Test checkParameters: float parameter
     */
    @Test
    public void testCheckParameters_FloatParam() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser(Mockito.anyString())).thenReturn(true);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(120.5)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail("Should throw IllegalArgumentException for float parameter");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("parameters need Long number"));
            }
        }
    }

    /**
     * Test checkParameters: negative parameter
     */
    @Test
    public void testCheckParameters_NegativeParam() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser(Mockito.anyString())).thenReturn(true);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(-100)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail("Should throw IllegalArgumentException for negative parameter");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("parameters is invalid Long number, need >= 0"));
            }
        }
    }

    /**
     * Test checkParameters: multiple parameters
     */
    @Test
    public void testCheckParameters_MultipleParams() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser(Mockito.anyString())).thenReturn(true);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(100, 200)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail("Should throw IllegalArgumentException for multiple parameters");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("parameters is not match 1 parameters"));
            }
        }
    }

    /**
     * Test checkParameters: boundary value 0 (should pass)
     */
    @Test
    public void testCheckParameters_BoundaryZero() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class);
            MockedStatic<ColumnarConfigHandler> configHandlerMockedStatic = mockStatic(ColumnarConfigHandler.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser(Mockito.anyString())).thenReturn(true);
            configHandlerMockedStatic.when(() -> ColumnarConfigHandler.setColumnarConfig(Mockito.any())).then(
                invocation -> null);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(0)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            procedure.execute(connection, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals(0L, cursor.getRows().get(0).getObject(0));
        }
    }

    /**
     * Test checkParameters: boundary value Long.MAX_VALUE (should pass)
     */
    @Test
    public void testCheckParameters_BoundaryMaxValue() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class);
            MockedStatic<ColumnarConfigHandler> configHandlerMockedStatic = mockStatic(ColumnarConfigHandler.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser(Mockito.anyString())).thenReturn(true);
            configHandlerMockedStatic.when(() -> ColumnarConfigHandler.setColumnarConfig(Mockito.any())).then(
                invocation -> null);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(" + Long.MAX_VALUE + ")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            procedure.execute(connection, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals(Long.MAX_VALUE, cursor.getRows().get(0).getObject(0));
        }
    }

    /**
     * Test checkPrivilege: non-polardbx_root user should be denied
     */
    @Test
    public void testCheckPrivilege_NonRootUser() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser("normal_user")).thenReturn(false);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("192.168.1.1");
            when(connection.getUser()).thenReturn("normal_user");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(12345)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail("Should throw RuntimeException for non-root user");
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("Access denied for user"));
                Assert.assertTrue(e.getMessage().contains("normal_user"));
                Assert.assertTrue(e.getMessage().contains("192.168.1.1"));
            }
        }
    }

    /**
     * Test checkPrivilege: polardbx_root user should pass
     */
    @Test
    public void testCheckPrivilege_RootUser() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class);
            MockedStatic<ColumnarConfigHandler> configHandlerMockedStatic = mockStatic(ColumnarConfigHandler.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser("polardbx_root")).thenReturn(true);
            configHandlerMockedStatic.when(() -> ColumnarConfigHandler.setColumnarConfig(Mockito.any())).then(
                invocation -> null);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(12345)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            // Should not throw exception
            procedure.execute(connection, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());
        }
    }

    /**
     * Test execute: verify ColumnarConfigHandler.setColumnarConfig is called with correct parameters
     */
    @Test
    public void testExecute_VerifySetColumnarConfigCalled() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class);
            MockedStatic<ColumnarConfigHandler> configHandlerMockedStatic = mockStatic(ColumnarConfigHandler.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser(Mockito.anyString())).thenReturn(true);

            ArgumentCaptor<ColumnarOption.Param> paramCaptor = ArgumentCaptor.forClass(ColumnarOption.Param.class);
            configHandlerMockedStatic.when(() -> ColumnarConfigHandler.setColumnarConfig(paramCaptor.capture())).then(
                invocation -> null);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();

            long testTso = 1234567890L;
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(" + testTso + ")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            procedure.execute(connection, statement, cursor);

            // Verify setColumnarConfig was called
            configHandlerMockedStatic.verify(() -> ColumnarConfigHandler.setColumnarConfig(Mockito.any()));

            // Verify the parameter content
            ColumnarOption.Param capturedParam = paramCaptor.getValue();
            Assert.assertEquals("columnar_rollback_tso", capturedParam.key);
            Assert.assertEquals(String.valueOf(testTso), capturedParam.value);
        }
    }

    /**
     * Test execute: verify cursor returns correct ROLLBACK_TSO column and value
     */
    @Test
    public void testExecute_VerifyCursorResult() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class);
            MockedStatic<ColumnarConfigHandler> configHandlerMockedStatic = mockStatic(ColumnarConfigHandler.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser(Mockito.anyString())).thenReturn(true);
            configHandlerMockedStatic.when(() -> ColumnarConfigHandler.setColumnarConfig(Mockito.any())).then(
                invocation -> null);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();

            long testTso = 9876543210L;
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(" + testTso + ")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            procedure.execute(connection, statement, cursor);

            // Verify cursor has correct columns
            Assert.assertEquals(1, cursor.getReturnColumns().size());
            Assert.assertEquals("ROLLBACK_TSO", cursor.getReturnColumns().get(0).getName());

            // Verify cursor has correct row data
            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals(testTso, cursor.getRows().get(0).getObject(0));
        }
    }

    /**
     * Test execute: verify with different TSO values
     */
    @Test
    public void testExecute_WithDifferentTsoValues() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class);
            MockedStatic<ColumnarConfigHandler> configHandlerMockedStatic = mockStatic(ColumnarConfigHandler.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser(Mockito.anyString())).thenReturn(true);

            ArgumentCaptor<ColumnarOption.Param> paramCaptor = ArgumentCaptor.forClass(ColumnarOption.Param.class);
            configHandlerMockedStatic.when(() -> ColumnarConfigHandler.setColumnarConfig(paramCaptor.capture())).then(
                invocation -> null);

            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();

            long[] testTsoValues = {1L, 100L, 999999L, 1234567890123456789L};
            for (long tso : testTsoValues) {
                ArrayResultCursor cursor = new ArrayResultCursor("test");
                SQLCallStatement statement =
                    (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(" + tso + ")",
                        SQLParserFeature.IgnoreNameQuotes).get(0);

                procedure.execute(connection, statement, cursor);

                Assert.assertEquals(1, cursor.getRows().size());
                Assert.assertEquals(tso, cursor.getRows().get(0).getObject(0));
            }
        }
    }

    /**
     * Test getColumnarNodeName: verify return format is ip@1@timestamp
     */
    @Test
    public void testGetColumnarNodeName_Format() {
        String nodeName = ColumnarRollbackProcedure.getColumnarNodeName();

        // Verify format: ip@1@timestamp
        String[] parts = nodeName.split("@");
        Assert.assertEquals("Node name should have 3 parts separated by @", 3, parts.length);

        // First part should be IP or "notFound"
        String ip = parts[0];
        Assert.assertFalse("IP should not be empty", ip.isEmpty());

        // Second part should be "1" (ROLLBACK_PID)
        Assert.assertEquals("PID should be 1", "1", parts[1]);

        // Third part should be a valid timestamp
        long timestamp = Long.parseLong(parts[2]);
        Assert.assertTrue("Timestamp should be positive", timestamp > 0);
        // Timestamp should be close to current time (within 1 second)
        long currentTime = System.currentTimeMillis();
        Assert.assertTrue("Timestamp should be close to current time",
            Math.abs(currentTime - timestamp) < 1000);
    }

    /**
     * Test getColumnarNodeName: multiple calls should have different timestamps
     */
    @Test
    public void testGetColumnarNodeName_DifferentTimestamps() throws InterruptedException {
        String nodeName1 = ColumnarRollbackProcedure.getColumnarNodeName();
        Thread.sleep(10); // Wait a bit to ensure different timestamp
        String nodeName2 = ColumnarRollbackProcedure.getColumnarNodeName();

        String[] parts1 = nodeName1.split("@");
        String[] parts2 = nodeName2.split("@");

        // IP and PID should be the same
        Assert.assertEquals("IP should be the same", parts1[0], parts2[0]);
        Assert.assertEquals("PID should be the same", parts1[1], parts2[1]);

        // Timestamps should be different
        long timestamp1 = Long.parseLong(parts1[2]);
        long timestamp2 = Long.parseLong(parts2[2]);
        Assert.assertTrue("Timestamp2 should be greater than timestamp1", timestamp2 > timestamp1);
    }

    /**
     * Legacy test: combined parameter test for backward compatibility
     */
    @Test
    public void paramTest() {
        ArrayResultCursor cursor = new ArrayResultCursor("test");
        ServerConnection connection = Mockito.mock(ServerConnection.class);
        when(connection.getHost()).thenReturn("127.0.0.1");
        when(connection.getUser()).thenReturn("user_one");
        ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();
        SQLCallStatement statement =
            (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(10)",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(connection, statement, cursor);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser("polardbx_root")).thenReturn(true);
            when(connection.getUser()).thenReturn("polardbx_root");

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback()",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(\"123\")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql(
                    "call polardbx.columnar_rollback(-120)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql(
                    "call polardbx.columnar_rollback(120.1)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql(
                    "call polardbx.columnar_rollback(120, 30)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    /**
     * Test execute with mocked ColumnarConfigHandler
     */
    @Test
    public void executeTest() {
        try (MockedStatic<PolarPrivUtil> polarPrivUtilMockedStatic = mockStatic(PolarPrivUtil.class);
            MockedStatic<ColumnarConfigHandler> configHandlerMockedStatic = mockStatic(ColumnarConfigHandler.class)) {
            polarPrivUtilMockedStatic.when(() -> PolarPrivUtil.isPolarxRootUser("polardbx_root")).thenReturn(true);
            configHandlerMockedStatic.when(() -> ColumnarConfigHandler.setColumnarConfig(Mockito.any())).then(
                invocation -> null);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(1234569)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            procedure.execute(connection, statement, cursor);

            // Verify setColumnarConfig was called
            configHandlerMockedStatic.verify(() -> ColumnarConfigHandler.setColumnarConfig(Mockito.any()));

            // Verify cursor result
            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals(1234569L, cursor.getRows().get(0).getObject(0));
        }
    }
}
