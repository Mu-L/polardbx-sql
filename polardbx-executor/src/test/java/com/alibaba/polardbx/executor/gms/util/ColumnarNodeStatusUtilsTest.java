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

package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.columnar.ColumnarNodeInfoRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ColumnarNodeStatusUtilsTest {

    @Test
    public void testGet() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            final MockedStatic<CdcUtils> cdcUtilsMockedStatic = mockStatic(
                CdcUtils.class);) {

            List<ColumnarTableMappingRecord> recordList = new ArrayList<>();

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarTableMappingRecord.class), Mockito.any())).thenReturn(recordList);

            ColumnarNodeStatusUtils.ColumnarNodeStatusInfo columnarNodeStatusInfo =
                ColumnarNodeStatusUtils.getColumnarNodeStatus();
            Assert.assertEquals(ColumnarNodeStatusUtils.ColumnarNodeStatus.NORMAL,
                columnarNodeStatusInfo.getColumnarNodeStatus());

            recordList.add(new ColumnarTableMappingRecord());

            List<ColumnarCheckpointsRecord> checkpointsRecords = new ArrayList<>();

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarCheckpointsRecord.class), Mockito.any())).thenReturn(checkpointsRecords);

            ColumnarNodeStatusUtils.getLastCheckTimeMs().set(0L);

            columnarNodeStatusInfo = ColumnarNodeStatusUtils.getColumnarNodeStatus();
            Assert.assertEquals(ColumnarNodeStatusUtils.ColumnarNodeStatus.PAUSED,
                columnarNodeStatusInfo.getColumnarNodeStatus());
            Assert.assertEquals("No columnar checkpoint", columnarNodeStatusInfo.getInfo());

            columnarNodeStatusInfo = ColumnarNodeStatusUtils.getColumnarNodeStatus();
            Assert.assertEquals(ColumnarNodeStatusUtils.ColumnarNodeStatus.PAUSED,
                columnarNodeStatusInfo.getColumnarNodeStatus());
            Assert.assertEquals("No columnar checkpoint", columnarNodeStatusInfo.getInfo());

            ColumnarCheckpointsRecord checkpointsRecord = new ColumnarCheckpointsRecord();
            checkpointsRecord.binlogTso = 7245839713959936000L;
            checkpointsRecords.add(checkpointsRecord);

            cdcUtilsMockedStatic.when(CdcUtils::getCdcTsoAndDelay)
                .thenReturn(Pair.of(7245854813454336000L, 1000 * 1000L));

            ColumnarNodeStatusUtils.getLastCheckTimeMs().set(0L);

            columnarNodeStatusInfo = ColumnarNodeStatusUtils.getColumnarNodeStatus();
            Assert.assertEquals(ColumnarNodeStatusUtils.ColumnarNodeStatus.PAUSED,
                columnarNodeStatusInfo.getColumnarNodeStatus());
            Assert.assertEquals("CDC delay too long, > 10min", columnarNodeStatusInfo.getInfo());

            cdcUtilsMockedStatic.when(CdcUtils::getCdcTsoAndDelay)
                .thenReturn(Pair.of(7245854813454336000L, 100L));

            ColumnarNodeStatusUtils.getLastCheckTimeMs().set(0L);

            columnarNodeStatusInfo = ColumnarNodeStatusUtils.getColumnarNodeStatus();
            Assert.assertEquals(ColumnarNodeStatusUtils.ColumnarNodeStatus.PAUSED,
                columnarNodeStatusInfo.getColumnarNodeStatus());
            Assert.assertEquals("Columnar delay too long, > 10min", columnarNodeStatusInfo.getInfo());

            checkpointsRecord.binlogTso = 7245854805065728000L;

            ColumnarNodeStatusUtils.getLastCheckTimeMs().set(0L);
            columnarNodeStatusInfo = ColumnarNodeStatusUtils.getColumnarNodeStatus();
            Assert.assertEquals(ColumnarNodeStatusUtils.ColumnarNodeStatus.NORMAL,
                columnarNodeStatusInfo.getColumnarNodeStatus());

            cdcUtilsMockedStatic.when(CdcUtils::getCdcTsoAndDelay)
                .thenThrow(new RuntimeException("get cdc tso and delay error"));

            try {
                ColumnarNodeStatusUtils.getLastCheckTimeMs().set(0L);
                ColumnarNodeStatusUtils.getColumnarNodeStatus();
                Assert.fail("should throw exception");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("get cdc tso and delay error"));
            }
        }
    }

    @Test
    public void testValidateColumnarNodeExists_EnabledParameter() {
        ExecutionContext ec = mock(ExecutionContext.class);
        ParamManager pm = mock(ParamManager.class);
        when(ec.getParamManager()).thenReturn(pm);
        when(pm.getBoolean(ConnectionParams.ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE)).thenReturn(true);

        // Should not throw exception when parameter is enabled
        try {
            ColumnarNodeStatusUtils.validateColumnarNodeExists(ec);
        } catch (Exception e) {
            Assert.fail("Should not throw exception when ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE is true: " + e.getMessage());
        }
    }

    @Test
    public void testValidateColumnarNodeExists_SkipDdlTasks() {
        ExecutionContext ec = mock(ExecutionContext.class);
        ParamManager pm = mock(ParamManager.class);
        Set<String> skipTasks = new HashSet<>();
        skipTasks.add("WaitColumnarTableCreationTask");

        when(ec.getParamManager()).thenReturn(pm);
        when(pm.getBoolean(ConnectionParams.ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE)).thenReturn(false);
        when(ec.skipDdlTasks()).thenReturn(skipTasks);

        // Should not throw exception when DDL task is skipped
        try {
            ColumnarNodeStatusUtils.validateColumnarNodeExists(ec);
        } catch (Exception e) {
            Assert.fail("Should not throw exception when WaitColumnarTableCreationTask is skipped: " + e.getMessage());
        }
    }

    @Test
    public void testValidateColumnarNodeExists_NoCdcNode() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Mock connection
            Connection mockConnection = mock(Connection.class);
            Statement mockStatement = mock(Statement.class);

            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            when(mockConnection.createStatement()).thenReturn(mockStatement);

            // Mock SQLException with ER_NO_SUCH_TABLE error code to simulate table doesn't exist
            SQLException tableNotExistException = new SQLException("Table 'BINLOG_NODE_INFO' doesn't exist", "42S02", ErrorCode.ER_NO_SUCH_TABLE.getCode());
            when(mockStatement.executeQuery("SHOW COLUMNS FROM BINLOG_NODE_INFO")).thenThrow(tableNotExistException);

            ExecutionContext ec = mock(ExecutionContext.class);
            ParamManager pm = mock(ParamManager.class);
            when(ec.getParamManager()).thenReturn(pm);
            when(pm.getBoolean(ConnectionParams.ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE)).thenReturn(false);
            when(ec.skipDdlTasks()).thenReturn(new HashSet<>());

            // Should throw exception when CDC node doesn't exist
            try {
                ColumnarNodeStatusUtils.validateColumnarNodeExists(ec);
                Assert.fail("Should throw exception when CDC node doesn't exist");
            } catch (TddlRuntimeException e) {
                Assert.assertTrue("Exception message should contain expected text",
                    e.getMessage().contains("Cannot create columnar index because no columnar node is available"));
            }
        } catch (SQLException e) {
            Assert.fail("Unexpected SQLException: " + e.getMessage());
        }
    }

    @Test
    public void testValidateColumnarNodeExists_CdcExistsButNoColumnarNode() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Mock connection and statement for CDC node check
            Connection mockConnection = mock(Connection.class);
            Statement mockStatement = mock(Statement.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            when(mockConnection.createStatement()).thenReturn(mockStatement);
            when(mockStatement.executeQuery("SHOW COLUMNS FROM BINLOG_NODE_INFO")).thenReturn(mockResultSet);

            // Mock empty result for columnar node query - no daemon master found
            List<ColumnarNodeInfoRecord> emptyResult = new ArrayList<>();
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), eq(ColumnarNodeInfoRecord.class), any()))
                .thenReturn(emptyResult);

            ExecutionContext ec = mock(ExecutionContext.class);
            ParamManager pm = mock(ParamManager.class);
            when(ec.getParamManager()).thenReturn(pm);
            when(pm.getBoolean(ConnectionParams.ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE)).thenReturn(false);
            when(ec.skipDdlTasks()).thenReturn(new HashSet<>());

            // Should throw exception when no columnar daemon master exists
            try {
                ColumnarNodeStatusUtils.validateColumnarNodeExists(ec);
                Assert.fail("Should throw exception when no columnar daemon master exists");
            } catch (TddlRuntimeException e) {
                Assert.assertTrue("Exception message should contain expected text",
                    e.getMessage().contains("Cannot create columnar index because no columnar node is available"));
            }
        } catch (SQLException e) {
            Assert.fail("Unexpected SQLException: " + e.getMessage());
        }
    }

    @Test
    public void testValidateColumnarNodeExists_Success() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Mock connection and statement for CDC node check
            Connection mockConnection = mock(Connection.class);
            Statement mockStatement = mock(Statement.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            when(mockConnection.createStatement()).thenReturn(mockStatement);
            when(mockStatement.executeQuery("SHOW COLUMNS FROM BINLOG_NODE_INFO")).thenReturn(mockResultSet);

            // Mock ColumnarNodeInfoRecord exists
            ColumnarNodeInfoRecord nodeInfoRecord = new ColumnarNodeInfoRecord();
            List<ColumnarNodeInfoRecord> result = new ArrayList<>();
            result.add(nodeInfoRecord);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), eq(ColumnarNodeInfoRecord.class), any()))
                .thenReturn(result);

            ExecutionContext ec = mock(ExecutionContext.class);
            ParamManager pm = mock(ParamManager.class);
            when(ec.getParamManager()).thenReturn(pm);
            when(pm.getBoolean(ConnectionParams.ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE)).thenReturn(false);
            when(ec.skipDdlTasks()).thenReturn(new HashSet<>());

            // Should not throw exception when both CDC and columnar nodes exist
            try {
                ColumnarNodeStatusUtils.validateColumnarNodeExists(ec);
            } catch (Exception e) {
                Assert.fail("Should not throw exception when both CDC and columnar nodes exist: " + e.getMessage());
            }
        } catch (SQLException e) {
            Assert.fail("Unexpected SQLException: " + e.getMessage());
        }
    }

    @Test
    public void testValidateColumnarNodeExists_ColumnarQueryError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Mock connection and statement for CDC node check
            Connection mockConnection = mock(Connection.class);
            Statement mockStatement = mock(Statement.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            when(mockConnection.createStatement()).thenReturn(mockStatement);
            when(mockStatement.executeQuery("SHOW COLUMNS FROM BINLOG_NODE_INFO")).thenReturn(mockResultSet);

            // Mock query failure for columnar node
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), eq(ColumnarNodeInfoRecord.class), any()))
                .thenThrow(new RuntimeException("Query execution failed"));

            ExecutionContext ec = mock(ExecutionContext.class);
            ParamManager pm = mock(ParamManager.class);
            when(ec.getParamManager()).thenReturn(pm);
            when(pm.getBoolean(ConnectionParams.ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE)).thenReturn(false);
            when(ec.skipDdlTasks()).thenReturn(new HashSet<>());

            // Should throw exception when query error occurs
            try {
                ColumnarNodeStatusUtils.validateColumnarNodeExists(ec);
                Assert.fail("Should throw exception when query error occurs");
            } catch (TddlRuntimeException e) {
                Assert.assertTrue("Exception message should contain expected text",
                    e.getMessage()
                        .contains("Cannot create columnar index because failed to check columnar node status"));
                Assert.assertTrue("Exception message should contain original error",
                    e.getMessage().contains("Query execution failed"));
            }
        } catch (SQLException e) {
            Assert.fail("Unexpected SQLException: " + e.getMessage());
        }
    }

    @Test
    public void testValidateColumnarNodeExists_CdcNodeQueryError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Mock connection
            Connection mockConnection = mock(Connection.class);
            Statement mockStatement = mock(Statement.class);

            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            when(mockConnection.createStatement()).thenReturn(mockStatement);

            // Mock SQLException with non-table-not-exist error code
            SQLException otherException = new SQLException("Connection error", "08001", 1234);
            when(mockStatement.executeQuery("SHOW COLUMNS FROM BINLOG_NODE_INFO")).thenThrow(otherException);

            ExecutionContext ec = mock(ExecutionContext.class);
            ParamManager pm = mock(ParamManager.class);
            when(ec.getParamManager()).thenReturn(pm);
            when(pm.getBoolean(ConnectionParams.ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE)).thenReturn(false);
            when(ec.skipDdlTasks()).thenReturn(new HashSet<>());

            // Should throw exception when CDC node query has other errors
            try {
                ColumnarNodeStatusUtils.validateColumnarNodeExists(ec);
                Assert.fail("Should throw exception when CDC node query has errors");
            } catch (TddlRuntimeException e) {
                Assert.assertTrue("Exception message should contain expected text",
                    e.getMessage().contains("Cannot create columnar index because failed to check columnar node status"));
            }
        } catch (SQLException e) {
            Assert.fail("Unexpected SQLException: " + e.getMessage());
        }
    }
}