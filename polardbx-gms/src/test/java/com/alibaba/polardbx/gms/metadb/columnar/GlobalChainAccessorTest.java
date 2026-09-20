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

package com.alibaba.polardbx.gms.metadb.columnar;

import com.alibaba.polardbx.gms.metadb.chain.GlobalChainAccessor;
import com.alibaba.polardbx.gms.metadb.chain.GlobalChainRecord;
import com.alibaba.polardbx.gms.metadb.chain.GlobalChainSimpleRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author lijiu.lzw
 */
public class GlobalChainAccessorTest {
    @Test
    public void recordTest() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getLong("block_id")).thenReturn(123L);
        when(rs.getString("trace_id")).thenReturn("trace_id");
        when(rs.getString("ip")).thenReturn("ip");
        when(rs.getLong("port")).thenReturn(111L);
        when(rs.getString("user")).thenReturn("user");
        when(rs.getString("schema_name")).thenReturn("schema_name");
        when(rs.getString("table_name")).thenReturn("table_name");
        when(rs.getString("op_hash")).thenReturn("op_hash");
        when(rs.getString("block_hash")).thenReturn("block_hash");
        when(rs.getString("extra")).thenReturn("extra");
        when(rs.getLong("tso")).thenReturn(234L);
        when(rs.getString("gmt_created")).thenReturn("2024-12-12");
        when(rs.getString("gmt_modified")).thenReturn("2024-12-12");

        final GlobalChainRecord result = new GlobalChainRecord().fill(rs);
        Assert.assertEquals(123L, result.blockId);
        Assert.assertEquals("trace_id", result.traceId);
        Assert.assertEquals("ip", result.ip);
        Assert.assertEquals(111L, result.port);
        Assert.assertEquals("user", result.user);
        Assert.assertEquals("schema_name", result.schemaName);
        Assert.assertEquals("table_name", result.tableName);
        Assert.assertEquals("op_hash", result.opHash);
        Assert.assertEquals("block_hash", result.blockHash);
        Assert.assertEquals("extra", result.extra);
        Assert.assertEquals(234L, result.tso);
        Assert.assertEquals("2024-12-12", result.createTime);
        Assert.assertEquals("2024-12-12", result.updateTime);

        result.buildInsertParams();
    }

    @Test
    public void simpleRecordTest() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getLong("block_id")).thenReturn(123L);
        when(rs.getString("schema_name")).thenReturn("schema_name");
        when(rs.getString("table_name")).thenReturn("table_name");

        GlobalChainSimpleRecord record = new GlobalChainSimpleRecord().fill(rs);
        Assert.assertEquals(123L, record.blockId);
        Assert.assertEquals("schema_name", record.schemaName);
        Assert.assertEquals("table_name", record.tableName);

    }

    @Test
    public void insertTest() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyList(),
                Mockito.any())).thenReturn(new int[] {1});
            GlobalChainAccessor accessor = new GlobalChainAccessor();
            accessor.insertRows(ImmutableList.of(new GlobalChainRecord()));

            //error
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyList(),
                Mockito.any())).thenThrow(new SQLException("mock exception"));
            try {
                accessor.insertRows(ImmutableList.of(new GlobalChainRecord()));
                Assert.fail();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void selectTest() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<GlobalChainRecord> records = ImmutableList.of(new GlobalChainRecord());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenReturn(records);
            GlobalChainAccessor accessor = new GlobalChainAccessor();
            List<GlobalChainRecord> result = accessor.queryLastRecord();
            Assert.assertEquals(1, result.size());

            //error
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenThrow(new RuntimeException("mock exception"));
            try {
                accessor.queryLastRecord();
                Assert.fail();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void selectTest2() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<GlobalChainSimpleRecord> records = ImmutableList.of(new GlobalChainSimpleRecord());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenReturn(records);
            GlobalChainAccessor accessor = new GlobalChainAccessor();
            List<GlobalChainSimpleRecord> result = accessor.queryLastRecordForEachTable();
            Assert.assertEquals(1, result.size());

            //error
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenThrow(new RuntimeException("mock exception"));
            try {
                accessor.queryLastRecordForEachTable();
                Assert.fail();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void updateTest() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);
            GlobalChainAccessor accessor = new GlobalChainAccessor();
            int affectRows = accessor.updateArchiveByBlockId(ImmutableList.of(new GlobalChainSimpleRecord()));
            Assert.assertEquals(1, affectRows);
            affectRows = accessor.updateDeleteBySchema("schemaName");
            Assert.assertEquals(1, affectRows);
            affectRows = accessor.updateDeleteBySchemaAndTable("schemaName", "tableName");
            Assert.assertEquals(1, affectRows);

            //error
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenThrow(new RuntimeException("mock exception"));
            try {
                accessor.updateArchiveByBlockId(ImmutableList.of(new GlobalChainSimpleRecord()));
                Assert.fail();
            } catch (Exception ignored) {
            }
            try {
                accessor.updateDeleteBySchema("schemaName");
                Assert.fail();
            } catch (Exception ignored) {
            }
            try {
                accessor.updateDeleteBySchemaAndTable("schemaName", "tableName");
                Assert.fail();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void deleteTest() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);
            GlobalChainAccessor accessor = new GlobalChainAccessor();
            int affectRows = accessor.deleteByBlockId(ImmutableList.of(new GlobalChainSimpleRecord()), 100);
            Assert.assertEquals(1, affectRows);

            //error
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenThrow(new RuntimeException("mock exception"));
            try {
                accessor.deleteByBlockId(ImmutableList.of(new GlobalChainSimpleRecord()), 100);
                Assert.fail();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void processorTest() throws Exception {
        GlobalChainAccessor accessor = new GlobalChainAccessor();
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
        doNothing().when(preparedStatement).setString(Mockito.anyInt(), Mockito.anyString());
        doNothing().when(preparedStatement).setLong(Mockito.anyInt(), Mockito.anyLong());
        when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getString(1)).thenReturn("1");

        accessor.setConnection(connection);

        accessor.processGlobalChain("schemaName", "tableName", 100L, record -> {
        });

        accessor.processGlobalChainById("schemaName", "tableName", 100L, record -> {
        });

        accessor.processGlobalChain(212L, new AtomicBoolean(false), record -> {
        });

        accessor.queryArchiveBlockHashById();

        when(resultSet.next()).thenThrow(new RuntimeException("mock exception"));
        try {
            accessor.queryArchiveBlockHashById();
            Assert.fail();
        } catch (Exception ignored) {
        }
    }
}
