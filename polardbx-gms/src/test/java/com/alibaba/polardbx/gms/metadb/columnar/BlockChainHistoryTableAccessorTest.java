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

import com.alibaba.polardbx.gms.metadb.table.BlockChainHistoryTableAccessor;
import com.alibaba.polardbx.gms.metadb.table.BlockChainHistoryTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author lijiu.lzw
 */
public class BlockChainHistoryTableAccessorTest {

    @Test
    public void recordTest() throws Exception {
        BlockChainHistoryTableRecord record = new BlockChainHistoryTableRecord();
        ResultSet rs = mock(ResultSet.class);
        when(rs.getLong("block_id")).thenReturn(123L);
        when(rs.getString("trace_id")).thenReturn("trace_id");
        when(rs.getString("start_time")).thenReturn("start_time");
        when(rs.getLong("rec_num")).thenReturn(456L);
        when(rs.getString("op_type")).thenReturn("op_type");
        when(rs.getString("extra")).thenReturn("extra");
        when(rs.getString("hash_ins")).thenReturn("hash_ins");
        when(rs.getString("hash_del")).thenReturn("hash_del");
        when(rs.getString("block_hash")).thenReturn("block_hash");
        when(rs.getLong("long_pk")).thenReturn(123L);
        when(rs.getBytes("bytes_pk")).thenReturn("bytes_pk".getBytes());
        when(rs.getLong("tso")).thenReturn(456L);
        when(rs.getString("extra")).thenReturn("extra");
        when(rs.getString("gmt_created")).thenReturn("2024-12-12");
        when(rs.getString("gmt_modified")).thenReturn("2024-12-12");

        record.fill(rs);
        Assert.assertEquals(123L, record.blockId);
        Assert.assertEquals("trace_id", record.traceId);
        Assert.assertEquals("start_time", record.startTime);
        Assert.assertEquals(456L, record.recNum);
        Assert.assertEquals("op_type", record.opType);
        Assert.assertEquals("extra", record.extra);
        Assert.assertEquals("hash_ins", record.hashIns);
        Assert.assertEquals("hash_del", record.hashDel);
        Assert.assertEquals("block_hash", record.blockHash);
        Assert.assertEquals(123L, (long) record.longPk);
        Assert.assertArrayEquals("bytes_pk".getBytes(), record.bytesPk);
        Assert.assertEquals(456L, record.tso);
        Assert.assertEquals("extra", record.extra);
        Assert.assertEquals("2024-12-12", record.createTime);
        Assert.assertEquals("2024-12-12", record.updateTime);

        record.buildInsertParams();
    }

    @Test
    public void insertTest() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyList(),
                Mockito.any())).thenReturn(new int[] {1});
            BlockChainHistoryTableAccessor accessor = new BlockChainHistoryTableAccessor();
            accessor.insertRows("schemaName", "tableName", ImmutableList.of(new BlockChainHistoryTableRecord()));

            //error
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyList(),
                Mockito.any())).thenThrow(new SQLException("mock exception"));
            try {
                accessor.insertRows("schemaName", "tableName", ImmutableList.of(new BlockChainHistoryTableRecord()));
                Assert.fail();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void selectTest() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<BlockChainHistoryTableRecord> records = ImmutableList.of(new BlockChainHistoryTableRecord());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenReturn(records);
            BlockChainHistoryTableAccessor accessor = new BlockChainHistoryTableAccessor();
            List<BlockChainHistoryTableRecord> result = accessor.queryLastRecord("schemaName", "tableName");
            Assert.assertEquals(1, result.size());

            result = accessor.queryByTraceId("schemaName", "tableName", "trace_id");
            Assert.assertEquals(1, result.size());

            result = accessor.queryByTraceIdLastRec("schemaName", "tableName", "trace_id");
            Assert.assertEquals(1, result.size());

            result = accessor.queryLastTraceIdRecord("schemaName", "tableName");
            Assert.assertEquals(1, result.size());

            //error
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenThrow(new RuntimeException("mock exception"));
            try {
                accessor.queryLastRecord("schemaName", "tableName");
                Assert.fail();
            } catch (Exception ignored) {
            }
            try {
                accessor.queryByTraceId("schemaName", "tableName", "trace_id");
                Assert.fail();
            } catch (Exception ignored) {
            }
            try {
                accessor.queryByTraceIdLastRec("schemaName", "tableName", "trace_id");
                Assert.fail();
            } catch (Exception ignored) {
            }
            try {
                accessor.queryLastTraceIdRecord("schemaName", "tableName");
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
            BlockChainHistoryTableAccessor accessor = new BlockChainHistoryTableAccessor();
            int affectRows = accessor.updateByTraceId("schemaName", "tableName", "trace_id", "e");
            Assert.assertEquals(1, affectRows);

            //error
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenThrow(new RuntimeException("mock exception"));
            try {
                accessor.updateByTraceId("schemaName", "tableName", "trace_id", "e");
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
            BlockChainHistoryTableAccessor accessor = new BlockChainHistoryTableAccessor();
            int affectRows = accessor.deleteById("schemaName", "tableName", 100);
            Assert.assertEquals(1, affectRows);

            //error
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenThrow(new RuntimeException("mock exception"));
            try {
                accessor.deleteById("schemaName", "tableName", 100);
                Assert.fail();
            } catch (Exception ignored) {
            }
        }
    }

}
