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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ColumnarPurgeHistoryAccessorTest {

    @Test
    public void testSelect() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<ColumnarPurgeHistoryRecord> recordList = new ArrayList<>();
            recordList.add(new ColumnarPurgeHistoryRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), any(),
                Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any())).thenReturn(recordList);
            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();

            List<ColumnarPurgeHistoryRecord> result = accessor.queryLastPurgeTso();
            Assert.assertEquals(1, result.size());

            result = accessor.queryLastPurgeTsoWithFlag(ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE);
            Assert.assertEquals(1, result.size());

            result = accessor.queryLastPurgeTsoWithFlag(ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_SUCCESS);
            Assert.assertEquals(1, result.size());

            result = accessor.queryLastPurgeTsoWithFlag(ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_FAIL);
            Assert.assertEquals(1, result.size());

            result = accessor.queryPurgeRecordByTso(100L);
            Assert.assertEquals(1, result.size());

            result = accessor.queryPurgeRecordByTsoForUpdate(100L);
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testUpdate() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger updateCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> updateCount.get());

            // Mock query for FOR UPDATE
            List<ColumnarPurgeHistoryRecord> recordList = new ArrayList<>();
            ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();
            record.tso = 100L;
            recordList.add(record);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any())).thenReturn(recordList);

            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();
            updateCount.set(100);
            int count = accessor.updateStatusByTso(ColumnarPurgeHistoryRecord.PurgeStatus.START, 100L);
            Assert.assertEquals(100, count);

            updateCount.set(1000);
            count = accessor.updateStatusByTso("schema", 200L);
            Assert.assertEquals(1000, count);

            updateCount.set(50);
            count = accessor.updateFlagAddByTso(ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE, 100L);
            Assert.assertEquals(50, count);

            updateCount.set(60);
            count = accessor.updateFlagAddByTso(ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_SUCCESS, 100L);
            Assert.assertEquals(60, count);

            updateCount.set(70);
            count = accessor.updateFlagAddByTso(ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_FAIL, 100L);
            Assert.assertEquals(70, count);
        }
    }

    @Test
    public void testInsert() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyList(),
                Mockito.any())).thenReturn(new int[1]);
            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();
            int[] result = accessor.insert(ImmutableList.of(record));
            Assert.assertEquals(1, result.length);

        }
    }

    @Test
    public void testColumnarPurgeHistoryRecord() throws Exception {
        ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString(anyString())).thenReturn("123");
        when(rs.getLong(anyString())).thenReturn(222L);
        when(rs.getTimestamp(anyString())).thenReturn(null);
        record.fill(rs);

        Assert.assertEquals(222L, record.id);
        Assert.assertEquals("123", record.status);
        Assert.assertNull(record.createTime);
    }

    @Test
    public void testCheckpointPurgeFlag() {
        ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();

        // Test default value
        Assert.assertFalse(record.getCheckpointPurgeFlag());

        // Test set flag to true
        record.setCheckpointPurgeFlag(true);
        Assert.assertTrue(record.getCheckpointPurgeFlag());
        Assert.assertEquals(1L, record.flag);

        // Test set flag to false
        record.setCheckpointPurgeFlag(false);
        Assert.assertFalse(record.getCheckpointPurgeFlag());
        Assert.assertEquals(0L, record.flag);

        // Test flag with other bits set
        record.flag = 6L; // binary: 110
        Assert.assertFalse(record.getCheckpointPurgeFlag()); // bit 0 is 0

        record.setCheckpointPurgeFlag(true);
        Assert.assertTrue(record.getCheckpointPurgeFlag());
        Assert.assertEquals(7L, record.flag); // binary: 111

        record.setCheckpointPurgeFlag(false);
        Assert.assertFalse(record.getCheckpointPurgeFlag());
        Assert.assertEquals(6L, record.flag); // binary: 110
    }

    @Test
    public void testFlagConstant() {
        // Test FLAG_CHECKPOINT_PURGE constant value
        Assert.assertEquals(1L, ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE);
        Assert.assertEquals(0x1, ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE);
    }

    @Test
    public void testBuildInsertParamsWithFlag() {
        ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();
        record.tso = 123456L;
        record.status = "START";
        record.info = "test info";
        record.extra = "test extra";
        record.flag = 1L;

        Map<Integer, ParameterContext> params = record.buildInsertParams();

        // Verify all parameters including flag
        Assert.assertEquals(5, params.size());
        Assert.assertEquals(123456L, params.get(1).getValue());
        Assert.assertEquals("START", params.get(2).getValue());
        Assert.assertEquals("test info", params.get(3).getValue());
        Assert.assertEquals("test extra", params.get(4).getValue());
        Assert.assertEquals(1L, params.get(5).getValue());
    }

    @Test
    public void testRecordFillWithFlag() throws Exception {
        ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();
        ResultSet rs = mock(ResultSet.class);
        when(rs.getLong("id")).thenReturn(1L);
        when(rs.getLong("tso")).thenReturn(100L);
        when(rs.getString("status")).thenReturn("FINISHED");
        when(rs.getString("info")).thenReturn("info");
        when(rs.getString("extra")).thenReturn("extra");
        when(rs.getLong("flag")).thenReturn(1L);
        when(rs.getTimestamp("gmt_created")).thenReturn(null);
        when(rs.getTimestamp("gmt_modified")).thenReturn(null);

        record.fill(rs);

        Assert.assertEquals(1L, record.id);
        Assert.assertEquals(100L, record.tso);
        Assert.assertEquals("FINISHED", record.status);
        Assert.assertEquals(1L, record.flag);
        Assert.assertTrue(record.getCheckpointPurgeFlag());
    }

    @Test
    public void testUpdateFlagAddCheckpointPurgeNoRecord() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Mock empty result for FOR UPDATE query
            List<ColumnarPurgeHistoryRecord> emptyList = new ArrayList<>();
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any())).thenReturn(emptyList);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(0);

            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();
            int count = accessor.updateFlagAddByTso(ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE, 999L);

            // Should return 0 when no record found
            Assert.assertEquals(0, count);
        }
    }

    @Test
    public void testUpdateFlagAddCheckpointPurgeSuccess() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Mock record found for FOR UPDATE query
            List<ColumnarPurgeHistoryRecord> recordList = new ArrayList<>();
            ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();
            record.tso = 100L;
            record.flag = 0L;
            recordList.add(record);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any())).thenReturn(recordList);

            // Mock update success
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);

            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();
            int count = accessor.updateFlagAddByTso(ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE, 100L);

            // Should return 1 when update success
            Assert.assertEquals(1, count);
        }
    }

    @Test
    public void testFlagBitOperations() {
        ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();

        // Test bit operation with FLAG_CHECKPOINT_PURGE constant
        record.flag = 0L;
        record.flag |= ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE;
        Assert.assertEquals(1L, record.flag);
        Assert.assertTrue(record.getCheckpointPurgeFlag());

        // Test bit AND operation
        record.flag = 5L; // binary: 101
        boolean hasFlag = (record.flag & ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE) != 0;
        Assert.assertTrue(hasFlag);

        record.flag = 4L; // binary: 100
        hasFlag = (record.flag & ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE) != 0;
        Assert.assertFalse(hasFlag);

        // Test bit clear operation
        record.flag = 7L; // binary: 111
        record.flag &= ~ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE;
        Assert.assertEquals(6L, record.flag); // binary: 110
        Assert.assertFalse(record.getCheckpointPurgeFlag());
    }

    @Test
    public void testMultipleFlagsConstants() {
        // Test all flag constants
        Assert.assertEquals(0x1, ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE);
        Assert.assertEquals(0x2, ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_SUCCESS);
        Assert.assertEquals(0x4, ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_FAIL);

        // Test combining multiple flags
        ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();
        record.flag = 0L;

        // Set checkpoint flag
        record.flag |= ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE;
        Assert.assertEquals(1L, record.flag);
        Assert.assertTrue((record.flag & ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE) != 0);

        // Add optimize success flag
        record.flag |= ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_SUCCESS;
        Assert.assertEquals(3L, record.flag); // 0x1 | 0x2 = 0x3
        Assert.assertTrue((record.flag & ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE) != 0);
        Assert.assertTrue((record.flag & ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_SUCCESS) != 0);
        Assert.assertFalse((record.flag & ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_FAIL) != 0);

        // Add optimize fail flag
        record.flag |= ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_FAIL;
        Assert.assertEquals(7L, record.flag); // 0x1 | 0x2 | 0x4 = 0x7
        Assert.assertTrue((record.flag & ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE) != 0);
        Assert.assertTrue((record.flag & ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_SUCCESS) != 0);
        Assert.assertTrue((record.flag & ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_FAIL) != 0);
    }

    @Test
    public void testQueryWithDifferentFlags() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<ColumnarPurgeHistoryRecord> recordList = new ArrayList<>();
            ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();
            record.tso = 100L;
            record.flag = 7L; // All flags set
            recordList.add(record);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any())).thenReturn(recordList);

            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();

            // Test query with each flag
            List<ColumnarPurgeHistoryRecord> result =
                accessor.queryLastPurgeTsoWithFlag(ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE);
            Assert.assertEquals(1, result.size());

            result = accessor.queryLastPurgeTsoWithFlag(ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_SUCCESS);
            Assert.assertEquals(1, result.size());

            result = accessor.queryLastPurgeTsoWithFlag(ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_FAIL);
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testUpdateWithDifferentFlags() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);

            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();

            // Test update with each flag
            int count = accessor.updateFlagAddByTso(ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE, 100L);
            Assert.assertEquals(1, count);

            count = accessor.updateFlagAddByTso(ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_SUCCESS, 100L);
            Assert.assertEquals(1, count);

            count = accessor.updateFlagAddByTso(ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_FAIL, 100L);
            Assert.assertEquals(1, count);
        }
    }

    @Test
    public void testQueryLastPurgeTsoError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.isNull(),
                    Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any()))
                .thenThrow(new RuntimeException("query last purge error"));

            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();
            try {
                accessor.queryLastPurgeTso();
                Assert.fail("Expected exception was not thrown");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("query last purge error"));
            }
        }
    }

    @Test
    public void testQueryLastPurgeTsoWithFlagError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any()))
                .thenThrow(new RuntimeException("query with flag error"));

            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();
            try {
                accessor.queryLastPurgeTsoWithFlag(ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE);
                Assert.fail("Expected exception was not thrown");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("query with flag error"));
            }
        }
    }

    @Test
    public void testQueryPurgeRecordByTsoError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any()))
                .thenThrow(new RuntimeException("query record by tso error"));

            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();
            try {
                accessor.queryPurgeRecordByTso(100L);
                Assert.fail("Expected exception was not thrown");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("query record by tso error"));
            }
        }
    }

    @Test
    public void testQueryPurgeRecordByTsoForUpdateError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any()))
                .thenThrow(new RuntimeException("query for update error"));

            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();
            try {
                accessor.queryPurgeRecordByTsoForUpdate(100L);
                Assert.fail("Expected exception was not thrown");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("query for update error"));
            }
        }
    }

    @Test
    public void testUpdateFlagAddByTsoError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.any()))
                .thenThrow(new RuntimeException("update flag error"));

            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();
            try {
                accessor.updateFlagAddByTso(ColumnarPurgeHistoryRecord.FLAG_CHECKPOINT_PURGE, 100L);
                Assert.fail("Expected exception was not thrown");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("update flag error"));
            }
        }
    }

    @Test
    public void testInsertError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyList(),
                    Mockito.any()))
                .thenThrow(new RuntimeException("insert error"));

            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();
            ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();
            record.tso = 123L;
            record.status = "START";
            try {
                accessor.insert(ImmutableList.of(record));
                Assert.fail("Expected exception was not thrown");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("insert error"));
            }
        }
    }

    @Test
    public void testRecordFillWithAllFlags() throws Exception {
        // Test FLAG_PURGE_OPTIMIZE_TABLE_SUCCESS
        ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();
        ResultSet rs = mock(ResultSet.class);
        when(rs.getLong("id")).thenReturn(2L);
        when(rs.getLong("tso")).thenReturn(200L);
        when(rs.getString("status")).thenReturn("FINISHED");
        when(rs.getString("info")).thenReturn("info2");
        when(rs.getString("extra")).thenReturn("extra2");
        when(rs.getLong("flag")).thenReturn(ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_SUCCESS);
        when(rs.getTimestamp("gmt_created")).thenReturn(null);
        when(rs.getTimestamp("gmt_modified")).thenReturn(null);
        record.fill(rs);

        Assert.assertEquals(2L, record.id);
        Assert.assertEquals(0x2, record.flag);
        Assert.assertFalse(record.getCheckpointPurgeFlag());
        Assert.assertTrue((record.flag & ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_SUCCESS) != 0);

        // Test FLAG_PURGE_OPTIMIZE_TABLE_FAIL
        ColumnarPurgeHistoryRecord record2 = new ColumnarPurgeHistoryRecord();
        when(rs.getLong("flag")).thenReturn(ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_FAIL);
        record2.fill(rs);
        Assert.assertEquals(0x4, record2.flag);
        Assert.assertFalse(record2.getCheckpointPurgeFlag());
        Assert.assertTrue((record2.flag & ColumnarPurgeHistoryRecord.FLAG_PURGE_OPTIMIZE_TABLE_FAIL) != 0);
    }

}
