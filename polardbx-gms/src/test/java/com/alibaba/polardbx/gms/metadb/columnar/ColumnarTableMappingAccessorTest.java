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

import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ColumnarTableMappingAccessorTest {

    @Test
    public void testSelect() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<ColumnarTableMappingRecord> recordList = new ArrayList<>();
            recordList.add(new ColumnarTableMappingRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarTableMappingRecord.class), Mockito.any())).thenReturn(recordList);
            TableInfoManager infoManager = new TableInfoManager();
            boolean result = infoManager.haveColumnarTable("schema", "table");
            Assert.assertFalse(result);
            result = infoManager.haveColumnarTable("schema", "10");
            Assert.assertTrue(result);
            result = infoManager.haveColumnarTable("schema", "10.1");
            Assert.assertFalse(result);
            result = infoManager.haveColumnarTable("schema", "102716261927172172171270127017021270172");
            Assert.assertFalse(result);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarTableMappingRecord.class), Mockito.any())).thenReturn(new ArrayList<>());
            result = infoManager.haveColumnarTable("schema", "table");
            Assert.assertFalse(result);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarTableMappingRecord.class), Mockito.any())).thenReturn(recordList);

            ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
            List<ColumnarTableMappingRecord> res = accessor.queryPurgeTablesByTso(16L);
            Assert.assertEquals(1, res.size());

            res = accessor.queryPurgeTablesWhichHavePurgeFilesByTso(18L);
            Assert.assertEquals(1, res.size());

            res = accessor.queryPurgeTablesWhichHavePurgeFilesByTsoAndType(199L, "snapshot");
            Assert.assertEquals(1, res.size());

            res = accessor.queryLimitOne();
            Assert.assertEquals(1, res.size());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(10);
            int count =
                accessor.updateStatusAndLastVersionIdByTableIdAndStatus(10L, 10L, ColumnarTableStatus.DROP.name(),
                    ColumnarTableStatus.PURGE.name());
            Assert.assertEquals(10, count);
        }
    }

    @Test
    public void testUpdate() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);
            ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
            int count =
                accessor.updateInfoByTableId(10L, "info");
            Assert.assertEquals(1, count);
        }
    }

    @Test
    public void testRecord() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getLong("table_id")).thenReturn(100L);
        when(rs.getString("table_schema")).thenReturn("db");
        when(rs.getString("table_name")).thenReturn("table");
        when(rs.getString("index_name")).thenReturn("index");
        when(rs.getLong("latest_version_id")).thenReturn(100L);
        when(rs.getString("status")).thenReturn("ok");
        when(rs.getString("info")).thenReturn("info");
        when(rs.getString("extra")).thenReturn("extra");
        when(rs.getString("type")).thenReturn("type");

        ColumnarTableMappingRecord record = new ColumnarTableMappingRecord().fill(rs);
        Assert.assertEquals(100L, record.tableId);
        Assert.assertEquals("db", record.tableSchema);
        Assert.assertEquals("table", record.tableName);
        Assert.assertEquals("index", record.indexName);
        Assert.assertEquals(100L, record.latestVersionId);
        Assert.assertEquals("ok", record.status);
        Assert.assertEquals("info", record.info);
        Assert.assertEquals("extra", record.extra);
        Assert.assertEquals("type", record.type);

        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.executeBatch(anyString(), anyList(), any()))
                .thenReturn(new int[] {1});

            final ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
            accessor.insert(ImmutableList.of(record));
        }
    }
}
