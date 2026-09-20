/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.metadb.cache;

import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class CacheFileMappingAccessorTest {

    private static final String FILE_NAME = "test_file.orc";
    private static final long ID = 12345L;
    private static final String LOGICAL_SCHEMA = "test_schema";
    private static final String LOGICAL_TABLE = "test_table";
    private static final String PART_NAME = "p0";
    private static final String ENGINE = "columnar";

    @Test
    public void testInsertOrGetMappingNewRecord() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Step 1: First query returns empty (not exists)
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());

            // Step 2: Insert succeeds, return auto-generated id
            metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.insertAndReturnLastInsertId(Mockito.anyString(), Mockito.anyList(),
                    Mockito.any())).thenReturn(ID);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();

            long result = accessor.insertOrGetMapping(FILE_NAME,
                LOGICAL_SCHEMA, LOGICAL_TABLE, PART_NAME, ENGINE);
            Assert.assertEquals(ID, result);
        }
    }

    @Test
    public void testInsertOrGetMappingExistingRecord() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Step 1: First query returns existing record
            CacheFileMappingRecord existingRecord = createTestRecord();
            existingRecord.id = 99999L;

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.singletonList(existingRecord));

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();

            // Should return existing id without insert
            long result = accessor.insertOrGetMapping(FILE_NAME,
                LOGICAL_SCHEMA, LOGICAL_TABLE, PART_NAME, ENGINE);
            Assert.assertEquals(99999L, result);
        }
    }

    @Test
    public void testInsertOrGetMappingConcurrentInsert() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Simulate concurrent insert scenario
            CacheFileMappingRecord concurrentRecord = createTestRecord();
            concurrentRecord.id = 88888L;

            // First query returns empty, then concurrent insert happens, then query returns record
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList())
                .thenReturn(Collections.singletonList(concurrentRecord));

            // Insert returns null (INSERT IGNORE did not insert due to concurrent insert)
            metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.insertAndReturnLastInsertId(Mockito.anyString(), Mockito.anyList(),
                    Mockito.any())).thenReturn(null);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();

            // Should return id from second query
            long result = accessor.insertOrGetMapping(FILE_NAME,
                LOGICAL_SCHEMA, LOGICAL_TABLE, PART_NAME, ENGINE);
            Assert.assertEquals(88888L, result);
        }
    }

    @Test
    public void testGetByFileName() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            CacheFileMappingRecord record = createTestRecord();

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.singletonList(record));

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            CacheFileMappingRecord result = accessor.getByFileName(FILE_NAME);

            Assert.assertNotNull(result);
            Assert.assertEquals(ID, result.id);
            Assert.assertEquals(FILE_NAME, result.fileName);
        }
    }

    @Test
    public void testGetByFileNameNotFound() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            CacheFileMappingRecord result = accessor.getByFileName("nonexistent.orc");

            Assert.assertNull(result);
        }
    }

    @Test
    public void testGetById() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            CacheFileMappingRecord record = createTestRecord();

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.singletonList(record));

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            CacheFileMappingRecord result = accessor.getById(ID);

            Assert.assertNotNull(result);
            Assert.assertEquals(ID, result.id);
        }
    }

    @Test
    public void testGetAll() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<CacheFileMappingRecord> records = new ArrayList<>();
            records.add(createTestRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(records);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            List<CacheFileMappingRecord> result = accessor.getAll();

            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testDeleteBySchemaTable() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(5);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            int result = accessor.deleteBySchemaTable(LOGICAL_SCHEMA, LOGICAL_TABLE);

            Assert.assertEquals(5, result);
        }
    }

    @Test
    public void testDeleteBySchema() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(10);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            int result = accessor.deleteBySchema(LOGICAL_SCHEMA);

            Assert.assertEquals(10, result);
        }
    }

    @Test
    public void testDeleteBySchemaTablePart() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(3);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            int result = accessor.deleteBySchemaTablePart(LOGICAL_SCHEMA, LOGICAL_TABLE, PART_NAME);

            Assert.assertEquals(3, result);
        }
    }

    @Test
    public void testDeleteByEngine() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(3);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            int result = accessor.deleteByEngine(ENGINE);

            Assert.assertEquals(3, result);
        }
    }

    @Test
    public void testDeleteById() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger deleteCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> deleteCount.get());

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();

            // Delete existing
            deleteCount.set(1);
            boolean result = accessor.deleteById(ID);
            Assert.assertTrue(result);

            // Delete non-existing
            deleteCount.set(0);
            result = accessor.deleteById(99999L);
            Assert.assertFalse(result);
        }
    }

    // ===================== Update methods =====================

    @Test
    public void testUpdateSchemaInfo() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            int result = accessor.updateSchemaInfo(ID, LOGICAL_SCHEMA, LOGICAL_TABLE, PART_NAME, ENGINE);
            Assert.assertEquals(1, result);
        }
    }

    @Test
    public void testUpdateMetaById() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            int result = accessor.updateMetaById(ID, new byte[] {1, 2, 3});
            Assert.assertEquals(1, result);
        }
    }

    @Test
    public void testUpdateMetaByIdAndVersion() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Version match — update succeeds
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            int result = accessor.updateMetaByIdAndVersion(ID, new byte[] {1, 2}, 5L);
            Assert.assertEquals(1, result);
        }
    }

    @Test
    public void testUpdateMetaByIdAndVersionMismatch() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Version mismatch — update returns 0
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(0);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            int result = accessor.updateMetaByIdAndVersion(ID, new byte[] {1, 2}, 99L);
            Assert.assertEquals(0, result);
        }
    }

    @Test
    public void testUpdateLockedById() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            int result = accessor.updateLockedById(ID, true);
            Assert.assertEquals(1, result);
        }
    }

    // ===================== Scan methods (via mock JDBC) =====================

    @Test
    public void testScanWithFileCheck() throws Exception {
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPs = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);

        when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);
        when(mockPs.executeQuery()).thenReturn(mockRs);
        // Two rows: one matched (ref_id=100), one orphan (ref_id=null)
        when(mockRs.next()).thenReturn(true, true, false);
        when(mockRs.getLong(1)).thenReturn(1L, 2L);
        when(mockRs.getLong(2)).thenReturn(100L, 0L);
        when(mockRs.wasNull()).thenReturn(false, true);

        CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
        accessor.setConnection(mockConn);

        List<Long[]> results = accessor.scanWithFileCheck(0, 100, new String[] {".orc", ".csv"});
        Assert.assertEquals(2, results.size());
        // First row: matched
        Assert.assertEquals(Long.valueOf(1L), results.get(0)[0]);
        Assert.assertEquals(Long.valueOf(100L), results.get(0)[1]);
        // Second row: orphan
        Assert.assertEquals(Long.valueOf(2L), results.get(1)[0]);
        Assert.assertNull(results.get(1)[1]);
    }

    @Test
    public void testScanWithColumnarAppendedFileCheck() throws Exception {
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPs = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);

        when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);
        when(mockPs.executeQuery()).thenReturn(mockRs);
        // One orphan row
        when(mockRs.next()).thenReturn(true, false);
        when(mockRs.getLong(1)).thenReturn(5L);
        when(mockRs.getLong(2)).thenReturn(0L);
        when(mockRs.wasNull()).thenReturn(true);

        CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
        accessor.setConnection(mockConn);

        List<Long[]> results = accessor.scanWithColumnarAppendedFileCheck(0, 100, new String[] {".PkIdx.log"});
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(Long.valueOf(5L), results.get(0)[0]);
        Assert.assertNull(results.get(0)[1]);
    }

    @Test
    public void testScanWithFileCheckEmptyResult() throws Exception {
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPs = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);

        when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);
        when(mockPs.executeQuery()).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(false);

        CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
        accessor.setConnection(mockConn);

        List<Long[]> results = accessor.scanWithFileCheck(0, 100, new String[] {".orc"});
        Assert.assertTrue(results.isEmpty());
    }

    // ===================== Batch delete =====================

    @Test
    public void testDeleteByIdsNull() {
        CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
        Assert.assertEquals(0, accessor.deleteByIds(null));
    }

    @Test
    public void testDeleteByIdsEmpty() {
        CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
        Assert.assertEquals(0, accessor.deleteByIds(Collections.emptyList()));
    }

    @Test
    public void testDeleteByIds() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(3);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            int result = accessor.deleteByIds(Arrays.asList(1L, 2L, 3L));
            Assert.assertEquals(3, result);
        }
    }

    // ===================== insertIgnoreAndGet direct =====================

    @Test
    public void testInsertIgnoreAndGetSuccess() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insertAndReturnLastInsertId(Mockito.anyString(),
                Mockito.anyList(), Mockito.any())).thenReturn(42L);

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            long result = accessor.insertIgnoreAndGet(FILE_NAME, LOGICAL_SCHEMA, LOGICAL_TABLE,
                PART_NAME, ENGINE, null, 0, false);
            Assert.assertEquals(42L, result);
        }
    }

    @Test
    public void testInsertIgnoreAndGetConcurrentFallback() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // INSERT returns null (ignored), then SELECT returns existing record
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insertAndReturnLastInsertId(Mockito.anyString(),
                Mockito.anyList(), Mockito.any())).thenReturn(null);

            CacheFileMappingRecord record = createTestRecord();
            record.id = 77L;
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.singletonList(record));

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            long result = accessor.insertIgnoreAndGet(FILE_NAME, LOGICAL_SCHEMA, LOGICAL_TABLE,
                PART_NAME, ENGINE, null, 0, false);
            Assert.assertEquals(77L, result);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testInsertIgnoreAndGetFailure() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // INSERT returns null and SELECT also returns empty
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insertAndReturnLastInsertId(Mockito.anyString(),
                Mockito.anyList(), Mockito.any())).thenReturn(null);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            accessor.insertIgnoreAndGet(FILE_NAME, LOGICAL_SCHEMA, LOGICAL_TABLE,
                PART_NAME, ENGINE, null, 0, false);
        }
    }

    // ===================== Error handling =====================

    @Test(expected = RuntimeException.class)
    public void testDeleteBySchemaTableError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenThrow(new RuntimeException("mock error"));

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            accessor.deleteBySchemaTable(LOGICAL_SCHEMA, LOGICAL_TABLE);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testUpdateSchemaInfoError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenThrow(new RuntimeException("mock error"));

            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            accessor.updateSchemaInfo(ID, LOGICAL_SCHEMA, LOGICAL_TABLE, PART_NAME, ENGINE);
        }
    }

    private CacheFileMappingRecord createTestRecord() {
        CacheFileMappingRecord record = new CacheFileMappingRecord();
        record.id = ID;
        record.fileName = FILE_NAME;
        record.logicalSchema = LOGICAL_SCHEMA;
        record.logicalTable = LOGICAL_TABLE;
        record.partName = PART_NAME;
        record.engine = ENGINE;
        record.meta = null;
        record.version = 0;
        record.locked = false;
        return record;
    }
}
