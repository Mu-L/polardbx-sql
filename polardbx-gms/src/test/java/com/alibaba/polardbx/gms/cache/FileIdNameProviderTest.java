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

package com.alibaba.polardbx.gms.cache;

import com.alibaba.polardbx.gms.metadb.cache.CacheFileMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/**
 * Unit tests for FileIdNameProvider.
 * Tests cache behavior, dual-table routing (.PkIdx.log vs others), and error handling.
 */
public class FileIdNameProviderTest {

    // ===================== getId: null/empty input =====================

    @Test
    public void testGetIdNull() {
        FileIdNameProvider provider = new FileIdNameProvider();
        Assert.assertEquals(0L, provider.getId(null));
    }

    @Test
    public void testGetIdEmpty() {
        FileIdNameProvider provider = new FileIdNameProvider();
        Assert.assertEquals(0L, provider.getId(""));
    }

    // ===================== getId: cache hit =====================

    @Test
    public void testGetIdCacheHit() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConn = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            // First call: record not found → insert returns id=42
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insertAndReturnLastInsertId(Mockito.anyString(),
                Mockito.anyList(), Mockito.any())).thenReturn(42L);

            // FilesAccessor returns empty (no schema info available)
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(FilesRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());

            FileIdNameProvider provider = new FileIdNameProvider();

            long id1 = provider.getId("test.orc");
            Assert.assertEquals(42L, id1);

            // Second call: should hit cache, no DB interaction needed
            // (We don't need to set up any more mocks — it would fail if it tried DB)
            long id2 = provider.getId("test.orc");
            Assert.assertEquals(42L, id2);

            // Cache size should be 1
            Assert.assertEquals(1, provider.getCacheSize());
        }
    }

    // ===================== getId: existing mapping with schema info (fast path) =====================

    @Test
    public void testGetIdExistingRecordWithSchema() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConn = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            // Record exists with schema info populated
            CacheFileMappingRecord record = new CacheFileMappingRecord();
            record.id = 100L;
            record.fileName = "data.orc";
            record.logicalSchema = "my_db";
            record.logicalTable = "my_table";

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.singletonList(record));

            FileIdNameProvider provider = new FileIdNameProvider();
            long result = provider.getId("data.orc");
            Assert.assertEquals(100L, result);
        }
    }

    // ===================== getId: existing mapping WITHOUT schema → enrich from files table =====================

    @Test
    public void testGetIdExistingRecordEnrichFromFilesTable() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConn = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            // Record exists but logicalSchema is null → needs enrichment
            CacheFileMappingRecord record = new CacheFileMappingRecord();
            record.id = 200L;
            record.fileName = "data.orc";
            record.logicalSchema = null;

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.singletonList(record));

            // FilesAccessor returns a record for enrichment
            FilesRecord filesRecord = new FilesRecord();
            filesRecord.logicalSchemaName = "enriched_db";
            filesRecord.logicalTableName = "enriched_table";
            filesRecord.partitionName = "p0";
            filesRecord.engine = "columnar";

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(FilesRecord.class), Mockito.any()))
                .thenReturn(Collections.singletonList(filesRecord));

            // updateSchemaInfo (via MetaDbUtil.update)
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);

            FileIdNameProvider provider = new FileIdNameProvider();
            long result = provider.getId("data.orc");
            Assert.assertEquals(200L, result);
        }
    }

    // ===================== getId: .PkIdx.log suffix → route to columnar_appended_files =====================

    @Test
    public void testGetIdPkIdxLogNewMapping() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConn = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            // Step 1: no existing mapping
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());

            // Step 2: querySchemaInfo → isPkIdxLogFile=true → query columnar_appended_files
            ColumnarAppendedFilesRecord cafRecord = new ColumnarAppendedFilesRecord();
            cafRecord.logicalSchema = "col_db";
            cafRecord.logicalTable = "col_table";
            cafRecord.partName = "p1";
            cafRecord.engine = "columnar";

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(ColumnarAppendedFilesRecord.class), Mockito.any()))
                .thenReturn(Collections.singletonList(cafRecord));

            // Step 3: insertIgnoreAndGet returns new id
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insertAndReturnLastInsertId(Mockito.anyString(),
                Mockito.anyList(), Mockito.any())).thenReturn(500L);

            FileIdNameProvider provider = new FileIdNameProvider();
            long result = provider.getId("test_data.PkIdx.log");
            Assert.assertEquals(500L, result);
        }
    }

    // ===================== getId: .PkIdx.log case-insensitive routing =====================

    @Test
    public void testGetIdPkIdxLogCaseInsensitive() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConn = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            // No existing mapping
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());

            // Should route to columnar_appended_files for .PKIDX.LOG (uppercase)
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(ColumnarAppendedFilesRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insertAndReturnLastInsertId(Mockito.anyString(),
                Mockito.anyList(), Mockito.any())).thenReturn(600L);

            FileIdNameProvider provider = new FileIdNameProvider();
            long result = provider.getId("test_data.PKIDX.LOG");
            Assert.assertEquals(600L, result);
        }
    }

    // ===================== getId: .orc file → route to files table =====================

    @Test
    public void testGetIdOrcNewMapping() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConn = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            // No existing mapping
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());

            // querySchemaInfo → isPkIdxLogFile=false → query files table
            FilesRecord filesRecord = new FilesRecord();
            filesRecord.logicalSchemaName = "orc_db";
            filesRecord.logicalTableName = "orc_table";
            filesRecord.partitionName = "p0";
            filesRecord.engine = "columnar";

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(FilesRecord.class), Mockito.any()))
                .thenReturn(Collections.singletonList(filesRecord));

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insertAndReturnLastInsertId(Mockito.anyString(),
                Mockito.anyList(), Mockito.any())).thenReturn(300L);

            FileIdNameProvider provider = new FileIdNameProvider();
            long result = provider.getId("test_data.orc");
            Assert.assertEquals(300L, result);
        }
    }

    // ===================== getId: existing .PkIdx.log record needs enrichment =====================

    @Test
    public void testGetIdPkIdxLogExistingEnrich() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConn = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            // Existing record with empty logicalSchema → needs enrichment
            CacheFileMappingRecord record = new CacheFileMappingRecord();
            record.id = 700L;
            record.fileName = "test.PkIdx.log";
            record.logicalSchema = "";

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.singletonList(record));

            // enrichSchemaInfo → isPkIdxLogFile=true → query columnar_appended_files
            ColumnarAppendedFilesRecord cafRecord = new ColumnarAppendedFilesRecord();
            cafRecord.logicalSchema = "enriched_col_db";
            cafRecord.logicalTable = "enriched_col_table";
            cafRecord.partName = "p2";
            cafRecord.engine = "columnar";

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(ColumnarAppendedFilesRecord.class), Mockito.any()))
                .thenReturn(Collections.singletonList(cafRecord));

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);

            FileIdNameProvider provider = new FileIdNameProvider();
            long result = provider.getId("test.PkIdx.log");
            Assert.assertEquals(700L, result);
        }
    }

    // ===================== Cache operations =====================

    @Test
    public void testInvalidate() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConn = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(FilesRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insertAndReturnLastInsertId(Mockito.anyString(),
                Mockito.anyList(), Mockito.any())).thenReturn(10L);

            FileIdNameProvider provider = new FileIdNameProvider();
            provider.getId("a.orc");
            Assert.assertEquals(1, provider.getCacheSize());

            provider.invalidate("a.orc");
            Assert.assertEquals(0, provider.getCacheSize());
        }
    }

    @Test
    public void testInvalidateNull() {
        FileIdNameProvider provider = new FileIdNameProvider();
        // Should not throw
        provider.invalidate(null);
        Assert.assertEquals(0, provider.getCacheSize());
    }

    @Test
    public void testInvalidateAll() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConn = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(FilesRecord.class), Mockito.any()))
                .thenReturn(Collections.emptyList());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insertAndReturnLastInsertId(Mockito.anyString(),
                Mockito.anyList(), Mockito.any())).thenReturn(10L, 20L);

            FileIdNameProvider provider = new FileIdNameProvider();
            provider.getId("a.orc");
            provider.getId("b.csv");
            Assert.assertEquals(2, provider.getCacheSize());

            provider.invalidateAll();
            Assert.assertEquals(0, provider.getCacheSize());
        }
    }

    // ===================== Custom cache config constructor =====================

    @Test
    public void testCustomCacheConfig() {
        FileIdNameProvider provider = new FileIdNameProvider(100, 60, TimeUnit.SECONDS);
        Assert.assertEquals(0, provider.getCacheSize());
    }

    // ===================== Error handling: DB failure → exception propagated =====================

    @Test(expected = RuntimeException.class)
    public void testGetIdDbError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection)
                .thenThrow(new RuntimeException("DB connection failed"));

            FileIdNameProvider provider = new FileIdNameProvider();
            provider.getId("error.orc");
        }
    }

    // ===================== Enrichment failure: gracefully handled =====================

    @Test
    public void testGetIdEnrichmentFailureDoesNotBlock() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConn = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            // Existing record with no schema
            CacheFileMappingRecord record = new CacheFileMappingRecord();
            record.id = 800L;
            record.fileName = "test.orc";
            record.logicalSchema = null;

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(CacheFileMappingRecord.class), Mockito.any()))
                .thenReturn(Collections.singletonList(record));

            // FilesAccessor throws exception during enrichment
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(FilesRecord.class), Mockito.any()))
                .thenThrow(new RuntimeException("files table error"));

            FileIdNameProvider provider = new FileIdNameProvider();
            // Should still return the id despite enrichment failure
            long result = provider.getId("test.orc");
            Assert.assertEquals(800L, result);
        }
    }
}
