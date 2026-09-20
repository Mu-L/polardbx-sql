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

import com.alibaba.polardbx.gms.metadb.table.FileInfoRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.metadb.table.OrcFileStatusRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class FilesAccessorTest {

    @Test
    public void testSelect() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<FileInfoRecord> recordList = new ArrayList<>();
            recordList.add(new FileInfoRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(FileInfoRecord.class), Mockito.any())).thenReturn(recordList);
            FilesAccessor accessor = new FilesAccessor();
            List<FileInfoRecord> result = accessor.queryFileInfoByLogicalSchemaTable("schema", "table");
            Assert.assertEquals(1, result.size());

            result = accessor.queryFileInfoByLogicalSchemaTableTso("schema", "10", 123L);
            Assert.assertEquals(1, result.size());

            result = accessor.queryFileInfoByLogicalSchemaTableTsoLimitOne("schema", "table", 123L);
            Assert.assertEquals(1, result.size());

            result = accessor.queryCSVFileInfoByLogicalSchemaTableRangeTso("schema", "table", 123L, 234L);
            Assert.assertEquals(1, result.size());

            result = accessor.queryCSVFileInfoByLogicalSchemaTableRangeTsoLimitOne("schema", "table", 123L, 234L);
            Assert.assertEquals(1, result.size());

            result = accessor.queryDelFileInfoByLogicalSchemaTableRangeTso("schema", "table", 123L, 234L);
            Assert.assertEquals(1, result.size());

            result = accessor.queryDelFileInfoByLogicalSchemaTableRangeTsoLimitOne("schema", "table", 123L, 234L);
            Assert.assertEquals(1, result.size());

            result = accessor.queryFileInfoByLogicalSchemaTableRangeTso("schema", "table", 123L);
            Assert.assertEquals(1, result.size());

            result = accessor.queryExpiredTableFileBySchemaTableTsoLimitOne("schema", "table", 123L);
            Assert.assertEquals(1, result.size());

            result = accessor.querySnapshotCsvDelFileInfoByTso(123L, 234L);
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testDelete() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger deleteCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> deleteCount.get());
            FilesAccessor accessor = new FilesAccessor();
            deleteCount.set(100);
            int count = accessor.deleteLimit("schema", "table", 100);
            Assert.assertEquals(100, count);
            deleteCount.set(1000);
            count = accessor.delete("schema", "table");
            Assert.assertEquals(1000, count);
        }
    }

    @Test
    public void testFileInfoRecord() throws Exception {
        FileInfoRecord record = new FileInfoRecord();
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString(anyString())).thenReturn("123");
        when(rs.getLong(anyString())).thenReturn(222L);
        when(rs.wasNull()).thenReturn(false);
        record.fill(rs);

        Assert.assertEquals(222L, record.fileId);
        Assert.assertEquals("123", record.fileName);
    }

    @Test
    public void testQueryColumnarSnapshotFilesByTsoAndTableId() {
        AtomicReference<String> querySql = new AtomicReference<>();
        FilesAccessor filesAccessor = new FilesAccessor();

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any())).thenAnswer(
                invocation -> {
                    querySql.set(invocation.getArgument(0));
                    return new ArrayList<>();
                }
            );
            filesAccessor.queryColumnarSnapshotFilesByTsoAndTableId(1L, "test", "test");
            System.out.println(querySql.get());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any()))
                .thenThrow(new RuntimeException("test_xxx"));
            try {
                filesAccessor.queryColumnarSnapshotFilesByTsoAndTableId(1L, "test", "test");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("test_xxx"));
            }
        }
    }

    @Test
    public void testQuerySnapshotWithChecksumByTsoAndTableId() {
        AtomicReference<String> querySql = new AtomicReference<>();
        FilesAccessor filesAccessor = new FilesAccessor();

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any())).thenAnswer(
                invocation -> {
                    querySql.set(invocation.getArgument(0));
                    return new ArrayList<>();
                }
            );
            filesAccessor.querySnapshotWithChecksumByTsoAndTableId(1L, "test", "test");
            System.out.println(querySql.get());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any()))
                .thenThrow(new RuntimeException("test_xxx"));
            try {
                filesAccessor.querySnapshotWithChecksumByTsoAndTableId(1L, "test", "test");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("test_xxx"));
            }
        }
    }

    @Test
    public void testDelete2() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger deleteCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> deleteCount.get());
            FilesAccessor accessor = new FilesAccessor();
            deleteCount.set(100);
            int count = accessor.deleteOrcMetaByCommitTso(1024L);
            Assert.assertEquals(100, count);
            deleteCount.set(1000);
            count = accessor.deleteThreeMetaByCommitTso(2048);
            Assert.assertEquals(1000, count);

            deleteCount.set(10);
            count = accessor.deleteCompactionFileByCommitTso("a", "b", 2048);
            Assert.assertEquals(10, count);

            deleteCount.set(12);
            count = accessor.deleteLimitByTableAndTso("a", "b", 2048L, 200L);
            Assert.assertEquals(12, count);

        }
    }

    @Test
    public void testUpdate() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger updateCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> updateCount.get());
            FilesAccessor accessor = new FilesAccessor();
            updateCount.set(100);
            int count = accessor.updateRemoveTsByTso(1024L);
            Assert.assertEquals(100, count);

            updateCount.set(10);
            count = accessor.updateCompactionRemoveTsByTso("a", "b", 2048);
            Assert.assertEquals(10, count);

            updateCount.set(12);
            count = accessor.updateFileSizeAndRowsByFileName(1212, 3234, "file2");
            Assert.assertEquals(12, count);

            updateCount.set(13);
            count = accessor.updateTableRemoveTs(null, "db", "table");
            Assert.assertEquals(13, count);

            updateCount.set(14);
            count = accessor.updateTableRemoveTsLimit(null, "db", "table", 100);
            Assert.assertEquals(14, count);

            updateCount.set(15);
            count = accessor.updateTableRemoveTsLimit(123L, "db", "table", 100);
            Assert.assertEquals(15, count);
        }
    }

    @Test
    public void testSelectOrcFileStatusRecord() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<OrcFileStatusRecord> recordList = new ArrayList<>();
            recordList.add(new OrcFileStatusRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(OrcFileStatusRecord.class), Mockito.any())).thenReturn(recordList);
            FilesAccessor accessor = new FilesAccessor();
            List<OrcFileStatusRecord> result =
                accessor.querySnapshotCSVFileStatusByTsoAndTableId(30, "schema", "table");
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testSelectFilesRecord() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<FilesRecord> recordList = new ArrayList<>();
            recordList.add(new FilesRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(FilesRecord.class), Mockito.any())).thenReturn(recordList);
            FilesAccessor accessor = new FilesAccessor();
            List<FilesRecord> result = accessor.queryCsvByRemoveTso(123L);
            Assert.assertEquals(1, result.size());
        }
    }

    public void testQueryByPartitionAndTypeOrderByCommitTsDescWithLimitOffset() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<FilesRecord> recordList = new ArrayList<>();
            FilesRecord record1 = new FilesRecord();
            FilesRecord record2 = new FilesRecord();
            recordList.add(record1);
            recordList.add(record2);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(FilesRecord.class), Mockito.any())).thenReturn(recordList);

            FilesAccessor accessor = new FilesAccessor();
            List<FilesRecord> result = accessor.queryByPartitionAndTypeOrderByCommitTsDesc(
                "test_schema", "test_table", "p0", "CSV", "COLUMNAR", 10L, 5L);
            Assert.assertEquals(2, result.size());

            // verify with different parameters
            recordList.clear();
            recordList.add(new FilesRecord());
            result = accessor.queryByPartitionAndTypeOrderByCommitTsDesc(
                "schema2", "table2", "p1", "ORC", "OSS", 100L, 0L);
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testQueryByPartitionAndTypeOrderByCommitTsDescWithLimitOffsetEmpty() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(FilesRecord.class), Mockito.any())).thenReturn(new ArrayList<>());

            FilesAccessor accessor = new FilesAccessor();
            List<FilesRecord> result = accessor.queryByPartitionAndTypeOrderByCommitTsDesc(
                "test_schema", "test_table", "p0", "CSV", "COLUMNAR", 10L, 0L);
            Assert.assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testQueryByPartitionAndTypeOrderByCommitTsDescWithLimitOffsetException() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(FilesRecord.class), Mockito.any()))
                .thenThrow(new RuntimeException("query_failed_test"));

            FilesAccessor accessor = new FilesAccessor();
            try {
                accessor.queryByPartitionAndTypeOrderByCommitTsDesc(
                    "test_schema", "test_table", "p0", "CSV", "COLUMNAR", 10L, 5L);
                Assert.fail("Expected exception was not thrown");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("query_failed_test"));
            }
        }
    }

    @Test
    public void testQueryByPartitionAndTypeOrderByVersionDescAfter() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            final String[] capturedSql = new String[1];
            List<FilesRecord> recordList = new ArrayList<>();
            recordList.add(new FilesRecord());
            recordList.add(new FilesRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(FilesRecord.class), Mockito.any())).thenAnswer(invocation -> {
                capturedSql[0] = invocation.getArgument(0);
                return recordList;
            });

            FilesAccessor accessor = new FilesAccessor();
            List<FilesRecord> result = accessor.queryByPartitionAndTypeOrderByVersionDescAfter(
                "test_schema", "test_table", "p0", "CSV", "COLUMNAR", Long.MAX_VALUE, 10L);
            Assert.assertEquals(2, result.size());
            // keyset/seek pagination must use `version` < ? as the seek boundary, never offset
            Assert.assertTrue(capturedSql[0].contains("`version` < ?"));
            Assert.assertFalse(capturedSql[0].contains("offset"));
        }
    }

    @Test
    public void testQueryByPartitionAndTypeOrderByVersionDescAfterEmpty() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(FilesRecord.class), Mockito.any())).thenReturn(new ArrayList<>());

            FilesAccessor accessor = new FilesAccessor();
            List<FilesRecord> result = accessor.queryByPartitionAndTypeOrderByVersionDescAfter(
                "test_schema", "test_table", "p0", "CSV", "COLUMNAR", 100L, 10L);
            Assert.assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testQueryByPartitionAndTypeOrderByVersionDescAfterException() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(), Mockito.eq(FilesRecord.class),
                    Mockito.any())).thenThrow(new RuntimeException("query_failed_test"));

            FilesAccessor accessor = new FilesAccessor();
            try {
                accessor.queryByPartitionAndTypeOrderByVersionDescAfter(
                    "test_schema", "test_table", "p0", "CSV", "COLUMNAR", Long.MAX_VALUE, 10L);
                Assert.fail("Expected exception was not thrown");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("query_failed_test"));
            }
        }
    }

    @Test
    public void testQueryValidDelInfoByLogicalSchemaTableRangeTso() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<FileInfoRecord> recordList = new ArrayList<>();
            recordList.add(new FileInfoRecord());
            recordList.add(new FileInfoRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(FileInfoRecord.class), Mockito.any())).thenReturn(recordList);

            FilesAccessor accessor = new FilesAccessor();
            List<FileInfoRecord> result =
                accessor.queryValidDelInfoByLogicalSchemaTableRangeTso("schema", "table", 500L);
            Assert.assertEquals(2, result.size());
        }
    }

    @Test
    public void testQueryValidDelInfoByLogicalSchemaTableRangeTsoEmpty() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(FileInfoRecord.class), Mockito.any())).thenReturn(new ArrayList<>());

            FilesAccessor accessor = new FilesAccessor();
            List<FileInfoRecord> result =
                accessor.queryValidDelInfoByLogicalSchemaTableRangeTso("schema", "table", 100L);
            Assert.assertNotNull(result);
            Assert.assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testQuerySnapshotCsvDelFileInfoByTsoError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(FileInfoRecord.class), Mockito.any()))
                .thenThrow(new RuntimeException("snapshot_csv_del_error"));

            FilesAccessor accessor = new FilesAccessor();
            try {
                accessor.querySnapshotCsvDelFileInfoByTso(100L, 200L);
                Assert.fail("Expected exception was not thrown");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("snapshot_csv_del_error"));
            }
        }
    }

    @Test
    public void testQueryDelFileInfoByLogicalSchemaTableRangeTsoLimitOneSingleResult() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<FileInfoRecord> oneRecord = new ArrayList<>();
            oneRecord.add(new FileInfoRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(FileInfoRecord.class), Mockito.any())).thenReturn(oneRecord);

            FilesAccessor accessor = new FilesAccessor();
            List<FileInfoRecord> result =
                accessor.queryDelFileInfoByLogicalSchemaTableRangeTsoLimitOne("schema", "table", 100L, 200L);
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testQueryFileInfoByLogicalSchemaTableRangeTsoMultipleResults() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<FileInfoRecord> records = new ArrayList<>();
            records.add(new FileInfoRecord());
            records.add(new FileInfoRecord());
            records.add(new FileInfoRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(FileInfoRecord.class), Mockito.any())).thenReturn(records);

            FilesAccessor accessor = new FilesAccessor();
            List<FileInfoRecord> result =
                accessor.queryFileInfoByLogicalSchemaTableRangeTso("myschema", "mytable", 999L);
            Assert.assertEquals(3, result.size());
        }
    }

}
