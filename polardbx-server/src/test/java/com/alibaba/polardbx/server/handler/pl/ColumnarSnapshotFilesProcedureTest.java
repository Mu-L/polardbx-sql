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

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.SpillableArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryRecord;
import com.alibaba.polardbx.gms.metadb.table.FileInfoSimpleRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.handler.pl.inner.ColumnarSnapshotFilesProcedure;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class ColumnarSnapshotFilesProcedureTest {

    /**
     * Test parameter validation - missing parameters
     */
    @Test
    public void paramTestMissingParam() {
        ArrayResultCursor cursor = new ArrayResultCursor("test");
        ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
        SQLCallStatement statement =
            (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_snapshot_files()",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(null, statement, cursor);
            Assert.fail("Should throw IllegalArgumentException for missing parameters");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("parameters is not match 1 parameters"));
        }
    }

    /**
     * Test parameter validation - non-integer parameter (decimal)
     */
    @Test
    public void paramTestDecimalParam() {
        ArrayResultCursor cursor = new ArrayResultCursor("test");
        ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
        SQLCallStatement statement =
            (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_snapshot_files(12.1)",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(null, statement, cursor);
            Assert.fail("Should throw IllegalArgumentException for decimal parameter");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("need Long number"));
        }
    }

    /**
     * Test parameter validation - string parameter
     */
    @Test
    public void paramTestStringParam() {
        ArrayResultCursor cursor = new ArrayResultCursor("test");
        ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
        SQLCallStatement statement =
            (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_snapshot_files(\"123\")",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(null, statement, cursor);
            Assert.fail("Should throw IllegalArgumentException for string parameter");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("need Long number"));
        }
    }

    /**
     * Test parameter validation - negative parameter
     */
    @Test
    public void paramTestNegativeParam() {
        ArrayResultCursor cursor = new ArrayResultCursor("test");
        ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
        SQLCallStatement statement =
            (SQLCallStatement) FastsqlUtils.parseSql(
                "call polardbx.columnar_snapshot_files(-120)",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(null, statement, cursor);
            Assert.fail("Should throw IllegalArgumentException for negative parameter");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("need >= 0"));
        }
    }

    /**
     * Test parameter validation - multiple parameters
     */
    @Test
    public void paramTestMultipleParams() {
        ArrayResultCursor cursor = new ArrayResultCursor("test");
        ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
        SQLCallStatement statement =
            (SQLCallStatement) FastsqlUtils.parseSql(
                "call polardbx.columnar_snapshot_files(120, 30)",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(null, statement, cursor);
            Assert.fail("Should throw IllegalArgumentException for multiple parameters");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("parameters is not match 1 parameters"));
        }
    }

    private static SQLCallStatement parseCall(long tso) {
        return (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_snapshot_files(" + tso + ")",
            SQLParserFeature.IgnoreNameQuotes).get(0);
    }

    private static void mockCheckpointAndPurge(MockedStatic<MetaDbUtil> metaDbUtilMockedStatic, long tso) {
        List<ColumnarCheckpointsRecord> checkpointRecords = new ArrayList<>();
        ColumnarCheckpointsRecord checkpointRecord = new ColumnarCheckpointsRecord();
        checkpointRecord.setBinlogTso(tso);
        checkpointRecord.setCheckpointTso(tso);
        checkpointRecords.add(checkpointRecord);
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
            Mockito.eq(ColumnarCheckpointsRecord.class), Mockito.any())).thenReturn(checkpointRecords);

        metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
            Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any())).thenReturn(new ArrayList<>());
    }

    private static FileInfoSimpleRecord fileInfo(String fileName, long extentSize) {
        FileInfoSimpleRecord record = new FileInfoSimpleRecord();
        record.setFileName(fileName);
        record.setExtentSize(extentSize);
        return record;
    }

    private static ColumnarAppendedFilesRecord appendedFile(String fileName, long appendOffset, long appendLength) {
        ColumnarAppendedFilesRecord record = new ColumnarAppendedFilesRecord();
        record.setFileName(fileName);
        record.setAppendOffset(appendOffset);
        record.setAppendLength(appendLength);
        return record;
    }

    /**
     * Stub MetaDbUtil.queryStream to feed given records to the consumer
     * when the sql contains the given keyword
     */
    @SuppressWarnings("unchecked")
    private static <T extends SystemTableRecord> void stubStream(MockedStatic<MetaDbUtil> metaDbUtilMockedStatic,
                                                                 String sqlContains,
                                                                 Class<T> clazz, List<T> records) {
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.queryStream(Mockito.contains(sqlContains), Mockito.any(),
            Mockito.eq(clazz), Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
            Consumer<T> consumer = invocation.getArgument(4);
            for (T record : records) {
                consumer.accept(record);
            }
            return null;
        });
    }

    private static void stubOrcFiles(MockedStatic<MetaDbUtil> ms, List<FileInfoSimpleRecord> records) {
        stubStream(ms, "= 'orc'", FileInfoSimpleRecord.class, records);
    }

    private static void stubSstFiles(MockedStatic<MetaDbUtil> ms, List<FileInfoSimpleRecord> records) {
        stubStream(ms, "= 'sst'", FileInfoSimpleRecord.class, records);
    }

    private static void stubSnapshotCsvDelFiles(MockedStatic<MetaDbUtil> ms, List<FileInfoSimpleRecord> records) {
        stubStream(ms, "('csv','del')", FileInfoSimpleRecord.class, records);
    }

    private static void stubLastValidAppendFiles(MockedStatic<MetaDbUtil> ms,
                                                 List<ColumnarAppendedFilesRecord> records) {
        // SELECT_CSV_DEL_SET_LAST_APPEND_BY_START_TSO_AND_END_TSO contains "group by"
        stubStream(ms, "group by", ColumnarAppendedFilesRecord.class, records);
    }

    private static void stubPkIdxFiles(MockedStatic<MetaDbUtil> ms, List<ColumnarAppendedFilesRecord> records) {
        // QUERY_ALL_BY_FILE_TYPE contains "`file_type` = ?"
        stubStream(ms, "`file_type` = ?", ColumnarAppendedFilesRecord.class, records);
    }

    /**
     * Test normal execution flow including all file types:
     * - orc files (fixed length, fileType=0)
     * - sst files (fixed length, fileType=0, pk index category)
     * - snapshot csv/del files (append, fileType=1)
     * - appended files (csv/del/set) (append, fileType=1)
     * - pk_idx_log_meta/pk_idx_log files (append, fileType=1)
     */
    @Test
    public void executeTest() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConnection = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            mockCheckpointAndPurge(metaDbUtilMockedStatic, 77665544L);

            List<FileInfoSimpleRecord> orcFiles = new ArrayList<>();
            orcFiles.add(fileInfo("a.orc", 1024L));
            stubOrcFiles(metaDbUtilMockedStatic, orcFiles);

            List<FileInfoSimpleRecord> sstFiles = new ArrayList<>();
            sstFiles.add(fileInfo("idx.sst", 2048L));
            stubSstFiles(metaDbUtilMockedStatic, sstFiles);

            List<FileInfoSimpleRecord> snapshotFiles = new ArrayList<>();
            snapshotFiles.add(fileInfo("snap.csv", 512L));
            stubSnapshotCsvDelFiles(metaDbUtilMockedStatic, snapshotFiles);

            List<ColumnarAppendedFilesRecord> appendedFiles = new ArrayList<>();
            appendedFiles.add(appendedFile("b.csv", 2048L, 4096L));
            stubLastValidAppendFiles(metaDbUtilMockedStatic, appendedFiles);

            List<ColumnarAppendedFilesRecord> pkIdxFiles = new ArrayList<>();
            pkIdxFiles.add(appendedFile("p.log", 512L, 1024L));
            stubPkIdxFiles(metaDbUtilMockedStatic, pkIdxFiles);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
            procedure.execute(null, parseCall(77665544L), cursor);

            List<Row> rows = cursor.getRows();
            Assert.assertNotNull(rows);
            // orc + sst + snapshot csv + appended csv + pk file (fed for both pk_idx_log_meta and pk_idx_log)
            Assert.assertEquals(6, rows.size());
            // 固定长度文件（type=0）先于追加写文件（type=1）输出
            assertRow(rows.get(0), "a.orc", 1024L, 0);
            assertRow(rows.get(1), "idx.sst", 2048L, 0);
            assertRow(rows.get(2), "snap.csv", 512L, 1);
            assertRow(rows.get(3), "b.csv", 2048L + 4096L, 1);
            assertRow(rows.get(4), "p.log", 512L + 1024L, 1);
            assertRow(rows.get(5), "p.log", 512L + 1024L, 1);
        }
    }

    private static void assertRow(Row row, String fileName, long fileLength, int fileType) {
        Assert.assertEquals(fileName, row.getObject(0));
        Assert.assertEquals(Long.valueOf(fileLength), row.getObject(1));
        Assert.assertEquals(Integer.valueOf(fileType), row.getObject(2));
    }

    /**
     * Test pk index related files (sst, pk_idx_log_meta, pk_idx_log) are excluded
     * when COLUMNAR_SNAPSHOT_INCLUDE_PK_INDEX_FILES is false
     */
    @Test
    public void testPkIndexFilesExcluded() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConnection = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            mockCheckpointAndPurge(metaDbUtilMockedStatic, 12345678L);

            List<FileInfoSimpleRecord> orcFiles = new ArrayList<>();
            orcFiles.add(fileInfo("a.orc", 1024L));
            stubOrcFiles(metaDbUtilMockedStatic, orcFiles);

            List<FileInfoSimpleRecord> sstFiles = new ArrayList<>();
            sstFiles.add(fileInfo("idx.sst", 2048L));
            stubSstFiles(metaDbUtilMockedStatic, sstFiles);

            List<ColumnarAppendedFilesRecord> pkIdxFiles = new ArrayList<>();
            pkIdxFiles.add(appendedFile("p.log", 512L, 1024L));
            stubPkIdxFiles(metaDbUtilMockedStatic, pkIdxFiles);

            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.COLUMNAR_SNAPSHOT_INCLUDE_PK_INDEX_FILES, "false");
            try {
                ArrayResultCursor cursor = new ArrayResultCursor("test");
                ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
                procedure.execute(null, parseCall(12345678L), cursor);

                List<Row> rows = cursor.getRows();
                Assert.assertEquals(1, rows.size());
                assertRow(rows.get(0), "a.orc", 1024L, 0);
            } finally {
                DynamicConfig.getInstance()
                    .loadValue(null, ConnectionProperties.COLUMNAR_SNAPSHOT_INCLUDE_PK_INDEX_FILES, "true");
            }
        }
    }

    /**
     * Test that files with appendOffset + appendLength == 0 are filtered out
     */
    @Test
    public void testZeroLengthFilesFiltered() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConnection = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            mockCheckpointAndPurge(metaDbUtilMockedStatic, 12345678L);

            List<ColumnarAppendedFilesRecord> appendedFiles = new ArrayList<>();
            appendedFiles.add(appendedFile("valid_file.csv", 100L, 200L));
            appendedFiles.add(appendedFile("zero_length_file.csv", 0L, 0L));
            stubLastValidAppendFiles(metaDbUtilMockedStatic, appendedFiles);

            List<ColumnarAppendedFilesRecord> pkIdxFiles = new ArrayList<>();
            pkIdxFiles.add(appendedFile("zero_pk.log", 0L, 0L));
            stubPkIdxFiles(metaDbUtilMockedStatic, pkIdxFiles);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
            procedure.execute(null, parseCall(12345678L), cursor);

            List<Row> rows = cursor.getRows();
            Assert.assertEquals(1, rows.size());
            assertRow(rows.get(0), "valid_file.csv", 300L, 1);
        }
    }

    /**
     * Test file length calculation: appendOffset + appendLength
     */
    @Test
    public void testFileLengthCalculation() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConnection = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            mockCheckpointAndPurge(metaDbUtilMockedStatic, 12345678L);

            List<ColumnarAppendedFilesRecord> appendedFiles = new ArrayList<>();
            appendedFiles.add(appendedFile("test_file.csv", 5000L, 3000L));
            stubLastValidAppendFiles(metaDbUtilMockedStatic, appendedFiles);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
            procedure.execute(null, parseCall(12345678L), cursor);

            List<Row> rows = cursor.getRows();
            Assert.assertEquals(1, rows.size());
            assertRow(rows.get(0), "test_file.csv", 8000L, 1);
        }
    }

    /**
     * Test empty result set handling
     */
    @Test
    public void testEmptyResultSet() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConnection = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            mockCheckpointAndPurge(metaDbUtilMockedStatic, 12345678L);
            // queryStream未打桩时默认不回调consumer，等价于空结果

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
            procedure.execute(null, parseCall(12345678L), cursor);

            List<Row> rows = cursor.getRows();
            Assert.assertNotNull(rows);
            Assert.assertEquals(0, rows.size());
        }
    }

    /**
     * Test getResultCursor returns a SpillableArrayResultCursor and rows can be read via next()
     */
    @Test
    public void testGetResultCursorWithSpillableCursor() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConnection = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            mockCheckpointAndPurge(metaDbUtilMockedStatic, 99887766L);

            List<FileInfoSimpleRecord> orcFiles = new ArrayList<>();
            orcFiles.add(fileInfo("data_001.orc", 10240L));
            stubOrcFiles(metaDbUtilMockedStatic, orcFiles);

            List<ColumnarAppendedFilesRecord> appendedFiles = new ArrayList<>();
            appendedFiles.add(appendedFile("data_002.csv", 1024L, 2048L));
            stubLastValidAppendFiles(metaDbUtilMockedStatic, appendedFiles);

            ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
            ResultCursor cursor = procedure.getResultCursor(null, parseCall(99887766L), "columnar_snapshot_files");
            try {
                Assert.assertTrue(cursor instanceof SpillableArrayResultCursor);
                Assert.assertEquals(3, cursor.getReturnColumns().size());

                Row row = cursor.next();
                Assert.assertNotNull(row);
                Assert.assertEquals("data_001.orc", String.valueOf(row.getObject(0)));
                row = cursor.next();
                Assert.assertNotNull(row);
                Assert.assertEquals("data_002.csv", String.valueOf(row.getObject(0)));
                Assert.assertNull(cursor.next());
            } finally {
                cursor.close(new ArrayList<>());
            }
        }
    }

    /**
     * Test getResultCursor closes the cursor and propagates exception on failure
     */
    @Test
    public void testGetResultCursorThrowsOnError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection)
                .thenThrow(new RuntimeException("mock connection error"));

            ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
            try {
                procedure.getResultCursor(null, parseCall(1L), "columnar_snapshot_files");
                Assert.fail("Should throw exception when metadb connection fails");
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("mock connection error")
                    || e.getCause() != null);
            }
        }
    }

    /**
     * Test wait columnar commit timeout
     */
    @Test
    public void testWaitColumnarCommitTimeout() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConnection = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            // 列存位点一直查不到
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.eq(ColumnarCheckpointsRecord.class), Mockito.any())).thenReturn(new ArrayList<>());

            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.WAIT_FOR_COLUMNAR_COMMIT_MS, "0");
            try {
                ArrayResultCursor cursor = new ArrayResultCursor("test");
                ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
                procedure.execute(null, parseCall(12345678L), cursor);
                Assert.fail("Should throw timeout exception");
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("timeout")
                    || (e.getCause() != null && e.getCause().getMessage().contains("timeout")));
            } finally {
                DynamicConfig.getInstance().loadValue(null, ConnectionProperties.WAIT_FOR_COLUMNAR_COMMIT_MS, "60000");
            }
        }
    }

    /**
     * Test formatSize auto unit conversion covering B/KB/MB/GB/TB branches
     */
    @Test
    public void testFormatSize() throws Exception {
        java.lang.reflect.Method formatSize =
            ColumnarSnapshotFilesProcedure.class.getDeclaredMethod("formatSize", long.class);
        formatSize.setAccessible(true);

        Assert.assertEquals("0B", formatSize.invoke(null, 0L));
        Assert.assertEquals("1023B", formatSize.invoke(null, 1023L));
        Assert.assertEquals("1.00KB", formatSize.invoke(null, 1024L));
        Assert.assertEquals("2.50MB", formatSize.invoke(null, (long) (2.5 * 1024 * 1024)));
        Assert.assertEquals("3.00GB", formatSize.invoke(null, 3L * 1024 * 1024 * 1024));
        Assert.assertEquals("4.97TB", formatSize.invoke(null, (long) (4.97 * 1024 * 1024 * 1024 * 1024)));
        // 超过TB不再升级单位，仍用TB表示
        Assert.assertEquals("2048.00TB", formatSize.invoke(null, 2048L * 1024 * 1024 * 1024 * 1024));
    }

    /**
     * Test execute with TB level file size, covering phase stats and total size accumulation
     */
    @Test
    public void testLargeFileSizeAccumulation() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            Connection mockConnection = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            mockCheckpointAndPurge(metaDbUtilMockedStatic, 12345678L);

            // TB级大文件，验证长度累加不溢出且行内容正确
            long tbSize = 5L * 1024 * 1024 * 1024 * 1024;
            List<FileInfoSimpleRecord> orcFiles = new ArrayList<>();
            orcFiles.add(fileInfo("big1.orc", tbSize));
            orcFiles.add(fileInfo("big2.orc", tbSize));
            stubOrcFiles(metaDbUtilMockedStatic, orcFiles);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
            procedure.execute(null, parseCall(12345678L), cursor);

            List<Row> rows = cursor.getRows();
            Assert.assertEquals(2, rows.size());
            assertRow(rows.get(0), "big1.orc", tbSize, 0);
            assertRow(rows.get(1), "big2.orc", tbSize, 0);
        }
    }
}
