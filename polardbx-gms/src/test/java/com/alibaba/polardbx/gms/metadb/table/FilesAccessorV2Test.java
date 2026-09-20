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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.columnar.VersionStorageStatistics;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class FilesAccessorV2Test {

    private MockedStatic<MetaDbUtil> metaDbUtilMockedStatic;
    private MockedStatic<GeneralUtil> generalUtilMockedStatic;
    private MockedStatic<VersionStorageStatistics> versionStorageStatisticsMockedStatic;
    private FilesAccessor filesAccessor;
    private Connection mockConnection;
    private PreparedStatement mockPreparedStatement;
    private Statement mockStatement;
    private ResultSet mockResultSet;

    @Before
    public void setUp() {
        metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
        generalUtilMockedStatic = mockStatic(GeneralUtil.class);
        versionStorageStatisticsMockedStatic = mockStatic(VersionStorageStatistics.class);

        filesAccessor = new FilesAccessor();
        mockConnection = mock(Connection.class);
        mockPreparedStatement = mock(PreparedStatement.class);
        mockStatement = mock(Statement.class);
        mockResultSet = mock(ResultSet.class);

        filesAccessor.setConnection(mockConnection);
    }

    @After
    public void tearDown() {
        if (metaDbUtilMockedStatic != null) {
            metaDbUtilMockedStatic.close();
        }
        if (generalUtilMockedStatic != null) {
            generalUtilMockedStatic.close();
        }
        if (versionStorageStatisticsMockedStatic != null) {
            versionStorageStatisticsMockedStatic.close();
        }
    }

    @Test
    public void testCreate() {
        FilesAccessor accessor = FilesAccessor.create();
        Assert.assertNotNull(accessor);
    }

    @Test
    public void testInsert() throws SQLException {
        List<FilesRecord> records = createMockFilesRecords();
        int[] expectedResult = {1, 1};

        metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(anyString(), any(List.class), any(Connection.class)))
            .thenReturn(expectedResult);

        int[] result = filesAccessor.insert(records, "testSchema", "testTable");
        Assert.assertArrayEquals(expectedResult, result);

        // Test exception case
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(anyString(), any(List.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.insert(records, "testSchema", "testTable");
            Assert.fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("batch insert into"));
        }
    }

    @Test
    public void testInsertWithTso() throws SQLException {
        List<FilesRecord> records = createMockFilesRecordsWithTso();
        int[] expectedResult = {1, 1};

        metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(anyString(), any(List.class), any(Connection.class)))
            .thenReturn(expectedResult);

        int[] result = filesAccessor.insertWithTso(records);
        Assert.assertArrayEquals(expectedResult, result);

        // Test exception case
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(anyString(), any(List.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.insertWithTso(records);
            Assert.fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("batch insert into"));
        }
    }

    @Test
    public void testInsertAndReturnLastInsertId() throws SQLException {
        FilesRecord record = createMockFilesRecord();
        Long expectedId = 123L;

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.insertAndReturnLastInsertId(anyString(), any(List.class), any(Connection.class)))
            .thenReturn(expectedId);

        Long result = filesAccessor.insertAndReturnLastInsertId(record, "testSchema", "testTable");
        Assert.assertEquals(expectedId, result);

        // Test exception case
        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.insertAndReturnLastInsertId(anyString(), any(List.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.insertAndReturnLastInsertId(record, "testSchema", "testTable");
            Assert.fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("batch insert into"));
        }
    }

    @Test
    public void testInsertWithTsoAndReturnLastInsertId() throws SQLException {
        FilesRecord record = createMockFilesRecordWithTso();
        Long expectedId = 456L;

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.insertAndReturnLastInsertId(anyString(), any(List.class), any(Connection.class)))
            .thenReturn(expectedId);

        Long result = filesAccessor.insertWithTsoAndReturnLastInsertId(record);
        Assert.assertEquals(expectedId, result);

        // Test exception case
        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.insertAndReturnLastInsertId(anyString(), any(List.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.insertWithTsoAndReturnLastInsertId(record);
            Assert.fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("batch insert into"));
        }
    }

    @Test
    public void testValidFile() throws Exception {
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenReturn(1);

        // Test validFile with 4 parameters
        filesAccessor.validFile(1L, new byte[] {1, 2, 3}, 1000L, 100L);

        // Test validFile with 5 parameters
        filesAccessor.validFile(1L, new byte[] {1, 2, 3}, 1000L, 100L, 12345L);

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.validFile(1L, new byte[] {1, 2, 3}, 1000L, 100L);
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testQueryByLogicalSchemaTable() {
        List<FilesRecord> expectedRecords = createMockFilesRecords();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecord> result = filesAccessor.queryByLogicalSchemaTable("testSchema", "testTable");
        Assert.assertEquals(expectedRecords.size(), result.size());
    }

    @Test
    public void testQueryFileInfoByLogicalSchemaTable() {
        List<FileInfoRecord> expectedRecords = createMockFileInfoRecords();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FileInfoRecord.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FileInfoRecord> result = filesAccessor.queryFileInfoByLogicalSchemaTable("testSchema", "testTable");
        Assert.assertEquals(expectedRecords.size(), result.size());
    }

    @Test
    public void testQueryTableFormatByLogicalSchemaTable() {
        List<FilesRecord> expectedRecords = createMockTableFormatFilesRecords();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecord> result = filesAccessor.queryTableFormatByLogicalSchemaTable("testSchema", "testTable");
        Assert.assertEquals(expectedRecords.size(), result.size());

        // Verify that all returned records are TABLE FORMAT files
        for (FilesRecord record : result) {
            Assert.assertEquals("TABLE FORMAT", record.fileType);
        }
    }

    @Test
    public void testQueryLatestFileByLogicalSchemaTable() {
        List<FilesRecord> expectedRecords = createMockLatestFilesRecords();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecord> result = filesAccessor.queryLatestFileByLogicalSchemaTable("testSchema", "testTable");
        Assert.assertEquals(expectedRecords.size(), result.size());

        // Verify that returned records have commit_ts and no remove_ts
        for (FilesRecord record : result) {
            Assert.assertNotNull(record.commitTs);
            Assert.assertNull(record.removeTs);
        }
    }

    @Test
    public void testQueryByLogicalSchema() {
        List<FilesRecord> expectedRecords = createMockFilesRecords();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecord> result = filesAccessor.queryByLogicalSchema("testSchema");
        Assert.assertEquals(expectedRecords.size(), result.size());

        // Verify that all returned records belong to the same schema
        for (FilesRecord record : result) {
            Assert.assertEquals("testSchema", record.logicalSchemaName);
        }
    }

    @Test
    public void testQueryByFileName() {
        List<FilesRecord> expectedRecords = createMockFilesRecords();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecord> result = filesAccessor.queryByFileName("testFile.orc");
        Assert.assertEquals(expectedRecords.size(), result.size());
    }

    @Test
    public void testQueryByEngine() {
        List<FilesRecord> expectedRecords = createMockFilesRecords();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecord> result = filesAccessor.queryByEngine(Engine.OSS);
        Assert.assertEquals(expectedRecords.size(), result.size());
    }

    @Test
    public void testQueryByLocalPartition() throws Exception {
        List<FilesRecord> expectedRecords = createMockFilesRecords();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecord> result =
            filesAccessor.queryByLocalPartition("logicalSchema", "logicalTable", "phySchema", "phyTable", "partition1");
        Assert.assertEquals(expectedRecords.size(), result.size());

        // Test exception case
        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.queryByLocalPartition("logicalSchema", "logicalTable", "phySchema", "phyTable", "partition1");
            Assert.fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("query"));
        }
    }

    @Test
    public void testQueryByTso() throws Exception {
        List<FilesRecord> expectedRecords = createMockFilesRecords();

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecord> result = filesAccessor.queryByTso(123L, "testSchema", "testTable", "partition1");
        Assert.assertEquals(expectedRecords.size(), result.size());

        // Test exception case
        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.queryByTso(123L, "testSchema", "testTable", "partition1");
            Assert.fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("query"));
        }
    }

    @Test
    public void testQueryColumnarByFileName() {
        List<FilesRecord> expectedRecords = createMockFilesRecords();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecord> result = filesAccessor.queryColumnarByFileName("testFile.orc");
        Assert.assertEquals(expectedRecords.size(), result.size());
    }

    @Test
    public void testQueryColumnarDeltaFilesByTsoAndTableId() {
        List<FilesRecordSimplified> expectedRecords = createMockFilesRecordSimplified();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecordSimplified.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecordSimplified> result =
            filesAccessor.queryColumnarDeltaFilesByTsoAndTableId(200L, 100L, "testSchema", "testTable");
        Assert.assertEquals(expectedRecords.size(), result.size());

        // Test exception case
        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecordSimplified.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.queryColumnarDeltaFilesByTsoAndTableId(200L, 100L, "testSchema", "testTable");
            Assert.fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("query"));
        }
    }

    @Test
    public void testQueryColumnarSnapshotFilesByTsoAndTableId() {
        List<FilesRecordSimplified> expectedRecords = createMockFilesRecordSimplified();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecordSimplified.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecordSimplified> result =
            filesAccessor.queryColumnarSnapshotFilesByTsoAndTableId(123L, "testSchema", "testTable");
        Assert.assertEquals(expectedRecords.size(), result.size());

        // Test exception case
        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecordSimplified.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.queryColumnarSnapshotFilesByTsoAndTableId(123L, "testSchema", "testTable");
            Assert.fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("query"));
        }
    }

    @Test
    public void testQuerySnapshotWithChecksumByTsoAndTableId() {
        List<FilesRecordSimplifiedWithChecksum> expectedRecords = createMockFilesRecordSimplifiedWithChecksum();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecordSimplifiedWithChecksum.class),
                    any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecordSimplifiedWithChecksum> result =
            filesAccessor.querySnapshotWithChecksumByTsoAndTableId(123L, "testSchema", "testTable");
        Assert.assertEquals(expectedRecords.size(), result.size());

        // Test exception case
        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecordSimplifiedWithChecksum.class),
                    any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.querySnapshotWithChecksumByTsoAndTableId(123L, "testSchema", "testTable");
            Assert.fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("query"));
        }
    }

    @Test
    public void testQueryOrcFileStatusByTsoAndTableId() {
        List<OrcFileStatusRecord> expectedRecords = createMockOrcFileStatusRecords();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(OrcFileStatusRecord.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<OrcFileStatusRecord> result =
            filesAccessor.queryOrcFileStatusByTsoAndTableId(123L, "testSchema", "testTable");
        Assert.assertEquals(expectedRecords.size(), result.size());

        // Test exception case
        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(OrcFileStatusRecord.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.queryOrcFileStatusByTsoAndTableId(123L, "testSchema", "testTable");
            Assert.fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("query"));
        }
    }

    @Test
    public void testUpdateFilesCommitTs() throws Exception {
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), any(Map.class), any(Connection.class)))
            .thenReturn(1);

        // Test with null ts
        filesAccessor.updateFilesCommitTs(null, "testSchema", "testTable", 123L);

        // Test with non-null ts
        filesAccessor.updateFilesCommitTs(456L, "testSchema", "testTable", 123L);

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), any(Map.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.updateFilesCommitTs(456L, "testSchema", "testTable", 123L);
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testQueryUncommitted() {
        List<FilesRecord> expectedRecords = createMockFilesRecords();
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenReturn(expectedRecords);

        List<FilesRecord> result = filesAccessor.queryUncommitted(123L, "testSchema", "testTable");
        Assert.assertEquals(expectedRecords.size(), result.size());

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(anyString(), any(Map.class), eq(FilesRecord.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.queryUncommitted(123L, "testSchema", "testTable");
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testDeleteByPartitionAndType() {
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), any(Map.class), any(Connection.class)))
            .thenReturn(5);

        int result = filesAccessor.deleteByPartitionAndType(
            "testSchema", "testTable", "partition1", "TABLE_FILE", "OSS");
        Assert.assertEquals(5, result);

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), any(Map.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.deleteByPartitionAndType("testSchema", "testTable", "partition1", "TABLE_FILE", "OSS");
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testDeleteByTableAndType() {
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), any(Map.class), any(Connection.class)))
            .thenReturn(10);

        int result = filesAccessor.deleteByTableAndType("testSchema", "testTable", "TABLE_FILE", "OSS");
        Assert.assertEquals(10, result);

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), any(Map.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.deleteByTableAndType("testSchema", "testTable", "TABLE_FILE", "OSS");
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testQueryFilesByNames() throws Exception {
        List<String> fileNames = Arrays.asList("file1.orc", "file2.orc", "file3.orc");
        VersionStorageStatistics mockStats = mock(VersionStorageStatistics.class);

        versionStorageStatisticsMockedStatic.when(VersionStorageStatistics::getThreadLocalStatistics)
            .thenReturn(mockStats);
        doNothing().when(mockStats).updateGmsStatistics(anyLong());

        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true, true, false);

        when(mockResultSet.getString(anyString())).thenReturn("testFile.orc");
        when(mockResultSet.getLong(anyString())).thenReturn(123L);
        when(mockResultSet.getBytes(anyString())).thenReturn(new byte[] {1, 2, 3});
        when(mockResultSet.wasNull()).thenReturn(false);

        List<FilesRecord> result = filesAccessor.queryFilesByNames(fileNames);
        Assert.assertEquals(2, result.size());

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        when(mockConnection.createStatement()).thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.queryFilesByNames(fileNames);
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testUpdateChangedFiles() throws Exception {
        List<String> fileNames = Arrays.asList("file1.orc", "file2.orc");

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeUpdate()).thenReturn(2);

        int result = filesAccessor.updateChangedFiles(fileNames);
        Assert.assertEquals(2, result);

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        when(mockConnection.prepareStatement(anyString())).thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.updateChangedFiles(fileNames);
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testDelete() {
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), any(Map.class), any(Connection.class)))
            .thenReturn(5);

        // Test delete by task id, schema and table
        filesAccessor.delete(123L, "testSchema", "testTable");

        // Test delete by schema and table
        int result = filesAccessor.delete("testSchema", "testTable");
        Assert.assertEquals(5, result);

        // Test delete by schema
        filesAccessor.delete("testSchema");

        // Test delete by file id
        filesAccessor.delete(456L);

        // Test exception cases
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), any(Map.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.delete(123L, "testSchema", "testTable");
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testRename() {
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenReturn(1);

        filesAccessor.rename("testSchema", "oldTable", "newTable");

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.rename("testSchema", "oldTable", "newTable");
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testValidByFileName() {
        List<String> paths = Arrays.asList("path1", "path2", "path3");

        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(List.class), any(Connection.class)))
            .thenReturn(new int[] {1});

        filesAccessor.validByFileName(paths);

        // Test empty paths
        filesAccessor.validByFileName(Collections.emptyList());

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(List.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.validByFileName(paths);
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testReady() {
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenReturn(1);

        filesAccessor.ready(123L, "testSchema", "testTable");

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.ready(123L, "testSchema", "testTable");
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testUpdateFilesRemoveTs() {
        List<String> files = Arrays.asList("file1.orc", "file2.orc");

        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenReturn(1);

        // Test with null ts
        filesAccessor.updateFilesRemoveTs(null, "testSchema", "testTable", files);

        // Test with non-null ts
        filesAccessor.updateFilesRemoveTs(123L, "testSchema", "testTable", files);

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.updateFilesRemoveTs(123L, "testSchema", "testTable", files);
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testUpdateTableRemoveTs() {
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenReturn(5);

        // Test with null ts
        int result = filesAccessor.updateTableRemoveTs(null, "testSchema", "testTable");
        Assert.assertEquals(5, result);

        // Test with non-null ts
        result = filesAccessor.updateTableRemoveTs(123L, "testSchema", "testTable");
        Assert.assertEquals(5, result);

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.updateTableRemoveTs(123L, "testSchema", "testTable");
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testUpdateTableRemoveTsLimit() {
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenReturn(10);

        // Test with null ts
        int result = filesAccessor.updateTableRemoveTsLimit(null, "testSchema", "testTable", 50L);
        Assert.assertEquals(10, result);

        // Test with non-null ts
        result = filesAccessor.updateTableRemoveTsLimit(123L, "testSchema", "testTable", 50L);
        Assert.assertEquals(10, result);

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.updateTableRemoveTsLimit(123L, "testSchema", "testTable", 50L);
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testUpdateTableSchema() {
        Set<Long> fileIds = new HashSet<>(Arrays.asList(1L, 2L, 3L));

        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenReturn(1);

        filesAccessor.updateTableSchema("newSchema", fileIds);

        // Test empty fileIds
        filesAccessor.updateTableSchema("newSchema", Collections.emptySet());

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.updateTableSchema("newSchema", fileIds);
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    @Test
    public void testUpdateFileSizeAndRowsByFileName() {
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenReturn(1);

        int result = filesAccessor.updateFileSizeAndRowsByFileName(1000L, 100L, "testFile.orc");
        Assert.assertEquals(1, result);

        // Test exception case
        generalUtilMockedStatic.when(() -> GeneralUtil.nestedException(any(Exception.class)))
            .thenReturn(new RuntimeException("Test exception"));
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), any(Map.class), any(Connection.class)))
            .thenThrow(new SQLException("Test exception"));

        try {
            filesAccessor.updateFileSizeAndRowsByFileName(1000L, 100L, "testFile.orc");
            Assert.fail("Should throw RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertEquals("Test exception", e.getMessage());
        }
    }

    // Helper methods to create mock objects
    private List<FilesRecord> createMockFilesRecords() {
        List<FilesRecord> records = new ArrayList<>();
        FilesRecord record1 = createMockFilesRecord();
        FilesRecord record2 = createMockFilesRecord();
        record2.fileName = "testFile2.orc";
        records.add(record1);
        records.add(record2);
        return records;
    }

    private FilesRecord createMockFilesRecord() {
        FilesRecord record = new FilesRecord();
        record.fileId = 123L;
        record.fileName = "testFile.orc";
        record.fileType = "TABLE_FILE";
        record.tablespaceName = "";
        record.tableCatalog = "catalog";
        record.tableSchema = "testSchema";
        record.tableName = "testTable";
        record.logfileGroupName = "group";
        record.logfileGroupNumber = 1L;
        record.engine = "OSS";
        record.fulltextKeys = "keys";
        record.deletedRows = 0L;
        record.updateCount = 0L;
        record.freeExtents = 0L;
        record.totalExtents = 1L;
        record.extentSize = 1000L;
        record.initialSize = 1000L;
        record.maximumSize = 10000L;
        record.autoextendSize = 1000L;
        record.creationTime = "2023-01-01 00:00:00";
        record.lastUpdateTime = "2023-01-01 00:00:00";
        record.lastAccessTime = "2023-01-01 00:00:00";
        record.recoverTime = 10000L;
        record.transactionCounter = 1L;
        record.version = 1L;
        record.rowFormat = "COMPRESSED";
        record.tableRows = 100L;
        record.avgRowLength = 10L;
        record.dataLength = 1000L;
        record.maxDataLength = 10000L;
        record.indexLength = 100L;
        record.dataFree = 0L;
        record.checkTime = "2023-01-01 00:00:00";
        record.checksum = 12345L;
        record.status = "NORMAL";
        record.extra = "extra";
        record.taskId = 456L;
        record.lifeCycle = 1;
        record.localPath = "/local/path";
        record.logicalSchemaName = "testSchema";
        record.logicalTableName = "logicalTable";
        record.localPartitionName = "partition1";
        record.partitionName = "partition1";
        return record;
    }

    private List<FilesRecord> createMockFilesRecordsWithTso() {
        List<FilesRecord> records = createMockFilesRecords();
        for (FilesRecord record : records) {
            record.commitTs = 123L;
            record.removeTs = null;
            record.schemaTs = 100L;
        }
        return records;
    }

    private FilesRecord createMockFilesRecordWithTso() {
        FilesRecord record = createMockFilesRecord();
        record.commitTs = 123L;
        record.removeTs = null;
        record.schemaTs = 100L;
        return record;
    }

    private List<FileInfoRecord> createMockFileInfoRecords() {
        List<FileInfoRecord> records = new ArrayList<>();
        FileInfoRecord record1 = new FileInfoRecord();
        record1.fileId = 123L;
        record1.fileName = "testFile.orc";
        FileInfoRecord record2 = new FileInfoRecord();
        record2.fileId = 124L;
        record2.fileName = "testFile2.orc";
        records.add(record1);
        records.add(record2);
        return records;
    }

    private List<FilesRecordSimplified> createMockFilesRecordSimplified() {
        List<FilesRecordSimplified> records = new ArrayList<>();
        FilesRecordSimplified record1 = new FilesRecordSimplified();
        record1.fileName = "testFile.orc";
        record1.partitionName = "partition1";
        record1.commitTs = 123L;
        record1.removeTs = null;
        record1.schemaTs = 100L;
        records.add(record1);
        return records;
    }

    private List<FilesRecordSimplifiedWithChecksum> createMockFilesRecordSimplifiedWithChecksum() {
        List<FilesRecordSimplifiedWithChecksum> records = new ArrayList<>();
        FilesRecordSimplifiedWithChecksum record1 = new FilesRecordSimplifiedWithChecksum();
        record1.fileName = "testFile.orc";
        record1.partitionName = "partition1";
        record1.commitTs = 123L;
        record1.removeTs = null;
        record1.schemaTs = 100L;
        record1.checksum = 12345L;
        record1.deletedChecksum = 0L;
        records.add(record1);
        return records;
    }

    private List<OrcFileStatusRecord> createMockOrcFileStatusRecords() {
        List<OrcFileStatusRecord> records = new ArrayList<>();
        OrcFileStatusRecord record1 = new OrcFileStatusRecord();
        record1.fileCounts = 10L;
        record1.rowCounts = 1000L;
        record1.fileSizes = 10000L;
        records.add(record1);
        return records;
    }

    private List<FilesRecord> createMockTableFormatFilesRecords() {
        List<FilesRecord> records = new ArrayList<>();
        FilesRecord record1 = createMockFilesRecord();
        record1.fileType = "TABLE FORMAT";
        record1.fileName = "tableFormat1.meta";
        FilesRecord record2 = createMockFilesRecord();
        record2.fileType = "TABLE FORMAT";
        record2.fileName = "tableFormat2.meta";
        records.add(record1);
        records.add(record2);
        return records;
    }

    private List<FilesRecord> createMockLatestFilesRecords() {
        List<FilesRecord> records = new ArrayList<>();
        FilesRecord record1 = createMockFilesRecord();
        record1.commitTs = 200L;
        record1.removeTs = null;
        record1.fileType = "TABLE_FILE";
        record1.fileName = "latest1.orc";
        records.add(record1);
        return records;
    }
}