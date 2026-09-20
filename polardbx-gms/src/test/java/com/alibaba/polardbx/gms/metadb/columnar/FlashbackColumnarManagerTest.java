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

import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplified;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit test for FlashbackColumnarManager Bug (AONE 82041527).
 * <p>
 * Bug: When autoPosition=true and columnar_checkpoints table has no records for
 * binlog_tso >= latestTso, queryColumnarTsoByBinlogTsoAndCheckpointTsoAsc() returns
 * an empty list, and FlashbackColumnarManager.&lt;init&gt;() directly calls .get(0) without
 * an empty check, causing IndexOutOfBoundsException.
 * <p>
 * Stack trace:
 * java.lang.IndexOutOfBoundsException: Index 0 out of bounds for length 0
 * at java.base/java.util.ArrayList.get(ArrayList.java:459)
 * at com.alibaba.polardbx.gms.metadb.columnar.FlashbackColumnarManager.&lt;init&gt;(FlashbackColumnarManager.java:71)
 */
public class FlashbackColumnarManagerTest {

    /**
     * Reproduces AONE 82041527:
     * When autoPosition=true and checkpoints query returns empty list,
     * FlashbackColumnarManager constructor should throw TddlRuntimeException
     * with a meaningful error message (after fix).
     * <p>
     * Before fix: throws IndexOutOfBoundsException at line 71 (.get(0) on empty list)
     * After fix: throws TddlRuntimeException with proper error message
     */
    @Test
    public void testAutoPositionWithEmptyCheckpointsThrowsTddlRuntimeException() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            // Mock connection
            Connection mockConnection = mock(Connection.class);
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            // Mock columnar table mapping found (so actualTableId != -1)
            List<ColumnarTableMappingRecord> mappingRecords = new ArrayList<>();
            ColumnarTableMappingRecord mappingRecord = new ColumnarTableMappingRecord();
            mappingRecord.tableId = 100L;
            mappingRecords.add(mappingRecord);

            // Mock snapshot files found (so we pass the actualTableId check)
            List<FilesRecordSimplified> snapshotFiles = new ArrayList<>();
            FilesRecordSimplified fileRecord = new FilesRecordSimplified();
            fileRecord.fileName = "test.orc";
            fileRecord.partitionName = "p1";
            fileRecord.schemaTs = 1000L;
            snapshotFiles.add(fileRecord);

            // Mock checkpoints query to return EMPTY list - this triggers the bug scenario
            List<ColumnarCheckpointsRecord> emptyCheckpoints = Collections.emptyList();

            // Intercept all MetaDbUtil.query calls
            metaDbUtilMock.when(() -> MetaDbUtil.query(any(String.class), any(Map.class),
                    any(Class.class), any(Connection.class)))
                .thenAnswer(invocation -> {
                    Class<?> resultClass = invocation.getArgument(2);
                    if (resultClass == ColumnarTableMappingRecord.class) {
                        return mappingRecords;
                    } else if (resultClass == FilesRecordSimplified.class) {
                        return snapshotFiles;
                    } else if (resultClass == ColumnarCheckpointsRecord.class) {
                        return emptyCheckpoints;
                    }
                    return Collections.emptyList();
                });

            // Mock MetaDbUtil.delete to avoid issues in cleanup paths
            metaDbUtilMock.when(() -> MetaDbUtil.delete(any(String.class), any(Map.class), any(Connection.class)))
                .thenAnswer(invocation -> 0);

            // After fix: should throw TddlRuntimeException with meaningful message
            // instead of IndexOutOfBoundsException
            try {
                new FlashbackColumnarManager(12345L, "test_schema", "col_test_table", true);
                org.junit.Assert.fail("Expected TddlRuntimeException, but constructor succeeded");
            } catch (com.alibaba.polardbx.common.exception.TddlRuntimeException e) {
                // Expected behavior after fix
                System.out.println("Caught TddlRuntimeException: " + e.getMessage());
                org.junit.Assert.assertTrue("Error message should mention checkpoint",
                    e.getMessage().toLowerCase().contains("checkpoint"));
                org.junit.Assert.assertTrue("Error message should mention the tso value",
                    e.getMessage().contains("12345"));
            } catch (IndexOutOfBoundsException e) {
                // This should NOT happen after the fix
                org.junit.Assert.fail("Bug not fixed: still throws IndexOutOfBoundsException instead of "
                    + "TddlRuntimeException. Message: " + e.getMessage());
            }
        }
    }
}
