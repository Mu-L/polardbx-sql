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

import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.CommonIntegerRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mockStatic;

public class ColumnarAppendedFilesAccessorTest {

    @Test
    public void testSelect() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<ColumnarAppendedFilesRecord> recordList = new ArrayList<>();
            recordList.add(new ColumnarAppendedFilesRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarAppendedFilesRecord.class), Mockito.any())).thenReturn(recordList);
            ColumnarAppendedFilesAccessor columnarAppendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            List<ColumnarAppendedFilesRecord> result =
                columnarAppendedFilesAccessor.queryLastValidAppendByTsoAndTableIdSubQuery(12L, "schema", "table");

            Assert.assertEquals(1, result.size());

            result = columnarAppendedFilesAccessor.queryLastValidAppendByStartTsoAndEndTso(111L, 123L);
            Assert.assertEquals(1, result.size());

            result = columnarAppendedFilesAccessor.queryFilesByFileType("pk_idx_log_meta");
            Assert.assertEquals(1, result.size());

            result = columnarAppendedFilesAccessor.queryFileLastAppendedRecord("file2");
            Assert.assertEquals(1, result.size());

            result = columnarAppendedFilesAccessor.queryLastValidCSVAppendByTsoAndTableId(111L, "db", "table");
            Assert.assertEquals(1, result.size());

            result = columnarAppendedFilesAccessor.queryFileLastAppendedRecord("file2");
            Assert.assertEquals(1, result.size());

            result = columnarAppendedFilesAccessor.queryFilesByTsoLimitTwo("file1", 123L);
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testDelete() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger deleteCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> deleteCount.get());
            ColumnarAppendedFilesAccessor columnarAppendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            deleteCount.set(100);
            int count = columnarAppendedFilesAccessor.deleteLimit("schema", "table", 100);
            Assert.assertEquals(100, count);
            deleteCount.set(1000);
            count = columnarAppendedFilesAccessor.deleteByTableAndFileName("schema", "table", "file1");
            Assert.assertEquals(1000, count);

            deleteCount.set(12);
            count = columnarAppendedFilesAccessor.deleteByTso(123L);
            Assert.assertEquals(12, count);

            deleteCount.set(16);
            count = columnarAppendedFilesAccessor.deleteByEqualTso(123L);
            Assert.assertEquals(16, count);

            deleteCount.set(18);
            count = columnarAppendedFilesAccessor.deleteLimitByTableAndFileName("schema", "table", "file1", 123L);
            Assert.assertEquals(18, count);

            deleteCount.set(20);
            count = columnarAppendedFilesAccessor.deleteLimitByFileNameAndTso("file1", 123L, 100L);
            Assert.assertEquals(20, count);
        }
    }

    @Test
    public void testError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                    Mockito.eq(ColumnarAppendedFilesRecord.class), Mockito.any()))
                .thenThrow(new RuntimeException("mock error"));
            ColumnarAppendedFilesAccessor columnarAppendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            try {
                columnarAppendedFilesAccessor.queryLastValidCSVAppendByTsoAndTableId(111L, "db", "table");
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("mock error"));
            }
        }
    }

    @Test
    public void testQueryFileHaveRowsTrue() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Mock: have rows, returns value=1
            List<CommonIntegerRecord> records = new ArrayList<>();
            CommonIntegerRecord intRecord = new CommonIntegerRecord();
            intRecord.value = 1;
            records.add(intRecord);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CommonIntegerRecord.class), Mockito.any())).thenReturn(records);

            ColumnarAppendedFilesAccessor accessor = new ColumnarAppendedFilesAccessor();
            boolean result = accessor.queryFileHaveRows("testFile.csv", 5L);
            Assert.assertTrue(result);
        }
    }

    @Test
    public void testQueryFileHaveRowsFalseValueZero() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Mock: not enough rows, returns value=0
            List<CommonIntegerRecord> records = new ArrayList<>();
            CommonIntegerRecord intRecord = new CommonIntegerRecord();
            intRecord.value = 0;
            records.add(intRecord);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CommonIntegerRecord.class), Mockito.any())).thenReturn(records);

            ColumnarAppendedFilesAccessor accessor = new ColumnarAppendedFilesAccessor();
            boolean result = accessor.queryFileHaveRows("testFile.csv", 5L);
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testQueryFileHaveRowsFalseEmptyList() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Mock: empty result list
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CommonIntegerRecord.class), Mockito.any())).thenReturn(new ArrayList<>());

            ColumnarAppendedFilesAccessor accessor = new ColumnarAppendedFilesAccessor();
            boolean result = accessor.queryFileHaveRows("nonexistent.csv", 3L);
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testQueryFileHaveRowsFalseNullList() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Mock: null result list
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CommonIntegerRecord.class), Mockito.any())).thenReturn(null);

            ColumnarAppendedFilesAccessor accessor = new ColumnarAppendedFilesAccessor();
            boolean result = accessor.queryFileHaveRows("nullFile.csv", 1L);
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testDeleteLimitByFileNameAndTso() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger deleteCount = new AtomicInteger(0);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> deleteCount.get());

            ColumnarAppendedFilesAccessor accessor = new ColumnarAppendedFilesAccessor();

            deleteCount.set(5);
            int count = accessor.deleteLimitByFileNameAndTso("file1.csv", 1000L, 10L);
            Assert.assertEquals(5, count);

            deleteCount.set(0);
            count = accessor.deleteLimitByFileNameAndTso("nonexistent.csv", 500L, 100L);
            Assert.assertEquals(0, count);

            deleteCount.set(100);
            count = accessor.deleteLimitByFileNameAndTso("bigfile.csv", Long.MAX_VALUE, 1000L);
            Assert.assertEquals(100, count);
        }
    }

    @Test
    public void testQueryFilesByTsoLimitTwoEmpty() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarAppendedFilesRecord.class), Mockito.any())).thenReturn(new ArrayList<>());

            ColumnarAppendedFilesAccessor accessor = new ColumnarAppendedFilesAccessor();
            List<ColumnarAppendedFilesRecord> result = accessor.queryFilesByTsoLimitTwo("nofile.csv", 999L);
            Assert.assertNotNull(result);
            Assert.assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testQueryFilesByTsoLimitTwoMultipleResults() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<ColumnarAppendedFilesRecord> twoRecords = new ArrayList<>();
            twoRecords.add(new ColumnarAppendedFilesRecord());
            twoRecords.add(new ColumnarAppendedFilesRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarAppendedFilesRecord.class), Mockito.any())).thenReturn(twoRecords);

            ColumnarAppendedFilesAccessor accessor = new ColumnarAppendedFilesAccessor();
            List<ColumnarAppendedFilesRecord> result = accessor.queryFilesByTsoLimitTwo("file1.csv", 2000L);
            Assert.assertEquals(2, result.size());
        }
    }
}
