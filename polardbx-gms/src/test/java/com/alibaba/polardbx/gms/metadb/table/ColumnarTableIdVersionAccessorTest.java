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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

@RunWith(MockitoJUnitRunner.class)
public class ColumnarTableIdVersionAccessorTest {

    @Mock
    private Connection mockConnection;

    private ColumnarTableIdVersionAccessor accessor;

    @Before
    public void setUp() {
        accessor = new ColumnarTableIdVersionAccessor();
        accessor.setConnection(mockConnection);
    }

    @Test
    public void testQueryByOldTableId() {
        // 准备测试数据
        Long tableId = 12345L;
        List<ColumnarTableIdVersionRecord> expectedRecords = new ArrayList<>();

        ColumnarTableIdVersionRecord record1 = new ColumnarTableIdVersionRecord();
        record1.newTableId = 100L;
        record1.oldTableId = tableId;
        record1.newVersionId = 1L;
        record1.oldVersionId = 0L;

        ColumnarTableIdVersionRecord record2 = new ColumnarTableIdVersionRecord();
        record2.newTableId = 200L;
        record2.oldTableId = tableId;
        record2.newVersionId = 2L;
        record2.oldVersionId = 1L;

        expectedRecords.add(record1);
        expectedRecords.add(record2);

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            // Mock MetaDbUtil.setParameter
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(
                eq(1),
                any(Map.class),
                eq(ParameterMethod.setLong),
                eq(tableId)
            )).thenAnswer(invocation -> {
                Map<Integer, ParameterContext> params = invocation.getArgument(1);
                ParameterContext parameterContext =
                    new ParameterContext(ParameterMethod.setLong, new Object[] {1, tableId});
                params.put(1, parameterContext);
                return null;
            });

            // Mock MetaDbUtil.query method
            metaDbUtilMock.when(() -> MetaDbUtil.query(
                anyString(),
                any(Map.class),
                eq(ColumnarTableIdVersionRecord.class),
                any(Connection.class)
            )).thenReturn(expectedRecords);

            // 执行测试
            List<ColumnarTableIdVersionRecord> actualRecords = accessor.queryByOldTableId(tableId);

            // 验证结果
            assertEquals(2, actualRecords.size());
            assertEquals(record1.newTableId, actualRecords.get(0).newTableId);
            assertEquals(record1.oldTableId, actualRecords.get(0).oldTableId);
            assertEquals(record2.newTableId, actualRecords.get(1).newTableId);
            assertEquals(record2.oldTableId, actualRecords.get(1).oldTableId);

            // 验证 MetaDbUtil.setParameter 被正确调用
            metaDbUtilMock.verify(() -> MetaDbUtil.setParameter(
                eq(1),
                any(Map.class),
                eq(ParameterMethod.setLong),
                eq(tableId)
            ), times(1));
        }
    }

    @Test
    public void testQueryByOldTableIdWithNullResult() {
        Long tableId = 99999L;
        List<ColumnarTableIdVersionRecord> emptyRecords = new ArrayList<>();

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(
                eq(1),
                any(Map.class),
                eq(ParameterMethod.setLong),
                eq(tableId)
            )).thenAnswer(invocation -> {
                Map<Integer, ParameterContext> params = invocation.getArgument(1);
                ParameterContext parameterContext =
                    new ParameterContext(ParameterMethod.setLong, new Object[] {1, tableId});
                params.put(1, parameterContext);
                return null;
            });

            metaDbUtilMock.when(() -> MetaDbUtil.query(
                anyString(),
                any(Map.class),
                eq(ColumnarTableIdVersionRecord.class),
                any(Connection.class)
            )).thenReturn(emptyRecords);

            List<ColumnarTableIdVersionRecord> actualRecords = accessor.queryByOldTableId(tableId);

            assertTrue(actualRecords.isEmpty());
        }
    }

    @Test
    public void testDeleteByOldTableIdWithValidList() {
        List<Long> tableIds = Arrays.asList(100L, 200L, 300L);
        int expectedDeletedRows = 3;

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            // Mock MetaDbUtil.buildParameters
            Map<Integer, ParameterContext> mockParams = new HashMap<>();
            metaDbUtilMock.when(() -> MetaDbUtil.buildParameters(
                eq(ParameterMethod.setLong),
                any(Object[].class)
            )).thenReturn(mockParams);

            // Mock MetaDbUtil.delete method
            metaDbUtilMock.when(() -> MetaDbUtil.delete(
                anyString(),
                any(Map.class),
                any(Connection.class)
            )).thenReturn(expectedDeletedRows);

            // 执行测试
            int actualDeletedRows = accessor.deleteByOldTableId(tableIds);

            // 验证结果
            assertEquals(expectedDeletedRows, actualDeletedRows);

            // 验证 MetaDbUtil.buildParameters 被正确调用
            metaDbUtilMock.verify(() -> MetaDbUtil.buildParameters(
                eq(ParameterMethod.setLong),
                any(Object[].class)
            ), times(1));
        }
    }

    @Test
    public void testDeleteByOldTableIdWithNullList() {
        int actualDeletedRows = accessor.deleteByOldTableId(null);
        assertEquals(0, actualDeletedRows);
    }

    @Test
    public void testDeleteByOldTableIdWithEmptyList() {
        List<Long> emptyList = new ArrayList<>();
        int actualDeletedRows = accessor.deleteByOldTableId(emptyList);
        assertEquals(0, actualDeletedRows);
    }

    @Test
    public void testDeleteByOldTableIdWithSingleItem() {
        List<Long> tableIds = Arrays.asList(100L);
        int expectedDeletedRows = 1;

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            Map<Integer, ParameterContext> mockParams = new HashMap<>();
            metaDbUtilMock.when(() -> MetaDbUtil.buildParameters(
                eq(ParameterMethod.setLong),
                any(Object[].class)
            )).thenReturn(mockParams);

            metaDbUtilMock.when(() -> MetaDbUtil.delete(
                anyString(),
                any(Map.class),
                any(Connection.class)
            )).thenReturn(expectedDeletedRows);

            int actualDeletedRows = accessor.deleteByOldTableId(tableIds);

            assertEquals(expectedDeletedRows, actualDeletedRows);
        }
    }

    @Test
    public void testDeleteByOldTableIdWithLargeList() {
        // 测试大量数据的情况
        List<Long> tableIds = new ArrayList<>();
        for (long i = 1; i <= 1000; i++) {
            tableIds.add(i);
        }
        int expectedDeletedRows = 1000;

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            Map<Integer, ParameterContext> mockParams = new HashMap<>();
            metaDbUtilMock.when(() -> MetaDbUtil.buildParameters(
                eq(ParameterMethod.setLong),
                any(Object[].class)
            )).thenReturn(mockParams);

            metaDbUtilMock.when(() -> MetaDbUtil.delete(
                anyString(),
                any(Map.class),
                any(Connection.class)
            )).thenReturn(expectedDeletedRows);

            int actualDeletedRows = accessor.deleteByOldTableId(tableIds);

            assertEquals(expectedDeletedRows, actualDeletedRows);
        }
    }
}