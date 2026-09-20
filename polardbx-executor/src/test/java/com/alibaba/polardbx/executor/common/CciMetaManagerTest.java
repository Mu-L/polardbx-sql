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

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnarIndexEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexesAccessor;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CciMetaManagerTest {

    @Spy
    CciMetaManager cciMetaManagerSpy;

    List<ColumnarTableEvolutionRecord> evolutionRecords = new ArrayList<>();
    List<TablePartitionRecord> partitionRecords = new ArrayList<>();
    List<IndexesRecord> primaryKeys = new ArrayList<>();
    List<IndexesRecord> sortKeys = new ArrayList<>();

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        ColumnarTableEvolutionRecord record = new ColumnarTableEvolutionRecord();
        record.tableId = 1L;
        record.tableName = "test_table";
        record.tableSchema = "test_schema";
        record.indexName = "test_index";
        record.versionId = 100L;
        record.ddlJobId = 101L;
        record.partitions = new java.util.ArrayList<>();
        record.primaryKeys = new java.util.ArrayList<>();
        record.sortKeys = new java.util.ArrayList<>();
        evolutionRecords.add(record);

        ColumnarTableEvolutionRecord record1 = new ColumnarTableEvolutionRecord();
        record1.tableId = 1L;
        record1.tableName = "test_table";
        record1.tableSchema = "test_schema";
        record1.indexName = "test_index";
        record1.versionId = 101L;
        record1.ddlJobId = 102L;
        record1.partitions = new java.util.ArrayList<>();
        record1.primaryKeys = new java.util.ArrayList<>();
        record1.sortKeys = new java.util.ArrayList<>();
        evolutionRecords.add(record1);

        ColumnarTableEvolutionRecord record2 = new ColumnarTableEvolutionRecord();
        record2.tableId = 2L;
        record2.tableName = "test_table_1";
        record2.tableSchema = "test_schema";
        record2.indexName = "test_index_1";
        record2.versionId = 102L;
        record2.ddlJobId = 103L;
        record2.partitions = new java.util.ArrayList<>();
        record2.primaryKeys = new java.util.ArrayList<>();
        record2.sortKeys = new java.util.ArrayList<>();
        evolutionRecords.add(record2);

        TablePartitionRecord partitionRecord = new TablePartitionRecord();
        partitionRecords.add(partitionRecord);
        IndexesRecord primaryKey = new IndexesRecord();
        primaryKeys.add(primaryKey);
        IndexesRecord sortKey = new IndexesRecord();
        sortKeys.add(sortKey);
    }

    @Test
    public void testDoInit() {
        ColumnarTableEvolutionAccessor columnarTableEvolutionAccessor = mock(ColumnarTableEvolutionAccessor.class);
        ColumnarPartitionEvolutionAccessor columnarPartitionEvolutionAccessor =
            mock(ColumnarPartitionEvolutionAccessor.class);
        ColumnarIndexEvolutionAccessor columnarIndexEvolutionAccessor =
            mock(ColumnarIndexEvolutionAccessor.class);
        TablePartitionAccessor tablePartition = mock(TablePartitionAccessor.class);
        IndexesAccessor indexes = mock(IndexesAccessor.class);
        when(columnarTableEvolutionAccessor.queryPartitionEmptyRecords()).thenReturn(evolutionRecords);
        when(columnarTableEvolutionAccessor.queryPrimaryKeyEmptyRecords()).thenReturn(evolutionRecords);
        when(columnarTableEvolutionAccessor.querySortKeyEmptyRecords()).thenReturn(evolutionRecords);
        when(tablePartition.getTablePartitionsByDbNameTbName(anyString(), anyString(), anyBoolean())).thenReturn(
            partitionRecords);
        when(indexes.queryPrimaryKeyBySchemaAndTable(anyString(), anyString())).thenReturn(primaryKeys);
        when(indexes.queryColumnarIndexColumnsByName(anyString(), anyString())).thenReturn(sortKeys);

        when(cciMetaManagerSpy.getColumnarTableEvolution()).thenReturn(columnarTableEvolutionAccessor);
        when(cciMetaManagerSpy.getColumnarPartitionEvolution()).thenReturn(columnarPartitionEvolutionAccessor);
        when(cciMetaManagerSpy.getColumnarIndexEvolution()).thenReturn(columnarIndexEvolutionAccessor);
        when(cciMetaManagerSpy.getTablePartition()).thenReturn(tablePartition);
        when(cciMetaManagerSpy.getIndexes()).thenReturn(indexes);

        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = mockStatic(ConfigDataMode.class)) {
            configDataModeMockedStatic.when(ConfigDataMode::isPolarDbX).thenReturn(true);

            try (MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic = mockStatic(MetaDbDataSource.class)) {
                MetaDbDataSource metaDbDataSourceMock = mock(MetaDbDataSource.class);
                Connection connectionMock = mock(Connection.class);
                when(metaDbDataSourceMock.getConnection()).thenReturn(connectionMock);
                metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(metaDbDataSourceMock);

                cciMetaManagerSpy.init();
                verify(cciMetaManagerSpy, times(1)).doInit();
                verify(cciMetaManagerSpy, times(1)).loadCciEvolutionMeta();
            }

        }
    }

    @Test
    public void testDoInitNotPolarDBX() {
        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = mockStatic(ConfigDataMode.class)) {
            configDataModeMockedStatic.when(ConfigDataMode::isPolarDbX).thenReturn(false);

            cciMetaManagerSpy.init();
            verify(cciMetaManagerSpy, times(1)).doInit();
            verify(cciMetaManagerSpy, times(0)).loadCciEvolutionMeta(); // 验证是否没有调用loadPartitions
        }
    }
}



