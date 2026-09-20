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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.ColumnarTableMeta;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.gsi.GsiManager;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.metadb.table.LackLocalIndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.repo.mysql.spi.MyJdbcHandler;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlShowColumnarIndex;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ShowColumnarIndexHandlerTest {

    private final String schemaName = "db";
    private final String tableName = "tb";
    private final String indexName = "cci";

    private GsiMetaManager.GsiMetaBean gsiMetaBean;

    private final Map<String, GsiMetaManager.GsiTableMetaBean> gsiTableMeta = new HashMap<>();

    Map<String, GsiMetaManager.GsiIndexMetaBean> indexMap = new HashMap<>();
    GsiMetaManager.GsiIndexMetaBean indexMetaBean =
        new GsiMetaManager.GsiIndexMetaBean(null, schemaName, tableName, true, schemaName, indexName,
            Collections.emptyList(), Collections.emptyList(), null, null, null, null, null,
            IndexStatus.PUBLIC, 1, true, true, IndexVisibility.VISIBLE,  LackLocalIndexStatus.NO_LACKIING);

    GsiMetaManager.GsiTableMetaBean tableMetaBean = new GsiMetaManager.GsiTableMetaBean(null,
        schemaName, tableName, GsiMetaManager.TableType.SHARDING, null, null, null,
        null, null, null, indexMap, null, indexMetaBean);

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        gsiMetaBean = new GsiMetaManager.GsiMetaBean();
        gsiMetaBean.setTableMeta(gsiTableMeta);
    }

    @Test
    public void testShowColumnarIndexWithTso() {
        MyRepository repo = new MockMyRepository();
        ShowColumnarIndexHandler handler = new ShowColumnarIndexHandler(repo);
        ExecutionContext executionContext = new ExecutionContext("db");
        LogicalDal logicalPlan = mock(LogicalDal.class);
        SqlShowColumnarIndex showColumnarIndex = mock(SqlShowColumnarIndex.class);
        when(logicalPlan.getNativeSqlNode()).thenReturn(showColumnarIndex);

        SqlIdentifier tableNameNode = new SqlIdentifier("tb", SqlParserPos.ZERO);
        SqlNumericLiteral tso = mock(SqlNumericLiteral.class);
        when(tso.getValue()).thenReturn(7244952534094184514L);
        when(tso.longValue(anyBoolean())).thenReturn(7244952534094184514L);

        when(showColumnarIndex.getTable()).thenReturn(tableNameNode);
        when(showColumnarIndex.getTso()).thenReturn(tso);

        PartitionInfo partInfo = mock(PartitionInfo.class);
        when(partInfo.getAllLevelPartitionStrategies()).thenReturn(Collections.emptyList());

        ColumnarTableMeta columnarTableMeta = mock(ColumnarTableMeta.class);
        when(columnarTableMeta.getPartitionInfo()).thenReturn(partInfo);
        when(columnarTableMeta.getOptions()).thenReturn(Collections.emptyMap());

        try (MockedStatic<ExecutorContext> mockedExecutorContext = mockStatic(ExecutorContext.class)) {
            mockedExecutorContext.when(() -> ExecutorContext.getContext(anyString()))
                .thenReturn(mock(ExecutorContext.class));

            try (MockedStatic<ColumnarManager> mockedColumnarManager = mockStatic(ColumnarManager.class)) {
                DynamicColumnarManager dynamicColumnarManager = mock(DynamicColumnarManager.class);
                when(dynamicColumnarManager.getColumnarTableMeta(anyLong(), anyString(), anyString())).thenReturn(
                    Collections.singletonList(columnarTableMeta));
                mockedColumnarManager.when(ColumnarManager::getInstance).thenReturn(dynamicColumnarManager);

                Cursor cursor = handler.handle(logicalPlan, executionContext);
                Assert.assertTrue(
                    cursor instanceof ArrayResultCursor && ((ArrayResultCursor) cursor).getRows().size() == 1);
            }
        }

    }

    @Test
    public void testShowColumnarIndex() {
        MyRepository repo = new MockMyRepository();
        ShowColumnarIndexHandler handler = new ShowColumnarIndexHandler(repo);
        ExecutionContext executionContext = spy(new ExecutionContext("db"));
        LogicalDal logicalPlan = mock(LogicalDal.class);
        SqlShowColumnarIndex showColumnarIndex = mock(SqlShowColumnarIndex.class);
        when(logicalPlan.getNativeSqlNode()).thenReturn(showColumnarIndex);

        SqlIdentifier tableNameNode = new SqlIdentifier("tb", SqlParserPos.ZERO);
        when(showColumnarIndex.getTable()).thenReturn(tableNameNode);

        ExecutorContext ec = mock(ExecutorContext.class);
        GsiManager gsiManager = mock(GsiManager.class);
        GsiMetaManager gsiMetaManager = mock(GsiMetaManager.class);
        when(ec.getGsiManager()).thenReturn(gsiManager);
        when(gsiManager.getGsiMetaManager()).thenReturn(gsiMetaManager);
        when(gsiMetaManager.getTableAndIndexMeta(anyString(), any())).thenReturn(gsiMetaBean);

        PartitionInfo partInfo = mock(PartitionInfo.class);
        when(partInfo.getAllLevelPartitionStrategies()).thenReturn(Collections.emptyList());

        ColumnarTableMeta columnarTableMeta = mock(ColumnarTableMeta.class);
        when(columnarTableMeta.getPartitionInfo()).thenReturn(partInfo);
        when(columnarTableMeta.getOptions()).thenReturn(Collections.emptyMap());

        try (MockedStatic<ExecutorContext> mockedExecutorContext = mockStatic(ExecutorContext.class)) {
            mockedExecutorContext.when(() -> ExecutorContext.getContext(anyString()))
                .thenReturn(ec);

            try (MockedStatic<OptimizerContext> mockedOptimizerContext = mockStatic(OptimizerContext.class)) {
                OptimizerContext optimizerContext = mock(OptimizerContext.class);
                mockedOptimizerContext.when(() -> OptimizerContext.getContext(anyString()))
                    .thenReturn(optimizerContext);
                SchemaManager schemaManager = mock(SchemaManager.class);
                TableMeta tableMeta = mock(TableMeta.class);
                when(schemaManager.getTableWithNull(anyString())).thenReturn(tableMeta);
                when(schemaManager.getTable(anyString())).thenReturn(tableMeta);
                when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);

                IndexMeta pk = mock(IndexMeta.class);
                ColumnMeta cm = mock(ColumnMeta.class);
                when(cm.getOriginColumnName()).thenReturn("id");
                when(pk.getKeyColumns()).thenReturn(Collections.singletonList(cm));
                when(tableMeta.getPrimaryIndex()).thenReturn(pk);

                indexMap.put(indexName, indexMetaBean);
                gsiTableMeta.put(tableName, tableMetaBean);

                Cursor cursor = handler.handle(logicalPlan, executionContext);
                Assert.assertTrue(
                    cursor instanceof ArrayResultCursor && ((ArrayResultCursor) cursor).getRows().size() == 1);
            }
        }

    }

    private static class MockMyRepository extends MyRepository {
        protected MyJdbcHandler createQueryHandler(ExecutionContext executionContext) {
            return mock(MyJdbcHandler.class);
        }
    }
}
