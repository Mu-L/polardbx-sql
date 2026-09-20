package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorTable;
import org.apache.calcite.sql.SqlDescribeExternalTable;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalDescribeExternalTableHandlerTest {

    @Test
    public void testHandleSuccess() {
        ExternalCatalogInfo info = mock(ExternalCatalogInfo.class);
        when(info.getConnector()).thenReturn("oss");
        when(info.getSecretName()).thenReturn(null);
        when(info.getProperties()).thenReturn(Collections.emptyMap());

        ColumnMeta mockCol = mock(ColumnMeta.class);
        when(mockCol.getName()).thenReturn("col1");
        DataType mockDataType = mock(DataType.class);
        when(mockDataType.getStringSqlType()).thenReturn("VARCHAR");
        when(mockCol.getDataType()).thenReturn(mockDataType);
        when(mockCol.isNullable()).thenReturn(true);
        Field mockField = mock(Field.class);
        when(mockField.getDefault()).thenReturn("default_val");
        when(mockCol.getField()).thenReturn(mockField);

        ConnectorTable table = new ConnectorTable(
            Collections.singletonList(mockCol), Collections.emptyList());

        ConnectorMetadata mockMetadata = mock(ConnectorMetadata.class);
        when(mockMetadata.getTable("db1", "table1")).thenReturn(Optional.of(table));

        try (MockedStatic<ExternalCatalogManager> mgrMock = mockStatic(ExternalCatalogManager.class);
            MockedStatic<ConnectorRegistry> crMock = mockStatic(ConnectorRegistry.class)) {
            ExternalCatalogManager mockMgr = mock(ExternalCatalogManager.class);
            mgrMock.when(ExternalCatalogManager::getInstance).thenReturn(mockMgr);
            when(mockMgr.get("cat1")).thenReturn(info);

            ConnectorRegistry mockRegistry = mock(ConnectorRegistry.class);
            crMock.when(ConnectorRegistry::getInstance).thenReturn(mockRegistry);
            ConnectorDescriptor mockDesc = mock(ConnectorDescriptor.class);
            when(mockRegistry.get("oss")).thenReturn(mockDesc);
            when(mockDesc.createMetadata(any(), any())).thenReturn(mockMetadata);

            LogicalShow plan = mock(LogicalShow.class);
            SqlDescribeExternalTable descNode = mock(SqlDescribeExternalTable.class);
            when(plan.getNativeSqlNode()).thenReturn(descNode);
            when(descNode.getCatalogName()).thenReturn("cat1");
            when(descNode.getExternalDbName()).thenReturn("DB1");
            when(descNode.getExternalTableName()).thenReturn("TABLE1");

            ExecutionContext ec = mock(ExecutionContext.class);
            IRepository repo = mock(IRepository.class);
            LogicalDescribeExternalTableHandler handler =
                new LogicalDescribeExternalTableHandler(repo);
            Cursor cursor = handler.handle(plan, ec);
            assertNotNull(cursor);
        }
    }

    @Test
    public void testHandleTableNotFound() {
        ExternalCatalogInfo info = mock(ExternalCatalogInfo.class);
        when(info.getConnector()).thenReturn("oss");
        when(info.getSecretName()).thenReturn(null);
        when(info.getProperties()).thenReturn(Collections.emptyMap());

        ConnectorMetadata mockMetadata = mock(ConnectorMetadata.class);
        when(mockMetadata.getTable("db1", "table1")).thenReturn(Optional.empty());

        try (MockedStatic<ExternalCatalogManager> mgrMock = mockStatic(ExternalCatalogManager.class);
            MockedStatic<ConnectorRegistry> crMock = mockStatic(ConnectorRegistry.class)) {
            ExternalCatalogManager mockMgr = mock(ExternalCatalogManager.class);
            mgrMock.when(ExternalCatalogManager::getInstance).thenReturn(mockMgr);
            when(mockMgr.get("cat1")).thenReturn(info);

            ConnectorRegistry mockRegistry = mock(ConnectorRegistry.class);
            crMock.when(ConnectorRegistry::getInstance).thenReturn(mockRegistry);
            ConnectorDescriptor mockDesc = mock(ConnectorDescriptor.class);
            when(mockRegistry.get("oss")).thenReturn(mockDesc);
            when(mockDesc.createMetadata(any(), any())).thenReturn(mockMetadata);

            LogicalShow plan = mock(LogicalShow.class);
            SqlDescribeExternalTable descNode = mock(SqlDescribeExternalTable.class);
            when(plan.getNativeSqlNode()).thenReturn(descNode);
            when(descNode.getCatalogName()).thenReturn("cat1");
            when(descNode.getExternalDbName()).thenReturn("DB1");
            when(descNode.getExternalTableName()).thenReturn("TABLE1");

            ExecutionContext ec = mock(ExecutionContext.class);
            IRepository repo = mock(IRepository.class);
            LogicalDescribeExternalTableHandler handler =
                new LogicalDescribeExternalTableHandler(repo);
            handler.handle(plan, ec);
            fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
    }
}
