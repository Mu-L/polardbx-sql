package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import org.apache.calcite.sql.SqlShowDatabasesFromCatalog;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalShowDatabasesFromCatalogHandlerTest {

    @Test
    public void testHandleSuccess() {
        ExternalCatalogInfo info = mock(ExternalCatalogInfo.class);
        when(info.getConnector()).thenReturn("oss");
        when(info.getSecretName()).thenReturn(null);
        when(info.getProperties()).thenReturn(Collections.emptyMap());

        ConnectorMetadata mockMetadata = mock(ConnectorMetadata.class);
        when(mockMetadata.listDatabases()).thenReturn(Arrays.asList("db1", "db2"));

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
            SqlShowDatabasesFromCatalog showNode = mock(SqlShowDatabasesFromCatalog.class);
            when(plan.getNativeSqlNode()).thenReturn(showNode);
            when(showNode.getCatalogName()).thenReturn("cat1");

            ExecutionContext ec = mock(ExecutionContext.class);
            IRepository repo = mock(IRepository.class);
            LogicalShowDatabasesFromCatalogHandler handler =
                new LogicalShowDatabasesFromCatalogHandler(repo);
            Cursor cursor = handler.handle(plan, ec);
            assertNotNull(cursor);
        }
    }

    @Test
    public void testHandleCatalogNotFound() {
        try (MockedStatic<ExternalCatalogManager> mgrMock = mockStatic(ExternalCatalogManager.class)) {
            ExternalCatalogManager mockMgr = mock(ExternalCatalogManager.class);
            mgrMock.when(ExternalCatalogManager::getInstance).thenReturn(mockMgr);
            when(mockMgr.get("cat1")).thenReturn(null);

            LogicalShow plan = mock(LogicalShow.class);
            SqlShowDatabasesFromCatalog showNode = mock(SqlShowDatabasesFromCatalog.class);
            when(plan.getNativeSqlNode()).thenReturn(showNode);
            when(showNode.getCatalogName()).thenReturn("cat1");

            ExecutionContext ec = mock(ExecutionContext.class);
            IRepository repo = mock(IRepository.class);
            LogicalShowDatabasesFromCatalogHandler handler =
                new LogicalShowDatabasesFromCatalogHandler(repo);
            handler.handle(plan, ec);
            fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
    }
}
