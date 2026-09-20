package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import org.apache.calcite.sql.SqlShowConnectors;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalShowConnectorsHandlerTest {

    @Test
    public void testHandleSimple() {
        ConnectorRegistry mockRegistry = mock(ConnectorRegistry.class);
        when(mockRegistry.visibleTypes()).thenReturn(new HashSet<>(Arrays.asList("oss", "odps")));

        try (MockedStatic<ConnectorRegistry> crMock = mockStatic(ConnectorRegistry.class)) {
            crMock.when(ConnectorRegistry::getInstance).thenReturn(mockRegistry);

            BaseDalOperation plan = mock(BaseDalOperation.class);
            SqlShowConnectors showNode = mock(SqlShowConnectors.class);
            when(plan.getNativeSqlNode()).thenReturn(showNode);
            when(showNode.isFull()).thenReturn(false);

            ExecutionContext ec = mock(ExecutionContext.class);
            IRepository repo = mock(IRepository.class);
            LogicalShowConnectorsHandler handler = new LogicalShowConnectorsHandler(repo);
            Cursor cursor = handler.handle(plan, ec);
            assertNotNull(cursor);
        }
    }

    @Test
    public void testHandleFull() {
        ConnectorRegistry mockRegistry = mock(ConnectorRegistry.class);
        ConnectorDescriptor mockDesc = mock(ConnectorDescriptor.class);
        when(mockRegistry.visibleTypes()).thenReturn(new HashSet<>(Arrays.asList("oss", "unknown_connector")));

        PropertyDefinition mockDef = mock(PropertyDefinition.class);
        when(mockDef.getType()).thenReturn("oss_secret");
        when(mockDef.getRequiredKeys()).thenReturn(new HashSet<>(Arrays.asList("access_key_id")));
        when(mockDef.getOptionalKeys()).thenReturn(new HashSet<>(Arrays.asList("token")));
        when(mockDef.getSensitiveKeys()).thenReturn(new HashSet<>(Arrays.asList("access_key_secret")));
        when(mockDef.isAllowUnknownKeys()).thenReturn(true);
        when(mockDesc.secretDefinitions()).thenReturn(Collections.singletonList(mockDef));

        try (MockedStatic<ConnectorRegistry> crMock = mockStatic(ConnectorRegistry.class)) {
            crMock.when(ConnectorRegistry::getInstance).thenReturn(mockRegistry);
            when(mockRegistry.getOrNull("oss")).thenReturn(mockDesc);
            when(mockRegistry.getOrNull("unknown_connector")).thenReturn(null);

            BaseDalOperation plan = mock(BaseDalOperation.class);
            SqlShowConnectors showNode = mock(SqlShowConnectors.class);
            when(plan.getNativeSqlNode()).thenReturn(showNode);
            when(showNode.isFull()).thenReturn(true);

            ExecutionContext ec = mock(ExecutionContext.class);
            IRepository repo = mock(IRepository.class);
            LogicalShowConnectorsHandler handler = new LogicalShowConnectorsHandler(repo);
            Cursor cursor = handler.handle(plan, ec);
            assertNotNull(cursor);
        }
    }
}
