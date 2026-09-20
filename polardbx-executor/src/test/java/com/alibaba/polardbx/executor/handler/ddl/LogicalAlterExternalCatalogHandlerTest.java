package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.ddl.job.factory.AlterExternalCatalogJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalExternalCatalogDdl;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalAlterExternalCatalogHandlerTest {

    @Test
    public void testGetObjectName() {
        LogicalExternalCatalogDdl ddl = mock(LogicalExternalCatalogDdl.class);
        when(ddl.getCatalogName()).thenReturn("cat1");
        LogicalAlterExternalCatalogHandler handler =
            new LogicalAlterExternalCatalogHandler(mock(IRepository.class));
        assertEquals("cat1", handler.getObjectName(ddl));
    }

    @Test
    public void testBuildDdlJobWithProps() {
        Map<String, String> props = new HashMap<>();
        props.put("key", "value");
        LogicalExternalCatalogDdl ddl = mock(LogicalExternalCatalogDdl.class);
        when(ddl.getCatalogName()).thenReturn("cat1");
        when(ddl.getSecretName()).thenReturn("secret1");
        when(ddl.getProperties()).thenReturn(props);
        when(ddl.getComment()).thenReturn("comment");
        ExecutionContext ec = mock(ExecutionContext.class);

        try (MockedStatic<PolarPrivilegeUtils> ppuMock = mockStatic(PolarPrivilegeUtils.class);
            MockedConstruction<AlterExternalCatalogJobFactory> fMock =
                mockConstruction(AlterExternalCatalogJobFactory.class)) {
            LogicalAlterExternalCatalogHandler handler =
                new LogicalAlterExternalCatalogHandler(mock(IRepository.class));
            DdlJob job = handler.buildDdlJob(ddl, ec);
            assertEquals(1, fMock.constructed().size());
        }
    }

    @Test
    public void testBuildDdlJobEmptyProps() {
        LogicalExternalCatalogDdl ddl = mock(LogicalExternalCatalogDdl.class);
        when(ddl.getCatalogName()).thenReturn("cat1");
        when(ddl.getSecretName()).thenReturn(null);
        when(ddl.getProperties()).thenReturn(new HashMap<>());
        when(ddl.getComment()).thenReturn(null);
        ExecutionContext ec = mock(ExecutionContext.class);

        try (MockedStatic<PolarPrivilegeUtils> ppuMock = mockStatic(PolarPrivilegeUtils.class);
            MockedConstruction<AlterExternalCatalogJobFactory> fMock =
                mockConstruction(AlterExternalCatalogJobFactory.class)) {
            LogicalAlterExternalCatalogHandler handler =
                new LogicalAlterExternalCatalogHandler(mock(IRepository.class));
            DdlJob job = handler.buildDdlJob(ddl, ec);
            assertEquals(1, fMock.constructed().size());
        }
    }
}
