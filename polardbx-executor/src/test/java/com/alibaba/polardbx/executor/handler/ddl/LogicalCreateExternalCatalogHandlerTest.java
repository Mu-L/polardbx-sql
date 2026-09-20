package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateExternalCatalogJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalExternalCatalogDdl;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalCreateExternalCatalogHandlerTest {

    private LogicalExternalCatalogDdl mockDdl(String catalogName, boolean ifNotExists) {
        LogicalExternalCatalogDdl ddl = mock(LogicalExternalCatalogDdl.class);
        when(ddl.getCatalogName()).thenReturn(catalogName);
        when(ddl.isIfNotExists()).thenReturn(ifNotExists);
        when(ddl.getConnector()).thenReturn("oss");
        when(ddl.getProperties()).thenReturn(new HashMap<>());
        when(ddl.getSecretName()).thenReturn("secret1");
        when(ddl.getComment()).thenReturn("comment");
        return ddl;
    }

    @Test
    public void testGetObjectName() {
        LogicalExternalCatalogDdl ddl = mockDdl("cat1", false);
        LogicalCreateExternalCatalogHandler handler =
            new LogicalCreateExternalCatalogHandler(mock(IRepository.class));
        assertEquals("cat1", handler.getObjectName(ddl));
    }

    @Test
    public void testHandleIfNotExistsAlreadyExists() {
        LogicalExternalCatalogDdl ddl = mockDdl("cat1", true);
        ExecutionContext ec = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);
        ExternalCatalogInfoRecord mockRecord = mock(ExternalCatalogInfoRecord.class);

        try (MockedStatic<PolarPrivilegeUtils> ppuMock = mockStatic(PolarPrivilegeUtils.class);
            MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalCatalogInfoAccessor> aMock =
                mockConstruction(ExternalCatalogInfoAccessor.class,
                    (m, ctx) -> when(m.selectByName("cat1")).thenReturn(mockRecord))) {
            mdbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            LogicalCreateExternalCatalogHandler handler =
                new LogicalCreateExternalCatalogHandler(mock(IRepository.class));
            Cursor cursor = handler.handle((RelNode) ddl, ec);
            assertTrue(cursor instanceof AffectRowCursor);
        }
    }

    @Test
    public void testBuildDdlJob() {
        Map<String, String> props = new HashMap<>();
        props.put("key", "value");
        LogicalExternalCatalogDdl ddl = mock(LogicalExternalCatalogDdl.class);
        when(ddl.getCatalogName()).thenReturn("cat1");
        when(ddl.getConnector()).thenReturn("oss");
        when(ddl.getProperties()).thenReturn(props);
        when(ddl.getSecretName()).thenReturn("secret1");
        when(ddl.getComment()).thenReturn("comment");
        ExecutionContext ec = mock(ExecutionContext.class);

        try (MockedConstruction<CreateExternalCatalogJobFactory> fMock =
            mockConstruction(CreateExternalCatalogJobFactory.class)) {
            LogicalCreateExternalCatalogHandler handler =
                new LogicalCreateExternalCatalogHandler(mock(IRepository.class));
            DdlJob job = handler.buildDdlJob(ddl, ec);
            assertEquals(1, fMock.constructed().size());
        }
    }
}
