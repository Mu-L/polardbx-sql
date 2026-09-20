package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.sql.SqlShow;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalShowExternalCatalogsHandlerTest {

    @Test
    public void testHandle() {
        ExternalCatalogManager mockMgr = mock(ExternalCatalogManager.class);
        ExternalCatalogInfo info = mock(ExternalCatalogInfo.class);
        when(info.getName()).thenReturn("cat1");
        when(info.getConnector()).thenReturn("oss");
        when(info.getSecretName()).thenReturn("secret1");
        when(info.getComment()).thenReturn("comment");

        try (MockedStatic<ExternalCatalogManager> mgrMock = mockStatic(ExternalCatalogManager.class)) {
            mgrMock.when(ExternalCatalogManager::getInstance).thenReturn(mockMgr);
            when(mockMgr.listAll()).thenReturn(Collections.singletonList(info));

            LogicalShow plan = mock(LogicalShow.class);
            SqlShow showNode = mock(SqlShow.class);
            when(plan.getNativeSqlNode()).thenReturn(showNode);

            ExecutionContext ec = mock(ExecutionContext.class);
            IRepository repo = mock(IRepository.class);
            LogicalShowExternalCatalogsHandler handler = new LogicalShowExternalCatalogsHandler(repo);
            Cursor cursor = handler.handle(plan, ec);
            assertNotNull(cursor);
        }
    }
}
