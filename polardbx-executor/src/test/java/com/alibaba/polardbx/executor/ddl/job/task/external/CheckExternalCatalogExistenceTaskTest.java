package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.sql.Connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class CheckExternalCatalogExistenceTaskTest {

    @Test
    public void testExecuteImplCatalogExists() {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalCatalogInfoAccessor> mc = mockConstruction(ExternalCatalogInfoAccessor.class,
                (mockAccessor, ctx) -> {
                    when(mockAccessor.selectByName(anyString())).thenReturn(mock(ExternalCatalogInfoRecord.class));
                })) {
            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            CheckExternalCatalogExistenceTask task = new CheckExternalCatalogExistenceTask("cat1", false);
            task.executeImpl(mockEc);
        }
    }

    @Test
    public void testExecuteImplCatalogNotFoundThrows() {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalCatalogInfoAccessor> mc = mockConstruction(ExternalCatalogInfoAccessor.class,
                (mockAccessor, ctx) -> {
                    when(mockAccessor.selectByName(anyString())).thenReturn(null);
                })) {
            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            CheckExternalCatalogExistenceTask task = new CheckExternalCatalogExistenceTask("cat1", false);
            task.executeImpl(mockEc);
            fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testExecuteImplCatalogNotFoundIfExists() {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalCatalogInfoAccessor> mc = mockConstruction(ExternalCatalogInfoAccessor.class,
                (mockAccessor, ctx) -> {
                    when(mockAccessor.selectByName(anyString())).thenReturn(null);
                })) {
            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            CheckExternalCatalogExistenceTask task = new CheckExternalCatalogExistenceTask("cat1", true);
            task.executeImpl(mockEc);
        }
    }

    @Test
    public void testGetters() {
        CheckExternalCatalogExistenceTask task = new CheckExternalCatalogExistenceTask("cat1", true);
        assertEquals("cat1", task.getCatalogName());
        assertTrue(task.isIfExists());
    }
}
