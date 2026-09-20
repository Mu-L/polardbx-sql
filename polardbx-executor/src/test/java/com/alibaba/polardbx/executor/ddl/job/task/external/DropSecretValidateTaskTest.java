package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoRecord;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DropSecretValidateTaskTest {

    @Test
    public void testExecuteImplSuccess() {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalSecretAccessor> saMock = mockConstruction(ExternalSecretAccessor.class,
                (m, ctx) -> when(m.selectByName(anyString())).thenReturn(mock(ExternalSecretRecord.class)));
            MockedConstruction<ExternalCatalogInfoAccessor> caMock = mockConstruction(ExternalCatalogInfoAccessor.class,
                (m, ctx) -> when(m.selectBySecretName(anyString())).thenReturn(Collections.emptyList()))) {
            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            DropSecretValidateTask task = new DropSecretValidateTask("secret1", false);
            task.executeImpl(mockEc);
        }
    }

    @Test
    public void testExecuteImplSecretNotFoundThrows() {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalSecretAccessor> saMock = mockConstruction(ExternalSecretAccessor.class,
                (m, ctx) -> when(m.selectByName(anyString())).thenReturn(null));
            MockedConstruction<ExternalCatalogInfoAccessor> caMock =
                mockConstruction(ExternalCatalogInfoAccessor.class)) {
            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            DropSecretValidateTask task = new DropSecretValidateTask("secret1", false);
            task.executeImpl(mockEc);
            fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testExecuteImplSecretNotFoundIfExists() {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalSecretAccessor> saMock = mockConstruction(ExternalSecretAccessor.class,
                (m, ctx) -> when(m.selectByName(anyString())).thenReturn(null));
            MockedConstruction<ExternalCatalogInfoAccessor> caMock =
                mockConstruction(ExternalCatalogInfoAccessor.class)) {
            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            DropSecretValidateTask task = new DropSecretValidateTask("secret1", true);
            task.executeImpl(mockEc);
        }
    }

    @Test
    public void testExecuteImplHasCatalogRefs() {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);

        ExternalCatalogInfoRecord refRecord = mock(ExternalCatalogInfoRecord.class);
        when(refRecord.getName()).thenReturn("cat1");

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalSecretAccessor> saMock = mockConstruction(ExternalSecretAccessor.class,
                (m, ctx) -> when(m.selectByName(anyString())).thenReturn(mock(ExternalSecretRecord.class)));
            MockedConstruction<ExternalCatalogInfoAccessor> caMock = mockConstruction(ExternalCatalogInfoAccessor.class,
                (m, ctx) -> when(m.selectBySecretName(anyString())).thenReturn(Collections.singletonList(refRecord)))) {
            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            DropSecretValidateTask task = new DropSecretValidateTask("secret1", false);
            task.executeImpl(mockEc);
            fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("referenced by catalog"));
        }
    }

    @Test
    public void testGetters() {
        DropSecretValidateTask task = new DropSecretValidateTask("secret1", true);
        assertEquals("secret1", task.getSecretName());
        assertTrue(task.isIfExists());
    }
}
