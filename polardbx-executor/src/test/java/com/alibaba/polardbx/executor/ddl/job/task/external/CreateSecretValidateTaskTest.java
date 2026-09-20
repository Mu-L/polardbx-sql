package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.sql.Connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class CreateSecretValidateTaskTest {

    @Test
    public void testExecuteImplSecretNotFound() {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalSecretAccessor> mc = mockConstruction(ExternalSecretAccessor.class,
                (mockAccessor, ctx) -> {
                    when(mockAccessor.selectByName(anyString())).thenReturn(null);
                })) {
            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            CreateSecretValidateTask task = new CreateSecretValidateTask("secret1");
            task.executeImpl(mockEc);
        }
    }

    @Test
    public void testExecuteImplSecretAlreadyExists() {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalSecretAccessor> mc = mockConstruction(ExternalSecretAccessor.class,
                (mockAccessor, ctx) -> {
                    when(mockAccessor.selectByName(anyString())).thenReturn(mock(ExternalSecretRecord.class));
                })) {
            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            CreateSecretValidateTask task = new CreateSecretValidateTask("secret1");
            task.executeImpl(mockEc);
            fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            // expected
        }
    }

    @Test
    public void testGetter() {
        CreateSecretValidateTask task = new CreateSecretValidateTask("secret1");
        assertEquals("secret1", task.getSecretName());
    }
}
