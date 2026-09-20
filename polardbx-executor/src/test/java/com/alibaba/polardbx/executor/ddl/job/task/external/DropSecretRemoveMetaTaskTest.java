package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.mockito.MockedConstruction;

import java.sql.Connection;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class DropSecretRemoveMetaTaskTest {

    @Test
    public void testExecuteImpl() {
        Connection mockConn = mock(Connection.class);
        ExecutionContext mockEc = mock(ExecutionContext.class);

        DropSecretRemoveMetaTask task = spy(new DropSecretRemoveMetaTask("secret1"));
        doNothing().when(task).updateSupportedCommands(anyBoolean(), anyBoolean(), any());

        try (MockedConstruction<ExternalSecretAccessor> mc = mockConstruction(ExternalSecretAccessor.class)) {
            task.executeImpl(mockConn, mockEc);
            assertEquals(1, mc.constructed().size());
            verify(mc.constructed().get(0)).deleteByName("secret1");
        }
    }

    @Test
    public void testGetter() {
        DropSecretRemoveMetaTask task = new DropSecretRemoveMetaTask("secret1");
        assertEquals("secret1", task.getSecretName());
    }
}
