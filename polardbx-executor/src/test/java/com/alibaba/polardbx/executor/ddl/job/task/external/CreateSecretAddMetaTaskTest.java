package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.mockito.MockedConstruction;

import java.sql.Connection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class CreateSecretAddMetaTaskTest {

    @Test
    public void testExecuteImpl() {
        Connection mockConn = mock(Connection.class);
        ExecutionContext mockEc = mock(ExecutionContext.class);
        byte[] kv = new byte[] {1, 2, 3};

        CreateSecretAddMetaTask task = spy(new CreateSecretAddMetaTask("secret1", "type1", kv));
        doNothing().when(task).updateSupportedCommands(anyBoolean(), anyBoolean(), any());

        try (MockedConstruction<ExternalSecretAccessor> mc = mockConstruction(ExternalSecretAccessor.class)) {
            task.executeImpl(mockConn, mockEc);
            assertEquals(1, mc.constructed().size());
            ExternalSecretAccessor accessor = mc.constructed().get(0);
            verify(accessor).setConnection(mockConn);
            verify(accessor).insert("secret1", "type1", kv);
        }
    }

    @Test
    public void testGetters() {
        byte[] kv = new byte[] {4, 5, 6};
        CreateSecretAddMetaTask task = new CreateSecretAddMetaTask("secret1", "type1", kv);
        assertEquals("secret1", task.getSecretName());
        assertEquals("type1", task.getType());
        assertArrayEquals(kv, task.getEncryptedKv());
    }
}
