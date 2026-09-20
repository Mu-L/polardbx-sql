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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mockConstruction;

public class AlterSecretUpdateMetaTaskTest {

    @Test
    public void testExecuteImpl() {
        Connection mockConn = mock(Connection.class);
        ExecutionContext mockEc = mock(ExecutionContext.class);
        byte[] kv = new byte[] {1, 2, 3};

        AlterSecretUpdateMetaTask task = spy(new AlterSecretUpdateMetaTask("secret1", kv));
        doNothing().when(task).updateSupportedCommands(anyBoolean(), anyBoolean(), any());

        try (MockedConstruction<ExternalSecretAccessor> mc = mockConstruction(ExternalSecretAccessor.class)) {
            task.executeImpl(mockConn, mockEc);
            assertEquals(1, mc.constructed().size());
            verify(mc.constructed().get(0)).updateEncryptedKv("secret1", kv);
        }
    }

    @Test
    public void testGetters() {
        byte[] kv = new byte[] {4, 5, 6};
        AlterSecretUpdateMetaTask task = new AlterSecretUpdateMetaTask("secret1", kv);
        assertEquals("secret1", task.getSecretName());
        assertArrayEquals(kv, task.getEncryptedKv());
    }
}
