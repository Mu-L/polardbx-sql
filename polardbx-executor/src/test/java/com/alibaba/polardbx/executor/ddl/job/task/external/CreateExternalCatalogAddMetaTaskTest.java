package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoAccessor;
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

public class CreateExternalCatalogAddMetaTaskTest {

    @Test
    public void testExecuteImpl() {
        Connection mockConn = mock(Connection.class);
        ExecutionContext mockEc = mock(ExecutionContext.class);
        byte[] props = new byte[] {1, 2};

        CreateExternalCatalogAddMetaTask task =
            spy(new CreateExternalCatalogAddMetaTask("cat1", "oss", props, "secret1", "comment"));
        doNothing().when(task).updateSupportedCommands(anyBoolean(), anyBoolean(), any());

        try (MockedConstruction<ExternalCatalogInfoAccessor> mc = mockConstruction(ExternalCatalogInfoAccessor.class)) {
            task.executeImpl(mockConn, mockEc);
            assertEquals(1, mc.constructed().size());
            verify(mc.constructed().get(0)).insert("cat1", "oss", props, "secret1", "comment");
        }
    }

    @Test
    public void testGetters() {
        byte[] props = new byte[] {3, 4};
        CreateExternalCatalogAddMetaTask task =
            new CreateExternalCatalogAddMetaTask("cat1", "oss", props, "secret1", "comment");
        assertEquals("cat1", task.getCatalogName());
        assertEquals("oss", task.getConnector());
        assertArrayEquals(props, task.getEncryptedProperties());
        assertEquals("secret1", task.getSecretName());
        assertEquals("comment", task.getComment());
    }
}
