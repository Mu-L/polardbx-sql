package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ExternalCatalogMetaTaskEncryptionTest {

    @Test
    public void testCreateTaskStoresEncryptedProperties() throws Exception {
        AtomicReference<byte[]> storedPropertiesByte = new AtomicReference<>();
        Connection conn = Mockito.mock(Connection.class);
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        Mockito.when(conn.prepareStatement(Mockito.anyString())).thenReturn(ps);
        Mockito.when(ps.executeUpdate()).thenReturn(1);
        Mockito.doAnswer(invocation -> {
            storedPropertiesByte.set(invocation.getArgument(1));
            return null;
        }).when(ps).setBytes(Mockito.anyInt(), Mockito.any());

        CreateExternalCatalogAddMetaTask task = new CreateExternalCatalogAddMetaTask(
            "oss_cat", "oss", propertiesByte(), "oss_secret", "comment");
        // The DDL engine assigns the job id before a task runs; updateSupportedCommands
        // in executeImpl unboxes it, so the test must provide one too.
        task.setJobId(1L);
        task.executeImpl(conn, new ExecutionContext());

        assertEncryptedProperties(storedPropertiesByte.get());
    }

    private void assertEncryptedProperties(byte[] storedPropertiesByte) {
        Assert.assertNotNull(storedPropertiesByte);
        Map<String, String> storedProperties = ExternalCredentialEncryptor.decryptToMap(storedPropertiesByte);
        Assert.assertFalse(storedProperties.containsKey("ak_value"));
        Assert.assertEquals("ak_value", storedProperties.get("access_key_id"));
    }

    private byte[] propertiesByte() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("connector", "oss");
        properties.put("access_key_id", "ak_value");
        properties.put("access_key_secret", "sk_value");
        return ExternalCredentialEncryptor.encryptMap(properties);
    }
}
