package com.alibaba.polardbx.gms.metadb.external;

import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ExternalCatalogInfoRecordTest {

    @Test
    public void testFill() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        byte[] props = new byte[] {1, 2, 3};
        when(rs.getString("name")).thenReturn("cat1");
        when(rs.getString("connector")).thenReturn("oss");
        when(rs.getBytes("properties")).thenReturn(props);
        when(rs.getString("secret_name")).thenReturn("secret1");
        when(rs.getString("comment")).thenReturn("comment");

        ExternalCatalogInfoRecord record = new ExternalCatalogInfoRecord();
        ExternalCatalogInfoRecord result = record.fill(rs);

        assertEquals(record, result);
        assertEquals("cat1", record.name);
        assertEquals("oss", record.connector);
        assertArrayEquals(props, record.properties);
        assertEquals("secret1", record.secretName);
        assertEquals("comment", record.comment);
    }

    @Test
    public void testGetters() {
        ExternalCatalogInfoRecord record = new ExternalCatalogInfoRecord();
        record.name = "cat1";
        record.connector = "oss";
        record.properties = new byte[] {1, 2};
        record.secretName = "secret1";
        record.comment = "comment";

        assertEquals("cat1", record.getName());
        assertEquals("oss", record.getConnector());
        assertArrayEquals(new byte[] {1, 2}, record.getProperties());
        assertEquals("secret1", record.getSecretName());
        assertEquals("comment", record.getComment());
    }

    @Test
    public void testToInfo() {
        ExternalCatalogInfoRecord record = new ExternalCatalogInfoRecord();
        record.name = "cat1";
        record.connector = "oss";
        record.properties = new byte[] {1, 2, 3};
        record.secretName = "secret1";
        record.comment = "comment";

        Map<String, String> props = new HashMap<>();
        props.put("key", "value");

        try (MockedStatic<ExternalCredentialEncryptor> eceMock = mockStatic(ExternalCredentialEncryptor.class)) {
            eceMock.when(() -> ExternalCredentialEncryptor.decryptToMap(new byte[] {1, 2, 3}))
                .thenReturn(props);

            ExternalCatalogInfo info = record.toInfo();
            assertEquals("cat1", info.getName());
            assertEquals("oss", info.getConnector());
            assertEquals(props, info.getProperties());
            assertEquals("secret1", info.getSecretName());
            assertEquals("comment", info.getComment());
        }
    }
}
