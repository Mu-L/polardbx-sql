package com.alibaba.polardbx.executor.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoRecord;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;

public class ExternalCatalogSyncActionTest {

    @Before
    public void setup() {
        ExternalCatalogManager.getInstance().invalidateAll();
    }

    @After
    public void teardown() {
        ExternalCatalogManager.getInstance().invalidateAll();
    }

    @Test
    public void testReloadDecryptsEncryptedProperties() {
        try (MockedStatic<MetaDbUtil> ignored = mockCatalogRecord(encryptedRecord("oss_cat"))) {
            new ExternalCatalogSyncAction("oss_cat", "ADD").sync();
        }

        ExternalCatalogInfo info = ExternalCatalogManager.getInstance().get("oss_cat");
        Assert.assertNotNull(info);
        Assert.assertEquals("ak_value", info.getProperties().get("access_key_id"));
        Assert.assertEquals("sk_value", info.getProperties().get("access_key_secret"));
    }

    @Test
    public void testReloadRejectsPlaintextProperties() {
        ExternalCatalogInfoRecord record = encryptedRecord("oss_cat");
        record.properties = plaintextProperties();

        try (MockedStatic<MetaDbUtil> ignored = mockCatalogRecord(record)) {
            try {
                new ExternalCatalogSyncAction("oss_cat", "ADD").sync();
                Assert.fail("Plaintext catalog properties must not be accepted during sync reload");
            } catch (RuntimeException expected) {
                Assert.assertTrue(expected.getMessage().contains("Failed to decrypt"));
            }
        }
    }

    @Test
    public void testReloadPropagatesGmsFailure() {
        try {
            new ExternalCatalogSyncAction("oss_cat", "ADD").sync();
            Assert.fail("sync() should throw when GMS connection is unavailable during reload");
        } catch (RuntimeException expected) {
            // expected: GMS read failure during reload must propagate, not be swallowed
        }
    }

    private MockedStatic<MetaDbUtil> mockCatalogRecord(ExternalCatalogInfoRecord record) {
        Connection conn = Mockito.mock(Connection.class);
        MockedStatic<MetaDbUtil> mocked = Mockito.mockStatic(MetaDbUtil.class);
        mocked.when(MetaDbUtil::getConnection).thenReturn(conn);
        mocked.when(() -> MetaDbUtil.query(
            Mockito.anyString(),
            Mockito.<Map<Integer, ParameterContext>>any(),
            eq(ExternalCatalogInfoRecord.class),
            eq(conn))).thenReturn(Collections.singletonList(record));
        return mocked;
    }

    private ExternalCatalogInfoRecord encryptedRecord(String name) {
        ExternalCatalogInfoRecord record = new ExternalCatalogInfoRecord();
        record.name = name;
        record.connector = "oss";
        record.properties = encryptedProperties();
        record.secretName = "oss_secret";
        record.comment = "comment";
        return record;
    }

    private byte[] encryptedProperties() {
        return ExternalCredentialEncryptor.encryptMap(properties());
    }

    private byte[] plaintextProperties() {
        return JSON.toJSONString(properties()).getBytes(StandardCharsets.UTF_8);
    }

    private Map<String, String> properties() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("connector", "oss");
        properties.put("access_key_id", "ak_value");
        properties.put("access_key_secret", "sk_value");
        return properties;
    }
}
