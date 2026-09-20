package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ExternalSchemaManager;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SecretSyncActionTest {

    private static final String SECRET_NAME = "oss_key";
    private static final String CATALOG_NAME = "oss_cat";
    private static final String SCHEMA_NAME = "oss_cat$$bucket";
    private static final String OTHER_SECRET_NAME = "other_key";
    private static final String OTHER_CATALOG_NAME = "other_cat";
    private static final String OTHER_SCHEMA_NAME = "other_cat$$bucket";

    @Before
    public void setup() {
        cleanup();
    }

    @After
    public void teardown() {
        cleanup();
    }

    private void cleanup() {
        SecretManager.getInstance().invalidateAll();
        ExternalCatalogManager.getInstance().invalidateAll();
        OptimizerContext.clearContext(SCHEMA_NAME);
        OptimizerContext.clearContext(OTHER_SCHEMA_NAME);
    }

    private byte[] encryptKv(Map<String, String> kv) {
        return ExternalCredentialEncryptor.encryptMap(kv);
    }

    @Test
    public void testRemoveInvalidatesCatalogSchema() {
        assertInvalidatesCatalogSchema("REMOVE");
    }

    @Test
    public void testUpdateInvalidatesCatalogSchema() {
        try (MockedStatic<MetaDbUtil> ignored = mockGmsSecret(SECRET_NAME)) {
            assertInvalidatesCatalogSchema("UPDATE");
        }
    }

    @Test
    public void testAddInvalidatesCatalogSchema() {
        try (MockedStatic<MetaDbUtil> ignored = mockGmsSecret(SECRET_NAME)) {
            assertInvalidatesCatalogSchema("ADD");
        }
    }

    @Test
    public void testUnrelatedSecretDoesNotInvalidateSchema() {
        registerSecret(SECRET_NAME);
        registerCatalog(CATALOG_NAME, SECRET_NAME);
        loadSchemaContext(SCHEMA_NAME);

        registerSecret(OTHER_SECRET_NAME);
        registerCatalog(OTHER_CATALOG_NAME, OTHER_SECRET_NAME);
        loadSchemaContext(OTHER_SCHEMA_NAME);

        assertTrue(contextLoaded(SCHEMA_NAME));
        assertTrue(contextLoaded(OTHER_SCHEMA_NAME));

        new SecretSyncAction(OTHER_SECRET_NAME, "REMOVE").sync();

        assertFalse(contextLoaded(OTHER_SCHEMA_NAME));
        assertTrue(contextLoaded(SCHEMA_NAME));
    }

    @Test
    public void testReloadPropagatesGmsFailure() {
        try {
            new SecretSyncAction(SECRET_NAME, "ADD").sync();
            fail("sync() should throw when GMS connection is unavailable during reload");
        } catch (RuntimeException expected) {
            // expected: GMS read failure during reload must propagate, not be swallowed
        }
    }

    private MockedStatic<MetaDbUtil> mockGmsSecret(String name) {
        Connection conn = Mockito.mock(Connection.class);
        ExternalSecretRecord record = new ExternalSecretRecord();
        record.name = name;
        record.type = "oss";
        Map<String, String> kv = new HashMap<>();
        kv.put("k", "v");
        record.encryptedKv = encryptKv(kv);

        MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
        mockedMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(conn);
        mockedMetaDbUtil.when(() -> MetaDbUtil.query(
            Mockito.anyString(),
            Mockito.<Map<Integer, ParameterContext>>any(),
            Mockito.eq(ExternalSecretRecord.class),
            Mockito.eq(conn))).thenReturn(Collections.singletonList(record));
        return mockedMetaDbUtil;
    }

    private void assertInvalidatesCatalogSchema(String action) {
        registerSecret(SECRET_NAME);
        registerCatalog(CATALOG_NAME, SECRET_NAME);
        loadSchemaContext(SCHEMA_NAME);

        assertTrue(contextLoaded(SCHEMA_NAME));

        new SecretSyncAction(SECRET_NAME, action).sync();

        assertFalse(contextLoaded(SCHEMA_NAME));
    }

    private void registerSecret(String name) {
        Map<String, String> kv = new HashMap<>();
        kv.put("k", "v");
        SecretManager.getInstance().register(name, "oss", encryptKv(kv));
    }

    private void registerCatalog(String catalogName, String secretName) {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo(catalogName, "mock", new HashMap<>(), secretName, ""));
    }

    private void loadSchemaContext(String schemaName) {
        OptimizerContext ctx = new OptimizerContext(schemaName);
        ctx.setSchemaManager(Mockito.mock(ExternalSchemaManager.class));
        ctx.setFinishInit(true);
        OptimizerContext.loadExternalContext(ctx);
    }

    @SuppressWarnings("unchecked")
    private boolean contextLoaded(String schemaName) {
        try {
            Field field = OptimizerContext.class.getDeclaredField("externalContextMap");
            field.setAccessible(true);
            Map<String, OptimizerContext> map = (Map<String, OptimizerContext>) field.get(null);
            return map.containsKey(schemaName.toLowerCase());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
