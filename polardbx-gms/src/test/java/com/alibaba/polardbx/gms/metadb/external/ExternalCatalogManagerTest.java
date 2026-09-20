package com.alibaba.polardbx.gms.metadb.external;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;

public class ExternalCatalogManagerTest {

    @Before
    public void setup() {
        ExternalCatalogManager.getInstance().invalidateAll();
    }

    @After
    public void teardown() {
        ExternalCatalogManager.getInstance().invalidateAll();
    }

    @Test
    public void testRegisterAndGet() {
        ExternalCatalogInfo info = new ExternalCatalogInfo("hive", "hive", new HashMap<>(), null, null);
        ExternalCatalogManager.getInstance().register(info);

        ExternalCatalogInfo got = ExternalCatalogManager.getInstance().get("hive");
        assertNotNull(got);
        assertEquals("hive", got.getName());
        assertEquals("hive", got.getConnector());
    }

    @Test
    public void testGetCaseInsensitive() {
        ExternalCatalogInfo info = new ExternalCatalogInfo("MyCatalog", "jdbc", new HashMap<>(), "s1", "comment");
        ExternalCatalogManager.getInstance().register(info);

        assertNotNull(ExternalCatalogManager.getInstance().get("mycatalog"));
        assertNotNull(ExternalCatalogManager.getInstance().get("MYCATALOG"));
        assertNotNull(ExternalCatalogManager.getInstance().get("MyCatalog"));
    }

    @Test
    public void testGetNonExistent() {
        assertNull(ExternalCatalogManager.getInstance().get("nonexist"));
    }

    @Test
    public void testRemove() {
        ExternalCatalogInfo info = new ExternalCatalogInfo("toremove", "oss", new HashMap<>(), null, null);
        ExternalCatalogManager.getInstance().register(info);
        assertTrue(ExternalCatalogManager.getInstance().exists("toremove"));

        ExternalCatalogManager.getInstance().remove("toremove");
        assertFalse(ExternalCatalogManager.getInstance().exists("toremove"));
    }

    @Test
    public void testListAll() {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("c1", "hive", new HashMap<>(), null, null));
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("c2", "jdbc", new HashMap<>(), null, null));

        Collection<ExternalCatalogInfo> all = ExternalCatalogManager.getInstance().listAll();
        assertEquals(2, all.size());
    }

    @Test
    public void testExists() {
        assertFalse(ExternalCatalogManager.getInstance().exists("x"));
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("x", "oss", new HashMap<>(), null, null));
        assertTrue(ExternalCatalogManager.getInstance().exists("x"));
        assertTrue(ExternalCatalogManager.getInstance().exists("X"));
    }

    @Test
    public void testReplaceWithEntriesRemovesStaleEntries() {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("stale_cat", "jdbc", new HashMap<>(), null, null));
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("active_cat", "oss", new HashMap<>(), null, null));

        assertTrue(ExternalCatalogManager.getInstance().exists("stale_cat"));
        assertTrue(ExternalCatalogManager.getInstance().exists("active_cat"));

        List<ExternalCatalogInfo> newInfos = new ArrayList<>();
        newInfos.add(new ExternalCatalogInfo("active_cat", "oss_v2", new HashMap<>(), "s1", "updated"));
        ExternalCatalogManager.getInstance().replaceWithEntries(newInfos);

        assertFalse(ExternalCatalogManager.getInstance().exists("stale_cat"));
        ExternalCatalogInfo active = ExternalCatalogManager.getInstance().get("active_cat");
        assertNotNull(active);
        assertEquals("oss_v2", active.getConnector());
    }

    @Test
    public void testReplaceWithEmptyRemovesAll() {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("c1", "jdbc", new HashMap<>(), null, null));
        assertTrue(ExternalCatalogManager.getInstance().exists("c1"));

        ExternalCatalogManager.getInstance().replaceWithEntries(Collections.emptyList());

        assertFalse(ExternalCatalogManager.getInstance().exists("c1"));
        assertTrue(ExternalCatalogManager.getInstance().isEmpty());
    }

    @Test
    public void testReplaceSwapsSnapshotAtomically() {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("old_cat", "jdbc", new HashMap<>(), null, null));
        Collection<ExternalCatalogInfo> snapshotBeforeReplace = ExternalCatalogManager.getInstance().listAll();

        List<ExternalCatalogInfo> newInfos = new ArrayList<>();
        newInfos.add(new ExternalCatalogInfo("new_cat", "oss", new HashMap<>(), null, null));
        ExternalCatalogManager.getInstance().replaceWithEntries(newInfos);

        // A reader holding the pre-replace snapshot keeps seeing the old consistent view,
        // never a half-replaced state.
        assertEquals(1, snapshotBeforeReplace.size());
        assertEquals("old_cat", snapshotBeforeReplace.iterator().next().getName());
        assertFalse(ExternalCatalogManager.getInstance().exists("old_cat"));
        assertTrue(ExternalCatalogManager.getInstance().exists("new_cat"));
    }

    @Test
    public void testLoadFromGmsDecryptsEncryptedProperties() {
        ExternalCatalogInfoRecord record = encryptedRecord("oss_cat");
        Connection conn = Mockito.mock(Connection.class);

        try (MockedStatic<MetaDbUtil> mocked = Mockito.mockStatic(MetaDbUtil.class)) {
            mocked.when(MetaDbUtil::getConnection).thenReturn(conn);
            mocked.when(() -> MetaDbUtil.query(
                Mockito.anyString(),
                Mockito.<Map<Integer, ParameterContext>>any(),
                eq(ExternalCatalogInfoRecord.class),
                eq(conn))).thenReturn(Collections.singletonList(record));

            ExternalCatalogManager.getInstance().loadFromGms();
        }

        ExternalCatalogInfo info = ExternalCatalogManager.getInstance().get("oss_cat");
        assertNotNull(info);
        assertEquals("ak_value", info.getProperties().get("access_key_id"));
        assertEquals("sk_value", info.getProperties().get("access_key_secret"));
    }

    @Test
    public void testLoadFromGmsSkipsCorruptRecordAndKeepsRest() {
        ExternalCatalogInfoRecord good = encryptedRecord("good_cat");
        ExternalCatalogInfoRecord corrupt = encryptedRecord("corrupt_cat");
        corrupt.properties = new byte[] {1, 2, 3};
        Connection conn = Mockito.mock(Connection.class);

        try (MockedStatic<MetaDbUtil> mocked = Mockito.mockStatic(MetaDbUtil.class)) {
            mocked.when(MetaDbUtil::getConnection).thenReturn(conn);
            mocked.when(() -> MetaDbUtil.query(
                Mockito.anyString(),
                Mockito.<Map<Integer, ParameterContext>>any(),
                eq(ExternalCatalogInfoRecord.class),
                eq(conn))).thenReturn(Arrays.asList(corrupt, good));

            ExternalCatalogManager.getInstance().loadFromGms();
        }

        assertNotNull(ExternalCatalogManager.getInstance().get("good_cat"));
        assertNull(ExternalCatalogManager.getInstance().get("corrupt_cat"));
    }

    private ExternalCatalogInfoRecord encryptedRecord(String name) {
        ExternalCatalogInfoRecord record = new ExternalCatalogInfoRecord();
        record.name = name;
        record.connector = "oss";
        record.properties = properties();
        record.secretName = "oss_secret";
        record.comment = "comment";
        return record;
    }

    private byte[] properties() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("connector", "oss");
        properties.put("access_key_id", "ak_value");
        properties.put("access_key_secret", "sk_value");
        return ExternalCredentialEncryptor.encryptMap(properties);
    }
}
