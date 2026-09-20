package com.alibaba.polardbx.optimizer.secret;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.fastjson.JSON;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SecretManagerTest {

    @Before
    public void setup() {
        SecretManager.getInstance().invalidateAll();
    }

    @After
    public void teardown() {
        SecretManager.getInstance().invalidateAll();
    }

    private byte[] encryptKv(Map<String, String> kv) {
        return ExternalCredentialEncryptor.encryptMap(kv);
    }

    @Test
    public void testRegisterAndResolve() {
        Map<String, String> kv = new HashMap<>();
        kv.put("access_key", "AKID123");
        kv.put("secret_key", "mysecret");

        SecretManager.getInstance().register("oss_prod", "oss", encryptKv(kv));

        SecretBundle bundle = SecretManager.getInstance().resolve("oss_prod", null);
        assertNotNull(bundle);
        assertEquals("AKID123", bundle.get("access_key"));
        assertEquals("mysecret", bundle.get("secret_key"));
    }

    @Test
    public void testResolveCaseInsensitive() {
        Map<String, String> kv = new HashMap<>();
        kv.put("key", "value");
        SecretManager.getInstance().register("My_Secret", "oss", encryptKv(kv));

        SecretBundle b1 = SecretManager.getInstance().resolve("my_secret", null);
        SecretBundle b2 = SecretManager.getInstance().resolve("MY_SECRET", null);
        assertEquals("value", b1.get("key"));
        assertEquals("value", b2.get("key"));
    }

    @Test(expected = TddlRuntimeException.class)
    public void testResolveNonExistent() {
        SecretManager.getInstance().resolve("nonexist", null);
    }

    @Test
    public void testResolveEmptyName() {
        SecretBundle bundle = SecretManager.getInstance().resolve("", null);
        assertSame(SecretBundle.EMPTY, bundle);
    }

    @Test
    public void testResolveNullName() {
        SecretBundle bundle = SecretManager.getInstance().resolve(null, null);
        assertSame(SecretBundle.EMPTY, bundle);
    }

    @Test
    public void testRemove() {
        Map<String, String> kv = new HashMap<>();
        kv.put("k", "v");
        SecretManager.getInstance().register("to_remove", "s3", encryptKv(kv));
        assertTrue(SecretManager.getInstance().exists("to_remove"));

        SecretManager.getInstance().remove("to_remove");
        assertFalse(SecretManager.getInstance().exists("to_remove"));
    }

    @Test
    public void testList() {
        Map<String, String> kv = new HashMap<>();
        kv.put("k", "v");
        SecretManager.getInstance().register("sec1", "oss", encryptKv(kv));
        SecretManager.getInstance().register("sec2", "s3", encryptKv(kv));

        List<SecretManager.SecretInfo> list = SecretManager.getInstance().list();
        assertEquals(2, list.size());
    }

    @Test
    public void testVersionIncrementsOnRegister() {
        Map<String, String> kv = new HashMap<>();
        kv.put("k", "v");
        SecretManager.getInstance().register("ver_test", "oss", encryptKv(kv));
        long v1 = SecretManager.getInstance().getGeneration("ver_test");
        assertTrue(v1 > 0);

        // Re-register (UPDATE) should increment version
        SecretManager.getInstance().register("ver_test", "oss", encryptKv(kv));
        long v2 = SecretManager.getInstance().getGeneration("ver_test");
        assertTrue(v2 > v1);
    }

    @Test
    public void testVersionIsolatedPerEntry() {
        Map<String, String> kv = new HashMap<>();
        kv.put("k", "v");
        SecretManager.getInstance().register("secret_a", "oss", encryptKv(kv));
        long vA = SecretManager.getInstance().getGeneration("secret_a");

        // Registering a different secret should NOT affect secret_a's version
        SecretManager.getInstance().register("secret_b", "s3", encryptKv(kv));
        long vA2 = SecretManager.getInstance().getGeneration("secret_a");
        assertEquals(vA, vA2);
    }

    @Test
    public void testVersionNeverReusedAfterDropAndRecreate() {
        Map<String, String> kv = new HashMap<>();
        kv.put("k", "v");
        SecretManager.getInstance().register("aba_test", "oss", encryptKv(kv));
        long v1 = SecretManager.getInstance().getGeneration("aba_test");

        SecretManager.getInstance().remove("aba_test");
        assertEquals(-1, SecretManager.getInstance().getGeneration("aba_test"));

        // Re-create: version must be greater than v1, never equal (ABA protection)
        SecretManager.getInstance().register("aba_test", "oss", encryptKv(kv));
        long v2 = SecretManager.getInstance().getGeneration("aba_test");
        assertTrue("Version after drop+recreate must be > original", v2 > v1);
    }

    @Test
    public void testGetGenerationByName() {
        Map<String, String> kv = new HashMap<>();
        kv.put("k", "v");
        SecretManager.getInstance().register("named_gen", "oss", encryptKv(kv));
        long gen = SecretManager.getInstance().getGeneration("named_gen");
        assertTrue(gen > 0);
        assertEquals(-1, SecretManager.getInstance().getGeneration("nonexist"));
        assertEquals(-1, SecretManager.getInstance().getGeneration(null));
        assertEquals(-1, SecretManager.getInstance().getGeneration(""));
    }

    @Test
    public void testGetInfoByName() {
        Map<String, String> kv = new HashMap<>();
        kv.put("access_key", "AK123");
        SecretManager.getInstance().register("info_test", "oss", encryptKv(kv));

        SecretManager.SecretInfo info = SecretManager.getInstance().getInfo("info_test");
        assertNotNull(info);
        assertEquals("info_test", info.name);
        assertEquals("oss", info.type);

        assertNull(SecretManager.getInstance().getInfo("nonexist"));
    }
}
