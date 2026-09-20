package com.alibaba.polardbx.common.secret;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CredentialUtilTest {

    @Test
    public void testMaskStandard() {
        assertEquals("******", CredentialUtil.mask("AKIAxxxxxxxxxx"));
    }

    @Test
    public void testMaskShortValue() {
        assertEquals("******", CredentialUtil.mask("AB"));
        assertEquals("******", CredentialUtil.mask("ABCD"));
    }

    @Test
    public void testMaskExactly5() {
        assertEquals("******", CredentialUtil.mask("12345"));
    }

    @Test
    public void testMaskNull() {
        assertNull(CredentialUtil.mask(null));
    }

    @Test
    public void testMaskEmpty() {
        assertEquals("******", CredentialUtil.mask(""));
    }

    @Test
    public void testIsSensitiveTrue() {
        assertTrue(CredentialUtil.isSensitive("access_key"));
        assertTrue(CredentialUtil.isSensitive("secret_key"));
        assertTrue(CredentialUtil.isSensitive("password"));
        assertTrue(CredentialUtil.isSensitive("private_key"));
        assertTrue(CredentialUtil.isSensitive("bearer_token"));
    }

    @Test
    public void testIsSensitiveCaseInsensitive() {
        assertTrue(CredentialUtil.isSensitive("Access_Key"));
        assertTrue(CredentialUtil.isSensitive("PASSWORD"));
        assertTrue(CredentialUtil.isSensitive("Secret_Key"));
    }

    @Test
    public void testIsSensitiveFalse() {
        assertFalse(CredentialUtil.isSensitive("endpoint"));
        assertFalse(CredentialUtil.isSensitive("bucket"));
        assertFalse(CredentialUtil.isSensitive("region"));
        assertFalse(CredentialUtil.isSensitive(null));
    }

    @Test
    public void testMaskAllOnlySensitiveKeys() {
        Map<String, String> props = new HashMap<>();
        props.put("endpoint", "oss-cn-hangzhou.aliyuncs.com");
        props.put("access_key", "AKID1234567890abcdef");
        props.put("secret_key", "mysecretkey12345");
        props.put("bucket", "my-bucket");

        Map<String, String> masked = CredentialUtil.maskAll(props);

        assertEquals("oss-cn-hangzhou.aliyuncs.com", masked.get("endpoint"));
        assertEquals("my-bucket", masked.get("bucket"));
        assertEquals("******", masked.get("access_key"));
        assertEquals("******", masked.get("secret_key"));
    }

    @Test
    public void testMaskAllPreservesOrder() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("connector", "oss");
        props.put("access_key", "AKID1234567890abcdef");
        props.put("endpoint", "oss-cn-hangzhou.aliyuncs.com");
        props.put("bucket", "my-bucket");

        assertEquals(new ArrayList<>(props.keySet()),
            new ArrayList<>(CredentialUtil.maskAll(props).keySet()));
    }

    @Test
    public void testMaskAllNull() {
        assertNull(CredentialUtil.maskAll(null));
    }

    @Test
    public void testMaskAllEmpty() {
        Map<String, String> result = CredentialUtil.maskAll(new HashMap<>());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
}
