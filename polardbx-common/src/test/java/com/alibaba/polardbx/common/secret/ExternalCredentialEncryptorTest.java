package com.alibaba.polardbx.common.secret;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ExternalCredentialEncryptorTest {

    @Test
    public void testEncryptDecryptRoundTrip() {
        String plaintext = "hello world secret";
        String encrypted = ExternalCredentialEncryptor.encrypt(plaintext);
        String decrypted = ExternalCredentialEncryptor.decrypt(encrypted);
        assertEquals(plaintext, decrypted);
    }

    @Test
    public void testEncryptProducesDifferentCiphertext() {
        String plain = "same input";
        String enc1 = ExternalCredentialEncryptor.encrypt(plain);
        String enc2 = ExternalCredentialEncryptor.encrypt(plain);
        // IV is random, so ciphertext should differ
        assertNotEquals(enc1, enc2);
        // But both decrypt to same value
        assertEquals(plain, ExternalCredentialEncryptor.decrypt(enc1));
        assertEquals(plain, ExternalCredentialEncryptor.decrypt(enc2));
    }

    @Test
    public void testDifferentPlaintextDifferentCiphertext() {
        String enc1 = ExternalCredentialEncryptor.encrypt("aaa");
        String enc2 = ExternalCredentialEncryptor.encrypt("bbb");
        assertNotEquals(enc1, enc2);
    }

    @Test
    public void testEncryptDecryptMap() {
        Map<String, String> original = new HashMap<>();
        original.put("access_key", "AKID12345");
        original.put("secret_key", "mysecret");
        original.put("endpoint", "oss.aliyuncs.com");

        byte[] encrypted = ExternalCredentialEncryptor.encryptMap(original);
        Map<String, String> decrypted = ExternalCredentialEncryptor.decryptToMap(encrypted);

        assertEquals(original, decrypted);
    }

    @Test
    public void testEmptyString() {
        String encrypted = ExternalCredentialEncryptor.encrypt("");
        String decrypted = ExternalCredentialEncryptor.decrypt(encrypted);
        assertEquals("", decrypted);
    }

    @Test
    public void testUnicodeContent() {
        String plaintext = "密钥=秘密值123";
        String encrypted = ExternalCredentialEncryptor.encrypt(plaintext);
        String decrypted = ExternalCredentialEncryptor.decrypt(encrypted);
        assertEquals(plaintext, decrypted);
    }
}
