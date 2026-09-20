package com.alibaba.polardbx.common.secret;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.fastjson.JSON;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;

public final class ExternalCredentialEncryptor {

    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 128;

    /**
     * Important Notice: The value of defaultKey can not be changed!!
     * It is used by AES-GCM for encrypt & decrypt of external secret credentials.
     * Overwrite by setting env EXTERNAL_SECRET_KEY.
     */
    private static final String defaultKey = "855a70external1f";
    private static final String ENV_KEY_NAME = "EXTERNAL_SECRET_KEY";

    private ExternalCredentialEncryptor() {
    }

    private static byte[] getKey() {
        String keyStr = defaultKey;
        // Overwrite key with environment variable
        if (System.getenv(ENV_KEY_NAME) != null) {
            keyStr = System.getenv(ENV_KEY_NAME);
        }
        byte[] raw = keyStr.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[32];
        System.arraycopy(raw, 0, key, 0, Math.min(raw.length, 32));
        return key;
    }

    static String encrypt(String plaintext) {
        try {
            byte[] iv = new byte[GCM_IV_LENGTH];
            new SecureRandom().nextBytes(iv);

            SecretKey secretKey = new SecretKeySpec(getKey(), ALGORITHM);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_LENGTH, iv));

            byte[] cipherText = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

            ByteBuffer buffer = ByteBuffer.allocate(iv.length + cipherText.length);
            buffer.put(iv);
            buffer.put(cipherText);

            return Base64.getEncoder().encodeToString(buffer.array());
        } catch (Exception e) {
            throw GeneralUtil.nestedException("Failed to encrypt credential", e);
        }
    }

    static String decrypt(String encryptedBase64) {
        try {
            byte[] decoded = Base64.getDecoder().decode(encryptedBase64);

            ByteBuffer buffer = ByteBuffer.wrap(decoded);
            byte[] iv = new byte[GCM_IV_LENGTH];
            buffer.get(iv);
            byte[] cipherText = new byte[buffer.remaining()];
            buffer.get(cipherText);

            SecretKey secretKey = new SecretKeySpec(getKey(), ALGORITHM);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_LENGTH, iv));

            byte[] plainText = cipher.doFinal(cipherText);
            return new String(plainText, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw GeneralUtil.nestedException("Failed to decrypt credential", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> decryptToMap(byte[] encrypted) {
        if (encrypted == null || encrypted.length <= GCM_IV_LENGTH) {
            throw GeneralUtil.nestedException("Encrypted credential is null or empty");
        }
        String base64 = Base64.getEncoder().encodeToString(encrypted);
        String json = decrypt(base64);
        try {
            return JSON.parseObject(json, Map.class);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(
                "Failed to parse decrypted credential as JSON: " + e.getClass().getSimpleName());
        }
    }

    public static byte[] encryptMap(Map<String, String> kv) {
        if (kv == null) {
            throw GeneralUtil.nestedException("Credential properties are null");
        }
        String json = JSON.toJSONString(kv);
        String base64 = encrypt(json);
        return Base64.getDecoder().decode(base64);
    }
}
