package com.alibaba.polardbx.common.secret;

import java.util.Collections;
import java.util.Map;

public class SecretBundle {
    public static final SecretBundle EMPTY = new SecretBundle(Collections.emptyMap());

    private final Map<String, String> decryptedKv;

    public SecretBundle(Map<String, String> decryptedKv) {
        this.decryptedKv = Collections.unmodifiableMap(decryptedKv);
    }

    public String get(String key) {
        return decryptedKv.get(key);
    }

    public Map<String, String> getAll() {
        return decryptedKv;
    }

    public boolean isEmpty() {
        return decryptedKv.isEmpty();
    }
}
