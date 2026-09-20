package com.alibaba.polardbx.common.secret;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class CredentialUtil {

    private static final Set<String> SENSITIVE_KEYS = new HashSet<>();

    static {
        SENSITIVE_KEYS.add("access_key");
        SENSITIVE_KEYS.add("secret_key");
        SENSITIVE_KEYS.add("access_key_id");
        SENSITIVE_KEYS.add("access_key_secret");
        SENSITIVE_KEYS.add("password");
        SENSITIVE_KEYS.add("secret");
        SENSITIVE_KEYS.add("private_key");
        SENSITIVE_KEYS.add("sas_token");
        SENSITIVE_KEYS.add("shared_key");
        SENSITIVE_KEYS.add("bearer_token");
        SENSITIVE_KEYS.add("oauth2_client_secret");
    }

    private CredentialUtil() {
    }

    public static String mask(String value) {
        if (value == null) {
            return null;
        }
        return "******";
    }

    public static Map<String, String> maskAll(Map<String, String> props) {
        if (props == null) {
            return null;
        }
        Map<String, String> result = new LinkedHashMap<>(props.size());
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (isSensitive(entry.getKey())) {
                result.put(entry.getKey(), mask(entry.getValue()));
            } else {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public static boolean isSensitive(String key) {
        if (key == null) {
            return false;
        }
        return SENSITIVE_KEYS.contains(key.toLowerCase());
    }
}
