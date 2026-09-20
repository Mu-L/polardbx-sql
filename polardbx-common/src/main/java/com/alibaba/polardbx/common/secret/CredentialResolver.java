package com.alibaba.polardbx.common.secret;

import java.util.Map;

public final class CredentialResolver {

    public interface SecretResolver {
        SecretBundle resolve(String secretName, Map<String, String> props);
    }

    private static volatile SecretResolver resolver = (name, props) -> SecretBundle.EMPTY;

    private CredentialResolver() {
    }

    public static void setResolver(SecretResolver r) {
        resolver = r;
    }

    public static String resolveOrDefault(String secretName, Map<String, String> props,
                                          String secretKey, String propsKey) {
        if (secretName != null && !secretName.isEmpty()) {
            SecretBundle bundle = resolver.resolve(secretName, props);
            String val = bundle.get(secretKey);
            if (val != null) {
                return val;
            }
        }
        return props.get(propsKey);
    }

    /**
     * Resolves the full {@link SecretBundle} for the given secret name.
     * Returns {@link SecretBundle#EMPTY} if the name is null or empty.
     */
    public static SecretBundle resolveBundle(String secretName, Map<String, String> props) {
        if (secretName != null && !secretName.isEmpty()) {
            return resolver.resolve(secretName, props);
        }
        return SecretBundle.EMPTY;
    }
}
