package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.optimizer.secret.SecretTypeRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.fail;

/**
 * Migrated from SecretValidationHelperTest: validates PropertyDefinition.validateAsSecret()
 * behavior which replaced the deleted SecretValidationHelper.
 */
public class PropertyDefinitionValidationTest {

    @Before
    public void setup() {
        SecretTypeRegistry.getInstance().clear();
        Set<String> required = new HashSet<>(Arrays.asList("access_key_id", "access_key_secret"));
        Set<String> optional = new HashSet<>(Arrays.asList("endpoint", "region"));
        Set<String> sensitive = new HashSet<>(Arrays.asList("access_key_secret"));
        SecretTypeRegistry.getInstance().register(
            new PropertyDefinition("oss", required, optional, sensitive));
    }

    @After
    public void teardown() {
        SecretTypeRegistry.getInstance().clear();
    }

    @Test
    public void testValidatePropertiesSuccess() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "oss");
        props.put("access_key_id", "AKID123");
        props.put("access_key_secret", "secret");
        props.put("endpoint", "oss-cn-hangzhou.aliyuncs.com");

        // Should not throw
        PropertyDefinition def = SecretTypeRegistry.getInstance().get("oss");
        def.validateAsSecret(props);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testValidatePropertiesMissingRequired() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "oss");
        props.put("access_key_id", "AKID123");
        // missing access_key_secret

        PropertyDefinition def = SecretTypeRegistry.getInstance().get("oss");
        def.validateAsSecret(props);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testValidatePropertiesUnknownKey() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "oss");
        props.put("access_key_id", "AKID123");
        props.put("access_key_secret", "secret");
        props.put("totally_unknown_key", "value");

        PropertyDefinition def = SecretTypeRegistry.getInstance().get("oss");
        def.validateAsSecret(props);
    }

    @Test
    public void testValidatePropertiesAllowUnknownKeys() {
        SecretTypeRegistry.getInstance().register(
            new PropertyDefinition("flexible", new HashSet<>(), new HashSet<>(),
                new HashSet<>(), true));

        Map<String, String> props = new HashMap<>();
        props.put("type", "flexible");
        props.put("any_key", "any_value");

        // Should not throw — allowUnknownKeys=true
        PropertyDefinition def = SecretTypeRegistry.getInstance().get("flexible");
        def.validateAsSecret(props);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testScopeRejectedAsUnknownKey() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "oss");
        props.put("scope", "oss://bucket/");
        props.put("access_key_id", "AKID123");
        props.put("access_key_secret", "secret");

        // scope is no longer a system key, should be rejected as unknown
        PropertyDefinition def = SecretTypeRegistry.getInstance().get("oss");
        def.validateAsSecret(props);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testProviderRejectedAsUnknownKey() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "oss");
        props.put("provider", "config");
        props.put("access_key_id", "AKID123");
        props.put("access_key_secret", "secret");

        // provider is no longer a system key, should be rejected as unknown
        PropertyDefinition def = SecretTypeRegistry.getInstance().get("oss");
        def.validateAsSecret(props);
    }

    @Test
    public void testTypeStillAllowedAsSystemKey() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "oss");
        props.put("access_key_id", "AKID123");
        props.put("access_key_secret", "secret");

        // type is still a system key, should not be rejected
        PropertyDefinition def = SecretTypeRegistry.getInstance().get("oss");
        def.validateAsSecret(props);
    }

    @Test
    public void testValidateAsCatalogSystemKeysAllowed() {
        PropertyDefinition catalogDef = new PropertyDefinition("jdbc",
            new HashSet<>(Arrays.asList("user", "password")),
            new HashSet<>(Arrays.asList("jdbc_properties")),
            new HashSet<>(), false);

        Map<String, String> props = new HashMap<>();
        props.put("type", "jdbc");
        props.put("connector", "jdbc");
        props.put("secret", "my_secret");
        props.put("jdbc_properties", "useSSL=false");

        // system keys (type, connector, secret) + optionalKeys should be allowed
        catalogDef.validateAsCatalog(props);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testValidateAsCatalogUnknownKeyRejected() {
        PropertyDefinition catalogDef = new PropertyDefinition("jdbc",
            new HashSet<>(Arrays.asList("user", "password")),
            new HashSet<>(Arrays.asList("jdbc_properties")),
            new HashSet<>(), false);

        Map<String, String> props = new HashMap<>();
        props.put("type", "jdbc");
        props.put("connector", "jdbc");
        props.put("secret", "my_secret");
        props.put("warehouze", "typo");

        catalogDef.validateAsCatalog(props);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testValidateAsCatalogRejectsRequiredKeys() {
        PropertyDefinition def = new PropertyDefinition("jdbc",
            new HashSet<>(Arrays.asList("user", "password", "jdbc_url")),
            new HashSet<>(Arrays.asList("jdbc_properties")),
            new HashSet<>(), false);

        Map<String, String> props = new HashMap<>();
        props.put("type", "jdbc");
        props.put("connector", "jdbc");
        props.put("secret", "my_secret");
        props.put("user", "admin");  // requiredKey should be rejected in catalog context

        def.validateAsCatalog(props);
    }
}
