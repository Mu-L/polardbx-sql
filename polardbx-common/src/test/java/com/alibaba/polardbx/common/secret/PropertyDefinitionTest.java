package com.alibaba.polardbx.common.secret;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PropertyDefinitionTest {

    @Test
    public void testRequiredAndOptionalKeys() {
        PropertyDefinition def = new PropertyDefinition(
            "jdbc",
            new HashSet<>(Arrays.asList("user", "password")),
            new HashSet<>(Arrays.asList("driver_class", "jdbc_url")),
            new HashSet<>(Arrays.asList("password"))
        );

        Assert.assertEquals("jdbc", def.getType());
        Assert.assertTrue(def.getRequiredKeys().contains("user"));
        Assert.assertTrue(def.getRequiredKeys().contains("password"));
        Assert.assertTrue(def.getOptionalKeys().contains("driver_class"));
        Assert.assertTrue(def.getOptionalKeys().contains("jdbc_url"));
    }

    @Test
    public void testIsKnownKey() {
        PropertyDefinition def = new PropertyDefinition(
            "jdbc",
            new HashSet<>(Arrays.asList("user", "password")),
            new HashSet<>(Arrays.asList("driver_class")),
            Collections.emptySet()
        );

        // Required keys are known
        Assert.assertTrue(def.isKnownKey("user"));
        Assert.assertTrue(def.isKnownKey("password"));
        // Optional keys are known
        Assert.assertTrue(def.isKnownKey("driver_class"));
        // System key 'type' is always known (for secret validation)
        Assert.assertTrue(def.isKnownKey("type"));
        // Catalog system keys are known when using CATALOG_SYSTEM_KEYS
        Assert.assertTrue(def.isKnownKey("connector", PropertyDefinition.CATALOG_SYSTEM_KEYS));
        Assert.assertTrue(def.isKnownKey("secret", PropertyDefinition.CATALOG_SYSTEM_KEYS));
        // Unknown key
        Assert.assertFalse(def.isKnownKey("foobar"));
        Assert.assertFalse(def.isKnownKey("typo_password"));
    }

    @Test
    public void testIsSensitive() {
        PropertyDefinition def = new PropertyDefinition(
            "oss",
            new HashSet<>(Arrays.asList("access_key", "secret_key")),
            Collections.emptySet(),
            new HashSet<>(Arrays.asList("access_key", "secret_key"))
        );

        Assert.assertTrue(def.isSensitive("access_key"));
        Assert.assertTrue(def.isSensitive("secret_key"));
        Assert.assertTrue(def.isSensitive("ACCESS_KEY")); // case insensitive
        Assert.assertFalse(def.isSensitive("endpoint"));
        Assert.assertFalse(def.isSensitive(null));
    }

    @Test
    public void testCaseInsensitiveKeys() {
        PropertyDefinition def = new PropertyDefinition(
            "JDBC",
            new HashSet<>(Arrays.asList("User", "PASSWORD")),
            Collections.emptySet(),
            new HashSet<>(Arrays.asList("Password"))
        );

        // Type is lowercased
        Assert.assertEquals("jdbc", def.getType());
        // Keys are lowercased
        Assert.assertTrue(def.getRequiredKeys().contains("user"));
        Assert.assertTrue(def.getRequiredKeys().contains("password"));
        // Sensitive check is case insensitive
        Assert.assertTrue(def.isSensitive("password"));
        Assert.assertTrue(def.isSensitive("PASSWORD"));
    }

    @Test
    public void testAllowUnknownKeys() {
        PropertyDefinition strict = new PropertyDefinition(
            "jdbc",
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            false
        );
        Assert.assertFalse(strict.isAllowUnknownKeys());

        PropertyDefinition relaxed = new PropertyDefinition(
            "mock",
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            true
        );
        Assert.assertTrue(relaxed.isAllowUnknownKeys());
    }

    @Test
    public void testNullKey() {
        PropertyDefinition def = new PropertyDefinition(
            "test",
            Collections.singleton("key1"),
            Collections.emptySet(),
            Collections.singleton("key1")
        );

        Assert.assertFalse(def.isKnownKey(null));
        Assert.assertFalse(def.isSensitive(null));
    }

    // ========================================================================
    // validate() / validateAsSecret() / validateAsCatalog()
    // ========================================================================

    @Test
    public void testValidateAsSecretMissingRequired() {
        PropertyDefinition def = new PropertyDefinition(
            "jdbc",
            new HashSet<>(Arrays.asList("user", "password")),
            Collections.emptySet(),
            new HashSet<>(Arrays.asList("password"))
        );

        Map<String, String> props = new HashMap<>();
        props.put("type", "jdbc");
        props.put("user", "root");
        // missing 'password'

        try {
            def.validateAsSecret(props);
            Assert.fail("Should throw TddlRuntimeException for missing required key");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Missing required property 'password'"));
        }
    }

    @Test
    public void testValidateAsSecretUnknownKey() {
        PropertyDefinition def = new PropertyDefinition(
            "jdbc",
            new HashSet<>(Arrays.asList("user", "password")),
            new HashSet<>(Arrays.asList("driver_class")),
            new HashSet<>(Arrays.asList("password"))
        );

        Map<String, String> props = new HashMap<>();
        props.put("type", "jdbc");
        props.put("user", "root");
        props.put("password", "secret");
        props.put("typo_key", "value");

        try {
            def.validateAsSecret(props);
            Assert.fail("Should throw TddlRuntimeException for unknown key");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Unknown property 'typo_key'"));
        }
    }

    @Test
    public void testValidateAsSecretSuccess() {
        PropertyDefinition def = new PropertyDefinition(
            "jdbc",
            new HashSet<>(Arrays.asList("user", "password")),
            new HashSet<>(Arrays.asList("driver_class")),
            new HashSet<>(Arrays.asList("password"))
        );

        Map<String, String> props = new HashMap<>();
        props.put("type", "jdbc");
        props.put("user", "root");
        props.put("password", "secret");
        props.put("driver_class", "com.mysql.cj.jdbc.Driver");

        // Should not throw
        def.validateAsSecret(props);
    }

    @Test
    public void testValidateAsCatalogRejectsRequiredKeys() {
        PropertyDefinition def = new PropertyDefinition(
            "jdbc",
            new HashSet<>(Arrays.asList("user", "password", "jdbc_url")),
            new HashSet<>(Arrays.asList("driver_class", "jdbc_properties", "jdbc_dialect")),
            new HashSet<>(Arrays.asList("password"))
        );

        Map<String, String> props = new HashMap<>();
        props.put("connector", "jdbc");
        props.put("secret", "my_secret");
        props.put("jdbc_dialect", "mysql");
        // 'user' is a required key (credential) — should be rejected at catalog level
        props.put("user", "root");

        try {
            def.validateAsCatalog(props);
            Assert.fail("Should reject required key 'user' at catalog level");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Unknown property 'user'"));
        }
    }

    @Test
    public void testValidateAsCatalogSuccess() {
        PropertyDefinition def = new PropertyDefinition(
            "jdbc",
            new HashSet<>(Arrays.asList("user", "password", "jdbc_url")),
            new HashSet<>(Arrays.asList("driver_class", "jdbc_properties", "jdbc_dialect")),
            new HashSet<>(Arrays.asList("password"))
        );

        Map<String, String> props = new HashMap<>();
        props.put("connector", "jdbc");
        props.put("secret", "my_secret");
        props.put("jdbc_dialect", "mysql");

        // Should not throw — only optional keys and system keys
        def.validateAsCatalog(props);
    }

    @Test
    public void testValidateAsCatalogAllowUnknownKeys() {
        PropertyDefinition def = new PropertyDefinition(
            "mock",
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            true // relaxed mode
        );

        Map<String, String> props = new HashMap<>();
        props.put("connector", "mock");
        props.put("secret", "my_secret");
        props.put("any_key", "any_value");

        // Should not throw — allowUnknownKeys=true
        def.validateAsCatalog(props);
    }
}
