package com.alibaba.polardbx.optimizer.secret;

import com.alibaba.polardbx.common.secret.PropertyDefinition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class SecretTypeRegistryTest {

    @Before
    public void setUp() {
        SecretTypeRegistry.getInstance().clear();
    }

    @After
    public void tearDown() {
        SecretTypeRegistry.getInstance().clear();
    }

    @Test
    public void testRegisterAndGet() {
        PropertyDefinition jdbcDef = new PropertyDefinition(
            "jdbc",
            new HashSet<>(Arrays.asList("user", "password")),
            new HashSet<>(Arrays.asList("driver_class")),
            new HashSet<>(Arrays.asList("password"))
        );

        SecretTypeRegistry.getInstance().register(jdbcDef);

        PropertyDefinition result = SecretTypeRegistry.getInstance().get("jdbc");
        Assert.assertNotNull(result);
        Assert.assertEquals("jdbc", result.getType());
        Assert.assertTrue(result.getRequiredKeys().contains("user"));
    }

    @Test
    public void testGetCaseInsensitive() {
        PropertyDefinition def = new PropertyDefinition(
            "JDBC",
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet()
        );

        SecretTypeRegistry.getInstance().register(def);

        Assert.assertNotNull(SecretTypeRegistry.getInstance().get("jdbc"));
        Assert.assertNotNull(SecretTypeRegistry.getInstance().get("JDBC"));
        Assert.assertNotNull(SecretTypeRegistry.getInstance().get("Jdbc"));
    }

    @Test
    public void testGetUnregistered() {
        Assert.assertNull(SecretTypeRegistry.getInstance().get("unknown"));
        Assert.assertNull(SecretTypeRegistry.getInstance().get(null));
    }

    @Test
    public void testRegisteredTypes() {
        SecretTypeRegistry.getInstance().register(new PropertyDefinition(
            "jdbc", Collections.emptySet(), Collections.emptySet(), Collections.emptySet()));
        SecretTypeRegistry.getInstance().register(new PropertyDefinition(
            "oss", Collections.emptySet(), Collections.emptySet(), Collections.emptySet()));

        Assert.assertTrue(SecretTypeRegistry.getInstance().registeredTypes().contains("jdbc"));
        Assert.assertTrue(SecretTypeRegistry.getInstance().registeredTypes().contains("oss"));
        Assert.assertEquals(2, SecretTypeRegistry.getInstance().registeredTypes().size());
    }

    @Test
    public void testIsRegistered() {
        SecretTypeRegistry.getInstance().register(new PropertyDefinition(
            "jdbc", Collections.emptySet(), Collections.emptySet(), Collections.emptySet()));

        Assert.assertTrue(SecretTypeRegistry.getInstance().isRegistered("jdbc"));
        Assert.assertTrue(SecretTypeRegistry.getInstance().isRegistered("JDBC"));
        Assert.assertFalse(SecretTypeRegistry.getInstance().isRegistered("oss"));
        Assert.assertFalse(SecretTypeRegistry.getInstance().isRegistered(null));
    }

    @Test
    public void testRegisterNull() {
        // Should not throw
        SecretTypeRegistry.getInstance().register(null);
        Assert.assertTrue(SecretTypeRegistry.getInstance().registeredTypes().isEmpty());
    }

    @Test
    public void testUnregister() {
        SecretTypeRegistry.getInstance().register(new PropertyDefinition(
            "jdbc", Collections.emptySet(), Collections.emptySet(), Collections.emptySet()));

        Assert.assertTrue(SecretTypeRegistry.getInstance().isRegistered("jdbc"));
        SecretTypeRegistry.getInstance().unregister("jdbc");
        Assert.assertFalse(SecretTypeRegistry.getInstance().isRegistered("jdbc"));
    }
}
