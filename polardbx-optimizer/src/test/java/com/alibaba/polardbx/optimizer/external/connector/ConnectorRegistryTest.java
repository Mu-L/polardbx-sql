package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.optimizer.secret.SecretTypeRegistry;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class ConnectorRegistryTest {

    @Test
    public void testSingleton() {
        ConnectorRegistry r1 = ConnectorRegistry.getInstance();
        ConnectorRegistry r2 = ConnectorRegistry.getInstance();
        assertSame(r1, r2);
    }

    @Test
    public void testGetUnknownTypeThrowsWithMessage() {
        try {
            ConnectorRegistry.getInstance().get("unknown_connector_xyz");
            fail("Should throw");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Unknown connector"));
        }
    }

    @Test
    public void testRegisterAndGet() {
        ConnectorDescriptor mock = new InMemoryConnectorDescriptor("test_type");
        ConnectorRegistry.getInstance().register(mock);
        try {
            ConnectorDescriptor got = ConnectorRegistry.getInstance().get("test_type");
            assertSame(mock, got);
        } finally {
            ConnectorRegistry.getInstance().unregister("test_type");
        }
    }

    @Test
    public void testReRegisterOverrides() {
        ConnectorDescriptor mock1 = new InMemoryConnectorDescriptor("re_reg_type");
        ConnectorDescriptor mock2 = new InMemoryConnectorDescriptor("re_reg_type");
        ConnectorRegistry.getInstance().register(mock1);
        ConnectorRegistry.getInstance().register(mock2);
        try {
            assertSame(mock2, ConnectorRegistry.getInstance().get("re_reg_type"));
        } finally {
            ConnectorRegistry.getInstance().unregister("re_reg_type");
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testUnregisterThenGetThrows() {
        ConnectorDescriptor mock = new InMemoryConnectorDescriptor("unreg_type");
        ConnectorRegistry.getInstance().register(mock);
        ConnectorRegistry.getInstance().unregister("unreg_type");
        ConnectorRegistry.getInstance().get("unreg_type");
    }

    @Test
    public void testGetOrNullReturnsNull() {
        assertNull(ConnectorRegistry.getInstance().getOrNull("does_not_exist"));
    }

    @Test
    public void testRegisterDoesNotTouchSecretTypeRegistry() {
        // After decoupling, ConnectorRegistry.register() should NOT auto-register secret types
        MockConnectorDescriptor mock = new MockConnectorDescriptor();
        SecretTypeRegistry.getInstance().clear();
        ConnectorRegistry.getInstance().register(mock);
        try {
            assertFalse("ConnectorRegistry should not auto-register secret types",
                SecretTypeRegistry.getInstance().isRegistered("mock"));
        } finally {
            ConnectorRegistry.getInstance().unregister(mock.type());
        }
    }

    @Test
    public void testUnregisterDoesNotTouchSecretTypeRegistry() {
        // After decoupling, ConnectorRegistry.unregister() should NOT auto-unregister secret types
        MockConnectorDescriptor mock = new MockConnectorDescriptor();
        ConnectorRegistry.getInstance().register(mock);
        // Manually register secret type (simulating what ConnectorRuntimeManager would do)
        for (PropertyDefinition def : mock.secretDefinitions()) {
            SecretTypeRegistry.getInstance().register(def);
        }
        assertTrue(SecretTypeRegistry.getInstance().isRegistered("mock"));

        ConnectorRegistry.getInstance().unregister(mock.type());
        // Secret type should still be there — ConnectorRegistry no longer manages it
        assertTrue("ConnectorRegistry should not auto-unregister secret types",
            SecretTypeRegistry.getInstance().isRegistered("mock"));

        // Cleanup
        SecretTypeRegistry.getInstance().clear();
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    String type = "concurrent_" + idx;
                    ConnectorRegistry.getInstance().register(new InMemoryConnectorDescriptor(type));
                    ConnectorDescriptor f = ConnectorRegistry.getInstance().get(type);
                    assertNotNull(f);
                    ConnectorRegistry.getInstance().unregister(type);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        latch.await();
        assertEquals(0, errors.get());
    }
}
