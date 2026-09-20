package com.alibaba.polardbx.optimizer.external.catalog;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ExternalSchemaManager;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorTable;
import com.alibaba.polardbx.optimizer.external.connector.InMemoryConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.InMemoryConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.PushdownCapability;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;

import static org.mockito.Mockito.mock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class ExternalSchemaBootstrapTest {

    @Before
    public void setup() {
        ExternalCatalogManager.getInstance().invalidateAll();
    }

    @After
    public void teardown() {
        ExternalCatalogManager.getInstance().invalidateAll();
        OptimizerContext.removeExternalSchemas("testcat");
    }

    @Test
    public void testTryBootstrapNoDollarDollar() {
        OptimizerContext result = ExternalSchemaBootstrap.tryBootstrap("normal_schema");
        assertNull(result);
    }

    @Test
    public void testTryBootstrapSingleDollar() {
        OptimizerContext result = ExternalSchemaBootstrap.tryBootstrap("my$schema");
        assertNull(result);
    }

    @Test
    public void testTryBootstrapCatalogNotRegistered() {
        OptimizerContext result = ExternalSchemaBootstrap.tryBootstrap("unknown$$db");
        assertNull(result);
    }

    @Test
    public void testTryBootstrapSuccess() {
        Map<String, String> props = new HashMap<>();
        props.put("connector", "mock_boot");
        ExternalCatalogInfo info = new ExternalCatalogInfo("testcat", "mock_boot", props, null, null);
        ExternalCatalogManager.getInstance().register(info);

        InMemoryConnectorMetadata metadata = new InMemoryConnectorMetadata();
        metadata.addTable("mydb", "placeholder", ConnectorTable.builder(
            Collections.emptyList()).build());
        ConnectorRegistry.getInstance().register(
            new InMemoryConnectorDescriptor("mock_boot", metadata,
                EnumSet.noneOf(PushdownCapability.class),
                -1, Collections.emptyMap()));
        try {
            OptimizerContext ctx = ExternalSchemaBootstrap.tryBootstrap("testcat$$mydb");
            assertNotNull(ctx);
            assertEquals("testcat$$mydb", ctx.getSchemaName());
            assertNotNull(ctx.getLatestSchemaManager());
        } finally {
            ConnectorRegistry.getInstance().unregister("mock_boot");
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testTryBootstrapDbNameContainsDollar() {
        ExternalCatalogInfo info = new ExternalCatalogInfo("testcat", "oss", new HashMap<>(), null, null);
        ExternalCatalogManager.getInstance().register(info);
        ExternalSchemaBootstrap.tryBootstrap("testcat$$bad$db");
    }

    @Test
    public void testTryBootstrapDollarDollarAtStart() {
        // "$$db" → idx=0, should return null
        OptimizerContext result = ExternalSchemaBootstrap.tryBootstrap("$$db");
        assertNull(result);
    }

    @Test
    public void testTryBootstrapDiscardsStaleWhenSecretChanges() {
        // Setup catalog and connector
        Map<String, String> props = new HashMap<>();
        props.put("connector", "mock_gen");
        ExternalCatalogInfo info = new ExternalCatalogInfo("testcat", "mock_gen", props, "my_secret", null);
        ExternalCatalogManager.getInstance().register(info);

        // Register the secret so resolve() won't throw
        Map<String, String> secretKv = new HashMap<>();
        secretKv.put("access_key", "AK123");
        SecretManager.getInstance().register("my_secret", "oss",
            ExternalCredentialEncryptor.encryptMap(secretKv));

        InMemoryConnectorMetadata metadata = new InMemoryConnectorMetadata();
        metadata.addTable("mydb", "t1", ConnectorTable.builder(Collections.emptyList()).build());
        ConnectorRegistry.getInstance().register(
            new InMemoryConnectorDescriptor("mock_gen", metadata,
                EnumSet.noneOf(PushdownCapability.class), -1, Collections.emptyMap()));

        try {
            // Verify per-entry isolation: changing a DIFFERENT secret should NOT
            // affect bootstrap of catalog using "my_secret"
            SecretManager.getInstance().register("unrelated_secret", "s3",
                ExternalCredentialEncryptor.encryptMap(secretKv));

            OptimizerContext ctx = ExternalSchemaBootstrap.tryBootstrap("testcat$$mydb");
            // Should succeed — "my_secret" version unchanged
            assertNotNull("Unrelated secret change should not discard bootstrap", ctx);
        } finally {
            ConnectorRegistry.getInstance().unregister("mock_gen");
            OptimizerContext.removeExternalSchemas("testcat");
            SecretManager.getInstance().invalidateAll();
        }
    }

    @Test
    public void testTryBootstrapThrowsWhenSecretRotatedDuringCreateMetadata() {
        // Setup catalog + connector + an initial secret (version v1)
        Map<String, String> props = new HashMap<>();
        props.put("connector", "mock_rotate");
        ExternalCatalogInfo info = new ExternalCatalogInfo("testcat", "mock_rotate", props, "rot_secret", null);
        ExternalCatalogManager.getInstance().register(info);

        Map<String, String> secretV1 = new HashMap<>();
        secretV1.put("access_key", "AK_V1");
        SecretManager.getInstance().register("rot_secret", "oss",
            ExternalCredentialEncryptor.encryptMap(secretV1));

        InMemoryConnectorMetadata metadata = new InMemoryConnectorMetadata();
        metadata.addTable("mydb", "t1", ConnectorTable.builder(Collections.emptyList()).build());
        ConnectorRegistry.getInstance().register(
            new InMemoryConnectorDescriptor("mock_rotate", metadata,
                EnumSet.noneOf(PushdownCapability.class), -1, Collections.emptyMap()) {
                @Override
                public ConnectorMetadata createMetadata(Map<String, String> catalogProps,
                                                        SecretBundle secret) {
                    // Simulate secret rotation during the slow createMetadata call:
                    // re-register pushes versionGenerator, so versionAfter != versionBefore.
                    Map<String, String> secretV2 = new HashMap<>();
                    secretV2.put("access_key", "AK_V2");
                    SecretManager.getInstance().register("rot_secret", "oss",
                        ExternalCredentialEncryptor.encryptMap(secretV2));
                    return super.createMetadata(catalogProps, secret);
                }
            });

        try {
            try {
                ExternalSchemaBootstrap.tryBootstrap("testcat$$mydb");
                fail("Should throw when secret rotated during createMetadata");
            } catch (TddlRuntimeException e) {
                assertTrue("Should use ERR_EXTERNAL_TABLE, got: " + e.getErrorCodeType(),
                    e.getErrorCodeType() == ErrorCode.ERR_EXTERNAL_TABLE);
                assertTrue("Message should mention rotation, got: " + e.getMessage(),
                    e.getMessage().contains("rotated"));
                assertTrue("Message should contain secret name, got: " + e.getMessage(),
                    e.getMessage().contains("rot_secret"));
            }
            assertContextNotRegistered("testcat$$mydb");
        } finally {
            ConnectorRegistry.getInstance().unregister("mock_rotate");
            OptimizerContext.removeExternalSchemas("testcat");
            SecretManager.getInstance().invalidateAll();
        }
    }

    @Test
    public void testTryBootstrapConnectorInitFailureWrappedAsERR_EXTERNAL_TABLE() {
        Map<String, String> props = new HashMap<>();
        props.put("connector", "failing_boot");
        ExternalCatalogInfo info = new ExternalCatalogInfo("testcat", "failing_boot", props, null, null);
        ExternalCatalogManager.getInstance().register(info);

        // Register a connector whose createMetadata throws RuntimeException
        ConnectorRegistry.getInstance().register(
            new InMemoryConnectorDescriptor("failing_boot") {
                @Override
                public ConnectorMetadata createMetadata(Map<String, String> catalogProps,
                                                        SecretBundle secret) {
                    throw new RuntimeException("Driver not found");
                }
            });
        try {
            // Metadata is now created eagerly for the existence check, so the failure
            // surfaces at bootstrap time instead of the first getTable
            try {
                ExternalSchemaBootstrap.tryBootstrap("testcat$$mydb");
                fail("Should throw TddlRuntimeException at bootstrap");
            } catch (TddlRuntimeException e) {
                assertTrue("Should use ERR_EXTERNAL_TABLE, got: " + e.getErrorCodeType(),
                    e.getErrorCodeType() == ErrorCode.ERR_EXTERNAL_TABLE);
                assertTrue("Should contain original message",
                    e.getMessage().contains("Driver not found"));
            }
            assertContextNotRegistered("testcat$$mydb");
        } finally {
            ConnectorRegistry.getInstance().unregister("failing_boot");
            OptimizerContext.removeExternalSchemas("testcat");
            ExternalCatalogManager.getInstance().remove("testcat");
        }
    }

    @Test
    public void testLoadExternalContextPreventsOverwrite() {
        String schemaName = "testcat$$preventdup";
        try {
            OptimizerContext ctx1 = new OptimizerContext(schemaName);
            ctx1.setSchemaManager(mock(ExternalSchemaManager.class));
            ctx1.setFinishInit(true);
            OptimizerContext ctx2 = new OptimizerContext(schemaName);
            ctx2.setSchemaManager(mock(ExternalSchemaManager.class));
            ctx2.setFinishInit(true);

            // First insertion should succeed (returns null)
            OptimizerContext result1 = OptimizerContext.loadExternalContext(ctx1);
            assertNull(result1);

            // Second insertion should return existing (ctx1)
            OptimizerContext result2 = OptimizerContext.loadExternalContext(ctx2);
            assertSame(ctx1, result2);
        } finally {
            OptimizerContext.clearContext(schemaName);
        }
    }

    @Test
    public void testConcurrentBootstrapSameSchemaOnlyOneWins() throws Exception {
        // Setup catalog + connector
        Map<String, String> props = new HashMap<>();
        ExternalCatalogInfo info = new ExternalCatalogInfo("testcat", "mock_concurrent", props, null, null);
        ExternalCatalogManager.getInstance().register(info);

        AtomicInteger metadataCreatedCount = new AtomicInteger(0);
        AtomicInteger metadataClosedCount = new AtomicInteger(0);
        InMemoryConnectorMetadata sharedMeta = new InMemoryConnectorMetadata() {
            @Override
            public void close() {
                metadataClosedCount.incrementAndGet();
            }
        };
        sharedMeta.addTable("db1", "t1",
            ConnectorTable.builder(Collections.emptyList()).build());
        ConnectorRegistry.getInstance().register(
            new InMemoryConnectorDescriptor("mock_concurrent", sharedMeta,
                EnumSet.noneOf(PushdownCapability.class), -1, Collections.emptyMap()) {
                @Override
                public ConnectorMetadata createMetadata(Map<String, String> catalogProps,
                                                        SecretBundle secret) {
                    metadataCreatedCount.incrementAndGet();
                    return super.createMetadata(catalogProps, secret);
                }
            });

        String schemaName = "testcat$$db1";
        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        @SuppressWarnings("unchecked")
        AtomicReference<OptimizerContext>[] results = new AtomicReference[threadCount];

        for (int i = 0; i < threadCount; i++) {
            results[i] = new AtomicReference<>();
            final int idx = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    results[idx].set(ExternalSchemaBootstrap.tryBootstrap(schemaName));
                } catch (Exception ignored) {
                } finally {
                    endLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue(endLatch.await(10, TimeUnit.SECONDS));

        try {
            // All non-null results should be the same context instance
            OptimizerContext winner = null;
            for (AtomicReference<OptimizerContext> ref : results) {
                if (ref.get() != null) {
                    if (winner == null) {
                        winner = ref.get();
                    }
                    assertSame("All threads should see the same context", winner, ref.get());
                }
            }
            assertNotNull("At least one thread should succeed", winner);

            // Each bootstrap attempt creates metadata for the existence check and
            // closes it via try-with-resources; the winner's schema manager creates
            // its own metadata lazily on first table access
            assertEquals("Every existence-check metadata should be closed",
                metadataCreatedCount.get(), metadataClosedCount.get());
        } finally {
            ConnectorRegistry.getInstance().unregister("mock_concurrent");
            OptimizerContext.clearContext(schemaName);
        }
    }

    @Test
    public void testTryBootstrapDbNotExistThrowsAndNotRegistered() {
        ExternalCatalogInfo info = new ExternalCatalogInfo("testcat", "mock_nodb", new HashMap<>(), null, null);
        ExternalCatalogManager.getInstance().register(info);

        AtomicInteger closedCount = new AtomicInteger(0);
        InMemoryConnectorMetadata metadata = new InMemoryConnectorMetadata() {
            @Override
            public void close() {
                closedCount.incrementAndGet();
            }
        };
        metadata.addTable("realdb", "t1", ConnectorTable.builder(Collections.emptyList()).build());
        ConnectorRegistry.getInstance().register(
            new InMemoryConnectorDescriptor("mock_nodb", metadata,
                EnumSet.noneOf(PushdownCapability.class), -1, Collections.emptyMap()));
        try {
            try {
                ExternalSchemaBootstrap.tryBootstrap("testcat$$nosuchdb");
                fail("Should throw for nonexistent remote database");
            } catch (TddlRuntimeException e) {
                assertEquals(ErrorCode.ERR_UNKNOWN_DATABASE,
                    e.getErrorCodeType());
                assertTrue(e.getMessage().contains("nosuchdb"));
            }
            assertEquals("Metadata should be closed on failure", 1, closedCount.get());
            assertContextNotRegistered("testcat$$nosuchdb");
        } finally {
            ConnectorRegistry.getInstance().unregister("mock_nodb");
        }
    }

    @Test
    public void testTryBootstrapExistenceCheckFailureWrapped() {
        ExternalCatalogInfo info = new ExternalCatalogInfo("testcat", "mock_conn_fail", new HashMap<>(), null, null);
        ExternalCatalogManager.getInstance().register(info);

        AtomicInteger closedCount = new AtomicInteger(0);
        InMemoryConnectorMetadata metadata = new InMemoryConnectorMetadata() {
            @Override
            public boolean databaseExists(String db) {
                throw new IllegalStateException("Connection refused");
            }

            @Override
            public void close() {
                closedCount.incrementAndGet();
            }
        };
        ConnectorRegistry.getInstance().register(
            new InMemoryConnectorDescriptor("mock_conn_fail", metadata,
                EnumSet.noneOf(PushdownCapability.class), -1, Collections.emptyMap()));
        try {
            try {
                ExternalSchemaBootstrap.tryBootstrap("testcat$$anydb");
                fail("Should throw when existence check cannot reach remote");
            } catch (TddlRuntimeException e) {
                assertEquals(ErrorCode.ERR_EXTERNAL_TABLE,
                    e.getErrorCodeType());
                assertTrue("Connect failure must not be reported as unknown database",
                    e.getMessage().contains("Connection refused"));
            }
            assertEquals("Metadata should be closed on failure", 1, closedCount.get());
            assertContextNotRegistered("testcat$$anydb");
        } finally {
            ConnectorRegistry.getInstance().unregister("mock_conn_fail");
        }
    }

    @Test
    public void testTryBootstrapExistenceCheckTddlExceptionPropagated() {
        ExternalCatalogInfo info = new ExternalCatalogInfo("testcat", "mock_auth_fail", new HashMap<>(), null, null);
        ExternalCatalogManager.getInstance().register(info);

        InMemoryConnectorMetadata metadata = new InMemoryConnectorMetadata() {
            @Override
            public boolean databaseExists(String db) {
                throw new TddlRuntimeException(
                    ErrorCode.ERR_EXTERNAL_TABLE,
                    "Access denied for user 'x'");
            }
        };
        ConnectorRegistry.getInstance().register(
            new InMemoryConnectorDescriptor("mock_auth_fail", metadata,
                EnumSet.noneOf(PushdownCapability.class), -1, Collections.emptyMap()));
        try {
            try {
                ExternalSchemaBootstrap.tryBootstrap("testcat$$anydb");
                fail("Should propagate TddlRuntimeException");
            } catch (TddlRuntimeException e) {
                assertEquals(ErrorCode.ERR_EXTERNAL_TABLE,
                    e.getErrorCodeType());
                assertTrue(e.getMessage().contains("Access denied"));
            }
            assertContextNotRegistered("testcat$$anydb");
        } finally {
            ConnectorRegistry.getInstance().unregister("mock_auth_fail");
        }
    }

    @Test
    public void testTryBootstrapClosesExistenceCheckMetadata() {
        ExternalCatalogInfo info = new ExternalCatalogInfo("testcat", "mock_reuse", new HashMap<>(), null, null);
        ExternalCatalogManager.getInstance().register(info);

        AtomicInteger createdCount = new AtomicInteger(0);
        AtomicInteger closedCount = new AtomicInteger(0);
        InMemoryConnectorMetadata metadata = new InMemoryConnectorMetadata() {
            @Override
            public void close() {
                closedCount.incrementAndGet();
            }
        };
        Field field = new Field("t1", "id", DataTypes.LongType);
        ColumnMeta col = new ColumnMeta("t1", "id", null, field);
        metadata.addTable("mydb", "t1",
            ConnectorTable.builder(Collections.singletonList(col)).build());
        ConnectorRegistry.getInstance().register(
            new InMemoryConnectorDescriptor("mock_reuse", metadata,
                EnumSet.noneOf(PushdownCapability.class), -1, Collections.emptyMap()) {
                @Override
                public ConnectorMetadata createMetadata(Map<String, String> catalogProps, SecretBundle secret) {
                    createdCount.incrementAndGet();
                    return super.createMetadata(catalogProps, secret);
                }
            });
        try {
            OptimizerContext ctx = ExternalSchemaBootstrap.tryBootstrap("testcat$$mydb");
            assertNotNull(ctx);
            assertEquals("Existence-check metadata should be closed after bootstrap",
                1, closedCount.get());
            assertNotNull(ctx.getLatestSchemaManager().getTable("t1"));
            assertEquals("Schema manager should lazily create its own metadata",
                2, createdCount.get());
        } finally {
            ConnectorRegistry.getInstance().unregister("mock_reuse");
        }
    }

    @Test
    public void testEvictAllExternalSchemas() {
        Map<String, String> props = new HashMap<>();
        props.put("connector", "mock_evict");
        ExternalCatalogInfo info = new ExternalCatalogInfo("testcat", "mock_evict", props, null, null);
        ExternalCatalogManager.getInstance().register(info);

        AtomicInteger closedCount = new AtomicInteger(0);
        InMemoryConnectorMetadata metadata = new InMemoryConnectorMetadata() {
            @Override
            public void close() {
                closedCount.incrementAndGet();
            }
        };
        metadata.addTable("mydb", "t1", ConnectorTable.builder(Collections.emptyList()).build());
        // Thread-safe so the manager retains the handle; a per-lookup connector keeps none,
        // leaving the eviction nothing to close.
        ConnectorRegistry.getInstance().register(
            new InMemoryConnectorDescriptor("mock_evict", metadata,
                EnumSet.noneOf(PushdownCapability.class), -1, Collections.emptyMap()) {
                @Override
                public boolean isThreadSafe() {
                    return true;
                }
            });
        try {
            OptimizerContext ctx = ExternalSchemaBootstrap.tryBootstrap("testcat$$mydb");
            assertNotNull(ctx);
            ctx.getLatestSchemaManager().getTable("t1");
            int closedBefore = closedCount.get();

            OptimizerContext.evictAllExternalSchemas();

            assertContextNotRegistered("testcat$$mydb");
            assertTrue("ESM metadata should be closed on evictAllExternalSchemas",
                closedCount.get() > closedBefore);
        } finally {
            ConnectorRegistry.getInstance().unregister("mock_evict");
            OptimizerContext.removeExternalSchemas("testcat");
        }
    }

    @Test
    public void testConcurrentEvictAllAndGetContextNoException() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("connector", "mock_conc_evict");
        ExternalCatalogInfo info = new ExternalCatalogInfo("testcat", "mock_conc_evict", props, null, null);
        ExternalCatalogManager.getInstance().register(info);

        InMemoryConnectorMetadata metadata = new InMemoryConnectorMetadata();
        metadata.addTable("mydb", "t1", ConnectorTable.builder(Collections.emptyList()).build());
        ConnectorRegistry.getInstance().register(
            new InMemoryConnectorDescriptor("mock_conc_evict", metadata,
                EnumSet.noneOf(PushdownCapability.class), -1, Collections.emptyMap()));
        try {
            ExternalSchemaBootstrap.tryBootstrap("testcat$$mydb");

            int threadCount = 10;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch endLatch = new CountDownLatch(threadCount);
            AtomicInteger errors = new AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < 100; j++) {
                            if (idx % 2 == 0) {
                                OptimizerContext.evictAllExternalSchemas();
                            } else {
                                OptimizerContext.getContext("testcat$$mydb");
                            }
                        }
                    } catch (Throwable t) {
                        errors.incrementAndGet();
                    } finally {
                        endLatch.countDown();
                    }
                }).start();
            }

            startLatch.countDown();
            assertTrue("Timed out waiting for threads", endLatch.await(30, TimeUnit.SECONDS));
            assertEquals("No exceptions expected during concurrent evict + get", 0, errors.get());
        } finally {
            ConnectorRegistry.getInstance().unregister("mock_conc_evict");
            OptimizerContext.removeExternalSchemas("testcat");
        }
    }

    private static void assertContextNotRegistered(String schemaName) {
        OptimizerContext probe = new OptimizerContext(schemaName);
        // Carries a manager only to satisfy the external map's entry-point invariant; the
        // probe exists solely to detect whether the schema is already registered.
        probe.setSchemaManager(mock(ExternalSchemaManager.class));
        try {
            assertNull("Context must not be registered for '" + schemaName + "'",
                OptimizerContext.loadExternalContext(probe));
        } finally {
            OptimizerContext.clearContext(schemaName);
        }
    }
}
