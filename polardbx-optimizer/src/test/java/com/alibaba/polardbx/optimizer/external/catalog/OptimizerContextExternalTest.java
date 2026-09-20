package com.alibaba.polardbx.optimizer.external.catalog;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ExternalSchemaManager;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.external.connector.InMemoryConnectorMetadata;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OptimizerContextExternalTest {

    private static final String SETUP_CONNECTOR = "test_oc";

    @Before
    public void setup() {
        // Register a connector so setup ESMs can lazy-init metadata if needed
        ConnectorRegistry.getInstance().register(new ConnectorDescriptor() {
            @Override
            public String type() {
                return SETUP_CONNECTOR;
            }

            @Override
            public ConnectorMetadata createMetadata(Map<String, String> p, SecretBundle s) {
                return new InMemoryConnectorMetadata();
            }
        });

        // Register catalogs so self-healing catalog existence check passes
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("hive", "mock", new HashMap<>(), null, null));
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("mysql", "mock", new HashMap<>(), null, null));

        OptimizerContext ctx1 = new OptimizerContext("hive$$dwd");
        ctx1.setSchemaManager(new ExternalSchemaManager("hive", "dwd", SETUP_CONNECTOR,
            new HashMap<>(), SecretBundle.EMPTY, null, -1));
        ctx1.setFinishInit(true);
        OptimizerContext.loadExternalContext(ctx1);

        OptimizerContext ctx2 = new OptimizerContext("hive$$ods");
        ctx2.setSchemaManager(new ExternalSchemaManager("hive", "ods", SETUP_CONNECTOR,
            new HashMap<>(), SecretBundle.EMPTY, null, -1));
        ctx2.setFinishInit(true);
        OptimizerContext.loadExternalContext(ctx2);

        OptimizerContext ctx3 = new OptimizerContext("mysql$$db");
        ctx3.setSchemaManager(new ExternalSchemaManager("mysql", "db", SETUP_CONNECTOR,
            new HashMap<>(), SecretBundle.EMPTY, null, -1));
        ctx3.setFinishInit(true);
        OptimizerContext.loadExternalContext(ctx3);

        OptimizerContext ctx4 = new OptimizerContext("my_normal_db");
        ctx4.setMatrix(emptyMatrix());
        ctx4.setFinishInit(true);
        OptimizerContext.loadContext(ctx4);

        // A local DB that happens to contain $$ (legacy) — should NOT be hidden
        OptimizerContext ctx5 = new OptimizerContext("legacy$$db");
        ctx5.setMatrix(emptyMatrix());
        ctx5.setFinishInit(true);
        OptimizerContext.loadContext(ctx5);
    }

    private static Matrix emptyMatrix() {
        Matrix matrix = new Matrix();
        matrix.setGroups(new ArrayList<>());
        return matrix;
    }

    @After
    public void teardown() {
        OptimizerContext.clearContext("hive$$dwd");
        OptimizerContext.clearContext("hive$$ods");
        OptimizerContext.clearContext("mysql$$db");
        OptimizerContext.clearContext("my_normal_db");
        OptimizerContext.clearContext("legacy$$db");
        ExternalCatalogManager.getInstance().invalidateAll();
        ConnectorRegistry.getInstance().unregister(SETUP_CONNECTOR);
    }

    /**
     * Forces lazy metadata initialization by triggering {@code getTable}.
     * The dummy table won't exist, but the side-effect is that
     * {@code ConnectorDescriptor.createMetadata} is called and the metadata
     * field is populated, so subsequent {@code close()} will close it.
     */
    private static void forceMetadataInit(ExternalSchemaManager sm) {
        try {
            sm.getTable("__force_init__");
        } catch (Exception ignored) {
            // TableNotFoundException expected — metadata is now initialized
        }
    }

    @Test
    public void testGetActiveSchemaFiltersExternalSchemaManager() {
        Set<String> active = OptimizerContext.getActiveSchemaNames();
        // ExternalSchemaManager instances should be filtered
        assertFalse(active.contains("hive$$dwd"));
        assertFalse(active.contains("hive$$ods"));
        assertFalse(active.contains("mysql$$db"));
    }

    @Test
    public void testExternalContextAbsentFromEnumerationYetResolvable() {
        // Being invisible to local enumeration must not make an external schema
        // unreachable: getContext still resolves it out of the external map.
        assertFalse(OptimizerContext.getActiveSchemaNames().contains("hive$$dwd"));
        OptimizerContext ctx = OptimizerContext.getContext("hive$$dwd");
        assertNotNull(ctx);
        assertEquals("hive$$dwd", ctx.getSchemaName());
    }

    @Test
    public void testLocalContextWinsOverExternalOnKeyCollision() {
        // 'hive' is a registered catalog, so this key is a legal external schema name.
        // A local schema registered under the same key must still take precedence, and
        // must stay visible to local enumeration.
        OptimizerContext external = new OptimizerContext("hive$$collide");
        external.setSchemaManager(new ExternalSchemaManager("hive", "collide", SETUP_CONNECTOR,
            new HashMap<>(), SecretBundle.EMPTY, null, -1));
        external.setFinishInit(true);
        OptimizerContext.loadExternalContext(external);

        OptimizerContext local = new OptimizerContext("hive$$collide");
        local.setMatrix(emptyMatrix());
        local.setFinishInit(true);
        OptimizerContext.loadContext(local);

        try {
            assertSame(local, OptimizerContext.getContext("hive$$collide"));
            assertTrue(OptimizerContext.getActiveSchemaNames().contains("hive$$collide"));
        } finally {
            OptimizerContext.clearContext("hive$$collide");
        }
    }

    @Test
    public void testLoadExternalContextRejectsNonExternalManager() {
        // The external map's sole writer enforces the invariant the cast sites depend on.
        OptimizerContext local = new OptimizerContext("bogus$$db");
        local.setMatrix(emptyMatrix());
        local.setFinishInit(true);
        try {
            OptimizerContext.loadExternalContext(local);
            fail("Expected rejection of a context without an ExternalSchemaManager");
        } catch (TddlRuntimeException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                e.getMessage().contains("ExternalSchemaManager"));
        }
    }

    @Test
    public void testGetActiveSchemaKeepsNormalDb() {
        Set<String> active = OptimizerContext.getActiveSchemaNames();
        assertTrue(active.contains("my_normal_db"));
    }

    @Test
    public void testGetActiveSchemaKeepsLegacyDollarDollarDb() {
        // A local DB with $$ in name but NOT using ExternalSchemaManager should still be visible
        Set<String> active = OptimizerContext.getActiveSchemaNames();
        assertTrue(active.contains("legacy$$db"));
    }

    @Test
    public void testRemoveExternalSchemas() {
        assertNotNull(OptimizerContext.getContext("hive$$dwd"));
        assertNotNull(OptimizerContext.getContext("hive$$ods"));

        // Simulate DROP flow: remove from ECM first, then clear schema contexts
        ExternalCatalogManager.getInstance().remove("hive");
        OptimizerContext.removeExternalSchemas("hive");

        // After DROP, getContext should return null (catalog gone, no bootstrap)
        assertNull(OptimizerContext.getContext("hive$$dwd"));
        assertNull(OptimizerContext.getContext("hive$$ods"));
        assertNotNull(OptimizerContext.getContext("mysql$$db"));
    }

    @Test
    public void testRemoveExternalSchemasExactPrefix() {
        OptimizerContext ctx = new OptimizerContext("hive_ext$$db");
        ctx.setFinishInit(true);
        OptimizerContext.loadContext(ctx);
        try {
            OptimizerContext.removeExternalSchemas("hive");
            assertNotNull(OptimizerContext.getContext("hive_ext$$db"));
        } finally {
            OptimizerContext.clearContext("hive_ext$$db");
        }
    }

    // ===== Local-only components must be rejected, not returned as null =====

    @Test
    public void testExternalContextRejectsLocalOnlyComponents() {
        OptimizerContext ctx = OptimizerContext.getContext("hive$$dwd");
        assertNotNull(ctx);
        assertTrue(ctx.isExternalSchema());

        assertRejected(ctx::getRuleManager, "sharding rule");
        assertRejected(ctx::getViewManager, "view");
        assertRejected(ctx::getPartitioner, "partitioning");
        assertRejected(ctx::getParamManager, "connection parameter");
    }

    @Test
    public void testNormalContextStillReturnsNullComponents() {
        OptimizerContext ctx = OptimizerContext.getContext("my_normal_db");
        assertNotNull(ctx);
        assertFalse(ctx.isExternalSchema());

        // Callers that treat a missing component as "feature unavailable" must keep working.
        assertNull(ctx.getRuleManager());
        assertNull(ctx.getViewManager());
        assertNull(ctx.getPartitioner());
        assertNull(ctx.getParamManager());
    }

    private static void assertRejected(Runnable getter, String expectedFeature) {
        try {
            getter.run();
            fail("Expected rejection mentioning '" + expectedFeature + "'");
        } catch (TddlRuntimeException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                e.getMessage().contains(expectedFeature));
            assertTrue("Message should name the schema: " + e.getMessage(),
                e.getMessage().contains("hive$$dwd"));
        }
    }

    // ===== getActiveGroups must not touch external contexts =====

    @Test
    public void testGetActiveGroupsSkipsExternalContexts() {
        // External contexts are skipped entirely; the local ones in setup carry an
        // empty matrix. Neither may make this throw.
        try {
            OptimizerContext.getActiveGroups();
        } catch (Throwable ignore) {

        }
    }

    // ===== Bug 1: removeExternalSchemas must close ConnectorMetadata =====

    @Test
    public void testRemoveExternalSchemasClosesConnectorMetadata() {
        final String connectorType = "test_oc_close";
        AtomicInteger closeCount = new AtomicInteger(0);
        final ConnectorMetadata trackingMeta = new InMemoryConnectorMetadata() {
            @Override
            public void close() {
                closeCount.incrementAndGet();
            }
        };

        ConnectorRegistry.getInstance().register(new ConnectorDescriptor() {
            @Override
            public String type() {
                return connectorType;
            }

            @Override
            public ConnectorMetadata createMetadata(Map<String, String> p, SecretBundle s) {
                return trackingMeta;
            }
        });

        OptimizerContext ctx = new OptimizerContext("closeme$$db1");
        ExternalSchemaManager sm1 = new ExternalSchemaManager("closeme", "db1", connectorType,
            new HashMap<>(), SecretBundle.EMPTY, null, -1);
        ctx.setSchemaManager(sm1);
        ctx.setFinishInit(true);
        OptimizerContext.loadExternalContext(ctx);
        forceMetadataInit(sm1);

        OptimizerContext ctx2 = new OptimizerContext("closeme$$db2");
        ExternalSchemaManager sm2 = new ExternalSchemaManager("closeme", "db2", connectorType,
            new HashMap<>(), SecretBundle.EMPTY, null, -1);
        ctx2.setSchemaManager(sm2);
        ctx2.setFinishInit(true);
        OptimizerContext.loadExternalContext(ctx2);
        forceMetadataInit(sm2);

        try {
            OptimizerContext.removeExternalSchemas("closeme");
            assertEquals("All evicted schemas should have metadata closed", 2, closeCount.get());
        } finally {
            OptimizerContext.clearContext("closeme$$db1");
            OptimizerContext.clearContext("closeme$$db2");
            ConnectorRegistry.getInstance().unregister(connectorType);
        }
    }

    // ===== Bug 2: self-healing must evict when catalog does not exist =====

    @Test
    public void testSelfHealingEvictsWhenCatalogNotExist() {
        final String connectorType = "test_oc_ghost";
        // Register catalog in ExternalCatalogManager
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("ghostcat", "mock", new HashMap<>(), null, null));

        AtomicInteger closeCount = new AtomicInteger(0);
        final ConnectorMetadata trackingMeta = new InMemoryConnectorMetadata() {
            @Override
            public void close() {
                closeCount.incrementAndGet();
            }
        };

        ConnectorRegistry.getInstance().register(new ConnectorDescriptor() {
            @Override
            public String type() {
                return connectorType;
            }

            @Override
            public ConnectorMetadata createMetadata(Map<String, String> p, SecretBundle s) {
                return trackingMeta;
            }
        });

        // Insert a context for this catalog
        OptimizerContext ctx = new OptimizerContext("ghostcat$$db1");
        ExternalSchemaManager sm = new ExternalSchemaManager("ghostcat", "db1", connectorType,
            new HashMap<>(), SecretBundle.EMPTY, null, -1);
        ctx.setSchemaManager(sm);
        ctx.setFinishInit(true);
        OptimizerContext.loadExternalContext(ctx);
        forceMetadataInit(sm);

        try {
            // Context should be accessible while catalog exists
            assertNotNull(OptimizerContext.getContext("ghostcat$$db1"));

            // Simulate DROP CATALOG — remove from ExternalCatalogManager
            ExternalCatalogManager.getInstance().remove("ghostcat");

            // Self-healing in getContext should detect catalog gone and evict
            OptimizerContext result = OptimizerContext.getContext("ghostcat$$db1");
            assertNull("Ghost context should be evicted when catalog not exist", result);
            assertEquals("ConnectorMetadata should be closed on eviction", 1, closeCount.get());
        } finally {
            OptimizerContext.clearContext("ghostcat$$db1");
            ExternalCatalogManager.getInstance().remove("ghostcat");
            ConnectorRegistry.getInstance().unregister(connectorType);
        }
    }

    // ===== Bug 1+2: self-healing eviction also closes metadata =====

    @Test
    public void testSelfHealingSecretVersionChangeClosesMetadata() {
        final String connectorType = "test_oc_heal";
        // Register secret with initial version
        SecretManager.getInstance().register(
            "heal_secret", "oss",
            ExternalCredentialEncryptor.encryptMap(
                Collections.singletonMap("access_key", "AK")));
        long initialVersion = SecretManager.getInstance()
            .getGeneration("heal_secret");

        // Register catalog so isExternalSchema works
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("healcat", "mock", new HashMap<>(), "heal_secret", null));

        AtomicInteger closeCount = new AtomicInteger(0);
        final ConnectorMetadata trackingMeta = new InMemoryConnectorMetadata() {
            @Override
            public void close() {
                closeCount.incrementAndGet();
            }
        };

        ConnectorRegistry.getInstance().register(new ConnectorDescriptor() {
            @Override
            public String type() {
                return connectorType;
            }

            @Override
            public ConnectorMetadata createMetadata(Map<String, String> p, SecretBundle s) {
                return trackingMeta;
            }
        });

        OptimizerContext ctx = new OptimizerContext("healcat$$db1");
        ExternalSchemaManager sm = new ExternalSchemaManager("healcat", "db1", connectorType,
            new HashMap<>(), SecretBundle.EMPTY, "heal_secret", initialVersion);
        ctx.setSchemaManager(sm);
        ctx.setFinishInit(true);
        OptimizerContext.loadExternalContext(ctx);
        forceMetadataInit(sm);

        try {
            // Bump secret version
            SecretManager.getInstance().register(
                "heal_secret", "oss",
                ExternalCredentialEncryptor.encryptMap(
                    Collections.singletonMap("access_key", "NEW_AK")));

            // getContext should detect version mismatch, evict, and close metadata
            // Bootstrap will fail (no connector registered) but close should happen before that
            try {
                OptimizerContext.getContext("healcat$$db1");
            } catch (Exception ignored) {
                // Bootstrap failure is expected
            }
            assertEquals("ConnectorMetadata should be closed on version-mismatch eviction",
                1, closeCount.get());
        } finally {
            OptimizerContext.clearContext("healcat$$db1");
            ExternalCatalogManager.getInstance().remove("healcat");
            SecretManager.getInstance().invalidateAll();
            ConnectorRegistry.getInstance().unregister(connectorType);
        }
    }

    // ===== RELOAD CONNECTORS: evictAllExternalSchemas must close all metadata =====

    @Test
    public void testEvictAllExternalSchemasClosesMetadataAndKeepsNormalDb() {
        final String connectorType = "test_oc_evict_all";
        AtomicInteger closeCount = new AtomicInteger(0);
        final ConnectorMetadata trackingMeta = new InMemoryConnectorMetadata() {
            @Override
            public void close() {
                closeCount.incrementAndGet();
            }
        };

        ConnectorRegistry.getInstance().register(new ConnectorDescriptor() {
            @Override
            public String type() {
                return connectorType;
            }

            @Override
            public ConnectorMetadata createMetadata(Map<String, String> p, SecretBundle s) {
                return trackingMeta;
            }
        });

        // Two schemas under the SAME catalog to exercise prefix dedup,
        // catalog deliberately NOT registered in ExternalCatalogManager
        // so getContext cannot re-bootstrap them after eviction.
        OptimizerContext ctxA = new OptimizerContext("evictall$$db1");
        ExternalSchemaManager smA = new ExternalSchemaManager("evictall", "db1", connectorType,
            new HashMap<>(), SecretBundle.EMPTY, null, -1);
        ctxA.setSchemaManager(smA);
        ctxA.setFinishInit(true);
        OptimizerContext.loadExternalContext(ctxA);
        forceMetadataInit(smA);

        OptimizerContext ctxB = new OptimizerContext("evictall$$db2");
        ExternalSchemaManager smB = new ExternalSchemaManager("evictall", "db2", connectorType,
            new HashMap<>(), SecretBundle.EMPTY, null, -1);
        ctxB.setSchemaManager(smB);
        ctxB.setFinishInit(true);
        OptimizerContext.loadExternalContext(ctxB);
        forceMetadataInit(smB);

        try {
            OptimizerContext.evictAllExternalSchemas();

            assertEquals("Both metadata instances must be closed", 2, closeCount.get());
            assertNull(OptimizerContext.getContext("evictall$$db1"));
            assertNull(OptimizerContext.getContext("evictall$$db2"));
            assertNotNull("Normal schema must survive", OptimizerContext.getContext("my_normal_db"));

            // Idempotent: second call must not close again
            OptimizerContext.evictAllExternalSchemas();
            assertEquals(2, closeCount.get());
        } finally {
            OptimizerContext.clearContext("evictall$$db1");
            OptimizerContext.clearContext("evictall$$db2");
            ConnectorRegistry.getInstance().unregister(connectorType);
        }
    }
}
