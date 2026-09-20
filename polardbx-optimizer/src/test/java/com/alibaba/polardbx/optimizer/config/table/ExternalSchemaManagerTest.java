package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorTable;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExternalSchemaManagerTest {

    private static final String CONNECTOR_TYPE = "test_esm";

    /**
     * Registers an anonymous ConnectorDescriptor whose createMetadata returns the
     * given mock. Must be paired with {@link #unregisterConnector()} in a
     * try-finally so the global ConnectorRegistry is left clean.
     */
    private void registerConnector(ConnectorMetadata mockMeta) {
        ConnectorRegistry.getInstance().register(new ConnectorDescriptor() {
            @Override
            public String type() {
                return CONNECTOR_TYPE;
            }

            @Override
            public ConnectorMetadata createMetadata(Map<String, String> props, SecretBundle secret) {
                return mockMeta;
            }
        });
    }

    private void unregisterConnector() {
        ConnectorRegistry.getInstance().unregister(CONNECTOR_TYPE);
    }

    /**
     * Registers a connector that asks {@code factory} for a ConnectorMetadata on every
     * createMetadata call and counts those calls, so a test can observe the lazy
     * re-creation that follows an idle reclaim.
     */
    private AtomicInteger registerCountingConnector(Supplier<ConnectorMetadata> factory) {
        AtomicInteger created = new AtomicInteger();
        ConnectorRegistry.getInstance().register(new ConnectorDescriptor() {
            @Override
            public String type() {
                return CONNECTOR_TYPE;
            }

            @Override
            public ConnectorMetadata createMetadata(Map<String, String> props, SecretBundle secret) {
                created.incrementAndGet();
                return factory.get();
            }
        });
        return created;
    }

    private static ConnectorMetadata metadataWithTable() {
        ConnectorMetadata mockMeta = mock(ConnectorMetadata.class);
        when(mockMeta.getTable(anyString(), anyString())).thenReturn(
            Optional.of(new ConnectorTable(Collections.emptyList(), null)));
        return mockMeta;
    }

    @Test
    public void getTable_emptyResult_thenNegativeCacheImmediately() {
        ConnectorMetadata mockMeta = mock(ConnectorMetadata.class);
        when(mockMeta.getTable(anyString(), anyString())).thenReturn(Optional.empty());

        registerConnector(mockMeta);
        try {
            ExternalSchemaManager esm = new ExternalSchemaManager("cat", "db", CONNECTOR_TYPE,
                new HashMap<>(), SecretBundle.EMPTY, null, -1);

            for (int i = 0; i < 2; i++) {
                try {
                    esm.getTable("missing");
                    fail("expected TableNotFoundException");
                } catch (TableNotFoundException e) {
                }
            }

            verify(mockMeta, times(1)).getTable("db", "missing");
        } finally {
            unregisterConnector();
        }
    }

    @Test
    public void invalidateAll_clearsNegativeCache() {
        ConnectorMetadata mockMeta = mock(ConnectorMetadata.class);
        when(mockMeta.getTable(anyString(), anyString())).thenReturn(Optional.empty());

        registerConnector(mockMeta);
        try {
            ExternalSchemaManager esm = new ExternalSchemaManager("cat", "db", CONNECTOR_TYPE,
                new HashMap<>(), SecretBundle.EMPTY, null, -1);

            try {
                esm.getTable("missing");
                fail("expected TableNotFoundException");
            } catch (TableNotFoundException e) {
            }
            verify(mockMeta, times(1)).getTable("db", "missing");

            esm.invalidateAll();

            try {
                esm.getTable("missing");
                fail("expected TableNotFoundException");
            } catch (TableNotFoundException e) {
            }
            verify(mockMeta, times(2)).getTable("db", "missing");
        } finally {
            unregisterConnector();
        }
    }

    @Test
    public void getTable_threeConsecutiveExternalErrors_notNegativeCached() {
        ConnectorMetadata mockMeta = mock(ConnectorMetadata.class);
        when(mockMeta.getTable(anyString(), anyString()))
            .thenThrow(new RuntimeException("connection refused"));

        registerConnector(mockMeta);
        try {
            ExternalSchemaManager esm = new ExternalSchemaManager("cat", "db", CONNECTOR_TYPE,
                new HashMap<>(), SecretBundle.EMPTY, null, -1);

            for (int i = 0; i < 4; i++) {
                try {
                    esm.getTable("missing");
                    fail("expected external table exception");
                } catch (TddlRuntimeException e) {
                    Assert.assertEquals(ErrorCode.ERR_EXTERNAL_TABLE, e.getErrorCodeType());
                }
            }

            verify(mockMeta, times(4)).getTable("db", "missing");
        } finally {
            unregisterConnector();
        }
    }

    @Test
    public void getTable_tableNotFoundException_thenNegativeCacheImmediately() {
        ConnectorMetadata mockMeta = mock(ConnectorMetadata.class);
        when(mockMeta.getTable(anyString(), anyString()))
            .thenThrow(new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, "missing"));

        registerConnector(mockMeta);
        try {
            ExternalSchemaManager esm = new ExternalSchemaManager("cat", "db", CONNECTOR_TYPE,
                new HashMap<>(), SecretBundle.EMPTY, null, -1);

            for (int i = 0; i < 2; i++) {
                try {
                    esm.getTable("missing");
                    fail("expected TableNotFoundException");
                } catch (TableNotFoundException e) {
                }
            }

            verify(mockMeta, times(1)).getTable("db", "missing");
        } finally {
            unregisterConnector();
        }
    }

    // ===== idle reclaim: detaches metadata without bricking the manager =====

    @Test
    public void detachIfIdle_thenNextLookupRecreatesMetadataTransparently() {
        AtomicInteger created = registerCountingConnector(
            ExternalSchemaManagerTest::metadataWithTable);
        try {
            ExternalSchemaManager esm = new ExternalSchemaManager("cat", "db", CONNECTOR_TYPE,
                new HashMap<>(), SecretBundle.EMPTY, null, -1);

            Assert.assertNotNull(esm.getTable("orders"));
            Assert.assertEquals(1, created.get());

            Assert.assertEquals(1, esm.detachIfIdle(0));

            // Must not throw "is closed": the reclaim leaves the manager usable.
            Assert.assertNotNull(esm.getTable("orders"));
            Assert.assertEquals("metadata should be lazily re-created", 2, created.get());
        } finally {
            unregisterConnector();
        }
    }

    @Test
    public void detachIfIdle_wipesTableMetaCache() {
        AtomicInteger created = registerCountingConnector(
            ExternalSchemaManagerTest::metadataWithTable);
        try {
            ExternalSchemaManager esm = new ExternalSchemaManager("cat", "db", CONNECTOR_TYPE,
                new HashMap<>(), SecretBundle.EMPTY, null, -1);

            esm.getTable("orders");
            Assert.assertEquals(1, esm.getAllTables().size());

            Assert.assertEquals(1, esm.detachIfIdle(0));
            Assert.assertTrue("cached table metas must be released", esm.getAllTables().isEmpty());
            Assert.assertEquals(1, created.get());
        } finally {
            unregisterConnector();
        }
    }

    @Test
    public void detachIfIdle_beforeTtlElapses_isNoOp() {
        AtomicInteger created = registerCountingConnector(
            ExternalSchemaManagerTest::metadataWithTable);
        try {
            ExternalSchemaManager esm = new ExternalSchemaManager("cat", "db", CONNECTOR_TYPE,
                new HashMap<>(), SecretBundle.EMPTY, null, -1);
            esm.getTable("orders");

            Assert.assertEquals(0, esm.detachIfIdle(TimeUnit.HOURS.toSeconds(1)));
            Assert.assertEquals(1, esm.getAllTables().size());
            Assert.assertEquals(1, created.get());
        } finally {
            unregisterConnector();
        }
    }

    @Test
    public void detachIfIdle_withNothingCached_touchesNoConnector() {
        AtomicInteger created = registerCountingConnector(
            ExternalSchemaManagerTest::metadataWithTable);
        try {
            ExternalSchemaManager esm = new ExternalSchemaManager("cat", "db", CONNECTOR_TYPE,
                new HashMap<>(), SecretBundle.EMPTY, null, -1);

            // Never queried, so there is no handle to release and no connector to call;
            // the sweep still reports the round because it swept the two caches.
            Assert.assertEquals(1, esm.detachIfIdle(0));
            Assert.assertEquals(0, created.get());
        } finally {
            unregisterConnector();
        }
    }

    /**
     * Shared-handle mode: the manager caches the instance it was built with, which is the
     * only mode where idle reclaim has a handle to detach.
     */
    private ExternalSchemaManager sharedHandleManager(ConnectorMetadata metadata) {
        return new ExternalSchemaManager("cat", "db", CONNECTOR_TYPE,
            new HashMap<>(), SecretBundle.EMPTY, null, -1, true, metadata);
    }

    @Test
    public void detachIfIdle_closesTheDetachedHandle() {
        ConnectorMetadata mockMeta = metadataWithTable();
        registerCountingConnector(() -> mockMeta);
        try {
            ExternalSchemaManager esm = sharedHandleManager(mockMeta);
            esm.getTable("orders");

            Assert.assertEquals(1, esm.detachIfIdle(0));
            verify(mockMeta, times(1)).close();
        } finally {
            unregisterConnector();
        }
    }

    @Test
    public void detachIfIdle_afterClose_isNoOp() {
        ConnectorMetadata mockMeta = metadataWithTable();
        registerCountingConnector(() -> mockMeta);
        try {
            ExternalSchemaManager esm = sharedHandleManager(mockMeta);
            esm.getTable("orders");
            esm.close();

            Assert.assertEquals(0, esm.detachIfIdle(0));
            verify(mockMeta, times(1)).close();
        } finally {
            unregisterConnector();
        }
    }

    // ===== shared handle vs one private instance per lookup =====

    @Test
    public void threadSafeConnector_reusesTheCachedHandle() {
        ConnectorMetadata mockMeta = metadataWithTable();
        AtomicInteger created = registerCountingConnector(() -> mockMeta);
        try {
            ExternalSchemaManager esm = sharedHandleManager(mockMeta);
            esm.getTable("orders");
            esm.getTable("items");

            Assert.assertEquals("the handle it was built with must be reused", 0, created.get());
            verify(mockMeta, times(0)).close();
        } finally {
            unregisterConnector();
        }
    }

    @Test
    public void nonThreadSafeConnector_createsAndClosesOneInstancePerLookup() {
        List<ConnectorMetadata> instances = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger created = registerCountingConnector(() -> {
            ConnectorMetadata m = metadataWithTable();
            instances.add(m);
            return m;
        });
        try {
            ExternalSchemaManager esm = new ExternalSchemaManager("cat", "db", CONNECTOR_TYPE,
                new HashMap<>(), SecretBundle.EMPTY, null, -1);
            esm.getTable("orders");
            esm.getTable("items");

            Assert.assertEquals("nothing may be shared between lookups", 2, created.get());
            for (ConnectorMetadata m : instances) {
                verify(m, times(1)).close();
            }
        } finally {
            unregisterConnector();
        }
    }

    @Test
    public void close_thenLookupIsRejected() {
        ConnectorMetadata mockMeta = metadataWithTable();
        registerCountingConnector(() -> mockMeta);
        try {
            ExternalSchemaManager esm = sharedHandleManager(mockMeta);
            esm.getTable("orders");
            esm.close();

            // A dropped catalog or a rotated secret must not keep being served.
            try {
                esm.getTable("items");
                fail("expected external table exception");
            } catch (TddlRuntimeException e) {
                Assert.assertEquals(ErrorCode.ERR_EXTERNAL_TABLE, e.getErrorCodeType());
            }
        } finally {
            unregisterConnector();
        }
    }

}
