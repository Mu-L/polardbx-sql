package com.alibaba.polardbx.optimizer.external.catalog;

import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ExternalSchemaManager;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorTable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExternalSchemaReclaimerTest {

    private static final String CONNECTOR_TYPE = "test_reclaimer";
    private static final String CATALOG = "reclaimcat";
    private static final String SCHEMA = "reclaimcat$$db1";

    @After
    public void teardown() {
        OptimizerContext.clearContext(SCHEMA);
        ExternalCatalogManager.getInstance().remove(CATALOG);
        ConnectorRegistry.getInstance().unregister(CONNECTOR_TYPE);
    }

    private ExternalSchemaManager loadExternalSchema(ConnectorMetadata metadata) {
        ConnectorRegistry.getInstance().register(new ConnectorDescriptor() {
            @Override
            public String type() {
                return CONNECTOR_TYPE;
            }

            @Override
            public boolean isThreadSafe() {
                return true;
            }

            @Override
            public ConnectorMetadata createMetadata(Map<String, String> props, SecretBundle secret) {
                return metadata;
            }
        });
        // Registered so the self-healing check in getContext does not treat this schema
        // as belonging to a dropped catalog.
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo(CATALOG, "mock", new HashMap<>(), null, null));

        // Shared-handle mode: only a cached handle gives the sweep something to detach.
        ExternalSchemaManager sm = new ExternalSchemaManager(CATALOG, "db1", CONNECTOR_TYPE,
            new HashMap<>(), SecretBundle.EMPTY, null, -1, true, metadata);
        OptimizerContext ctx = new OptimizerContext(SCHEMA);
        ctx.setSchemaManager(sm);
        ctx.setFinishInit(true);
        OptimizerContext.loadExternalContext(ctx);
        return sm;
    }

    private static ConnectorMetadata metadataWithTable() {
        ConnectorMetadata metadata = mock(ConnectorMetadata.class);
        when(metadata.getTable(anyString(), anyString())).thenReturn(
            Optional.of(new ConnectorTable(Collections.emptyList(), null)));
        return metadata;
    }

    @Test
    public void start_isIdempotent() {
        ExternalSchemaReclaimer reclaimer = ExternalSchemaReclaimer.getInstance();
        reclaimer.init();
        reclaimer.init();
        reclaimer.init();
        // Starting repeatedly must neither throw nor start a second scheduler.
    }

    @Test
    public void loadedExternalSchemaIsVisibleToTheSweep() {
        ExternalSchemaManager sm = loadExternalSchema(metadataWithTable());
        Assert.assertTrue(OptimizerContext.getExternalSchemaManagers().contains(sm));
    }

    @Test
    public void reclaimIdle_releasesMetadataButKeepsTheEntryUsable() {
        ConnectorMetadata metadata = metadataWithTable();
        ExternalSchemaManager sm = loadExternalSchema(metadata);
        sm.getTable("orders");
        Assert.assertEquals(1, sm.getAllTables().size());

        Assert.assertEquals(1, ExternalSchemaReclaimer.getInstance().reclaimIdle(0));
        Assert.assertTrue(sm.getAllTables().isEmpty());
        // The detach closes the handle inline, so it is already done here.
        verify(metadata, times(1)).close();

        // The entry is never removed, so the schema resolves without a re-bootstrap,
        // and the manager still serves lookups.
        Assert.assertNotNull(OptimizerContext.getContext(SCHEMA));
        Assert.assertTrue(OptimizerContext.getExternalSchemaManagers().contains(sm));
        Assert.assertNotNull(sm.getTable("orders"));
    }

    @Test
    public void reclaimIdle_beforeTtlElapses_reclaimsNothing() {
        ConnectorMetadata metadata = metadataWithTable();
        ExternalSchemaManager sm = loadExternalSchema(metadata);
        sm.getTable("orders");

        Assert.assertEquals(0,
            ExternalSchemaReclaimer.getInstance().reclaimIdle(TimeUnit.HOURS.toSeconds(1)));
        verify(metadata, times(0)).close();
        Assert.assertEquals(1, sm.getAllTables().size());
    }

    @Test
    public void reclaimIdle_countsTheSchemaEvenWhenClosingFails() {
        ConnectorMetadata metadata = metadataWithTable();
        // Error rather than Exception, mirroring ConnectorRuntimeManagerTest: the sweep
        // must report the reclaim regardless, because the memory was freed by the detach.
        org.mockito.Mockito.doThrow(new Error("close boom")).when(metadata).close();

        ExternalSchemaManager sm = loadExternalSchema(metadata);
        sm.getTable("orders");

        Assert.assertEquals(1, ExternalSchemaReclaimer.getInstance().reclaimIdle(0));
        Assert.assertTrue("table metadata must be released even if closing failed",
            sm.getAllTables().isEmpty());
    }

    @Test
    public void reclaimIdle_closesTheDetachedHandleInline() {
        ConnectorMetadata metadata = metadataWithTable();
        ExternalSchemaManager sm = loadExternalSchema(metadata);
        sm.getTable("orders");

        // The close runs on the sweep thread itself: it has already happened when
        // reclaimIdle returns, which also means an unresponsive remote stalls the sweep.
        Assert.assertEquals(1, ExternalSchemaReclaimer.getInstance().reclaimIdle(0));
        verify(metadata, times(1)).close();

        Assert.assertTrue(sm.getAllTables().isEmpty());
        // The manager is not closed, so it rebuilds a handle on the next lookup.
        Assert.assertNotNull(sm.getTable("orders"));
    }

    @Test
    public void reclaimIdle_withNoExternalSchemaLoaded_reclaimsNothing() {
        Assert.assertEquals(0, ExternalSchemaReclaimer.getInstance().reclaimIdle(0));
    }

    @Test
    public void sweepOnce_neverThrows() {
        loadExternalSchema(metadataWithTable()).getTable("orders");
        // Reads the TTL through InstConfUtil, which has no MetaDB behind it here; a
        // sweep must still return normally so the schedule is not cancelled.
        ExternalSchemaReclaimer.getInstance().sweepOnce();
    }
}
