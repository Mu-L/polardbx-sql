package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.external.ConnectorRuntimeManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Unit tests for {@link ConnectorSyncAction}.
 */
public class ConnectorSyncActionTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @After
    public void tearDown() {
        ConnectorRuntimeManager.setInstance(null);
    }

    @Test
    public void sync_skipsWhenManagerNotInitialized() {
        ConnectorRuntimeManager.setInstance(null);
        ConnectorSyncAction action = new ConnectorSyncAction();
        // mock mode or uninitialized node: warn and skip, no exception
        Assert.assertNull(action.sync());
    }

    @Test
    public void sync_reloadsWhenManagerAvailable() throws IOException {
        File emptyDir = tempFolder.newFolder("connectors");
        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(emptyDir.toPath());
        manager.init();
        ConnectorRuntimeManager.setInstance(manager);

        ConnectorSyncAction action = new ConnectorSyncAction();
        Assert.assertNull(action.sync());

        manager.shutdown();
    }
}
