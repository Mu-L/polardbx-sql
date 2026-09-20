package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class ExternalCatalogSyncTaskTest {

    @Test
    public void testExecuteImplSuccess() {
        ExecutionContext mockEc = mock(ExecutionContext.class);

        try (MockedStatic<ExtensionLoader> extMock = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class)) {
            extMock.when(() -> ExtensionLoader.load(any())).thenReturn(null);

            ExternalCatalogSyncTask task =
                new ExternalCatalogSyncTask("cat1", ExternalCatalogSyncTask.SyncAction.ADD);
            task.executeImpl(mockEc);
        }
    }

    @Test
    public void testExecuteImplFailure() {
        ExecutionContext mockEc = mock(ExecutionContext.class);

        try (MockedStatic<ExtensionLoader> extMock = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class)) {
            extMock.when(() -> ExtensionLoader.load(any())).thenReturn(null);
            syncMock.when(() -> SyncManagerHelper.syncThrowExceptions(any(), any()))
                .thenThrow(new RuntimeException("sync failed"));

            ExternalCatalogSyncTask task =
                new ExternalCatalogSyncTask("cat1", ExternalCatalogSyncTask.SyncAction.REMOVE);
            task.executeImpl(mockEc);
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testEnumValues() {
        assertEquals(4, ExternalCatalogSyncTask.SyncAction.values().length);
        assertEquals(ExternalCatalogSyncTask.SyncAction.ADD,
            ExternalCatalogSyncTask.SyncAction.valueOf("ADD"));
        assertEquals(ExternalCatalogSyncTask.SyncAction.UPDATE,
            ExternalCatalogSyncTask.SyncAction.valueOf("UPDATE"));
        assertEquals(ExternalCatalogSyncTask.SyncAction.REMOVE,
            ExternalCatalogSyncTask.SyncAction.valueOf("REMOVE"));
        assertEquals(ExternalCatalogSyncTask.SyncAction.REFRESH,
            ExternalCatalogSyncTask.SyncAction.valueOf("REFRESH"));
    }

    @Test
    public void testGetters() {
        ExternalCatalogSyncTask task =
            new ExternalCatalogSyncTask("cat1", ExternalCatalogSyncTask.SyncAction.UPDATE);
        assertEquals("cat1", task.getCatalogName());
        assertEquals(ExternalCatalogSyncTask.SyncAction.UPDATE, task.getAction());
    }
}
