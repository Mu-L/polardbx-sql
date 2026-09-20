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

public class SecretSyncTaskTest {

    @Test
    public void testExecuteImplSuccess() {
        ExecutionContext mockEc = mock(ExecutionContext.class);

        try (MockedStatic<ExtensionLoader> extMock = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncMock = mockStatic(SyncManagerHelper.class)) {
            extMock.when(() -> ExtensionLoader.load(any())).thenReturn(null);

            SecretSyncTask task = new SecretSyncTask("secret1", SecretSyncTask.SecretOpType.ADD);
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

            SecretSyncTask task = new SecretSyncTask("secret1", SecretSyncTask.SecretOpType.REMOVE);
            task.executeImpl(mockEc);
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testEnumValues() {
        assertEquals(3, SecretSyncTask.SecretOpType.values().length);
        assertEquals(SecretSyncTask.SecretOpType.ADD, SecretSyncTask.SecretOpType.valueOf("ADD"));
        assertEquals(SecretSyncTask.SecretOpType.UPDATE, SecretSyncTask.SecretOpType.valueOf("UPDATE"));
        assertEquals(SecretSyncTask.SecretOpType.REMOVE, SecretSyncTask.SecretOpType.valueOf("REMOVE"));
    }

    @Test
    public void testGetters() {
        SecretSyncTask task = new SecretSyncTask("secret1", SecretSyncTask.SecretOpType.UPDATE);
        assertEquals("secret1", task.getSecretName());
        assertEquals(SecretSyncTask.SecretOpType.UPDATE, task.getAction());
    }
}
