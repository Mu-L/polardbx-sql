package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableGroupSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ReloadTableMetaAfterChangeTableGroupTaskTest {

    private static final String SCHEMA_NAME = "ut_schema_tg_reload";
    private static final String TARGET_TABLE_GROUP = "ut_tg_target";
    private static final String SOURCE_TABLE_GROUP = "ut_tg_source";

    private MockedConstruction<DdlJobManager> mockJobManagerWithPrevTask() {
        AlterTableSetTableGroupChangeMetaOnlyTask prevTask = mock(AlterTableSetTableGroupChangeMetaOnlyTask.class);
        when(prevTask.getTargetTableGroup()).thenReturn(TARGET_TABLE_GROUP);
        when(prevTask.getCurTableGroup()).thenReturn(SOURCE_TABLE_GROUP);
        List<DdlTask> prevTasks = Collections.singletonList(prevTask);
        return mockConstruction(DdlJobManager.class,
            (jobManager, context) -> when(jobManager.getTasksFromMetaDB(anyLong(), anyString()))
                .thenReturn(prevTasks));
    }

    @Test
    public void testReloadTableGroupSuccess() {
        ReloadTableMetaAfterChangeTableGroupTask task =
            new ReloadTableMetaAfterChangeTableGroupTask(SCHEMA_NAME, "ut_tg_old");
        task.setJobId(1L);

        try (MockedConstruction<DdlJobManager> jobManagerMock = mockJobManagerWithPrevTask();
            MockedStatic<SyncManagerHelper> syncManagerMock = mockStatic(SyncManagerHelper.class)) {

            syncManagerMock.when(() -> SyncManagerHelper.syncThrowExceptions(any(TableGroupSyncAction.class),
                any(SyncScope.class))).thenReturn(Collections.emptyList());

            task.reloadTableGroup();
        }
        Assert.assertEquals(TARGET_TABLE_GROUP, task.getTargetTableGroup());
    }

    @Test
    public void testReloadTableGroupFailureContainsContext() {
        ReloadTableMetaAfterChangeTableGroupTask task =
            new ReloadTableMetaAfterChangeTableGroupTask(SCHEMA_NAME, "ut_tg_old");
        task.setJobId(1L);

        try (MockedConstruction<DdlJobManager> jobManagerMock = mockJobManagerWithPrevTask();
            MockedStatic<SyncManagerHelper> syncManagerMock = mockStatic(SyncManagerHelper.class)) {

            syncManagerMock.when(() -> SyncManagerHelper.syncThrowExceptions(any(TableGroupSyncAction.class),
                any(SyncScope.class))).thenThrow(new NullPointerException());

            try {
                task.reloadTableGroup();
                Assert.fail("expected an exception when table group sync fails");
            } catch (TddlNestableRuntimeException e) {
                String message = String.valueOf(e.getMessage());
                Assert.assertTrue("message should contain the target table group, but got: " + message,
                    message.contains(TARGET_TABLE_GROUP));
                Assert.assertTrue("message should contain the source table group, but got: " + message,
                    message.contains(SOURCE_TABLE_GROUP));
                Assert.assertTrue("message should contain the schema name, but got: " + message,
                    message.contains(SCHEMA_NAME));
            }
        }
    }
}
