package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.scheduler.executor.warmup.WarmupTaskManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

public class CancelWarmupTaskSyncActionTest {

    private MockedStatic<ExecUtils> execUtilsMockedStatic;
    private MockedStatic<WarmupTaskManager> warmupTaskManagerMockedStatic;

    @Before
    public void setupCluster() {
        execUtilsMockedStatic = Mockito.mockStatic(ExecUtils.class);
        execUtilsMockedStatic.when(() -> ExecUtils.hasLeadership(any())).thenReturn(true);

        WarmupTaskManager warmupTaskManager = Mockito.mock(WarmupTaskManager.class);
        warmupTaskManagerMockedStatic = Mockito.mockStatic(WarmupTaskManager.class);
        warmupTaskManagerMockedStatic.when(() -> WarmupTaskManager.getInstance()).thenReturn(warmupTaskManager);

        Mockito.doNothing().when(warmupTaskManager).cancelTask(anyLong());
        Mockito.doNothing().when(warmupTaskManager).cancelAll();
    }

    @Test
    public void test() {
        CancelWarmupTaskSyncAction action = new CancelWarmupTaskSyncAction(1, false);
        ResultCursor cursor = action.sync();
        Row row;
        while ((row = cursor.next()) != null) {
            System.out.println(row);
        }
    }

    @After
    public void tearDown() {
        if (execUtilsMockedStatic != null) {
            execUtilsMockedStatic.close();
        }
        if (warmupTaskManagerMockedStatic != null) {
            warmupTaskManagerMockedStatic.close();
        }
    }
}