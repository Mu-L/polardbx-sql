package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.BlockingReason;
import com.alibaba.polardbx.common.BlockingState;
import com.alibaba.polardbx.executor.mpp.operator.Driver;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;

public class TaskExecutorTest {

    @Test
    public void test() {
        TaskId taskId = Mockito.mock(TaskId.class);
        TaskExecutor.TaskHandle taskHandle = new TaskExecutor.TaskHandle(taskId);

        DriverContext.DriverRuntimeStatisticsUpdater updater = new DriverContext.DriverRuntimeStatisticsUpdater();
        Driver driver = Mockito.mock(Driver.class);
        DriverContext driverContext = Mockito.mock(DriverContext.class);
        Mockito.when(driver.getDriverContext()).thenReturn(driverContext);
        Mockito.when(driverContext.getSplitStatisticsMap()).thenReturn(new HashMap<>());
        Mockito.when(driverContext.getUpdater()).thenReturn(updater);

        SplitRunner splitRunner = new DriverSplitRunner(driver);

        Mockito.when(driverContext.getInputPositions()).thenReturn(999L);
        Mockito.when(driverContext.getIOReadBytes()).thenReturn(1999L);
        Mockito.when(driverContext.getOutputPositions()).thenReturn(2999L);
        //Mockito.when(splitRunner.getSplitStatistics()).thenReturn("orcSplit:2|csvSplit:1");

        TaskExecutor.PrioritizedSplitRunner prioritizedSplitRunner = new TaskExecutor.PrioritizedSplitRunner(
            taskHandle, splitRunner
        );

        prioritizedSplitRunner.updateBlockingState(
            BlockingState.create(BlockingReason.WAIT_FOR_PRE_PREPROCESSOR, 1000)
        );

        prioritizedSplitRunner.updateBlockingState(
            BlockingState.create(BlockingReason.WAIT_FOR_PRE_PREPROCESSOR, 2000)
        );

        prioritizedSplitRunner.updateBlockingState(
            BlockingState.create(BlockingReason.LOCAL_BUFFER_NOT_EMPTY, 1000)
        );

        prioritizedSplitRunner.updateBlockingState(
            BlockingState.create(BlockingReason.LOCAL_BUFFER_NOT_EMPTY, 500)
        );

        driverContext.getUpdater().build(0, 0, 0, "");
    }
}