package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.executor.mpp.execution.PipelineContext;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.executor.operator.SourceExec;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;

public class DriverContextTest {
    @Test
    public void test() {
        PipelineContext pipelineContext = Mockito.mock(PipelineContext.class);
        DriverContext driverContext = new DriverContext(pipelineContext, false, 0);

        DriverExec driverExec = Mockito.mock(DriverExec.class);

        // mock source exec.
        SourceExec sourceExec1 = Mockito.mock(SourceExec.class);
        Mockito.when(sourceExec1.getInputPositions()).thenReturn(1000L);
        Mockito.when(sourceExec1.getIoBytesSize()).thenReturn(1024L);

        SourceExec sourceExec2 = Mockito.mock(SourceExec.class);
        Mockito.when(sourceExec2.getInputPositions()).thenReturn(900L);
        Mockito.when(sourceExec2.getIoBytesSize()).thenReturn(765L);

        HashMap<Integer, List<SourceExec>> sourceExecMap = new HashMap<>();
        sourceExecMap.put(0, ImmutableList.of(sourceExec1));
        sourceExecMap.put(1, ImmutableList.of(sourceExec2));

        Mockito.when(driverExec.getSourceExecs()).thenReturn(sourceExecMap);

        driverContext.setDriverExecRef(driverExec);
        driverContext.addOutputSize(100);
        driverContext.addOutputSize(200);
        driverContext.addOutputSize(300);

        // test new interface of performance.
        Assert.assertTrue(driverContext.getIOReadBytes() == 1789);
        Assert.assertTrue(driverContext.getInputPositions() == 1900);
        Assert.assertTrue(driverContext.getOutputPositions() == 600);

    }

    @Test
    public void testGetDriverStatsWithDriverExec() {
        // Setup
        PipelineContext pipelineContext = Mockito.mock(PipelineContext.class);
        DriverContext driverContext = new DriverContext(pipelineContext, false, 1);

        // Mock DriverExec
        DriverExec driverExec = Mockito.mock(DriverExec.class);

        // Mock SourceExec instances
        SourceExec sourceExec1 = Mockito.mock(SourceExec.class);
        Mockito.when(sourceExec1.getInputDataSize()).thenReturn(1024L);
        Mockito.when(sourceExec1.getInputPositions()).thenReturn(100L);

        SourceExec sourceExec2 = Mockito.mock(SourceExec.class);
        Mockito.when(sourceExec2.getInputDataSize()).thenReturn(2048L);
        Mockito.when(sourceExec2.getInputPositions()).thenReturn(200L);

        // Setup source exec map
        HashMap<Integer, List<SourceExec>> sourceExecMap = new HashMap<>();
        sourceExecMap.put(0, ImmutableList.of(sourceExec1, sourceExec2));
        Mockito.when(driverExec.getSourceExecs()).thenReturn(sourceExecMap);

        // Mock OutputCollector as consumer
        OutputCollector outputCollector = Mockito.mock(OutputCollector.class);
        Mockito.when(outputCollector.getOutputDataSize()).thenReturn(512L);
        Mockito.when(outputCollector.getOutputPositions()).thenReturn(50L);
        Mockito.when(driverExec.getConsumer()).thenReturn(outputCollector);

        // Set driver exec reference
        driverContext.setDriverExecRef(driverExec);

        // Start process timer to initialize timing
        driverContext.startProcessTimer();

        // Execute
        DriverStats driverStats = driverContext.getDriverStats();
    }

    @Test
    public void testGetDriverStatsWithoutDriverExec() {
        // Setup
        PipelineContext pipelineContext = Mockito.mock(PipelineContext.class);
        DriverContext driverContext = new DriverContext(pipelineContext, false, 2);

        // Don't set driver exec reference (null case)

        // Execute
        DriverStats driverStats = driverContext.getDriverStats();
    }

    @Test
    public void testGetDriverStatsWithNonOutputCollectorConsumer() {
        // Setup
        PipelineContext pipelineContext = Mockito.mock(PipelineContext.class);
        DriverContext driverContext = new DriverContext(pipelineContext, false, 3);

        // Mock DriverExec
        DriverExec driverExec = Mockito.mock(DriverExec.class);

        // Mock SourceExec
        SourceExec sourceExec = Mockito.mock(SourceExec.class);
        Mockito.when(sourceExec.getInputDataSize()).thenReturn(1000L);
        Mockito.when(sourceExec.getInputPositions()).thenReturn(80L);

        HashMap<Integer, List<SourceExec>> sourceExecMap = new HashMap<>();
        sourceExecMap.put(0, ImmutableList.of(sourceExec));
        Mockito.when(driverExec.getSourceExecs()).thenReturn(sourceExecMap);

        // Mock a consumer that is NOT OutputCollector
        ConsumerExecutor nonOutputCollectorConsumer = Mockito.mock(ConsumerExecutor.class);
        Mockito.when(driverExec.getConsumer()).thenReturn(nonOutputCollectorConsumer);

        // Set driver exec reference and add some output size manually
        driverContext.setDriverExecRef(driverExec);
        driverContext.addOutputSize(150L); // This should be used when consumer is not OutputCollector

        // Start timing
        driverContext.startProcessTimer();

        // Execute
        DriverStats driverStats = driverContext.getDriverStats();
    }

    @Test
    public void testGetDriverStatsWithFinishedDriver() {
        // Setup
        PipelineContext pipelineContext = Mockito.mock(PipelineContext.class);
        DriverContext driverContext = new DriverContext(pipelineContext, false, 4);

        // Mock DriverExec
        DriverExec driverExec = Mockito.mock(DriverExec.class);

        // Mock empty source exec map
        HashMap<Integer, List<SourceExec>> emptySourceExecMap = new HashMap<>();
        Mockito.when(driverExec.getSourceExecs()).thenReturn(emptySourceExecMap);

        // Mock OutputCollector
        OutputCollector outputCollector = Mockito.mock(OutputCollector.class);
        Mockito.when(outputCollector.getOutputDataSize()).thenReturn(256L);
        Mockito.when(outputCollector.getOutputPositions()).thenReturn(25L);
        Mockito.when(driverExec.getConsumer()).thenReturn(outputCollector);

        // Set driver exec and start timing
        driverContext.setDriverExecRef(driverExec);
        driverContext.startProcessTimer();

        // Simulate driver finishing
        try {
            Thread.sleep(1); // Small delay to ensure timing difference
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        driverContext.finished();

        // Execute
        DriverStats driverStats = driverContext.getDriverStats();
    }

    @Test
    public void testGetDriverStatsMultipleCalls() {
        // Setup
        PipelineContext pipelineContext = Mockito.mock(PipelineContext.class);
        DriverContext driverContext = new DriverContext(pipelineContext, false, 5);

        // Mock DriverExec with changing data
        DriverExec driverExec = Mockito.mock(DriverExec.class);

        SourceExec sourceExec = Mockito.mock(SourceExec.class);
        Mockito.when(sourceExec.getInputDataSize()).thenReturn(500L).thenReturn(1000L);
        Mockito.when(sourceExec.getInputPositions()).thenReturn(50L).thenReturn(100L);

        HashMap<Integer, List<SourceExec>> sourceExecMap = new HashMap<>();
        sourceExecMap.put(0, ImmutableList.of(sourceExec));
        Mockito.when(driverExec.getSourceExecs()).thenReturn(sourceExecMap);

        OutputCollector outputCollector = Mockito.mock(OutputCollector.class);
        Mockito.when(outputCollector.getOutputDataSize()).thenReturn(100L).thenReturn(200L);
        Mockito.when(outputCollector.getOutputPositions()).thenReturn(10L).thenReturn(20L);
        Mockito.when(driverExec.getConsumer()).thenReturn(outputCollector);

        driverContext.setDriverExecRef(driverExec);
        driverContext.startProcessTimer();

        DriverStats firstStats = driverContext.getDriverStats();
    }
}