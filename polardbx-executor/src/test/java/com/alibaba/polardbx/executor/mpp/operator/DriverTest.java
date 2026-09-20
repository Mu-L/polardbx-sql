package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.executor.mpp.execution.PipelineContext;
import com.alibaba.polardbx.executor.mpp.execution.TaskContext;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.metadata.SplitType;
import com.alibaba.polardbx.executor.mpp.spi.ConnectorSplit;
import com.alibaba.polardbx.executor.operator.SourceExec;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DriverTest {
    @Test
    public void test1() {
        PipelineDepTree pipelineDepTree = Mockito.mock(PipelineDepTree.class);
        Mockito.when(pipelineDepTree.getNode(Mockito.anyInt()))
            .thenReturn(Mockito.mock(PipelineDepTree.TreeNode.class));

        TaskContext taskContext = Mockito.mock(TaskContext.class);
        Mockito.when(taskContext.getPipelineDepTree()).thenReturn(pipelineDepTree);

        PipelineContext pipelineContext = Mockito.mock(PipelineContext.class);
        Mockito.when(pipelineContext.getTaskContext()).thenReturn(taskContext);

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
        sourceExecMap.put(0, ImmutableList.of(sourceExec1, sourceExec2));

        Mockito.when(driverExec.getSourceExecs()).thenReturn(sourceExecMap);

        // mock splits
        Split split1 = Mockito.mock(Split.class);
        Split split2 = Mockito.mock(Split.class);
        ConnectorSplit connectorSplit1 = Mockito.mock(ConnectorSplit.class);
        ConnectorSplit connectorSplit2 = Mockito.mock(ConnectorSplit.class);

        List<Split> splits = ImmutableList.of(
            split1, split2
        );

        Mockito.when(split1.getConnectorSplit()).thenReturn(connectorSplit1);
        Mockito.when(split2.getConnectorSplit()).thenReturn(connectorSplit2);
        Mockito.when(connectorSplit1.getSplitType()).thenReturn(SplitType.CSV);
        Mockito.when(connectorSplit2.getSplitType()).thenReturn(SplitType.ORC);

        Driver driver = new Driver(driverContext, driverExec);
        driver.processNewSources(0, splits, false, true);

        String statistics = print(driverContext.getSplitStatisticsMap());
        System.out.println(statistics);
    }

    @Test
    public void test2() {
        PipelineDepTree pipelineDepTree = Mockito.mock(PipelineDepTree.class);
        Mockito.when(pipelineDepTree.getNode(Mockito.anyInt()))
            .thenReturn(Mockito.mock(PipelineDepTree.TreeNode.class));

        TaskContext taskContext = Mockito.mock(TaskContext.class);
        Mockito.when(taskContext.getPipelineDepTree()).thenReturn(pipelineDepTree);

        PipelineContext pipelineContext = Mockito.mock(PipelineContext.class);
        Mockito.when(pipelineContext.getTaskContext()).thenReturn(taskContext);

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
        sourceExecMap.put(0, ImmutableList.of(sourceExec1, sourceExec2));

        Mockito.when(driverExec.getSourceExecs()).thenReturn(sourceExecMap);

        // mock splits
        Split split1 = Mockito.mock(Split.class);
        Split split2 = Mockito.mock(Split.class);
        ConnectorSplit connectorSplit1 = Mockito.mock(ConnectorSplit.class);
        ConnectorSplit connectorSplit2 = Mockito.mock(ConnectorSplit.class);

        List<Split> splits = ImmutableList.of(
            split1, split2
        );

        Mockito.when(split1.getConnectorSplit()).thenReturn(connectorSplit1);
        Mockito.when(split2.getConnectorSplit()).thenReturn(connectorSplit2);
        Mockito.when(connectorSplit1.getSplitType()).thenReturn(SplitType.CSV);
        Mockito.when(connectorSplit2.getSplitType()).thenReturn(SplitType.ORC);

        Driver driver = new Driver(driverContext, driverExec);
        driver.processNewSources(0, splits, true, true);

        String statistics = print(driverContext.getSplitStatisticsMap());
        System.out.println(statistics);
    }

    private static String print(Map<SplitType, Integer> splitStatisticsMap) {
        StringBuilder stringBuilder = new StringBuilder();
        int size = splitStatisticsMap.size();
        int count = 0;
        for (Map.Entry<SplitType, Integer> entry : splitStatisticsMap.entrySet()) {
            stringBuilder.append(entry.getKey() + ":" + entry.getValue());
            count++;
            if (count < size) {
                stringBuilder.append(" | ");
            }
        }
        return stringBuilder.toString();
    }
}