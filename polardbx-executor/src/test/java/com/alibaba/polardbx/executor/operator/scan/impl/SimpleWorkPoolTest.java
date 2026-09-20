package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.mpp.metadata.SplitType;
import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermitManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.ScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SimpleWorkPoolTest {

    private SimpleWorkPool workPool;

    @Before
    public void setUp() {
        workPool = new SimpleWorkPool();
    }

    private ColumnarSplit createCsvColumnarSplit() {
        ExecutionContext ec = mock(ExecutionContext.class);
        ParamManager pm = mock(ParamManager.class);
        when(pm.getBoolean(ConnectionParams.ENABLE_COLUMNAR_SCAN_RANDOM_SPLIT)).thenReturn(true);
        when(ec.getParamManager()).thenReturn(pm);

        ScanPreProcessor preProcessor = mock(ScanPreProcessor.class);
        when(preProcessor.isPrepared()).thenReturn(true);
        return new CsvColumnarSplit(ec, mock(DynamicColumnarManager.class), 0, 0L, new Path("test1.csv"), 1, 0,
            Lists.newArrayList(1), Lists.newArrayList(1), preProcessor, null, 0, 0, false, null);
    }

    private ColumnarSplit createMorselColumnarSplit() throws IOException {
        ExecutionContext ec = mock(ExecutionContext.class);
        ParamManager pm = mock(ParamManager.class);
        when(pm.getBoolean(ConnectionParams.ENABLE_COLUMNAR_SCAN_RANDOM_SPLIT)).thenReturn(true);
        when(ec.getParamManager()).thenReturn(pm);

        ScanPreProcessor preProcessor = mock(ScanPreProcessor.class);
        when(preProcessor.isPrepared()).thenReturn(true);
        return new MorselColumnarSplit(ec, mock(ExecutorService.class), mock(ColumnarMemoryPermitManager.class),
            Engine.OSS, mock(FileSystem.class),
            new Configuration(), 0, 1,
            new Path("test2.orc"), null, new int[0], Lists.newArrayList(), Lists.newArrayList(), 1000, null, 1, null,
            preProcessor, 0, 0, null, null, null, null, null, null, false, null, null, null);
    }

    @Test
    public void testAddSplitAndPickUp() throws IOException {
        try (MockedConstruction<MorselColumnarSplit.ScanWorkIterator> mockedConstruction = Mockito.mockConstruction(
            MorselColumnarSplit.ScanWorkIterator.class, (mock, context) -> {
                when(mock.hasNext()).thenReturn(true, false);
                when(mock.next()).thenReturn(mock(ScanWork.class), (ScanWork<ColumnarSplit, Chunk>) null);
            })) {

            int driverId = 1;
            int splitCount = 64;

            for (int i = 0; i < splitCount; i++) {
                workPool.addSplit(driverId, createCsvColumnarSplit());
            }
            for (int i = 0; i < splitCount; i++) {
                workPool.addSplit(driverId, createMorselColumnarSplit());
            }
            workPool.noMoreSplits(driverId);

            // Pick up the scan work
            ScanWork<ColumnarSplit, Chunk> pickedWork;
            int pickedWorkCount = 0;
            while ((pickedWork = workPool.pickUp(driverId)) != null) {
                pickedWorkCount++;
                pickedWork.close();
            }
            assertEquals(splitCount * 2, pickedWorkCount);
        }
    }
}