package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.metadata.SplitType;
import com.alibaba.polardbx.executor.mpp.planner.EarlyStopManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RoundRobinWorkPoolTest {

    private RoundRobinWorkPool workPool;

    @Before
    public void setUp() {
        workPool = new RoundRobinWorkPool(Mockito.mock(EarlyStopManager.class));
    }

    @Test
    public void testCSVTypeSplits() {
        int driverId = 1;
        List<ColumnarSplit> csvSplits = new ArrayList<>();

        // Create two CSV-type ColumnarSplits
        for (int i = 0; i < 2; i++) {
            ColumnarSplit csvSplit = mock(ColumnarSplit.class);
            when(csvSplit.getSplitType()).thenReturn(SplitType.CSV);

            List<ScanWork<ColumnarSplit, Chunk>> scanWorks = new ArrayList<>();
            for (int j = 0; j < 5; j++) { // Generating 5 ScanWorks for each split
                ScanWork<ColumnarSplit, Chunk> scanWork = mock(ScanWork.class);
                when(scanWork.getWorkId()).thenReturn("csv-file_" + i + "-unit_" + j);
                scanWorks.add(scanWork);
            }
            // Configure to return scanWorks in sequence, then null
            when(csvSplit.nextWork()).thenAnswer(invocation -> scanWorks.isEmpty() ? null : scanWorks.remove(0));

            csvSplits.add(csvSplit);
            workPool.addSplit(driverId, csvSplit);
        }

        workPool.noMoreSplits(driverId);

        int totalScanWorks = 0;
        ScanWork<ColumnarSplit, Chunk> work;
        while ((work = workPool.pickUp(driverId)) != null) {
            totalScanWorks++;
        }

        assertEquals(10, totalScanWorks); // 2 splits * 5 works each
    }

    @Test
    public void testORCTypeSplits() {
        int driverId = 1;
        List<ColumnarSplit> orcSplits = new ArrayList<>();

        // Create six ORC-type ColumnarSplits
        for (int i = 0; i < 6; i++) {
            ColumnarSplit orcSplit = mock(ColumnarSplit.class);
            when(orcSplit.getSplitType()).thenReturn(SplitType.ORC);

            List<ScanWork<ColumnarSplit, Chunk>> scanWorks = new ArrayList<>();
            for (int j = 0; j < 8; j++) { // Generating 8 ScanWorks for each split
                ScanWork<ColumnarSplit, Chunk> scanWork = mock(ScanWork.class);
                when(scanWork.getWorkId()).thenReturn("orc-file_" + i + "-unit_" + j);
                scanWorks.add(scanWork);
            }
            // Configure to return scanWorks in sequence, then null
            when(orcSplit.nextWork()).thenAnswer(invocation -> scanWorks.isEmpty() ? null : scanWorks.remove(0));

            orcSplits.add(orcSplit);
            workPool.addSplit(driverId, orcSplit);
        }

        workPool.noMoreSplits(driverId);

        int totalScanWorks = 0;
        ScanWork<ColumnarSplit, Chunk> work;
        while ((work = workPool.pickUp(driverId)) != null) {
            totalScanWorks++;
        }

        assertEquals(48, totalScanWorks); // 6 splits * 8 works each
    }

    @Test
    public void testMixOrcAndCsvSplits() {

        int driverId = 1;
        List<ColumnarSplit> orcSplits = new ArrayList<>();

        // Create six ORC-type ColumnarSplits
        for (int i = 0; i < 6; i++) {
            ColumnarSplit orcSplit = mock(ColumnarSplit.class);
            when(orcSplit.getSplitType()).thenReturn(SplitType.ORC);

            List<ScanWork<ColumnarSplit, Chunk>> scanWorks = new ArrayList<>();
            for (int j = 0; j < 8; j++) { // Generating 8 ScanWorks for each split
                ScanWork<ColumnarSplit, Chunk> scanWork = mock(ScanWork.class);
                when(scanWork.getWorkId()).thenReturn("orc-file_" + i + "-unit_" + j);
                scanWorks.add(scanWork);
            }
            // Configure to return scanWorks in sequence, then null
            when(orcSplit.nextWork()).thenAnswer(invocation -> scanWorks.isEmpty() ? null : scanWorks.remove(0));

            orcSplits.add(orcSplit);
            workPool.addSplit(driverId, orcSplit);
        }

        List<ColumnarSplit> csvSplits = new ArrayList<>();

        // Create two CSV-type ColumnarSplits
        for (int i = 0; i < 2; i++) {
            ColumnarSplit csvSplit = mock(ColumnarSplit.class);
            when(csvSplit.getSplitType()).thenReturn(SplitType.CSV);

            List<ScanWork<ColumnarSplit, Chunk>> scanWorks = new ArrayList<>();
            for (int j = 0; j < 5; j++) { // Generating 5 ScanWorks for each split
                ScanWork<ColumnarSplit, Chunk> scanWork = mock(ScanWork.class);
                when(scanWork.getWorkId()).thenReturn("csv-file_" + i + "-unit_" + j);
                scanWorks.add(scanWork);
            }
            // Configure to return scanWorks in sequence, then null
            when(csvSplit.nextWork()).thenAnswer(invocation -> scanWorks.isEmpty() ? null : scanWorks.remove(0));

            csvSplits.add(csvSplit);
            workPool.addSplit(driverId, csvSplit);
        }

        workPool.noMoreSplits(driverId);

        int totalScanWorks = 0;
        ScanWork<ColumnarSplit, Chunk> work;
        while ((work = workPool.pickUp(driverId)) != null) {
            System.out.println(work.getWorkId());

            Assert.assertEquals(expected.get(totalScanWorks), work.getWorkId());
            totalScanWorks++;
        }
    }

    private static List<String> expected = new ArrayList<>();

    static {
        expected.add("orc-file_0-unit_0");
        expected.add("orc-file_1-unit_0");
        expected.add("orc-file_2-unit_0");
        expected.add("orc-file_3-unit_0");
        expected.add("orc-file_4-unit_0");
        expected.add("orc-file_5-unit_0");
        expected.add("csv-file_0-unit_0");
        expected.add("csv-file_0-unit_1");
        expected.add("csv-file_0-unit_2");
        expected.add("csv-file_0-unit_3");
        expected.add("csv-file_0-unit_4");
        expected.add("csv-file_1-unit_0");
        expected.add("csv-file_1-unit_1");
        expected.add("csv-file_1-unit_2");
        expected.add("csv-file_1-unit_3");
        expected.add("csv-file_1-unit_4");
        expected.add("orc-file_0-unit_1");
        expected.add("orc-file_1-unit_1");
        expected.add("orc-file_2-unit_1");
        expected.add("orc-file_3-unit_1");
        expected.add("orc-file_4-unit_1");
        expected.add("orc-file_5-unit_1");
        expected.add("orc-file_0-unit_2");
        expected.add("orc-file_1-unit_2");
        expected.add("orc-file_2-unit_2");
        expected.add("orc-file_3-unit_2");
        expected.add("orc-file_4-unit_2");
        expected.add("orc-file_5-unit_2");
        expected.add("orc-file_0-unit_3");
        expected.add("orc-file_1-unit_3");
        expected.add("orc-file_2-unit_3");
        expected.add("orc-file_3-unit_3");
        expected.add("orc-file_4-unit_3");
        expected.add("orc-file_5-unit_3");
        expected.add("orc-file_0-unit_4");
        expected.add("orc-file_1-unit_4");
        expected.add("orc-file_2-unit_4");
        expected.add("orc-file_3-unit_4");
        expected.add("orc-file_4-unit_4");
        expected.add("orc-file_5-unit_4");
        expected.add("orc-file_0-unit_5");
        expected.add("orc-file_1-unit_5");
        expected.add("orc-file_2-unit_5");
        expected.add("orc-file_3-unit_5");
        expected.add("orc-file_4-unit_5");
        expected.add("orc-file_5-unit_5");
        expected.add("orc-file_0-unit_6");
        expected.add("orc-file_1-unit_6");
        expected.add("orc-file_2-unit_6");
        expected.add("orc-file_3-unit_6");
        expected.add("orc-file_4-unit_6");
        expected.add("orc-file_5-unit_6");
        expected.add("orc-file_0-unit_7");
        expected.add("orc-file_1-unit_7");
        expected.add("orc-file_2-unit_7");
        expected.add("orc-file_3-unit_7");
        expected.add("orc-file_4-unit_7");
        expected.add("orc-file_5-unit_7");
    }
}

