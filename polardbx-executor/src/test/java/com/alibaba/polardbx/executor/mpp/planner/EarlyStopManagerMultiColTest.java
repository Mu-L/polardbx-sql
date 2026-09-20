package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.operator.util.GlobalTopNThreshold;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

public class EarlyStopManagerMultiColTest {
    @Test
    public void testRowTotalDesc() {
        // top-n order by : A int desc, B bigint desc
        // scan sort key: A int asc, B bigint asc (col num = 1, 3)
        List<OrderByOption> topNOrderByOptions = new ArrayList<>();
        topNOrderByOptions.add(new OrderByOption(0, false, true));
        topNOrderByOptions.add(new OrderByOption(1, false, true));

        List<String> columnNames = new ArrayList<>();
        columnNames.add("A");
        columnNames.add("B");

        final int topSize = 1_000_000;

        // table meta
        final String logicalSchema = "db1";
        final String logicalTable = "tb1";

        List<OrderByOption> sortOptions = new ArrayList<>();
        sortOptions.add(new OrderByOption(1, true, true));
        sortOptions.add(new OrderByOption(3, true, true));

        EarlyStopManager earlyStopManager = new EarlyStopManagerImpl(
            topNOrderByOptions, columnNames, topSize
        );
        Assert.assertEquals(topSize, earlyStopManager.getTopNSize());

        ExecutionContext context = Mockito.mock(ExecutionContext.class);

        // logical view
        OSSTableScan logicalView = Mockito.mock(OSSTableScan.class);
        Mockito.when(logicalView.getSchemaName()).thenReturn(logicalSchema);
        Mockito.when(logicalView.getLogicalTableName()).thenReturn(logicalTable);

        // schema & table
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        Mockito.when(context.getSchemaManager("db1")).thenReturn(schemaManager);
        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        Mockito.when(schemaManager.getTable("tb1")).thenReturn(tableMeta);

        // columns
        List<ColumnMeta> columnMetas = new ArrayList<>();
        ColumnMeta columnMeta1 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta1.getName()).thenReturn("col-xx");
        ColumnMeta columnMeta2 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta2.getName()).thenReturn("A"); // ref 1
        ColumnMeta columnMeta3 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta3.getName()).thenReturn("col-zz");
        ColumnMeta columnMeta4 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta4.getName()).thenReturn("B"); // ref 3
        ColumnMeta columnMeta5 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta5.getName()).thenReturn("col-ll");
        ColumnMeta columnMeta6 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta6.getName()).thenReturn("D"); // ref 5
        columnMetas.add(columnMeta1);
        columnMetas.add(columnMeta2);
        columnMetas.add(columnMeta3);
        columnMetas.add(columnMeta4);
        columnMetas.add(columnMeta5);
        columnMetas.add(columnMeta6);
        Mockito.when(tableMeta.getAllColumns()).thenReturn(columnMetas);

        // columnar manager
        Mockito.when(logicalView.isColumnarIndex()).thenReturn(true);
        try (MockedStatic<ColumnarManager> columnarManagerMockedStatic = Mockito.mockStatic(ColumnarManager.class)) {
            DynamicColumnarManager columnarManager = Mockito.mock(DynamicColumnarManager.class);
            columnarManagerMockedStatic.when(ColumnarManager::getInstance).thenReturn(columnarManager);
            Mockito.when(columnarManager.getTableId(anyLong(), anyString(), anyString(), any(TableMeta.class)))
                .thenReturn(1L);
            Mockito.when(columnarManager.latestTso()).thenReturn(0L);
            Mockito.when(tableMeta.getColumnarSortKeys(1L)).thenReturn(sortOptions);

            earlyStopManager.check(logicalView, context);

            // check result.
            System.out.println(earlyStopManager.getOrderRelation());
            Assert.assertEquals("(DESC, [A DESC, B DESC])", earlyStopManager.getOrderRelation().toString());
            Assert.assertTrue(earlyStopManager.isDesc());
            Assert.assertTrue(earlyStopManager.isEnabled());

            // register global threshold
            GlobalTopNThreshold globalTopNThreshold = Mockito.mock(GlobalTopNThreshold.class);
            Mockito.when(globalTopNThreshold.getTopNThresholdType())
                .thenReturn(GlobalTopNThreshold.TopNThresholdType.ROW);
            earlyStopManager.registerThreshold(globalTopNThreshold);

            // scan order by options
            RelDataType relDataType = Mockito.mock(RelDataType.class);
            List<String> fieldNames = new ArrayList<>();
            fieldNames.add("A");
            fieldNames.add("B");
            Mockito.when(logicalView.getOutputColumnOriginalNames()).thenReturn(fieldNames);
            OrcTableScan orcTableScan = Mockito.mock(OrcTableScan.class);
            Mockito.when(logicalView.getOrcNode()).thenReturn(orcTableScan);
            ImmutableList<Integer> outProjectIndexes = ImmutableList.of(0, 1);
            Mockito.when(orcTableScan.getOutProjects()).thenReturn(outProjectIndexes);
            List<OrderByOption> scanOrderByOptions = earlyStopManager.getOrderByOptionsForScan(logicalView);

            // data from scan.
            int comparisonPosition = 999;
            String workId = "ScanWork$trace1" + "$file1" + "$0" + "$0"; // file1, stripe0, row-group0
            Chunk chunk = Mockito.mock(Chunk.class);

            // Threshold has not been initialized, don't early stop.
            Mockito.when(globalTopNThreshold.isInitialized()).thenReturn(false);
            boolean needEarlyStop =
                earlyStopManager.needEarlyStop(chunk, scanOrderByOptions, comparisonPosition, workId);
            Assert.assertFalse(needEarlyStop);

            // When mode is desc, threshold < current row, don't early stop.
            Chunk.ChunkRow r1 = Mockito.mock(Chunk.ChunkRow.class);
            Chunk.ChunkRow r2 = Mockito.mock(Chunk.ChunkRow.class);
            Mockito.when(chunk.rowAt(anyInt())).thenReturn(r1);
            Mockito.when(globalTopNThreshold.getRowThreshold()).thenReturn(r2);

            Mockito.when(r1.compareAssertedSameType(0, r2, 0)).thenReturn(0); // r1 col0= r2 col0
            Mockito.when(r1.compareAssertedSameType(1, r2, 1)).thenReturn(1); // r1 col1> r2 col1

            Mockito.when(globalTopNThreshold.isInitialized()).thenReturn(true);
            needEarlyStop = earlyStopManager.needEarlyStop(chunk, scanOrderByOptions, comparisonPosition, workId);
            Assert.assertFalse(needEarlyStop);

            // When mode is desc, threshold > current row, early stop.
            Mockito.when(r1.compareAssertedSameType(0, r2, 0)).thenReturn(0); // r1 col0= r2 col0
            Mockito.when(r1.compareAssertedSameType(1, r2, 1)).thenReturn(-1); // r1 col1< r2 col1

            Mockito.when(globalTopNThreshold.isInitialized()).thenReturn(true);
            needEarlyStop = earlyStopManager.needEarlyStop(chunk, scanOrderByOptions, comparisonPosition, workId);
            Assert.assertTrue(needEarlyStop);

            // register this work id.
            earlyStopManager.registerEarlyStop(workId);

            // Check if work id recorded.
            String checkedWorkId = "ScanWork$trace1" + "$file1" + "$1" + "$0";
            Assert.assertTrue(earlyStopManager.isEarlyStopRegistered(checkedWorkId));

            checkedWorkId = "ScanWork$trace1" + "$file1" + "$3" + "$6";
            Assert.assertTrue(earlyStopManager.isEarlyStopRegistered(checkedWorkId));

            checkedWorkId = "ScanWork$trace1" + "$file2" + "$1" + "$0";
            Assert.assertFalse(
                earlyStopManager.isEarlyStopRegistered(checkedWorkId)); // don't early stop for different file.
        }
    }

    @Test
    public void testRowPrefixDesc() {
        // top-n order by : A int desc, B bigint desc, C int asc
        // scan sort key: A int asc, B bigint asc, D int asc (col num = 1, 3, 5)
        List<OrderByOption> topNOrderByOptions = new ArrayList<>();
        topNOrderByOptions.add(new OrderByOption(0, false, true));
        topNOrderByOptions.add(new OrderByOption(1, false, true));
        topNOrderByOptions.add(new OrderByOption(2, true, true));

        List<String> columnNames = new ArrayList<>();
        columnNames.add("A");
        columnNames.add("B");
        columnNames.add("C");

        final int topSize = 1_000_000;

        // table meta
        final String logicalSchema = "db1";
        final String logicalTable = "tb1";

        List<OrderByOption> sortOptions = new ArrayList<>();
        sortOptions.add(new OrderByOption(1, true, true));
        sortOptions.add(new OrderByOption(3, true, true));
        sortOptions.add(new OrderByOption(5, true, true));

        EarlyStopManager earlyStopManager = new EarlyStopManagerImpl(
            topNOrderByOptions, columnNames, topSize
        );
        Assert.assertEquals(topSize, earlyStopManager.getTopNSize());

        ExecutionContext context = Mockito.mock(ExecutionContext.class);

        // logical view
        OSSTableScan logicalView = Mockito.mock(OSSTableScan.class);
        Mockito.when(logicalView.getSchemaName()).thenReturn(logicalSchema);
        Mockito.when(logicalView.getLogicalTableName()).thenReturn(logicalTable);

        // schema & table
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        Mockito.when(context.getSchemaManager("db1")).thenReturn(schemaManager);
        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        Mockito.when(schemaManager.getTable("tb1")).thenReturn(tableMeta);

        // columns
        List<ColumnMeta> columnMetas = new ArrayList<>();
        ColumnMeta columnMeta1 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta1.getName()).thenReturn("col-xx");
        ColumnMeta columnMeta2 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta2.getName()).thenReturn("A"); // ref 1
        ColumnMeta columnMeta3 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta3.getName()).thenReturn("col-zz");
        ColumnMeta columnMeta4 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta4.getName()).thenReturn("B"); // ref 3
        ColumnMeta columnMeta5 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta5.getName()).thenReturn("col-ll");
        ColumnMeta columnMeta6 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta6.getName()).thenReturn("D"); // ref 5
        columnMetas.add(columnMeta1);
        columnMetas.add(columnMeta2);
        columnMetas.add(columnMeta3);
        columnMetas.add(columnMeta4);
        columnMetas.add(columnMeta5);
        columnMetas.add(columnMeta6);
        Mockito.when(tableMeta.getAllColumns()).thenReturn(columnMetas);

        // columnar manager
        Mockito.when(logicalView.isColumnarIndex()).thenReturn(true);
        try (MockedStatic<ColumnarManager> columnarManagerMockedStatic = Mockito.mockStatic(ColumnarManager.class)) {
            DynamicColumnarManager columnarManager = Mockito.mock(DynamicColumnarManager.class);
            columnarManagerMockedStatic.when(ColumnarManager::getInstance).thenReturn(columnarManager);
            Mockito.when(columnarManager.getTableId(anyLong(), anyString(), anyString(), any(TableMeta.class)))
                .thenReturn(1L);
            Mockito.when(columnarManager.latestTso()).thenReturn(0L);
            Mockito.when(tableMeta.getColumnarSortKeys(1L)).thenReturn(sortOptions);

            earlyStopManager.check(logicalView, context);

            // check result.
            System.out.println(earlyStopManager.getOrderRelation());
            Assert.assertEquals("(PREFIX_DESC, [A DESC, B DESC])", earlyStopManager.getOrderRelation().toString());
            Assert.assertTrue(earlyStopManager.isDesc());
            Assert.assertTrue(earlyStopManager.isEnabled());

            // register global threshold
            GlobalTopNThreshold globalTopNThreshold = Mockito.mock(GlobalTopNThreshold.class);
            Mockito.when(globalTopNThreshold.getTopNThresholdType())
                .thenReturn(GlobalTopNThreshold.TopNThresholdType.ROW);
            earlyStopManager.registerThreshold(globalTopNThreshold);

            // scan order by options
            RelDataType relDataType = Mockito.mock(RelDataType.class);
            List<String> fieldNames = new ArrayList<>();
            fieldNames.add("A");
            fieldNames.add("B");
            fieldNames.add("D");
            Mockito.when(logicalView.getOutputColumnOriginalNames()).thenReturn(fieldNames);
            OrcTableScan orcTableScan = Mockito.mock(OrcTableScan.class);
            Mockito.when(logicalView.getOrcNode()).thenReturn(orcTableScan);
            ImmutableList<Integer> outProjectIndexes = ImmutableList.of(0, 1, 3);
            Mockito.when(orcTableScan.getOutProjects()).thenReturn(outProjectIndexes);
            List<OrderByOption> scanOrderByOptions = earlyStopManager.getOrderByOptionsForScan(logicalView);

            // data from scan.
            int comparisonPosition = 999;
            String workId = "ScanWork$trace1" + "$file1" + "$0" + "$0"; // file1, stripe0, row-group0
            Chunk chunk = Mockito.mock(Chunk.class);

            // Threshold has not been initialized, don't early stop.
            Mockito.when(globalTopNThreshold.isInitialized()).thenReturn(false);
            boolean needEarlyStop =
                earlyStopManager.needEarlyStop(chunk, scanOrderByOptions, comparisonPosition, workId);
            Assert.assertFalse(needEarlyStop);

            // When mode is desc, threshold < current row, don't early stop.
            Chunk.ChunkRow r1 = Mockito.mock(Chunk.ChunkRow.class);
            Chunk.ChunkRow r2 = Mockito.mock(Chunk.ChunkRow.class);
            Mockito.when(chunk.rowAt(anyInt())).thenReturn(r1);
            Mockito.when(globalTopNThreshold.getRowThreshold()).thenReturn(r2);

            Mockito.when(r1.compareAssertedSameType(0, r2, 0)).thenReturn(0); // r1 col0= r2 col0
            Mockito.when(r1.compareAssertedSameType(1, r2, 1)).thenReturn(1); // r1 col1> r2 col1

            Mockito.when(globalTopNThreshold.isInitialized()).thenReturn(true);
            needEarlyStop = earlyStopManager.needEarlyStop(chunk, scanOrderByOptions, comparisonPosition, workId);
            Assert.assertFalse(needEarlyStop);

            // When mode is desc, threshold > current row, early stop.
            Mockito.when(r1.compareAssertedSameType(0, r2, 0)).thenReturn(0); // r1 col0= r2 col0
            Mockito.when(r1.compareAssertedSameType(1, r2, 1)).thenReturn(-1); // r1 col1> r2 col1

            Mockito.when(globalTopNThreshold.isInitialized()).thenReturn(true);
            needEarlyStop = earlyStopManager.needEarlyStop(chunk, scanOrderByOptions, comparisonPosition, workId);
            Assert.assertTrue(needEarlyStop);

            // register this work id.
            earlyStopManager.registerEarlyStop(workId);

            // Check if work id recorded.
            String checkedWorkId = "ScanWork$trace1" + "$file1" + "$1" + "$0";
            Assert.assertTrue(earlyStopManager.isEarlyStopRegistered(checkedWorkId));

            checkedWorkId = "ScanWork$trace1" + "$file1" + "$3" + "$6";
            Assert.assertTrue(earlyStopManager.isEarlyStopRegistered(checkedWorkId));

            checkedWorkId = "ScanWork$trace1" + "$file2" + "$1" + "$0";
            Assert.assertFalse(
                earlyStopManager.isEarlyStopRegistered(checkedWorkId)); // don't early stop for different file.
        }
    }

    @Test
    public void testRowTotalAsc() {
        // top-n order by : A int asc, B bigint asc
        // scan sort key: A int asc, B bigint asc (col num = 1, 3)
        List<OrderByOption> topNOrderByOptions = new ArrayList<>();
        topNOrderByOptions.add(new OrderByOption(0, true, true));
        topNOrderByOptions.add(new OrderByOption(1, true, true));

        List<String> columnNames = new ArrayList<>();
        columnNames.add("A");
        columnNames.add("B");

        final int topSize = 1_000_000;

        // table meta
        final String logicalSchema = "db1";
        final String logicalTable = "tb1";

        List<OrderByOption> sortOptions = new ArrayList<>();
        sortOptions.add(new OrderByOption(1, true, true));
        sortOptions.add(new OrderByOption(3, true, true));

        EarlyStopManager earlyStopManager = new EarlyStopManagerImpl(
            topNOrderByOptions, columnNames, topSize
        );
        Assert.assertEquals(topSize, earlyStopManager.getTopNSize());

        ExecutionContext context = Mockito.mock(ExecutionContext.class);

        // logical view
        OSSTableScan logicalView = Mockito.mock(OSSTableScan.class);
        Mockito.when(logicalView.getSchemaName()).thenReturn(logicalSchema);
        Mockito.when(logicalView.getLogicalTableName()).thenReturn(logicalTable);

        // schema & table
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        Mockito.when(context.getSchemaManager("db1")).thenReturn(schemaManager);
        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        Mockito.when(schemaManager.getTable("tb1")).thenReturn(tableMeta);

        // columns
        List<ColumnMeta> columnMetas = new ArrayList<>();
        ColumnMeta columnMeta1 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta1.getName()).thenReturn("col-xx");
        ColumnMeta columnMeta2 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta2.getName()).thenReturn("A"); // ref 1
        ColumnMeta columnMeta3 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta3.getName()).thenReturn("col-zz");
        ColumnMeta columnMeta4 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta4.getName()).thenReturn("B"); // ref 3
        ColumnMeta columnMeta5 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta5.getName()).thenReturn("col-ll");
        ColumnMeta columnMeta6 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta6.getName()).thenReturn("D"); // ref 5
        columnMetas.add(columnMeta1);
        columnMetas.add(columnMeta2);
        columnMetas.add(columnMeta3);
        columnMetas.add(columnMeta4);
        columnMetas.add(columnMeta5);
        columnMetas.add(columnMeta6);
        Mockito.when(tableMeta.getAllColumns()).thenReturn(columnMetas);

        // columnar manager
        Mockito.when(logicalView.isColumnarIndex()).thenReturn(true);
        try (MockedStatic<ColumnarManager> columnarManagerMockedStatic = Mockito.mockStatic(ColumnarManager.class)) {
            DynamicColumnarManager columnarManager = Mockito.mock(DynamicColumnarManager.class);
            columnarManagerMockedStatic.when(ColumnarManager::getInstance).thenReturn(columnarManager);
            Mockito.when(columnarManager.getTableId(anyLong(), anyString(), anyString(), any(TableMeta.class)))
                .thenReturn(1L);
            Mockito.when(columnarManager.latestTso()).thenReturn(0L);
            Mockito.when(tableMeta.getColumnarSortKeys(1L)).thenReturn(sortOptions);

            earlyStopManager.check(logicalView, context);

            // check result.
            System.out.println(earlyStopManager.getOrderRelation());
            Assert.assertEquals("(ASC, [A ASC, B ASC])", earlyStopManager.getOrderRelation().toString());
            Assert.assertFalse(earlyStopManager.isDesc());
            Assert.assertTrue(earlyStopManager.isEnabled());

            // register global threshold
            GlobalTopNThreshold globalTopNThreshold = Mockito.mock(GlobalTopNThreshold.class);
            Mockito.when(globalTopNThreshold.getTopNThresholdType())
                .thenReturn(GlobalTopNThreshold.TopNThresholdType.ROW);
            earlyStopManager.registerThreshold(globalTopNThreshold);

            // scan order by options
            RelDataType relDataType = Mockito.mock(RelDataType.class);
            List<String> fieldNames = new ArrayList<>();
            fieldNames.add("A");
            fieldNames.add("B");
            Mockito.when(logicalView.getOutputColumnOriginalNames()).thenReturn(fieldNames);
            OrcTableScan orcTableScan = Mockito.mock(OrcTableScan.class);
            Mockito.when(logicalView.getOrcNode()).thenReturn(orcTableScan);
            ImmutableList<Integer> outProjectIndexes = ImmutableList.of(0, 1);
            Mockito.when(orcTableScan.getOutProjects()).thenReturn(outProjectIndexes);
            List<OrderByOption> scanOrderByOptions = earlyStopManager.getOrderByOptionsForScan(logicalView);

            // data from scan.
            int comparisonPosition = 0;
            String workId = "ScanWork$trace1" + "$file1" + "$0" + "$0"; // file1, stripe0, row-group0
            Chunk chunk = Mockito.mock(Chunk.class);

            // Threshold has not been initialized, don't early stop.
            Mockito.when(globalTopNThreshold.isInitialized()).thenReturn(false);
            boolean needEarlyStop =
                earlyStopManager.needEarlyStop(chunk, scanOrderByOptions, comparisonPosition, workId);
            Assert.assertFalse(needEarlyStop);

            // When mode is asc, threshold > current row, don't early stop.
            Chunk.ChunkRow r1 = Mockito.mock(Chunk.ChunkRow.class);
            Chunk.ChunkRow r2 = Mockito.mock(Chunk.ChunkRow.class);
            Mockito.when(chunk.rowAt(anyInt())).thenReturn(r1);
            Mockito.when(globalTopNThreshold.getRowThreshold()).thenReturn(r2);

            Mockito.when(r1.compareAssertedSameType(0, r2, 0)).thenReturn(0); // r1 col0= r2 col0
            Mockito.when(r1.compareAssertedSameType(1, r2, 1)).thenReturn(-1); // r1 col1 < r2 col1

            Mockito.when(globalTopNThreshold.isInitialized()).thenReturn(true);
            needEarlyStop = earlyStopManager.needEarlyStop(chunk, scanOrderByOptions, comparisonPosition, workId);
            Assert.assertFalse(needEarlyStop);

            // When mode is desc, threshold < current row, early stop.
            Mockito.when(r1.compareAssertedSameType(0, r2, 0)).thenReturn(0); // r1 col0= r2 col0
            Mockito.when(r1.compareAssertedSameType(1, r2, 1)).thenReturn(1); // r1 col1> r2 col1

            Mockito.when(globalTopNThreshold.isInitialized()).thenReturn(true);
            needEarlyStop = earlyStopManager.needEarlyStop(chunk, scanOrderByOptions, comparisonPosition, workId);
            Assert.assertTrue(needEarlyStop);

            // register this work id.
            earlyStopManager.registerEarlyStop(workId);

            // Check if work id recorded.
            String checkedWorkId = "ScanWork$trace1" + "$file1" + "$1" + "$0";
            Assert.assertTrue(earlyStopManager.isEarlyStopRegistered(checkedWorkId));

            checkedWorkId = "ScanWork$trace1" + "$file1" + "$3" + "$6";
            Assert.assertTrue(earlyStopManager.isEarlyStopRegistered(checkedWorkId));

            checkedWorkId = "ScanWork$trace1" + "$file2" + "$1" + "$0";
            Assert.assertFalse(
                earlyStopManager.isEarlyStopRegistered(checkedWorkId)); // don't early stop for different file.
        }
    }

    @Test
    public void testRowPrefixAsc() {
        // top-n order by : A int asc, B bigint asc, C int asc
        // scan sort key: A int asc, B bigint asc, D int asc (col num = 1, 3, 5)
        List<OrderByOption> topNOrderByOptions = new ArrayList<>();
        topNOrderByOptions.add(new OrderByOption(0, true, true));
        topNOrderByOptions.add(new OrderByOption(1, true, true));
        topNOrderByOptions.add(new OrderByOption(2, true, true));

        List<String> columnNames = new ArrayList<>();
        columnNames.add("A");
        columnNames.add("B");
        columnNames.add("C");

        final int topSize = 1_000_000;

        // table meta
        final String logicalSchema = "db1";
        final String logicalTable = "tb1";

        List<OrderByOption> sortOptions = new ArrayList<>();
        sortOptions.add(new OrderByOption(1, true, true));
        sortOptions.add(new OrderByOption(3, true, true));
        sortOptions.add(new OrderByOption(5, true, true));

        EarlyStopManager earlyStopManager = new EarlyStopManagerImpl(
            topNOrderByOptions, columnNames, topSize
        );
        Assert.assertEquals(topSize, earlyStopManager.getTopNSize());

        ExecutionContext context = Mockito.mock(ExecutionContext.class);

        // logical view
        OSSTableScan logicalView = Mockito.mock(OSSTableScan.class);
        Mockito.when(logicalView.getSchemaName()).thenReturn(logicalSchema);
        Mockito.when(logicalView.getLogicalTableName()).thenReturn(logicalTable);

        // schema & table
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        Mockito.when(context.getSchemaManager("db1")).thenReturn(schemaManager);
        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        Mockito.when(schemaManager.getTable("tb1")).thenReturn(tableMeta);

        // columns
        List<ColumnMeta> columnMetas = new ArrayList<>();
        ColumnMeta columnMeta1 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta1.getName()).thenReturn("col-xx");
        ColumnMeta columnMeta2 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta2.getName()).thenReturn("A"); // ref 1
        ColumnMeta columnMeta3 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta3.getName()).thenReturn("col-zz");
        ColumnMeta columnMeta4 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta4.getName()).thenReturn("B"); // ref 3
        ColumnMeta columnMeta5 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta5.getName()).thenReturn("col-ll");
        ColumnMeta columnMeta6 = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta6.getName()).thenReturn("D"); // ref 5
        columnMetas.add(columnMeta1);
        columnMetas.add(columnMeta2);
        columnMetas.add(columnMeta3);
        columnMetas.add(columnMeta4);
        columnMetas.add(columnMeta5);
        columnMetas.add(columnMeta6);
        Mockito.when(tableMeta.getAllColumns()).thenReturn(columnMetas);

        // columnar manager
        Mockito.when(logicalView.isColumnarIndex()).thenReturn(true);
        try (MockedStatic<ColumnarManager> columnarManagerMockedStatic = Mockito.mockStatic(ColumnarManager.class)) {
            DynamicColumnarManager columnarManager = Mockito.mock(DynamicColumnarManager.class);
            columnarManagerMockedStatic.when(ColumnarManager::getInstance).thenReturn(columnarManager);
            Mockito.when(columnarManager.getTableId(anyLong(), anyString(), anyString(), any(TableMeta.class)))
                .thenReturn(1L);
            Mockito.when(columnarManager.latestTso()).thenReturn(0L);
            Mockito.when(tableMeta.getColumnarSortKeys(1L)).thenReturn(sortOptions);

            earlyStopManager.check(logicalView, context);

            // check result.
            System.out.println(earlyStopManager.getOrderRelation());
            Assert.assertEquals("(PREFIX_ASC, [A ASC, B ASC])", earlyStopManager.getOrderRelation().toString());
            Assert.assertTrue(!earlyStopManager.isDesc());
            Assert.assertTrue(earlyStopManager.isEnabled());

            // register global threshold
            GlobalTopNThreshold globalTopNThreshold = Mockito.mock(GlobalTopNThreshold.class);
            Mockito.when(globalTopNThreshold.getTopNThresholdType())
                .thenReturn(GlobalTopNThreshold.TopNThresholdType.ROW);
            earlyStopManager.registerThreshold(globalTopNThreshold);

            // scan order by options
            RelDataType relDataType = Mockito.mock(RelDataType.class);
            List<String> fieldNames = new ArrayList<>();
            fieldNames.add("A");
            fieldNames.add("B");
            fieldNames.add("D");
            Mockito.when(logicalView.getOutputColumnOriginalNames()).thenReturn(fieldNames);
            OrcTableScan orcTableScan = Mockito.mock(OrcTableScan.class);
            Mockito.when(logicalView.getOrcNode()).thenReturn(orcTableScan);
            ImmutableList<Integer> outProjectIndexes = ImmutableList.of(0, 1, 3);
            Mockito.when(orcTableScan.getOutProjects()).thenReturn(outProjectIndexes);
            List<OrderByOption> scanOrderByOptions = earlyStopManager.getOrderByOptionsForScan(logicalView);

            // data from scan.
            int comparisonPosition = 999;
            String workId = "ScanWork$trace1" + "$file1" + "$0" + "$0"; // file1, stripe0, row-group0
            Chunk chunk = Mockito.mock(Chunk.class);

            // Threshold has not been initialized, don't early stop.
            Mockito.when(globalTopNThreshold.isInitialized()).thenReturn(false);
            boolean needEarlyStop =
                earlyStopManager.needEarlyStop(chunk, scanOrderByOptions, comparisonPosition, workId);
            Assert.assertFalse(needEarlyStop);

            // When mode is asc, threshold > current row, don't early stop.
            Chunk.ChunkRow r1 = Mockito.mock(Chunk.ChunkRow.class);
            Chunk.ChunkRow r2 = Mockito.mock(Chunk.ChunkRow.class);
            Mockito.when(chunk.rowAt(anyInt())).thenReturn(r1);
            Mockito.when(globalTopNThreshold.getRowThreshold()).thenReturn(r2);

            Mockito.when(r1.compareAssertedSameType(0, r2, 0)).thenReturn(0); // r1 col0= r2 col0
            Mockito.when(r1.compareAssertedSameType(1, r2, 1)).thenReturn(-1); // r1 col1 <  r2 col1

            Mockito.when(globalTopNThreshold.isInitialized()).thenReturn(true);
            needEarlyStop = earlyStopManager.needEarlyStop(chunk, scanOrderByOptions, comparisonPosition, workId);
            Assert.assertFalse(needEarlyStop);

            // When mode is asc, threshold < current row, early stop.
            Mockito.when(r1.compareAssertedSameType(0, r2, 0)).thenReturn(0); // r1 col0= r2 col0
            Mockito.when(r1.compareAssertedSameType(1, r2, 1)).thenReturn(1); // r1 col1> r2 col1

            Mockito.when(globalTopNThreshold.isInitialized()).thenReturn(true);
            needEarlyStop = earlyStopManager.needEarlyStop(chunk, scanOrderByOptions, comparisonPosition, workId);
            Assert.assertTrue(needEarlyStop);

            // register this work id.
            earlyStopManager.registerEarlyStop(workId);

            // Check if work id recorded.
            String checkedWorkId = "ScanWork$trace1" + "$file1" + "$1" + "$0";
            Assert.assertTrue(earlyStopManager.isEarlyStopRegistered(checkedWorkId));

            checkedWorkId = "ScanWork$trace1" + "$file1" + "$3" + "$6";
            Assert.assertTrue(earlyStopManager.isEarlyStopRegistered(checkedWorkId));

            checkedWorkId = "ScanWork$trace1" + "$file2" + "$1" + "$0";
            Assert.assertFalse(
                earlyStopManager.isEarlyStopRegistered(checkedWorkId)); // don't early stop for different file.
        }
    }
}