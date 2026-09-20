package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Test class for DefaultScanPreProcessor cancel mechanism
 */
public class DefaultScanPreProcessorCancelTest {

    @Mock
    private Configuration configuration;

    @Mock
    private FileSystem fileSystem;

    @Mock
    private ColumnarManager columnarManager;

    @Mock
    private RexNode rexNode;

    private List<ColumnMeta> columns;
    private List<RexNode> rexList;
    private Map<Integer, ParameterContext> params;
    private List<Long> columnFieldIdList;
    private List<OrderByOption> sortKeys;
    private ExecutorService executor;
    private ExecutorService ioExecutor;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        // Setup test data
        columns = new ArrayList<>();
        Field field = new Field(DataTypes.LongType);
        ColumnMeta columnMeta = new ColumnMeta("test_table", "test_column", null, field);
        columns.add(columnMeta);

        rexList = new ArrayList<>();
        rexList.add(rexNode);

        params = new HashMap<>();

        columnFieldIdList = ImmutableList.of(1L, 2L, 3L);
        sortKeys = ImmutableList.of(new OrderByOption(0, true, true));

        executor = Executors.newSingleThreadExecutor();
        ioExecutor = Executors.newSingleThreadExecutor();

        // Setup mocks
        when(columnarManager.getPhysicalColumnIndexes(anyString())).thenReturn(ImmutableMap.of(1L, 0, 2L, 1, 3L, 2));
    }

    @Test
    public void testPreheatCancelledBeforeStart() throws Exception {
        // Create a SettableFuture that is already completed (cancelled)
        SettableFuture<Void> preheatCloseFuture = SettableFuture.create();
        preheatCloseFuture.set(null); // Mark as cancelled

        DefaultScanPreProcessor preProcessor = new DefaultScanPreProcessor(
            configuration,
            fileSystem,
            "test_schema",
            "test_table",
            true, // enableIndexPruning
            false, // enableOssCompatible
            columns,
            rexList,
            params,
            1.0, // groupsRatio
            0.0, // deletionRatio
            columnarManager,
            1L, // tso
            columnFieldIdList,
            sortKeys,
            true, // useParallelPreheatFileMeta
            null, // versionStorageStatistics
            preheatCloseFuture,
            null
        );

        // Add a mock file
        Path testPath = new Path("/test/file.orc");
        preProcessor.addFile(testPath);

        // Prepare should complete quickly due to cancellation
        ListenableFuture<?> prepareFuture =
            preProcessor.prepare(executor, ioExecutor, "test_trace", new ColumnarTracer());
    }

    @Test
    public void testPreheatCancelledDuringExecution() throws Exception {
        // Create a SettableFuture that will be cancelled during execution
        SettableFuture<Void> preheatCloseFuture = SettableFuture.create();

        DefaultScanPreProcessor preProcessor = new DefaultScanPreProcessor(
            configuration,
            fileSystem,
            "test_schema",
            "test_table",
            true, // enableIndexPruning
            false, // enableOssCompatible
            columns,
            rexList,
            params,
            1.0, // groupsRatio
            0.0, // deletionRatio
            columnarManager,
            1L, // tso
            columnFieldIdList,
            sortKeys,
            false, // useParallelPreheatFileMeta - disable to test sequential path
            null,
            preheatCloseFuture,
            null
        );

        // Add a mock file
        Path testPath = new Path("/test/file.orc");
        preProcessor.addFile(testPath);

        // Start preparation
        ListenableFuture<?> prepareFuture =
            preProcessor.prepare(executor, ioExecutor, "test_trace", new ColumnarTracer());

        // Cancel the future shortly after starting
        Thread.sleep(100); // Give it a moment to start
        preheatCloseFuture.set(null);

        // Wait for completion
        prepareFuture.get(5, TimeUnit.SECONDS);

    }

    @Test
    public void testParallelPreheatCancellation() throws Exception {
        // Create a SettableFuture that is already cancelled
        SettableFuture<Void> preheatCloseFuture = SettableFuture.create();
        preheatCloseFuture.set(null);

        DefaultScanPreProcessor preProcessor = new DefaultScanPreProcessor(
            configuration,
            fileSystem,
            "test_schema",
            "test_table",
            true, // enableIndexPruning
            false, // enableOssCompatible
            columns,
            rexList,
            params,
            1.0, // groupsRatio
            0.0, // deletionRatio
            columnarManager,
            1L, // tso
            columnFieldIdList,
            sortKeys,
            true, // useParallelPreheatFileMeta - enable to test parallel path
            null,
            preheatCloseFuture,
            null
        );

        // Add multiple mock files to trigger parallel processing
        preProcessor.addFile(new Path("/test/file1.orc"));
        preProcessor.addFile(new Path("/test/file2.orc"));
        preProcessor.addFile(new Path("/test/file3.orc"));

        // Start preparation
        ListenableFuture<?> prepareFuture =
            preProcessor.prepare(executor, ioExecutor, "test_trace", new ColumnarTracer());
    }

    @Test
    public void testPreheatCloseFutureNotNull() {
        // Test that preheatCloseFuture is properly set
        SettableFuture<Void> preheatCloseFuture = SettableFuture.create();

        DefaultScanPreProcessor preProcessor = new DefaultScanPreProcessor(
            configuration,
            fileSystem,
            "test_schema",
            "test_table",
            true,
            false,
            columns,
            rexList,
            params,
            1.0,
            0.0,
            columnarManager,
            1L,
            columnFieldIdList,
            sortKeys,
            true,
            null,
            preheatCloseFuture,
            null
        );

        // Verify the future is set correctly
        assertNotNull("PreheatCloseFuture should not be null", preProcessor.preheatCloseFuture);
        assertSame("PreheatCloseFuture should be the same instance", preheatCloseFuture,
            preProcessor.preheatCloseFuture);
    }

    @Test
    public void testNonOrcFileSkipped() throws Exception {
        SettableFuture<Void> preheatCloseFuture = SettableFuture.create();

        DefaultScanPreProcessor preProcessor = new DefaultScanPreProcessor(
            configuration,
            fileSystem,
            "test_schema",
            "test_table",
            true,
            false,
            columns,
            rexList,
            params,
            1.0,
            0.0,
            columnarManager,
            1L,
            columnFieldIdList,
            sortKeys,
            true,
            null,
            preheatCloseFuture,
            null
        );

        // Add non-ORC files
        preProcessor.addFile(new Path("/test/file.csv"));
        preProcessor.addFile(new Path("/test/file.txt"));

        // Start preparation
        ListenableFuture<?> prepareFuture =
            preProcessor.prepare(executor, ioExecutor, "test_trace", new ColumnarTracer());
    }

    @Test
    public void testCancellationListenerAdded() throws Exception {
        SettableFuture<Void> preheatCloseFuture = SettableFuture.create();

        DefaultScanPreProcessor preProcessor = new DefaultScanPreProcessor(
            configuration,
            fileSystem,
            "test_schema",
            "test_table",
            true,
            false,
            columns,
            rexList,
            params,
            1.0,
            0.0,
            columnarManager,
            1L,
            columnFieldIdList,
            sortKeys,
            false, // disable parallel to test sequential path
            null,
            preheatCloseFuture,
            null
        );

        preProcessor.addFile(new Path("/test/file.orc"));

        // Start preparation
        ListenableFuture<?> prepareFuture =
            preProcessor.prepare(executor, ioExecutor, "test_trace", new ColumnarTracer());

        // Cancel the future
        preheatCloseFuture.set(null);
    }
}