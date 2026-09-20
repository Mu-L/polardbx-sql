package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.executor.operator.scan.ColumnarMemoryPermitManager;
import com.alibaba.polardbx.executor.operator.scan.impl.DefaultScanPreProcessor;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Test class for ColumnarScanExec cancel mechanism
 */
public class ColumnarScanExecCancelTest {

    @Mock
    private OSSTableScan ossTableScan;

    @Mock
    private ExecutionContext context;

    @Mock
    private ParamManager paramManager;

    @Mock
    private MemoryPool memoryPool;

    @Mock
    private MemoryAllocatorCtx memoryAllocatorCtx;

    @Mock
    private OrcTableScan orcTableScan;

    private ColumnarScanExec columnarScanExec;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        // Setup mocks
        when(context.getParamManager()).thenReturn(paramManager);
        when(context.getMemoryPool()).thenReturn(memoryPool);
        when(memoryPool.getMemoryAllocatorCtx()).thenReturn(memoryAllocatorCtx);
        when(memoryPool.getOrCreatePool(anyString(), any())).thenReturn(memoryPool);

        when(paramManager.getInt(ConnectionParams.CHUNK_SIZE)).thenReturn(1000);
        when(paramManager.getInt(ConnectionParams.COLUMNAR_WORK_UNIT)).thenReturn(100);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_VERBOSE_METRICS_REPORT)).thenReturn(false);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_COLUMNAR_METRICS)).thenReturn(false);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_INDEX_PRUNING)).thenReturn(true);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_COLUMNAR_DEBUG)).thenReturn(false);

        when(ossTableScan.getOrcNode()).thenReturn(orcTableScan);
        when(ossTableScan.getRelatedId()).thenReturn(1);
        when(orcTableScan.getFilters()).thenReturn(com.google.common.collect.ImmutableList.of());

        columnarScanExec = new ColumnarScanExec(ossTableScan, context, new ArrayList<>(),
            Mockito.mock(ExecutorService.class), Mockito.mock(ColumnarMemoryPermitManager.class));
    }

    @Test
    public void testPreheatCloseFutureInitialization() {
        // Test that preheatCloseFuture is properly initialized
        assertNotNull("PreheatCloseFuture should be initialized", columnarScanExec.preheatCloseFuture);
        assertFalse("PreheatCloseFuture should not be done initially", columnarScanExec.preheatCloseFuture.isDone());
    }

    @Test
    public void testDoCloseSetsPreheatCloseFuture() {
        // Test that doClose() properly sets the preheatCloseFuture
        ListenableFuture<?> originalFuture = columnarScanExec.preheatCloseFuture;
        // Verify initial state
        assertFalse("Future should not be done initially", originalFuture.isDone());

        // Call doClose
        columnarScanExec.doClose();

        // Verify that the future is now completed
        assertTrue("Future should be done after doClose", originalFuture.isDone());
    }

    @Test
    public void testGetPreProcessorWithPreheatCloseFuture() {
        // Setup mocks
        OssSplit ossSplit = mock(OssSplit.class);
        TableMeta tableMeta = mock(TableMeta.class);
        FileSystem fileSystem = mock(FileSystem.class);
        Configuration configuration = new Configuration();
        ColumnarManager columnarManager = mock(ColumnarManager.class);

        when(ossSplit.getDeltaReadOption()).thenReturn(null);
        when(ossSplit.getCheckpointTso()).thenReturn(1L);
        when(tableMeta.getColumnarFieldIdList(1L)).thenReturn(new ArrayList<>());
        when(tableMeta.getColumnarSortKeys(1L)).thenReturn(new ArrayList<>());
        when(ossTableScan.isFlashbackQuery()).thenReturn(false);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_COLUMNAR_CSV_CACHE)).thenReturn(true);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_COLUMNAR_DEL_CACHE)).thenReturn(true);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_PARALLEL_PREHEAT_FILE_META)).thenReturn(true);

        // Create a custom SettableFuture to test
        SettableFuture<Void> testFuture = SettableFuture.create();

        // Execute
        try {
            DefaultScanPreProcessor preProcessor = columnarScanExec.getPreProcessor(
                ossSplit, "testSchema", "testTable", tableMeta, fileSystem, configuration, columnarManager, testFuture
            );
        } catch (Throwable e) {

        }
    }

    @Test
    public void testMultipleDoCloseCallsAreSafe() {
        // Test that multiple calls to doClose() are safe
        ListenableFuture<?> originalFuture = columnarScanExec.preheatCloseFuture;
        // Call doClose multiple times
        columnarScanExec.doClose();
        assertTrue("Future should be done after first doClose", originalFuture.isDone());

        columnarScanExec.doClose();
        assertTrue("Future should still be done after second doClose", originalFuture.isDone());

        columnarScanExec.doClose();
        assertTrue("Future should still be done after third doClose", originalFuture.isDone());
    }
}