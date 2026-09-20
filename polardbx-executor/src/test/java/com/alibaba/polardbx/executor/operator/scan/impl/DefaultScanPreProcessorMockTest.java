package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.orc.PreheatFileMeta;
import com.alibaba.polardbx.common.orc.PreheatMetaManager;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultScanPreProcessorMockTest {
    @Mock
    private Configuration configuration;

    @Mock
    private ColumnarManager columnarManager;

    @Test
    public void test() throws Throwable {
        // Arrange
        List<ColumnMeta> columns = new ArrayList<>();
        List<RexNode> rexList = new ArrayList<>();
        HashMap<Integer, ParameterContext> params = new HashMap<>();
        List<Long> columnFieldIdList = new ArrayList<>();

        DefaultScanPreProcessor processor = new DefaultScanPreProcessor(
            configuration,
            Mockito.mock(FileSystem.class),
            "schema",
            "table",
            true, // enableIndexPruning
            false, // enableOssCompatible
            columns,
            rexList,
            params,
            1.0,
            0.0,
            columnarManager,
            null,
            columnFieldIdList,
            new ArrayList<>(),
            true, // useParallelPreheatFileMeta
            null,
            SettableFuture.create(),
            null
        );

        // add 10 orc files
        List<Path> pathList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Path path = Mockito.mock(Path.class);
            when(path.getName()).thenReturn("file:///tmp/" + i + ".orc");

            pathList.add(path);
            processor.addFile(path);
        }
        // add 10 csv files
        for (int i = 0; i < 3; i++) {
            Path path = Mockito.mock(Path.class);
            when(path.getName()).thenReturn("file:///tmp/" + i + ".csv");

            pathList.add(path);
            processor.addFile(path);
        }

        ExecutorService scanExecutor = Executors.newFixedThreadPool(2);
        ExecutorService ioExecutor = Executors.newFixedThreadPool(2);
        ColumnarTracer tracer = mock(ColumnarTracer.class);

        try (MockedStatic<PreheatMetaManager> preheatMetaManagerMockedStatic = Mockito.mockStatic(
            PreheatMetaManager.class)) {
            PreheatMetaManager preheatMetaManager = Mockito.mock(PreheatMetaManager.class);

            preheatMetaManagerMockedStatic.when(PreheatMetaManager::getInstance).thenReturn(preheatMetaManager);

            when(preheatMetaManager.get(any(Path.class), any(FileSystem.class))).thenReturn(
                Mockito.mock(PreheatFileMeta.class));

            // Act
            ListenableFuture<?> future = processor.prepare(scanExecutor, ioExecutor, "traceId", tracer);
            future.get(); // block until complete
        }

    }
}