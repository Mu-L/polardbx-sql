package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DateBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.operator.SpilledTopNExec;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.topnutils.LongIndexRow;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DateTopNHeapTest {
    private ExecutionContext context;
    private ConcurrentLinkedQueue<Chunk.ChunkRow> checkResult;
    private Random random;

    @Before
    public void before() {
        context = new ExecutionContext();
        context.setTraceId("mock_trace_id");
        context.setMemoryPool(
            MemoryManager.getInstance().createQueryMemoryPool(true, context.getTraceId(), context.getExtraCmds()));

        checkResult = new ConcurrentLinkedQueue<>();
        random = new Random();
    }

    @Test
    public void testOneHeapAsc() {
        doTestOneHeap(true);
    }

    @Test
    public void testOneHeapDesc() {
        doTestOneHeap(false);
    }

    @Test
    public void testMultiHeapAsc() {
        doTestMultiHeap(true);
    }

    @Test
    public void testMultiHeapDesc() {
        doTestMultiHeap(false);
    }

    @Test
    public void testMultiThreadHeapAsc() throws InterruptedException {
        doTestMultiThreadHeap(true);
    }

    @Test
    public void testMultiThreadHeapDesc() throws InterruptedException {
        doTestMultiThreadHeap(false);
    }

    private void doTestOneHeap(boolean asc) {
        final long topSize = 1000;
        final int chunkLimit = 1000;

        List<DataType> sourceTypes = ImmutableList.of(
            DataTypes.LongType,
            DataTypes.DateType, // sort column.
            DataTypes.LongType
        );

        // sort column id = 0.
        OrderByOption orderByOption = new OrderByOption(1, asc, true);

        SpillerFactory spillerFactory = Mockito.mock(SpillerFactory.class);

        GlobalTopNThreshold globalTopNThreshold = new DateTopNHeap.LongGlobalTopNThresholdImpl((int) topSize, asc);

        long topN = topSize;

        int compactThreshold = SpilledTopNExec.COMPACT_THRESHOLD;

        boolean spillEnabled = spillerFactory != null;
        MemoryPool memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool("top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);

        SpillMonitor spillMonitor = Mockito.mock(SpillMonitor.class);

        Long limitedFetch = null;
        boolean inputSorted = false;

        SettableFuture<GlobalTopNThreshold> parentThresholdFuture = SettableFuture.create();
        OperatorStatistics operatorStatistics = new OperatorStatistics();

        final DateTopNHeap dateTopNHeap =
            new DateTopNHeap(sourceTypes, orderByOption, spillerFactory, globalTopNThreshold, topN,
                compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, inputSorted,
                parentThresholdFuture, operatorStatistics, 0);

        MemoryCountable.checkDeviation(dateTopNHeap, 0d, true);

        for (int x = 0; x < 10; x++) {
            Chunk chunk = generateChunk(chunkLimit);
            dateTopNHeap.processChunk(chunk);

            dateTopNHeap.first(LongIndexRow.class);
        }
        MemoryCountable.checkDeviation(dateTopNHeap, 0d, true);

        // intTopNHeap.startMemoryRevoke();

        // intTopNHeap.finishMemoryRevoke();

        dateTopNHeap.buildResult();
        MemoryCountable.checkDeviation(dateTopNHeap, 0d, true);

        Comparator<Chunk.ChunkRow> rowComparator =
            ExecUtils.getAssertedSameTypeComparator(ImmutableList.of(orderByOption), sourceTypes);
        ArrayList<Chunk.ChunkRow> list = new ArrayList<>(checkResult);
        Collections.sort(list, (r1, r2) -> rowComparator.compare(r1, r2));
        Iterator<Chunk.ChunkRow> checkResultIterator = list.iterator();

        Chunk result;
        while ((result = dateTopNHeap.nextChunk()) != null) {
            for (int i = 0; i < result.getPositionCount(); i++) {

                Chunk.ChunkRow actual = result.rowAt(i);

                Chunk.ChunkRow expected = checkResultIterator.hasNext() ? checkResultIterator.next() : null;

                try {
                    Assert.assertTrue(expected.compareAssertedSameType(1, actual, 1) == 0);
                } catch (Throwable t) {

                    PrintStream printStream = System.out;
                    printStream.println("dump");
                    Iterator<Chunk.ChunkRow> iterator = list.iterator();
                    while (iterator.hasNext()) {
                        printStream.println(iterator.next());
                    }

                    PrintStream printStream1 = System.out;
                    printStream1.println("dump");
                    for (int j = 0; j < result.getPositionCount(); j++) {
                        Chunk.ChunkRow actual1 = result.rowAt(j);
                        printStream1.println(actual1);
                    }

                    throw t;
                }

            }
        }

        MemoryCountable.checkDeviation(dateTopNHeap, 0d, true);

        Assert.assertFalse(dateTopNHeap.useLimitedFetch());

        dateTopNHeap.close();
    }

    private void doTestMultiHeap(boolean asc) {
        final long topSize = 10000;
        final int chunkLimit = 1000;

        List<DataType> sourceTypes = ImmutableList.of(
            DataTypes.LongType,
            DataTypes.DateType,// sort column.
            DataTypes.LongType
        );

        // sort column id = 0.
        OrderByOption orderByOption = new OrderByOption(1, asc, true);

        SpillerFactory spillerFactory = Mockito.mock(SpillerFactory.class);

        long topN = topSize;

        int compactThreshold = SpilledTopNExec.COMPACT_THRESHOLD;

        boolean spillEnabled = spillerFactory != null;
        MemoryPool memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool("top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);

        SpillMonitor spillMonitor = Mockito.mock(SpillMonitor.class);

        Long limitedFetch = null;
        boolean inputSorted = false;

        GlobalTopNThreshold parentThreshold = new DateTopNHeap.LongGlobalTopNThresholdImpl((int) topSize, asc);
        SettableFuture<GlobalTopNThreshold> parentThresholdFuture = SettableFuture.create();
        parentThresholdFuture.set(parentThreshold);

        OperatorStatistics operatorStatistics = new OperatorStatistics();

        DateTopNHeap[] dateTopNHeaps = new DateTopNHeap[5];
        for (int parallelism = 0; parallelism < 5; parallelism++) {
            GlobalTopNThreshold globalTopNThreshold = new DateTopNHeap.LongGlobalTopNThresholdImpl((int) topSize, true);
            final DateTopNHeap dateTopNHeap =
                new DateTopNHeap(sourceTypes, orderByOption, spillerFactory, globalTopNThreshold, topN,
                    compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, inputSorted,
                    parentThresholdFuture, operatorStatistics, parallelism);
            dateTopNHeaps[parallelism] = dateTopNHeap;
        }

        for (int parallelism = 0; parallelism < 5; parallelism++) {
            MemoryCountable.checkDeviation(dateTopNHeaps[parallelism], 0d, true);
        }

        final DateTopNHeap parentTopN =
            new DateTopNHeap(sourceTypes, orderByOption, spillerFactory, parentThreshold, topN,
                compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, true,
                null, operatorStatistics, 0);

        for (int parallelism = 0; parallelism < 5; parallelism++) {
            DateTopNHeap dateTopNHeap = dateTopNHeaps[parallelism];

            for (int x = 0; x < 100; x++) {
                Chunk chunk = generateChunk(chunkLimit);
                dateTopNHeap.processChunk(chunk);

                dateTopNHeap.first(LongIndexRow.class);
            }

            dateTopNHeap.buildResult();

            Assert.assertFalse(dateTopNHeap.useLimitedFetch());

            Chunk result;
            while ((result = dateTopNHeap.nextChunk()) != null) {
                synchronized (parentTopN) {
                    parentTopN.processChunk(result);
                }
            }

            dateTopNHeap.close();
        }

        for (int parallelism = 0; parallelism < 5; parallelism++) {
            MemoryCountable.checkDeviation(dateTopNHeaps[parallelism], 0d, true);
        }

        MemoryCountable.checkDeviation(parentTopN, 0d, true);

        // intTopNHeap.startMemoryRevoke();

        // intTopNHeap.finishMemoryRevoke();

        parentTopN.buildResult();
        Comparator<Chunk.ChunkRow> rowComparator =
            ExecUtils.getAssertedSameTypeComparator(ImmutableList.of(orderByOption), sourceTypes);
        ArrayList<Chunk.ChunkRow> list = new ArrayList<>(checkResult);
        Collections.sort(list, (r1, r2) -> rowComparator.compare(r1, r2));
        Iterator<Chunk.ChunkRow> checkResultIterator = list.iterator();

        Chunk result;
        while ((result = parentTopN.nextChunk()) != null) {
            for (int i = 0; i < result.getPositionCount(); i++) {
                Chunk.ChunkRow actual = result.rowAt(i);

                Chunk.ChunkRow expected = checkResultIterator.hasNext() ? checkResultIterator.next() : null;

                try {
                    Assert.assertTrue(expected.compareAssertedSameType(1, actual, 1) == 0);
                } catch (Throwable t) {

                    PrintStream printStream = System.out;
                    printStream.println("dump");
                    Iterator<Chunk.ChunkRow> iterator = list.iterator();
                    while (iterator.hasNext()) {
                        printStream.println(iterator.next());
                    }

                    PrintStream printStream1 = System.out;
                    printStream1.println("dump");
                    for (int j = 0; j < result.getPositionCount(); j++) {
                        Chunk.ChunkRow actual1 = result.rowAt(j);
                        printStream1.println(actual1);
                    }

                    throw t;
                }
            }
        }

    }

    private void doTestMultiThreadHeap(boolean asc) throws InterruptedException {
        final long topSize = 1000;
        final int chunkLimit = 1000;

        List<DataType> sourceTypes = ImmutableList.of(
            DataTypes.LongType,
            DataTypes.DateType, // sort column.
            DataTypes.LongType
        );

        // sort column id = 0.
        OrderByOption orderByOption = new OrderByOption(1, asc, true);

        SpillerFactory spillerFactory = Mockito.mock(SpillerFactory.class);

        long topN = topSize;

        int compactThreshold = SpilledTopNExec.COMPACT_THRESHOLD;

        boolean spillEnabled = spillerFactory != null;
        MemoryPool memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool("top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);

        SpillMonitor spillMonitor = Mockito.mock(SpillMonitor.class);

        Long limitedFetch = null;
        boolean inputSorted = false;

        GlobalTopNThreshold parentThreshold = new DateTopNHeap.LongGlobalTopNThresholdImpl((int) topSize, asc);
        SettableFuture<GlobalTopNThreshold> parentThresholdFuture = SettableFuture.create();
        parentThresholdFuture.set(parentThreshold);

        OperatorStatistics operatorStatistics = new OperatorStatistics();

        DateTopNHeap[] dateTopNHeaps = new DateTopNHeap[5];
        for (int parallelism = 0; parallelism < 5; parallelism++) {
            GlobalTopNThreshold globalTopNThreshold = new DateTopNHeap.LongGlobalTopNThresholdImpl((int) topSize, asc);
            final DateTopNHeap dateTopNHeap =
                new DateTopNHeap(sourceTypes, orderByOption, spillerFactory, globalTopNThreshold, topN,
                    compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, inputSorted,
                    parentThresholdFuture, operatorStatistics, parallelism);
            dateTopNHeaps[parallelism] = dateTopNHeap;
        }

        final DateTopNHeap parentTopN =
            new DateTopNHeap(sourceTypes, orderByOption, spillerFactory, parentThreshold, topN,
                compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, true,
                null, operatorStatistics, 0);

        ExecutorService executor = Executors.newFixedThreadPool(8);

        CountDownLatch latch = new CountDownLatch(5);
        executor.submit(() -> {
            try {
                for (int parallelism = 0; parallelism < 5; parallelism++) {
                    DateTopNHeap dateTopNHeap = dateTopNHeaps[parallelism];

                    for (int x = 0; x < 100; x++) {
                        Chunk chunk = generateChunk(chunkLimit);
                        dateTopNHeap.processChunk(chunk);

                        dateTopNHeap.first(LongIndexRow.class);
                    }

                    dateTopNHeap.buildResult();

                    Assert.assertFalse(dateTopNHeap.useLimitedFetch());

                    Chunk result;
                    while ((result = dateTopNHeap.nextChunk()) != null) {
                        synchronized (parentTopN) {
                            parentTopN.processChunk(result);
                        }
                    }

                    dateTopNHeap.close();
                    latch.countDown();
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }

        });

        latch.await(10, TimeUnit.SECONDS);

        // intTopNHeap.startMemoryRevoke();

        // intTopNHeap.finishMemoryRevoke();

        parentTopN.buildResult();
        Comparator<Chunk.ChunkRow> rowComparator =
            ExecUtils.getAssertedSameTypeComparator(ImmutableList.of(orderByOption), sourceTypes);
        ArrayList<Chunk.ChunkRow> list = new ArrayList<>(checkResult);
        Collections.sort(list, (r1, r2) -> rowComparator.compare(r1, r2));
        Iterator<Chunk.ChunkRow> checkResultIterator = list.iterator();

        Chunk result;
        while ((result = parentTopN.nextChunk()) != null) {
            for (int i = 0; i < result.getPositionCount(); i++) {

                Chunk.ChunkRow actual = result.rowAt(i);

                Chunk.ChunkRow expected = checkResultIterator.hasNext() ? checkResultIterator.next() : null;

                try {
                    Assert.assertTrue(expected.compareAssertedSameType(1, actual, 1) == 0);
                } catch (Throwable t) {

                    PrintStream printStream = System.out;
                    printStream.println("dump");
                    Iterator<Chunk.ChunkRow> iterator = list.iterator();
                    while (iterator.hasNext()) {
                        printStream.println(iterator.next());
                    }

                    PrintStream printStream1 = System.out;
                    printStream1.println("dump");
                    for (int j = 0; j < result.getPositionCount(); j++) {
                        Chunk.ChunkRow actual1 = result.rowAt(j);
                        printStream1.println(actual1);
                    }

                    throw t;
                }
            }
        }

    }

    private Chunk generateChunk(int positionCount) {
        BlockBuilder longBlockBuilder = new LongBlockBuilder(positionCount);
        DateBlockBuilder dateBlockBuilder = new DateBlockBuilder(positionCount, DataTypes.DateType, context);
        BlockBuilder longBlockBuilder2 = new LongBlockBuilder(positionCount);

        for (int i = 0; i < positionCount; i++) {
            longBlockBuilder.writeLong(i);

            if (random.nextInt(5) == 1) {
                MysqlDateTime t = new MysqlDateTime(
                    1990 + random.nextInt(35),
                    1 + random.nextInt(12),
                    1 + random.nextInt(28),
                    0, 0, 0, 0
                );
                dateBlockBuilder.writeMysqlDatetime(t);
            }

            longBlockBuilder2.writeLong(i);
        }

        Chunk chunk = new Chunk(
            longBlockBuilder.build(),
            dateBlockBuilder.build(),
            longBlockBuilder2.build()
        );

        for (int i = 0; i < positionCount; i++) {
            checkResult.add(chunk.rowAt(i));
        }
        return chunk;
    }
}
