package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.operator.SpilledTopNExec;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
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

public class DecimalTopNHeapTest {
    private ExecutionContext context;
    private ConcurrentLinkedQueue<Chunk.ChunkRow> checkResult;
    private Random random;
    private DecimalType decimalType = new DecimalType(30, 5);

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
            decimalType, // sort column.
            DataTypes.LongType
        );

        // sort column id = 0.
        OrderByOption orderByOption = new OrderByOption(1, asc, true);

        SpillerFactory spillerFactory = Mockito.mock(SpillerFactory.class);

        GlobalTopNThreshold globalTopNThreshold =
            new DecimalTopNHeap.RowGlobalThresholdImpl(sourceTypes, ImmutableList.of(orderByOption), (int) topSize);

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

        final DecimalTopNHeap decimalTopNHeap =
            new DecimalTopNHeap(sourceTypes, orderByOption, spillerFactory, globalTopNThreshold, topN,
                compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, inputSorted,
                parentThresholdFuture, operatorStatistics, 0);

        MemoryCountable.checkDeviation(decimalTopNHeap, 0d, true);

        for (int x = 0; x < 10; x++) {
            Chunk chunk = generateChunk(chunkLimit);
            decimalTopNHeap.processChunk(chunk);

            decimalTopNHeap.first(Chunk.ChunkRow.class);
        }

        // intTopNHeap.startMemoryRevoke();

        // intTopNHeap.finishMemoryRevoke();

        MemoryCountable.checkDeviation(decimalTopNHeap, 0d, true);
        decimalTopNHeap.buildResult();
        MemoryCountable.checkDeviation(decimalTopNHeap, 0d, true);

        Comparator<Chunk.ChunkRow> rowComparator =
            ExecUtils.getAssertedSameTypeComparator(ImmutableList.of(orderByOption), sourceTypes);
        ArrayList<Chunk.ChunkRow> list = new ArrayList<>(checkResult);
        Collections.sort(list, (r1, r2) -> rowComparator.compare(r1, r2));
        Iterator<Chunk.ChunkRow> checkResultIterator = list.iterator();

        Chunk result;
        while ((result = decimalTopNHeap.nextChunk()) != null) {
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

        MemoryCountable.checkDeviation(decimalTopNHeap, 0d, true);

        Assert.assertFalse(decimalTopNHeap.useLimitedFetch());

        decimalTopNHeap.close();
        MemoryCountable.checkDeviation(decimalTopNHeap, 0d, true);
    }

    private void doTestMultiHeap(boolean asc) {
        final long topSize = 10000;
        final int chunkLimit = 1000;

        List<DataType> sourceTypes = ImmutableList.of(
            DataTypes.LongType,
            decimalType,// sort column.
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

        GlobalTopNThreshold parentThreshold =
            new DecimalTopNHeap.RowGlobalThresholdImpl(sourceTypes, ImmutableList.of(orderByOption), (int) topSize);
        SettableFuture<GlobalTopNThreshold> parentThresholdFuture = SettableFuture.create();
        parentThresholdFuture.set(parentThreshold);

        OperatorStatistics operatorStatistics = new OperatorStatistics();

        DecimalTopNHeap[] decimalTopNHeaps = new DecimalTopNHeap[5];
        for (int parallelism = 0; parallelism < 5; parallelism++) {
            GlobalTopNThreshold globalTopNThreshold =
                new DecimalTopNHeap.RowGlobalThresholdImpl(sourceTypes, ImmutableList.of(orderByOption), (int) topSize);
            final DecimalTopNHeap decimalTopNHeap =
                new DecimalTopNHeap(sourceTypes, orderByOption, spillerFactory, globalTopNThreshold, topN,
                    compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, inputSorted,
                    parentThresholdFuture, operatorStatistics, parallelism);
            decimalTopNHeaps[parallelism] = decimalTopNHeap;
        }

        final DecimalTopNHeap parentTopN =
            new DecimalTopNHeap(sourceTypes, orderByOption, spillerFactory, parentThreshold, topN,
                compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, true,
                null, operatorStatistics, 0);

        for (int parallelism = 0; parallelism < 5; parallelism++) {
            MemoryCountable.checkDeviation(decimalTopNHeaps[parallelism], 0d, true);
        }

        MemoryCountable.checkDeviation(parentTopN, 0d, true);

        for (int parallelism = 0; parallelism < 5; parallelism++) {
            DecimalTopNHeap decimalTopNHeap = decimalTopNHeaps[parallelism];

            for (int x = 0; x < 100; x++) {
                Chunk chunk = generateChunk(chunkLimit);
                decimalTopNHeap.processChunk(chunk);

                decimalTopNHeap.first(Chunk.ChunkRow.class);
            }

            decimalTopNHeap.buildResult();

            Assert.assertFalse(decimalTopNHeap.useLimitedFetch());

            Chunk result;
            while ((result = decimalTopNHeap.nextChunk()) != null) {
                synchronized (parentTopN) {
                    parentTopN.processChunk(result);
                }
            }

            decimalTopNHeap.close();
        }

        for (int parallelism = 0; parallelism < 5; parallelism++) {
            MemoryCountable.checkDeviation(decimalTopNHeaps[parallelism], 0d, true);
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

        MemoryCountable.checkDeviation(parentTopN, 0d, true);

    }

    private void doTestMultiThreadHeap(boolean asc) throws InterruptedException {
        final long topSize = 10000;
        final int chunkLimit = 1000;

        List<DataType> sourceTypes = ImmutableList.of(
            DataTypes.LongType,
            decimalType, // sort column.
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

        GlobalTopNThreshold parentThreshold =
            new DecimalTopNHeap.RowGlobalThresholdImpl(sourceTypes, ImmutableList.of(orderByOption), (int) topSize);
        SettableFuture<GlobalTopNThreshold> parentThresholdFuture = SettableFuture.create();
        parentThresholdFuture.set(parentThreshold);

        OperatorStatistics operatorStatistics = new OperatorStatistics();

        DecimalTopNHeap[] decimalTopNHeaps = new DecimalTopNHeap[5];
        for (int parallelism = 0; parallelism < 5; parallelism++) {
            GlobalTopNThreshold globalTopNThreshold =
                new DecimalTopNHeap.RowGlobalThresholdImpl(sourceTypes, ImmutableList.of(orderByOption), (int) topSize);
            final DecimalTopNHeap decimalTopNHeap =
                new DecimalTopNHeap(sourceTypes, orderByOption, spillerFactory, globalTopNThreshold, topN,
                    compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, inputSorted,
                    parentThresholdFuture, operatorStatistics, parallelism);
            decimalTopNHeaps[parallelism] = decimalTopNHeap;
        }

        final DecimalTopNHeap parentTopN =
            new DecimalTopNHeap(sourceTypes, orderByOption, spillerFactory, parentThreshold, topN,
                compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, true,
                null, operatorStatistics, 0);

        ExecutorService executor = Executors.newFixedThreadPool(8);

        CountDownLatch latch = new CountDownLatch(5);
        executor.submit(() -> {
            for (int parallelism = 0; parallelism < 5; parallelism++) {
                DecimalTopNHeap decimalTopNHeap = decimalTopNHeaps[parallelism];

                for (int x = 0; x < 100; x++) {
                    Chunk chunk = generateChunk(chunkLimit);
                    decimalTopNHeap.processChunk(chunk);

                    decimalTopNHeap.first(Chunk.ChunkRow.class);
                }

                decimalTopNHeap.buildResult();

                Assert.assertFalse(decimalTopNHeap.useLimitedFetch());

                Chunk result;
                while ((result = decimalTopNHeap.nextChunk()) != null) {
                    synchronized (parentTopN) {
                        parentTopN.processChunk(result);
                    }
                }

                decimalTopNHeap.close();
                latch.countDown();
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
        DecimalBlockBuilder decimalBlockBuilder = new DecimalBlockBuilder(positionCount, decimalType);
        BlockBuilder longBlockBuilder2 = new LongBlockBuilder(positionCount);

        for (int i = 0; i < positionCount; i++) {
            longBlockBuilder.writeLong(i);

            if (random.nextInt(5) == 1) {
                decimalBlockBuilder.appendNull();
            } else {
                byte[] decimalStr = randomDecimal(30, 5, false);
                Decimal d = Decimal.fromString(new String(decimalStr));
                decimalBlockBuilder.writeDecimal(d);
            }

            longBlockBuilder2.writeLong(i);
        }

        Chunk chunk = new Chunk(
            longBlockBuilder.build(),
            decimalBlockBuilder.build(),
            longBlockBuilder2.build()
        );

        for (int i = 0; i < positionCount; i++) {
            checkResult.add(chunk.rowAt(i));
        }
        return chunk;
    }

    private static final String NUMBER_STR = "0123456789";

    /**
     * Generate random decimal value in specified max precision & max scale.
     *
     * @param maxPrecision maximum precision.
     * @param maxScale maximum scale.
     * @param isSigned TRUE if use '-'.
     * @return random decimal value in bytes.
     */
    private byte[] randomDecimal(int maxPrecision, int maxScale, boolean isSigned) {
        int precision = random.nextInt(maxPrecision) + 1;
        int scale = random.nextInt(Math.min(precision, maxScale + 1));
        if (precision == scale) {
            scale--;
        }

        boolean isNeg = isSigned && random.nextInt() % 2 == 0;

        byte[] res = new byte[(scale == 0 ? precision : precision + 1) + (isNeg ? 1 : 0)];
        int i = 0;
        if (isNeg) {
            res[i++] = '-';
        }
        res[i++] = (byte) NUMBER_STR.charAt(random.nextInt(9) + 1);
        for (; i < precision - scale + (isNeg ? 1 : 0); i++) {
            res[i] = (byte) NUMBER_STR.charAt(random.nextInt(10));
        }
        if (scale == 0) {
            return res;
        }
        res[i++] = '.';
        for (; i < precision + 1 + (isNeg ? 1 : 0); i++) {
            res[i] = (byte) NUMBER_STR.charAt(random.nextInt(10));
        }
        return res;
    }
}
