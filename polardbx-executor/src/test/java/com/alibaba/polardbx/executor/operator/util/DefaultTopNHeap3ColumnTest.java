package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.chunk.TimestampBlockBuilder;
import com.alibaba.polardbx.executor.operator.SpilledTopNExec;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
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

public class DefaultTopNHeap3ColumnTest {
    private ExecutionContext context;
    private ConcurrentLinkedQueue<Chunk.ChunkRow> checkResult;
    private Random random;
    private DataType decimalType = new DecimalType(30, 5);
    private DataType datetimeType = new TimestampType(5);

    private final List<DataType> sourceTypes = ImmutableList.of(
        DataTypes.LongType,
        DataTypes.LongType, // sort column.
        DataTypes.LongType,
        decimalType, // sort column.
        datetimeType, // sort column.
        DataTypes.LongType
    );

    // sort column id = 0.
    private List<OrderByOption> orderByOptions = ImmutableList.of(
        new OrderByOption(1, true, true),
        new OrderByOption(3, true, true),
        new OrderByOption(4, true, true)
    );

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
    public void testOneHeap1() {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, true, true),
            new OrderByOption(3, true, true),
            new OrderByOption(4, true, true)
        );
        doTestOneHeap();
    }

    @Test
    public void testOneHeap2() {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, true, true),
            new OrderByOption(3, false, true),
            new OrderByOption(4, true, true)
        );
        doTestOneHeap();
    }

    @Test
    public void testOneHeap3() {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, true, true),
            new OrderByOption(3, true, true),
            new OrderByOption(4, false, true)
        );
        doTestOneHeap();
    }

    @Test
    public void testOneHeap4() {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, false, true),
            new OrderByOption(3, true, true),
            new OrderByOption(4, true, true)
        );
        doTestOneHeap();
    }

    @Test
    public void testOneHeap5() {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, false, true),
            new OrderByOption(3, true, true),
            new OrderByOption(4, false, true)
        );
        doTestOneHeap();
    }

    @Test
    public void testOneHeap6() {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, true, true),
            new OrderByOption(3, false, true),
            new OrderByOption(4, false, true)
        );
        doTestOneHeap();
    }

    @Test
    public void testMultiHeap1() {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, true, true),
            new OrderByOption(3, true, true),
            new OrderByOption(4, true, true)
        );
        doTestMultiHeap();
    }

    @Test
    public void testMultiHeap2() {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, true, true),
            new OrderByOption(3, false, true),
            new OrderByOption(4, true, true)
        );
        doTestMultiHeap();
    }

    @Test
    public void testMultiHeap3() {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, true, true),
            new OrderByOption(3, true, true),
            new OrderByOption(4, false, true)
        );
        doTestMultiHeap();
    }

    @Test
    public void testMultiHeap4() {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, false, true),
            new OrderByOption(3, true, true),
            new OrderByOption(4, true, true)
        );
        doTestMultiHeap();
    }

    @Test
    public void testMultiHeap5() {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, false, true),
            new OrderByOption(3, true, true),
            new OrderByOption(4, false, true)
        );
        doTestMultiHeap();
    }

    @Test
    public void testMultiHeap6() {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, true, true),
            new OrderByOption(3, false, true),
            new OrderByOption(4, false, true)
        );
        doTestMultiHeap();
    }

    @Test
    public void testMultiThreadHeap1() throws InterruptedException {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, true, true),
            new OrderByOption(3, true, true),
            new OrderByOption(4, true, true)
        );
        doTestMultiThreadHeap();
    }

    @Test
    public void testMultiThreadHeap2() throws InterruptedException {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, true, true),
            new OrderByOption(3, false, true),
            new OrderByOption(4, true, true)
        );
        doTestMultiThreadHeap();
    }

    @Test
    public void testMultiThreadHeap3() throws InterruptedException {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, true, true),
            new OrderByOption(3, true, true),
            new OrderByOption(4, false, true)
        );
        doTestMultiThreadHeap();
    }

    @Test
    public void testMultiThreadHeap4() throws InterruptedException {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, false, true),
            new OrderByOption(3, true, true),
            new OrderByOption(4, true, true)
        );
        doTestMultiThreadHeap();
    }

    @Test
    public void testMultiThreadHeap5() throws InterruptedException {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, false, true),
            new OrderByOption(3, true, true),
            new OrderByOption(4, false, true)
        );
        doTestMultiThreadHeap();
    }

    @Test
    public void testMultiThreadHeap6() throws InterruptedException {
        orderByOptions = ImmutableList.of(
            new OrderByOption(1, true, true),
            new OrderByOption(3, false, true),
            new OrderByOption(4, false, true)
        );
        doTestMultiThreadHeap();
    }

    private void doTestOneHeap() {
        final long topSize = 1000;
        final int chunkLimit = 1000;

        SpillerFactory spillerFactory = Mockito.mock(SpillerFactory.class);

        GlobalTopNThreshold globalTopNThreshold =
            new DefaultTopNHeap.RowGlobalThresholdImpl(sourceTypes, orderByOptions, (int) topSize);

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

        final DefaultTopNHeap defaultTopNHeap =
            new DefaultTopNHeap(sourceTypes, orderByOptions, globalTopNThreshold, spillerFactory, topN,
                compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, inputSorted,
                parentThresholdFuture, operatorStatistics, 0);
        MemoryCountable.checkDeviation(defaultTopNHeap, 0d, true);

        for (int x = 0; x < 10; x++) {
            Chunk chunk = generateChunk(chunkLimit);
            defaultTopNHeap.processChunk(chunk);

            defaultTopNHeap.first(Chunk.ChunkRow.class);
        }

        // intTopNHeap.startMemoryRevoke();

        // intTopNHeap.finishMemoryRevoke();

        MemoryCountable.checkDeviation(defaultTopNHeap, 0d, true);
        defaultTopNHeap.buildResult();
        MemoryCountable.checkDeviation(defaultTopNHeap, 0d, true);

        Comparator<Chunk.ChunkRow> rowComparator =
            ExecUtils.getAssertedSameTypeComparator(orderByOptions, sourceTypes);
        ArrayList<Chunk.ChunkRow> list = new ArrayList<>(checkResult);
        Collections.sort(list, (r1, r2) -> rowComparator.compare(r1, r2));
        Iterator<Chunk.ChunkRow> checkResultIterator = list.iterator();

        Chunk result;
        while ((result = defaultTopNHeap.nextChunk()) != null) {
            for (int i = 0; i < result.getPositionCount(); i++) {

                Chunk.ChunkRow actual = result.rowAt(i);

                Chunk.ChunkRow expected = checkResultIterator.hasNext() ? checkResultIterator.next() : null;

                try {
                    Assert.assertTrue(
                        expected.compareAssertedSameType(1, actual, 1) == 0
                            && expected.compareAssertedSameType(3, actual, 3) == 0
                            && expected.compareAssertedSameType(4, actual, 4) == 0
                    );
                } catch (Throwable t) {
                    System.out.println("dump");
                    Iterator<Chunk.ChunkRow> iterator = list.iterator();
                    while (iterator.hasNext()) {
                        System.out.println(iterator.next());
                    }
                    throw t;
                }

            }
        }
        MemoryCountable.checkDeviation(defaultTopNHeap, 0d, true);

        Assert.assertFalse(defaultTopNHeap.useLimitedFetch());

        defaultTopNHeap.close();
        MemoryCountable.checkDeviation(defaultTopNHeap, 0d, true);
    }

    private void doTestMultiHeap() {
        final long topSize = 10000;
        final int chunkLimit = 1000;

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
            new DefaultTopNHeap.RowGlobalThresholdImpl(sourceTypes, orderByOptions, (int) topSize);
        SettableFuture<GlobalTopNThreshold> parentThresholdFuture = SettableFuture.create();
        parentThresholdFuture.set(parentThreshold);

        OperatorStatistics operatorStatistics = new OperatorStatistics();

        DefaultTopNHeap[] defaultTopNHeaps = new DefaultTopNHeap[5];
        for (int parallelism = 0; parallelism < 5; parallelism++) {
            GlobalTopNThreshold globalTopNThreshold =
                new DefaultTopNHeap.RowGlobalThresholdImpl(sourceTypes, orderByOptions, (int) topSize);
            final DefaultTopNHeap defaultTopNHeap =
                new DefaultTopNHeap(sourceTypes, orderByOptions, globalTopNThreshold, spillerFactory,
                    topN,
                    compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, inputSorted,
                    parentThresholdFuture, operatorStatistics, parallelism);
            defaultTopNHeaps[parallelism] = defaultTopNHeap;
        }

        final DefaultTopNHeap parentTopN =
            new DefaultTopNHeap(sourceTypes, orderByOptions, parentThreshold, spillerFactory, topN,
                compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, true,
                null, operatorStatistics, 0);

        for (int parallelism = 0; parallelism < 5; parallelism++) {
            MemoryCountable.checkDeviation(defaultTopNHeaps[parallelism], 0d, true);
        }
        MemoryCountable.checkDeviation(parentTopN, 0d, true);

        for (int parallelism = 0; parallelism < 5; parallelism++) {
            DefaultTopNHeap defaultTopNHeap = defaultTopNHeaps[parallelism];

            for (int x = 0; x < 100; x++) {
                Chunk chunk = generateChunk(chunkLimit);
                defaultTopNHeap.processChunk(chunk);

                defaultTopNHeap.first(Chunk.ChunkRow.class);
            }

            defaultTopNHeap.buildResult();

            Assert.assertFalse(defaultTopNHeap.useLimitedFetch());

            Chunk result;
            while ((result = defaultTopNHeap.nextChunk()) != null) {
                synchronized (parentTopN) {
                    parentTopN.processChunk(result);
                }
            }

            defaultTopNHeap.close();
        }

        // intTopNHeap.startMemoryRevoke();

        // intTopNHeap.finishMemoryRevoke();

        parentTopN.buildResult();

        for (int parallelism = 0; parallelism < 5; parallelism++) {
            MemoryCountable.checkDeviation(defaultTopNHeaps[parallelism], 0d, true);
        }
        MemoryCountable.checkDeviation(parentTopN, 0d, true);

        Comparator<Chunk.ChunkRow> rowComparator =
            ExecUtils.getAssertedSameTypeComparator(orderByOptions, sourceTypes);
        ArrayList<Chunk.ChunkRow> list = new ArrayList<>(checkResult);
        Collections.sort(list, (r1, r2) -> rowComparator.compare(r1, r2));
        Iterator<Chunk.ChunkRow> checkResultIterator = list.iterator();

        Chunk result;
        while ((result = parentTopN.nextChunk()) != null) {
            for (int i = 0; i < result.getPositionCount(); i++) {

                Chunk.ChunkRow actual = result.rowAt(i);

                Chunk.ChunkRow expected = checkResultIterator.hasNext() ? checkResultIterator.next() : null;

                try {
                    Assert.assertTrue(
                        expected.compareAssertedSameType(1, actual, 1) == 0
                            && expected.compareAssertedSameType(3, actual, 3) == 0
                            && expected.compareAssertedSameType(4, actual, 4) == 0
                    );
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

    private void doTestMultiThreadHeap() throws InterruptedException {
        final long topSize = 10000;
        final int chunkLimit = 1000;

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
            new DefaultTopNHeap.RowGlobalThresholdImpl(sourceTypes, orderByOptions, (int) topSize);
        SettableFuture<GlobalTopNThreshold> parentThresholdFuture = SettableFuture.create();
        parentThresholdFuture.set(parentThreshold);

        OperatorStatistics operatorStatistics = new OperatorStatistics();

        DefaultTopNHeap[] defaultTopNHeaps = new DefaultTopNHeap[5];
        for (int parallelism = 0; parallelism < 5; parallelism++) {
            GlobalTopNThreshold globalTopNThreshold =
                new DefaultTopNHeap.RowGlobalThresholdImpl(sourceTypes, orderByOptions, (int) topSize);
            final DefaultTopNHeap defaultTopNHeap =
                new DefaultTopNHeap(sourceTypes, orderByOptions, globalTopNThreshold, spillerFactory,
                    topN,
                    compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, inputSorted,
                    parentThresholdFuture, operatorStatistics, parallelism);
            defaultTopNHeaps[parallelism] = defaultTopNHeap;
        }

        final DefaultTopNHeap parentTopN =
            new DefaultTopNHeap(sourceTypes, orderByOptions, parentThreshold, spillerFactory, topN,
                compactThreshold, memoryAllocator, chunkLimit, spillMonitor, context, limitedFetch, true,
                null, operatorStatistics, 0);

        ExecutorService executor = Executors.newFixedThreadPool(8);

        CountDownLatch latch = new CountDownLatch(5);
        executor.submit(() -> {
            for (int parallelism = 0; parallelism < 5; parallelism++) {
                DefaultTopNHeap defaultTopNHeap = defaultTopNHeaps[parallelism];

                for (int x = 0; x < 100; x++) {
                    Chunk chunk = generateChunk(chunkLimit);
                    defaultTopNHeap.processChunk(chunk);

                    defaultTopNHeap.first(Chunk.ChunkRow.class);
                }

                defaultTopNHeap.buildResult();

                Assert.assertFalse(defaultTopNHeap.useLimitedFetch());

                Chunk result;
                while ((result = defaultTopNHeap.nextChunk()) != null) {
                    synchronized (parentTopN) {
                        parentTopN.processChunk(result);
                    }
                }

                defaultTopNHeap.close();
                latch.countDown();
            }
        });

        latch.await(10, TimeUnit.SECONDS);

        // intTopNHeap.startMemoryRevoke();

        // intTopNHeap.finishMemoryRevoke();

        parentTopN.buildResult();

        Comparator<Chunk.ChunkRow> rowComparator =
            ExecUtils.getAssertedSameTypeComparator(orderByOptions, sourceTypes);
        ArrayList<Chunk.ChunkRow> list = new ArrayList<>(checkResult);
        Collections.sort(list, (r1, r2) -> rowComparator.compare(r1, r2));
        Iterator<Chunk.ChunkRow> checkResultIterator = list.iterator();

        Chunk result;
        while ((result = parentTopN.nextChunk()) != null) {
            for (int i = 0; i < result.getPositionCount(); i++) {

                Chunk.ChunkRow actual = result.rowAt(i);

                Chunk.ChunkRow expected = checkResultIterator.hasNext() ? checkResultIterator.next() : null;

                try {
                    Assert.assertTrue(
                        expected.compareAssertedSameType(1, actual, 1) == 0
                            && expected.compareAssertedSameType(3, actual, 3) == 0
                            && expected.compareAssertedSameType(4, actual, 4) == 0
                    );
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

    //        DataTypes.LongType,
    //        DataTypes.LongType, // sort column.
    //        DataTypes.LongType,
    //        decimalType, // sort column.
    //        datetimeType, // sort column.
    //        DataTypes.LongType
    private Chunk generateChunk(int positionCount) {
        BlockBuilder longBlockBuilder = new LongBlockBuilder(positionCount);
        BlockBuilder longBlockBuilder1 = new LongBlockBuilder(positionCount);
        BlockBuilder longBlockBuilder2 = new LongBlockBuilder(positionCount);
        BlockBuilder decimalBlockBuilder = new DecimalBlockBuilder(positionCount, decimalType);
        TimestampBlockBuilder datetimeBlockBuilder = new TimestampBlockBuilder(positionCount, datetimeType, context);
        BlockBuilder longBlockBuilder3 = new LongBlockBuilder(positionCount);

        for (int i = 0; i < positionCount; i++) {
            longBlockBuilder.writeLong(i);

            if (random.nextInt(5) == 1) {
                longBlockBuilder1.appendNull();
            } else {
                longBlockBuilder1.writeLong(random.nextLong());
            }

            longBlockBuilder2.writeLong(i);

            if (random.nextInt(5) == 1) {
                decimalBlockBuilder.appendNull();
            } else {
                byte[] decimalStr = randomDecimal(30, 5, false);
                Decimal d = Decimal.fromString(new String(decimalStr));
                decimalBlockBuilder.writeDecimal(d);
            }

            if (random.nextInt(5) == 1) {
                datetimeBlockBuilder.appendNull();
            } else {
                MysqlDateTime t = new MysqlDateTime(
                    1990 + random.nextInt(35),
                    1 + random.nextInt(12),
                    1 + random.nextInt(28),
                    random.nextInt(24),
                    random.nextInt(60),
                    random.nextInt(60),
                    random.nextInt(1000)
                );
                datetimeBlockBuilder.writeMysqlDatetime(t);
            }

            longBlockBuilder3.writeLong(i);
        }

        Chunk result = new Chunk(
            longBlockBuilder.build(),
            longBlockBuilder1.build(),
            longBlockBuilder2.build(),
            decimalBlockBuilder.build(),
            datetimeBlockBuilder.build(),
            longBlockBuilder3.build()
        );

        for (int i = 0; i < positionCount; i++) {
            checkResult.add(result.rowAt(i));
        }

        return result;
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
