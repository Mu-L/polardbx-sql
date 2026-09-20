package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.ScanTestBase;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public class ColumnarSplitNextWorkTest extends ScanTestBase {
    protected final static ExecutorService SCAN_WORK_EXECUTOR = Executors.newFixedThreadPool(4);

    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    public static final int MORSEL_UNIT = 5;
    public static final double RATIO = .3D;

    // need trace_id or other session-level parameters.
    protected ExecutionContext context;

    // partial cache
    protected BlockCacheManager<Block> blockCacheManager;

    // NOTE: The inputRefs in RexNode must be in consistent with inputRefForFilter.
    private RexNode predicate;
    protected List<Integer> inputRefsForFilter;
    protected LazyEvaluator<Chunk, BitSet> evaluator;
    protected RoaringBitmap deletionBitmap;

    @Before
    public void prepare() throws IOException {
        context = new ExecutionContext();
        context.setTraceId(TRACE_ID);

        // Cached ranges
        final int cachedStripeId = 0;
        final int[] columnIds = new int[] {1, 2};
        final int[] cachedGroupIds = new int[] {0, 2};
        blockCacheManager = prepareCache(
            cachedStripeId, columnIds, cachedGroupIds
        );

        // ($0 + 10000L) >= 0L
        predicate = buildCondition(0,
            TddlOperatorTable.PLUS, 10000L,
            TddlOperatorTable.GREATER_THAN_OR_EQUAL, 0L
        );

        // NOTE: The inputRefs in RexNode must be in consistent with inputRefForFilter.
        evaluator = DefaultLazyEvaluator.builder()
            .setContext(context)
            .setRexNode(predicate)
            .setRatio(RATIO)
            .setInputTypes(ImmutableList.of(DataTypes.LongType)) // input type: bigint.
            .build();

        inputRefsForFilter = ImmutableList.of(0);

        deletionBitmap = buildDeletionBitmap((int) (1_000_000L / 2));
    }

    @Test
    public void testAsc1() throws Throwable {
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2),
            ImmutableList.of(
                "0$0",
                "0$1",
                "0$2"
            )
        );
    }

    @Test
    public void testAsc2() throws Throwable {
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));
        matrix.put(1, fromRowGroupIds(1, new int[] {2, 4, 7, 11, 19, 20}));
        matrix.put(2, fromRowGroupIds(2, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));
        matrix.put(3, fromRowGroupIds(3, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2, 3),
            ImmutableList.of(
                "0$0",
                "0$1",
                "0$2",
                "1$3",
                "1$4",
                "2$5",
                "2$6",
                "2$7",
                "3$8",
                "3$9",
                "3$10"
            )
        );
    }

    @Test
    public void testAsc3() throws Throwable {
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));
        matrix.put(1, fromRowGroupIds(1, new int[] {2, 4, 7, 11, 19, 20}));
        matrix.put(3, fromRowGroupIds(3, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2, 3),
            ImmutableList.of(
                "0$0",
                "0$1",
                "0$2",
                "1$3",
                "1$4",
                "3$5",
                "3$6",
                "3$7"
            )
        );
    }

    @Test
    public void testDesc1() throws Throwable {
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));

        doTest(
            true,
            matrix,
            ImmutableList.of(0, 1, 2),
            ImmutableList.of(
                "0$2",
                "0$1",
                "0$0"
            )
        );
    }

    @Test
    public void testDesc2() throws Throwable {
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));
        matrix.put(1, fromRowGroupIds(1, new int[] {2, 4, 7, 11, 19, 20}));
        matrix.put(2, fromRowGroupIds(2, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));
        matrix.put(3, fromRowGroupIds(3, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));

        doTest(
            true,
            matrix,
            ImmutableList.of(0, 1, 2, 3),
            ImmutableList.of(
                "3$10",
                "3$9",
                "3$8",
                "2$7",
                "2$6",
                "2$5",
                "1$4",
                "1$3",
                "0$2",
                "0$1",
                "0$0"
            )
        );
    }

    @Test
    public void testDesc3() throws Throwable {
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));
        matrix.put(1, fromRowGroupIds(1, new int[] {2, 4, 7, 11, 19, 20}));
        matrix.put(3, fromRowGroupIds(3, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));

        doTest(
            true,
            matrix,
            ImmutableList.of(0, 1, 2, 3),
            ImmutableList.of(
                "3$7",
                "3$6",
                "3$5",
                "1$4",
                "1$3",
                "0$2",
                "0$1",
                "0$0"
            )
        );
    }

    public void doTest(
        SortedMap<Integer, boolean[]> rowGroupMatrix,
        List<Integer> inputRefsForProject,
        List<String> expectedWorkId
    ) throws Throwable {
        doTest(false, rowGroupMatrix, inputRefsForProject, expectedWorkId);
    }

    public void doTest(
        boolean useDesc,
        SortedMap<Integer, boolean[]> rowGroupMatrix,
        List<Integer> inputRefsForProject,
        List<String> expectedWorkId
    )
        throws Throwable {
        context = new ExecutionContext();
        context.setTraceId(TRACE_ID);

        final int morselUnit = MORSEL_UNIT;

        NonBlockedScanPreProcessor preProcessor = new NonBlockedScanPreProcessor(
            preheatFileMeta, rowGroupMatrix, deletionBitmap
        );
        preProcessor.addFile(FILE_PATH);

        Set<Integer> refSet = new TreeSet<>();
        refSet.addAll(inputRefsForProject);
        refSet.addAll(inputRefsForFilter);
        List<ColumnMeta> columnMetas = refSet.stream().map(COLUMN_METAS::get).collect(Collectors.toList());
        List<Integer> locInOrc = refSet.stream().map(LOC_IN_ORC::get).collect(Collectors.toList());

        ColumnarSplit split = MorselColumnarSplit.newBuilder()
            .executionContext(context)
            .ioExecutor(IO_EXECUTOR)
            .fileSystem(FILESYSTEM, Engine.LOCAL_DISK)
            .configuration(CONFIGURATION)
            .sequenceId(SEQUENCE_ID)
            .file(FILE_PATH, FILE_ID)
            .columnTransformer(new OSSColumnTransformer(columnMetas, columnMetas, null, null, locInOrc))
            .inputRefs(inputRefsForFilter, inputRefsForProject)
            .cacheManager(blockCacheManager)
            .chunkLimit(DEFAULT_CHUNK_LIMIT)
            .morselUnit(morselUnit)
            .pushDown(evaluator)
            .prepare(preProcessor)
            .columnarManager(mockColumnarManager)
            .memoryAllocator(memoryAllocatorCtx)
            .operatorStatistic(new OperatorStatistics())
            .useDescending(useDesc)
            .build();

        int index = 0;
        ScanWork<ColumnarSplit, Chunk> scanWork;
        while ((scanWork = split.nextWork()) != null) {
            String suffix = MorselColumnarSplit.getSuffix(scanWork.getWorkId());
            System.out.println(suffix);
            //Assert.assertEquals(expectedWorkId.get(index++), suffix);
        }

    }

    private RoaringBitmap buildDeletionBitmap(int markedCount) {
        RoaringBitmap result = new RoaringBitmap();
        for (int i = 0; i < markedCount * 2; i += 2) {
            result.add(i);
        }
        return result;
    }

    // (col op1 const1) op2 const2
    // for example:
    // ($2 + 1000) >= 150000
    private RexNode buildCondition(int inputRefIndex,
                                   SqlOperator op1, long const1,
                                   SqlOperator op2, long const2) {
        // column a and literal 1
        RexInputRef inputRef = REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), inputRefIndex);
        RexLiteral literal = REX_BUILDER.makeLiteral(const1, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), BIGINT);

        // call: a+1
        RexNode plus = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            op1,
            ImmutableList.of(
                // column
                inputRef,
                // const
                literal
            )
        );

        // call: (a+1) >= 1000
        RexNode predicate = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            op2,
            ImmutableList.of(
                // column
                plus,
                // const
                REX_BUILDER.makeLiteral(const2, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), BIGINT)
            )
        );
        return predicate;
    }
}

