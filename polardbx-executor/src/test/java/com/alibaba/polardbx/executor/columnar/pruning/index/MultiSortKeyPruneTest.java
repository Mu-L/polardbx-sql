package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.orc.ORCMetaReader;
import com.alibaba.polardbx.common.orc.PreheatFileMeta;
import com.alibaba.polardbx.executor.columnar.pruning.ColumnarPruneManager;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils.transformRexToIndexMergeTree;

public class MultiSortKeyPruneTest {
    public static final int DEFAULT_CHUNK_LIMIT = 1000;
    public static final int IO_THREADS = 2;
    // IO params.
    protected static Configuration CONFIGURATION;
    protected static Path FILE_PATH;
    protected static FileSystem FILESYSTEM;
    protected static ExecutorService IO_EXECUTOR;
    final ExecutionContext context = new ExecutionContext();
    // for compression
    protected CompressionKind compressionKind;
    protected int compressionSize;
    // stripe-level and file-level meta
    protected TreeMap<Integer, StripeInformation> stripeInformationMap;
    protected TypeDescription fileSchema;
    protected OrcFile.WriterVersion version;
    protected ReaderEncryption encryption;
    protected boolean ignoreNonUtf8BloomFilter;
    protected int maxBufferSize;
    protected int indexStride;
    protected int maxDiskRangeChunkLimit;
    protected long maxMergeDistance;
    // need preheating
    protected PreheatFileMeta preheatFileMeta;
    protected OrcTail orcTail;
    protected List<DataType> inputTypes;
    protected SortedMap<Integer, OrcProto.ColumnEncoding[]> encodingMap;
    protected MemoryAllocatorCtx memoryAllocatorCtx;

    protected static String getFileFromClasspath(String name) {
        URL url = ClassLoader.getSystemResource(name);
        if (url == null) {
            throw new IllegalArgumentException("Could not find " + name);
        }
        return url.getPath();
    }

    protected static final String TEST_ORC_FILE_NAME = "multi_sort_key_test.orc";

    private List<ColumnMeta> columns;
    private List<OrderByOption> sortKeys;
    private List<Integer> orcIndexes;

    // for Rex
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    private RexNode rexNode;

    @Before
    public void prepareStaticParams() throws IOException {
        IO_EXECUTOR = Executors.newFixedThreadPool(IO_THREADS);
        CONFIGURATION = new Configuration();
        OrcConf.ROW_INDEX_STRIDE.setInt(CONFIGURATION, 10000);
        OrcConf.STRIPE_SIZE.setLong(CONFIGURATION, 4 * 1024);
        Path workDir = new Path(this.getClass().getClassLoader().getResource(".").toString());
        FILE_PATH = new Path(workDir, TEST_ORC_FILE_NAME);
        FILESYSTEM = FileSystem.get(
            FILE_PATH.toUri(), CONFIGURATION
        );
    }

    @Before
    public void prepareFile() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("f1", TypeDescription.createLong())
            .addField("f2", TypeDescription.createLong())
            .addField("f3", TypeDescription.createLong())
            .addField("f4", TypeDescription.createString())
            .addField("f5", TypeDescription.createLong());

        columns = new ArrayList<>();
        columns.add(new ColumnMeta("test", "f1", "f1", new Field(DataTypes.TinyIntType)));
        columns.add(new ColumnMeta("test", "f2", "f2", new Field(DataTypes.LongType)));
        columns.add(new ColumnMeta("test", "f3", "f3", new Field(DataTypes.DateType)));
        columns.add(new ColumnMeta("test", "f4", "f4", new Field(DataTypes.VarcharType)));
        columns.add(new ColumnMeta("test", "f5", "f5", new Field(DataTypes.LongType)));

        sortKeys = new ArrayList<>();
        sortKeys.add(new OrderByOption(0, true, true));
        sortKeys.add(new OrderByOption(1, true, true));
        sortKeys.add(new OrderByOption(2, true, true));
        sortKeys.add(new OrderByOption(3, true, true));

        orcIndexes = new ArrayList<>();
        orcIndexes.add(1);
        orcIndexes.add(2);
        orcIndexes.add(3);
        orcIndexes.add(4);
        orcIndexes.add(5);

        int rowCount = 31500;
        try (Writer writer = OrcFile.createWriter(FILE_PATH,
            OrcFile.writerOptions(CONFIGURATION)
                .fileSystem(FILESYSTEM)
                .overwrite(true)
                .rowIndexStride(1000)
                .setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch(1000);
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                // Populate the rowIdx
                ((LongColumnVector) batch.cols[0]).vector[batch.size] = rowIndex / 10000; // low-card: tag
                ((LongColumnVector) batch.cols[1]).vector[batch.size] = rowIndex; // high-card
                ((LongColumnVector) batch.cols[2]).vector[batch.size] = rowIndex; // date
                ((BytesColumnVector) batch.cols[3]).setVal(batch.size, ("" + rowIndex).getBytes());
                ((LongColumnVector) batch.cols[4]).vector[batch.size] = rowIndex;
                batch.size += 1;
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size > 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
    }

    public PreheatFileMeta preheat() throws IOException {
        ORCMetaReader metaReader = null;
        try {
            metaReader = ORCMetaReader.create(CONFIGURATION, FILESYSTEM);
            PreheatFileMeta preheatFileMeta = metaReader.preheat(FILE_PATH);
            return preheatFileMeta;
        } finally {
            metaReader.close();
        }
    }

    @Test
    public void test1() throws IOException, ExecutionException {
        // rex list

        // f1 = 2
        RexNode rexNode1 = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.EQUALS,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT), 0),
                REX_BUILDER.makeBigIntLiteral(2L)
            )
        );

        // f2 > 10000
        RexNode rexNode2 = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.GREATER_THAN,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 1),
                REX_BUILDER.makeBigIntLiteral(10000L)
            )
        );

        // f1 = 2 and f2 > 10000
        rexNode = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.AND,
            ImmutableList.of(rexNode1, rexNode2)
        );
        List<RexNode> rexList = new ArrayList<>();
        rexList.add(rexNode);

        // index Prune Context
        IndexPruneContext indexPruneContext = new IndexPruneContext();
        indexPruneContext.setParameters(new Parameters());

        // rex+pc -> distribution segment condition + indexes merge tree
        ColumnPredicatePruningInf columnPredicate =
            transformRexToIndexMergeTree(rexList, indexPruneContext);

        PreheatFileMeta preheat = preheat();

        IndexPruner indexPruner =
            ColumnarPruneManager.getIndexPruner(
                FILE_PATH, preheat, columns, sortKeys,
                orcIndexes, false
            );

        RoaringBitmap rr =
            indexPruner.prune("test", columns, columnPredicate, indexPruneContext);

        // {20,21,22,23,24,25,26,27,28,29}
        System.out.println(rr);
        Assert.assertTrue("{20,21,22,23,24,25,26,27,28,29}".equals(rr.toString()));
    }

    @Test
    public void test2() throws IOException, ExecutionException {
        // rex list

        // f1 = 2
        RexNode rexNode1 = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.EQUALS,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT), 0),
                REX_BUILDER.makeBigIntLiteral(2L)
            )
        );

        // f2 < 20000
        RexNode rexNode2 = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.LESS_THAN,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 1),
                REX_BUILDER.makeBigIntLiteral(20000L)
            )
        );

        // f1 = 2 and f2 < 20000
        rexNode = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.AND,
            ImmutableList.of(rexNode1, rexNode2)
        );
        List<RexNode> rexList = new ArrayList<>();
        rexList.add(rexNode);

        // index Prune Context
        IndexPruneContext indexPruneContext = new IndexPruneContext();
        indexPruneContext.setParameters(new Parameters());

        // rex+pc -> distribution segment condition + indexes merge tree
        ColumnPredicatePruningInf columnPredicate =
            transformRexToIndexMergeTree(rexList, indexPruneContext);

        PreheatFileMeta preheat = preheat();

        IndexPruner indexPruner =
            ColumnarPruneManager.getIndexPruner(
                FILE_PATH, preheat, columns, sortKeys,
                orcIndexes, false
            );

        RoaringBitmap rr =
            indexPruner.prune("test", columns, columnPredicate, indexPruneContext);

        // {20}
        System.out.println(rr);
        Assert.assertTrue("{20}".equals(rr.toString()));
    }

    @Test
    public void testSearch() throws IOException, ExecutionException {
        // rex list

        // f1 = 2
        RexNode rexNode1 = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.EQUALS,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT), 0),
                REX_BUILDER.makeBigIntLiteral(2L)
            )
        );

        // f2 < 20000
        RexNode rexNode2 = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.LESS_THAN,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 1),
                REX_BUILDER.makeBigIntLiteral(20000L)
            )
        );

        // f1 = 2 and f2 < 20000
        rexNode = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.AND,
            ImmutableList.of(rexNode1, rexNode2)
        );
        List<RexNode> rexList = new ArrayList<>();
        rexList.add(rexNode);

        // index Prune Context
        IndexPruneContext indexPruneContext = new IndexPruneContext();
        indexPruneContext.setParameters(new Parameters());

        // rex+pc -> distribution segment condition + indexes merge tree
        ColumnPredicatePruningInf columnPredicate =
            transformRexToIndexMergeTree(rexList, indexPruneContext);

        PreheatFileMeta preheat = preheat();

        IndexPruner indexPruner =
            ColumnarPruneManager.getIndexPruner(
                FILE_PATH, preheat, columns, sortKeys,
                orcIndexes, false
            );

        RoaringBitmap rr = RoaringBitmap.bitmapOfRange(0, 100);

        MultiSortKeyIndex multiSortKeyIndex = indexPruner.getMultiSortKeyIndex();
        multiSortKeyIndex.checkSupport(100, null);
        multiSortKeyIndex.groupSize(0);
        multiSortKeyIndex.groupSize(1);
        multiSortKeyIndex.groupSize(2);
        multiSortKeyIndex.groupSize(3);
        multiSortKeyIndex.groupSize(4);
        multiSortKeyIndex.groupSize(5);
        multiSortKeyIndex.search(0, 2, SqlKind.EQUALS, rr);
        multiSortKeyIndex.search(0, 2, SqlKind.GREATER_THAN, rr);
        multiSortKeyIndex.search(0, 2, SqlKind.GREATER_THAN_OR_EQUAL, rr);
        multiSortKeyIndex.search(0, 2, SqlKind.LESS_THAN, rr);
        multiSortKeyIndex.search(1, 9999, SqlKind.EQUALS, rr);
        multiSortKeyIndex.search(1, 9999, SqlKind.GREATER_THAN, rr);
        multiSortKeyIndex.search(1, 9999, SqlKind.GREATER_THAN_OR_EQUAL, rr);
        multiSortKeyIndex.search(1, 9999, SqlKind.LESS_THAN, rr);

        multiSortKeyIndex.prune(0, 0, false, 127, false, RoaringBitmap.bitmapOfRange(0, 100), null);
    }
}
