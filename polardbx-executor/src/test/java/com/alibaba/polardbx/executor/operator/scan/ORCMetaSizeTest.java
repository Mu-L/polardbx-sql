package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.orc.ORCMetaReader;
import com.alibaba.polardbx.common.orc.PreheatFileMeta;
import com.alibaba.polardbx.executor.operator.scan.impl.StaticStripePlanner;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadUtil;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
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
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Ignore
public class ORCMetaSizeTest {
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
    private PreheatFileMeta preheatFileMeta1;

    protected static String getFileFromClasspath(String name) {
        URL url = ClassLoader.getSystemResource(name);
        if (url == null) {
            throw new IllegalArgumentException("Could not find " + name);
        }
        return url.getPath();
    }

    protected static final String TEST_ORC_FILE_NAME = "bill_test_135_281b176f-85cd-4437-aa78-0450b9d6b660.orc";

    @Before
    public void prepareStaticParams() throws IOException {
        IO_EXECUTOR = Executors.newFixedThreadPool(IO_THREADS);

        CONFIGURATION = new Configuration();
        OrcConf.ROW_INDEX_STRIDE.setInt(CONFIGURATION, 10000);
        OrcConf.USE_REDUNDANT_META_DATA.setBoolean(CONFIGURATION, true);
        OrcConf.USE_BINARY_META_DATA.setBoolean(CONFIGURATION, true);
        OrcConf.STRIPE_SIZE.setLong(CONFIGURATION, 4 * 1024);

        Path workDir = new Path(this.getClass().getClassLoader().getResource(".").toString());
        FILE_PATH = new Path(workDir, TEST_ORC_FILE_NAME);

        System.out.println(FILE_PATH);

        FILESYSTEM = FileSystem.get(
            FILE_PATH.toUri(), CONFIGURATION
        );
    }

    // @Before
    public void prepareFile() throws Exception {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("f1", TypeDescription.createLong())
            .addField("f2", TypeDescription.createDouble())
            .addField("f3", TypeDescription.createVarchar())
            .addField("f4", TypeDescription.createLong())
            .addField("f5", TypeDescription.createLong());

        int rowCount = 31500;

        try (Writer writer = OrcFile.createWriter(FILE_PATH,
            OrcFile.writerOptions(CONFIGURATION)
                .fileSystem(FILESYSTEM)
                .overwrite(true)
                .rowIndexStride(1000)
                .setSchema(schema))) {

            VectorizedRowBatch batch = schema.createRowBatch(1000);

            for (int rowId = 0; rowId < rowCount; rowId++) {
                // Populate the rowIdx
                ((LongColumnVector) batch.cols[0]).vector[batch.size] = rowId;
                ((DoubleColumnVector) batch.cols[1]).vector[batch.size] = rowId * 0.99d;
                ((BytesColumnVector) batch.cols[2]).setVal(batch.size, ("row: " + rowId).getBytes());
                ((LongColumnVector) batch.cols[3]).vector[batch.size] = rowId;
                ((LongColumnVector) batch.cols[4]).vector[batch.size] = rowId;

                if (rowId % 99 == 0) {
                    batch.cols[4].isNull[batch.size] = true;
                    batch.cols[4].noNulls = false;
                }

                if (rowId % 88 == 0) {
                    batch.cols[3].isNull[batch.size] = true;
                    batch.cols[3].noNulls = false;
                }

                if (rowId % 77 == 0) {
                    batch.cols[2].isNull[batch.size] = true;
                    batch.cols[2].noNulls = false;
                }

                if (rowId % 66 == 0) {
                    batch.cols[1].isNull[batch.size] = true;
                    batch.cols[1].noNulls = false;
                }

                if (rowId % 55 == 0) {
                    batch.cols[0].isNull[batch.size] = true;
                    batch.cols[0].noNulls = false;
                }

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
            preheatFileMeta1 = metaReader.preheat(FILE_PATH);

            return preheatFileMeta1;
        } finally {
            metaReader.close();
        }
    }

    @Ignore
    @Test
    public void testPrepareParams() throws IOException {
        preheatFileMeta = preheat();

        orcTail = preheatFileMeta.getPreheatTail();

        // compression info
        compressionKind = orcTail.getCompressionKind();
        compressionSize = orcTail.getCompressionBufferSize();

        // get the mapping from stripe id to stripe information.
        List<StripeInformation> stripeInformationList = orcTail.getStripes();
        stripeInformationMap = stripeInformationList.stream().collect(Collectors.toMap(
            stripe -> (int) stripe.getStripeId(),
            stripe -> stripe,
            (s1, s2) -> s1,
            () -> new TreeMap<>()
        ));

        fileSchema = orcTail.getSchema();
        version = orcTail.getWriterVersion();

        System.out.println(fileSchema);

        // encryption info for reading.
        OrcProto.Footer footer = orcTail.getFooter();
        encryption = new ReaderEncryption(footer, fileSchema,
            orcTail.getStripeStatisticsOffset(), orcTail.getTailBuffer(), stripeInformationList,
            null, CONFIGURATION);

        // should the reader ignore the obsolete non-UTF8 bloom filters.
        ignoreNonUtf8BloomFilter = OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(CONFIGURATION);

        // max buffer size in single IO task.
        maxBufferSize = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(CONFIGURATION);

        // the max row count in one row group.
        indexStride = orcTail.getFooter().getRowIndexStride();

        maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(CONFIGURATION);
        maxMergeDistance = OrcConf.MAX_MERGE_DISTANCE.getLong(CONFIGURATION);

        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, true);
        encodingMap = stripeInformationList.stream().collect(Collectors.toMap(
            stripe -> (int) stripe.getStripeId(),
            stripe -> preheatFileMeta.getColumnEncodings((int) stripe.getStripeId()),
            (s1, s2) -> s1,
            () -> new TreeMap<>()
        ));

        ExecutionContext context = new ExecutionContext();
        context.setTraceId("mock_trace_id");
        if (context.getMemoryPool() == null) {
            context.setMemoryPool(MemoryManager.getInstance().createQueryMemoryPool(
                WorkloadUtil.isApWorkload(
                    context.getWorkloadType()), context.getTraceId(), context.getExtraCmds()));
        }

        MemoryPool memoryPool = MemoryPoolUtils
            .createOperatorTmpTablePool("ColumnarScanExec@" + System.identityHashCode(this),
                context.getMemoryPool());
        this.memoryAllocatorCtx = memoryPool.getMemoryAllocatorCtx();

        try {
            Thread.sleep(1000 * 3600);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
