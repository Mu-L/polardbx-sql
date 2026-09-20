package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.orc.PreheatFileMeta;
import com.alibaba.polardbx.common.orc.PreheatMetaMemoryUtils;
import com.alibaba.polardbx.common.orc.PreheatStripeMeta;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CachingFileSystem;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.executor.operator.scan.impl.StaticStripePlanner;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadUtil;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.MetadataDeserializeUtils;
import org.apache.orc.impl.MetadataSerializeUtils;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.RecordReaderUtils;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.StripePlanner;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.openjdk.jol.info.GraphLayout;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class BinaryMetadataTest {
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

    protected static final String TEST_ORC_FILE_NAME = "binary_metadata_test.orc";

    @Before
    public void prepareStaticParams() throws IOException {
        IO_EXECUTOR = Executors.newFixedThreadPool(IO_THREADS);

        CONFIGURATION = new Configuration();
        OrcConf.ROW_INDEX_STRIDE.setInt(CONFIGURATION, 10000);
        OrcConf.USE_REDUNDANT_META_DATA.setBoolean(CONFIGURATION, true);
        OrcConf.USE_BINARY_META_DATA.setBoolean(CONFIGURATION, false);
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
        PreheatFileMeta preheatFileMeta = preheat(FILE_PATH);
        return preheatFileMeta;
    }

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
            stripe -> preheatFileMeta.buildEncodings((int) stripe.getStripeId(), columnIncluded),
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
    }

    public PreheatFileMeta preheat(Path path) throws IOException {
        PreheatFileMeta result = new PreheatFileMeta();
        // 0. build and cache file reader
        ReaderImpl fileReader = null;
        try {
            // BugFix: This method is called by the preheat cache's load process.
            // It should not use the filesystem's getStatus method to obtain file status,
            // as it may recursively call the preheat cache.
            FileStatus fileStatus;
            if (FILESYSTEM instanceof OSSFileSystem) {
                fileStatus = ((OSSFileSystem) FILESYSTEM).getFileStatusImpl(path);
            } else if (FILESYSTEM instanceof CachingFileSystem
                && ((CachingFileSystem) FILESYSTEM).getDataTier() instanceof OSSFileSystem) {
                fileStatus = ((OSSFileSystem) ((CachingFileSystem) FILESYSTEM).getDataTier())
                    .getFileStatusImpl(path);
            } else {
                fileStatus = FILESYSTEM.getFileStatus(path);
            }
            result.setFileStatus(fileStatus);

            fileReader = (ReaderImpl) OrcFile.createReader(path,
                OrcFile.readerOptions(CONFIGURATION).filesystem(FILESYSTEM)
            );

            // 1. extract orc tail and cache it
            OrcTail orcTail = fileReader.getOrcTail();
            orcTail.clearForPreheat();
            result.setPreheatTail(orcTail);

            // 2. build and cache each stripe metadata in file.
            Map<Long, PreheatStripeMeta> preheatContextMap;
            OrcProto.RedundantMetadata redundantMetadata = orcTail.getFooter().getRedundantMetadata();

            System.out.println(
                "OrcProto.RedundantMetadata: " + GraphLayout.parseInstance(redundantMetadata).totalSize());

            OrcProto.BinaryMetadata binaryMetadata =
                MetadataSerializeUtils.transformToRedundantMetadata(redundantMetadata);

            System.out.println("OrcProto.BinaryMetadata: " + GraphLayout.parseInstance(binaryMetadata).totalSize());

            OrcProto.RedundantMetadata deserializedRedundantMetadata =
                MetadataDeserializeUtils.transformToRedundantMetadata(binaryMetadata);

            System.out.println(
                "deserializedRedundantMetadata: " + GraphLayout.parseInstance(deserializedRedundantMetadata)
                    .totalSize());

            if (DynamicConfig.getInstance().useRedundantMetaData()
                && redundantMetadata != null
                && redundantMetadata.getStripesCount() > 0) {
                TypeDescription schema = orcTail.getSchema();
                preheatContextMap = preheatStripeFromRedundantMetadata(redundantMetadata, schema);

                // remove redundant metadata from footer.
                OrcProto.Footer newFooter = OrcProto.Footer
                    .newBuilder(orcTail.getFooter())
                    .clearRedundantMetadata().build();

                OrcProto.FileTail newFileTail = OrcProto.FileTail
                    .newBuilder(orcTail.getFileTail())
                    .clearFooter()
                    .setFooter(newFooter)
                    .build();

                orcTail.resetTail(newFileTail);

            } else {
                // compatible for old version without redundant metadata.
                preheatContextMap = preheatStripe(path, fileReader, FILESYSTEM);
            }

            result.setPreheatStripes(preheatContextMap);
        } finally {
            // prevent from IO resource leak
            if (fileReader != null) {
                fileReader.close();
            }
        }

        if (result != null) {
            long memorySize;
            if (DynamicConfig.getInstance().enablePreheatMemoryPreciseCount()) {
                memorySize = GraphLayout.parseInstance(result).totalSize();
            } else {
                memorySize = PreheatMetaMemoryUtils.estimatedMemorySizeOf(result);
            }
            result.setMemorySize(memorySize);
        }

        return result;
    }

    private Map<Long, PreheatStripeMeta> preheatStripeFromRedundantMetadata(
        OrcProto.RedundantMetadata redundantMetadata, TypeDescription schema) {
        boolean enableZoneMapPrune = DynamicConfig.getInstance().enableZoneMapPrune();

        Map<Long, PreheatStripeMeta> result = new ConcurrentHashMap<>();

        //  redundant meta store all row-index and footer for each stripe.
        List<OrcProto.RedundantStripeMetadata> redundantStripeMetadataList = redundantMetadata.getStripesList();
        for (int i = 0; i < redundantStripeMetadataList.size(); i++) {
            OrcProto.RedundantStripeMetadata redundantStripeMetadata = redundantStripeMetadataList.get(i);

            // Get stripe footer from redundant meta in file tail.
            OrcProto.StripeFooterWithId stripeFooterWithId = redundantStripeMetadata.getStripeFooter();
            int stripeNumber = stripeFooterWithId.getStripe();
            OrcProto.StripeFooter stripeFooter = stripeFooterWithId.getStripeFooter();

            List<OrcProto.RowIndexWithColumn> rowIndexWithColumnList = redundantStripeMetadata.getRowIndexList();

            int typeCount = schema.getMaximumId() + 1;
            OrcIndex orcIndex = new OrcIndex(new OrcProto.RowIndex[typeCount],
                new OrcProto.Stream.Kind[typeCount],
                new OrcProto.BloomFilterIndex[typeCount],
                new OrcProto.BitmapIndex[typeCount]);
            OrcProto.RowIndex[] indexes = orcIndex.getRowGroupIndex();

            for (int index = 0; index < rowIndexWithColumnList.size(); index++) {
                OrcProto.RowIndexWithColumn rowIndexWithColumn = rowIndexWithColumnList.get(index);
                int column = rowIndexWithColumn.getColumn();
                OrcProto.RowIndex rowIndex = rowIndexWithColumn.getRowIndex();

                indexes[column] = rowIndex;
            }

            PreheatStripeMeta preheatStripeMeta = new PreheatStripeMeta(
                stripeNumber, orcIndex, stripeFooter);
            result.put((long) stripeNumber, preheatStripeMeta);
        }

        return result;
    }

    private Map<Long, PreheatStripeMeta> preheatStripe(Path path, ReaderImpl fileReader, FileSystem preheatFileSystem)
        throws IOException {
        Map<Long, PreheatStripeMeta> result = new ConcurrentHashMap<>();

        // 1. build data reader
        try (DataReader dataReader = buildDataReader(path, fileReader, preheatFileSystem)) {
            for (StripeInformation stripe : fileReader.getStripes()) {

                // 2. build stripe planner for each stripe.
                StripePlanner planner = buildStripePlanner(fileReader, dataReader);

                boolean[] allColumns = new boolean[fileReader.getSchema().getMaximumId() + 1];
                Arrays.fill(allColumns, true);

                // 3. get stripe footer
                OrcProto.StripeFooter stripeFooter = dataReader.readStripeFooter(stripe);

                // 4. planner parse meta info of Stripe
                // get info of data streams + index streams and cache it in planner object.
                planner.parseStripe(stripe, allColumns, stripeFooter);

                // 5. get row index
                // NOTE: Stripe Planner will NOT cache the row indexes already fetched.
                OrcIndex index = planner.readRowIndex(allColumns, null);

                // 6. for preheated meta, don't cache bitmap index.
                index.clearBitmapIndex();

                planner.clearDataReader();
                PreheatStripeMeta preheatStripeMeta = new PreheatStripeMeta(
                    stripe.getStripeId(), index, stripeFooter);

                result.put(stripe.getStripeId(), preheatStripeMeta);
            }
        }

        return result;
    }

    @NotNull
    private StripePlanner buildStripePlanner(ReaderImpl fileReader, DataReader dataReader) {
        int maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(CONFIGURATION);
        boolean ignoreNonUtf8BloomFilter = OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(CONFIGURATION);

        StripePlanner planner = new StripePlanner(
            fileReader.getSchema(),
            fileReader.getEncryption(),
            dataReader,
            fileReader.getWriterVersion(),
            ignoreNonUtf8BloomFilter,
            maxDiskRangeChunkLimit);
        return planner;
    }

    private DataReader buildDataReader(Path path, ReaderImpl fileReader, FileSystem fileSystem) throws IOException {
        int maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(CONFIGURATION);
        Reader.Options options = fileReader.options();

        InStream.StreamOptions unencryptedOptions =
            InStream.options()
                .withCodec(OrcCodecPool.getCodec(fileReader.getCompressionKind()))
                .withBufferSize(fileReader.getCompressionSize());
        DataReaderProperties.Builder builder =
            DataReaderProperties.builder()
                .withCompression(unencryptedOptions)
                .withFileSystemSupplier(() -> fileSystem)
                .withPath(path)
                .withMaxDiskRangeChunkLimit(maxDiskRangeChunkLimit)
                .withZeroCopy(options.getUseZeroCopy());
        FSDataInputStream file = fileSystem.open(path);
        if (file != null) {
            builder.withFile(file);
        }

        DataReader dataReader = RecordReaderUtils.createDefaultDataReader(
            builder.build());
        return dataReader;
    }
}
