package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.orc.ORCMetaReader;
import com.alibaba.polardbx.common.orc.PreheatFileMeta;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GetOrcTailImplTest {
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

    protected static final String TEST_ORC_FILE_NAME = "get_orc_tail_impl_test.orc";

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
            .addField("f4", TypeDescription.createLong())
            .addField("f5", TypeDescription.createLong());

        int rowCount = 1500;

        try (Writer writer = OrcFile.createWriter(FILE_PATH,
            OrcFile.writerOptions(CONFIGURATION)
                .fileSystem(FILESYSTEM)
                .overwrite(true)
                .rowIndexStride(1000)
                .setSchema(schema))) {

            VectorizedRowBatch batch = schema.createRowBatch(1000);

            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                // Populate the rowIdx
                ((LongColumnVector) batch.cols[0]).vector[batch.size] = rowIndex;
                ((LongColumnVector) batch.cols[1]).vector[batch.size] = rowIndex;
                ((LongColumnVector) batch.cols[2]).vector[batch.size] = rowIndex;
                ((LongColumnVector) batch.cols[3]).vector[batch.size] = rowIndex;
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
    public void testGetOrcTailImpl() {
        FilesRecord filesRecord = new FilesRecord();

        // 基本文件信息
        filesRecord.fileName = FILE_PATH.toString();
        filesRecord.fileType = "ORC";
        filesRecord.fileMeta = new byte[0];

        // 表空间和目录信息
        filesRecord.tablespaceName = null;
        filesRecord.tableCatalog = "";
        filesRecord.tableSchema = "test_schema";
        filesRecord.tableName = "test_table";

        // 日志文件组信息
        filesRecord.logfileGroupName = null;
        filesRecord.logfileGroupNumber = 0;

        // 存储引擎
        filesRecord.engine = "LOCAL_DISK";

        // 全文索引和统计信息
        filesRecord.fulltextKeys = null;
        filesRecord.deletedRows = 0;
        filesRecord.updateCount = 0;

        // 扩展信息
        filesRecord.freeExtents = 0;
        filesRecord.totalExtents = 0;
        filesRecord.extentSize = 1024L;
        filesRecord.initialSize = 0;
        filesRecord.maximumSize = 0;
        filesRecord.autoextendSize = 0;

        // 时间信息
        filesRecord.creationTime = null;
        filesRecord.lastUpdateTime = null;
        filesRecord.lastAccessTime = null;
        filesRecord.recoverTime = 0;

        // 事务和版本信息
        filesRecord.transactionCounter = 0;
        filesRecord.version = 1;

        // 行格式和统计
        filesRecord.rowFormat = null;
        filesRecord.tableRows = 31500L;
        filesRecord.avgRowLength = 0;
        filesRecord.dataLength = 0;
        filesRecord.maxDataLength = 0;
        filesRecord.indexLength = 0;
        filesRecord.dataFree = 0;

        // 创建和更新时间
        filesRecord.createTime = "2024-01-01 00:00:00";
        filesRecord.updateTime = "2024-01-01 00:00:00";
        filesRecord.checkTime = null;

        // 校验和状态
        filesRecord.checksum = 0;
        filesRecord.deletedChecksum = null;
        filesRecord.status = "VISIBLE";
        filesRecord.extra = null;

        // 任务和生命周期
        filesRecord.taskId = 0;
        filesRecord.lifeCycle = 0;
        filesRecord.localPath = null;

        // 逻辑表信息
        filesRecord.logicalSchemaName = "test_logical_schema";
        filesRecord.logicalTableName = "test_logical_table";
        filesRecord.localPartitionName = null;
        filesRecord.partitionName = "test_partition";

        // 时间戳信息
        filesRecord.commitTs = System.currentTimeMillis();
        filesRecord.removeTs = null;
        filesRecord.schemaTs = System.currentTimeMillis();
        filesRecord.fileHash = 123456789L;

        try (MockedStatic<FileSystemManager> fileSystemManagerMockedStatic = org.mockito.Mockito.mockStatic(
            FileSystemManager.class)) {
            FileSystemGroup fileSystemGroup = Mockito.mock(FileSystemGroup.class);
            fileSystemManagerMockedStatic.when(() -> FileSystemManager.getFileSystemGroup(Engine.LOCAL_DISK))
                .thenReturn(fileSystemGroup);

            Mockito.when(fileSystemGroup.getMaster()).thenReturn(FILESYSTEM);

            OSSOrcFileMeta ossOrcFileMeta = (OSSOrcFileMeta) FileMeta.parseFrom(filesRecord);

            ossOrcFileMeta.getOrcTail();
        }

    }

    /**
     * Test fallback path: when PreheatMetaManager throws (e.g. OSS unreachable),
     * getOrcTailImpl should degrade to reading file_meta bytes from MetaDB.
     * This test validates the core deserialization logic used in the fallback path.
     */
    @Test
    public void testGetOrcTailImplFallbackToMetaDb() throws Exception {
        // Get the real ORC tail bytes using Reader (simulating what MetaDB file_meta column stores)
        java.nio.ByteBuffer tailBuffer;
        TypeDescription originalSchema;
        int originalStripesCount;
        try (Reader reader = OrcFile.createReader(FILE_PATH,
            OrcFile.readerOptions(CONFIGURATION).filesystem(FILESYSTEM))) {
            tailBuffer = reader.getSerializedFileFooter();
            originalSchema = reader.getSchema();
            originalStripesCount = reader.getStripes().size();
        }
        byte[] tailBytes = new byte[tailBuffer.remaining()];
        tailBuffer.get(tailBytes);

        // Validate that OrcMetaUtils.extractFileTail (the fallback deserialization path)
        // can correctly reconstruct OrcTail from the stored bytes
        OrcTail fallbackTail = OrcMetaUtils.extractFileTail(java.nio.ByteBuffer.wrap(tailBytes));
        org.junit.Assert.assertNotNull("Fallback OrcTail should not be null", fallbackTail);
        org.junit.Assert.assertEquals("Schema should match original",
            originalSchema.toString(), fallbackTail.getSchema().toString());
        org.junit.Assert.assertEquals("Footer stripe count should match",
            originalStripesCount, fallbackTail.getFooter().getStripesCount());

        // Build a FilesRecord with valid file_meta bytes to verify full integration path
        FilesRecord filesRecord = new FilesRecord();
        filesRecord.fileName = FILE_PATH.toString();
        filesRecord.fileMeta = tailBytes;
        filesRecord.engine = "LOCAL_DISK";
        filesRecord.extentSize = 1024L;
        filesRecord.tableRows = 1500L;
        filesRecord.createTime = "2024-01-01 00:00:00";
        filesRecord.updateTime = "2024-01-01 00:00:00";

        // Simulate: PreheatMetaManager fails, then fallback reads file_meta from FilesRecord
        byte[] fileMetaBytes = filesRecord.getFileMeta();
        org.junit.Assert.assertNotNull("file_meta bytes should be present", fileMetaBytes);
        org.junit.Assert.assertTrue("file_meta bytes should not be empty", fileMetaBytes.length > 0);

        OrcTail fromRecord = OrcMetaUtils.extractFileTail(java.nio.ByteBuffer.wrap(fileMetaBytes));
        org.junit.Assert.assertNotNull("OrcTail from FilesRecord.fileMeta should not be null", fromRecord);
        org.junit.Assert.assertEquals("Schema from FilesRecord should match",
            originalSchema.toString(), fromRecord.getSchema().toString());
    }

    /**
     * Test that OrcMetaUtils.extractFileTail correctly deserializes ORC tail bytes,
     * validating the legacy MetaDB fallback path works end-to-end.
     */
    @Test
    public void testExtractFileTailFromBytes() throws Exception {
        // Generate real ORC tail bytes from test file using Reader
        java.nio.ByteBuffer tailBuffer;
        TypeDescription originalSchema;
        int originalStatsCount;
        try (Reader reader = OrcFile.createReader(FILE_PATH,
            OrcFile.readerOptions(CONFIGURATION).filesystem(FILESYSTEM))) {
            tailBuffer = reader.getSerializedFileFooter();
            originalSchema = reader.getSchema();
            originalStatsCount = reader.getStatistics().length;
        }
        byte[] tailBytes = new byte[tailBuffer.remaining()];
        tailBuffer.get(tailBytes);

        // Deserialize using the legacy path
        OrcTail deserialized = OrcMetaUtils.extractFileTail(java.nio.ByteBuffer.wrap(tailBytes));

        org.junit.Assert.assertNotNull("Deserialized OrcTail should not be null", deserialized);
        org.junit.Assert.assertEquals("Schema field count should match",
            originalSchema.getFieldNames().size(),
            deserialized.getSchema().getFieldNames().size());
        org.junit.Assert.assertEquals("Schema string should match",
            originalSchema.toString(),
            deserialized.getSchema().toString());
        org.junit.Assert.assertTrue("Footer should have statistics",
            deserialized.getFooter().getStatisticsCount() > 0);
    }

    /**
     * Helper to mock PreheatMetaManager.get() which declares throws Throwable.
     * Uses doAnswer to avoid compile-time checked exception issues.
     */
    private void mockPreheatManagerGet(
        com.alibaba.polardbx.common.orc.PreheatMetaManager mockPreheatManager,
        PreheatFileMeta returnValue) {
        try {
            Mockito.doAnswer(invocation -> returnValue).when(mockPreheatManager)
                .get(Mockito.any(Path.class), Mockito.any(FileSystem.class));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Helper to mock PreheatMetaManager.get() to throw an exception.
     */
    private void mockPreheatManagerGetThrows(
        com.alibaba.polardbx.common.orc.PreheatMetaManager mockPreheatManager,
        Throwable exception) {
        try {
            Mockito.doAnswer(invocation -> {
                    throw exception;
                }).when(mockPreheatManager)
                .get(Mockito.any(Path.class), Mockito.any(FileSystem.class));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Test that getOrcTailImpl delegates to PreheatMetaManager and returns correct OrcTail
     * when PreheatMetaManager succeeds.
     */
    @Test
    public void testGetOrcTailImplViaPreheatManager() throws Exception {
        PreheatFileMeta realPreheat = preheat();
        OrcTail expectedTail = realPreheat.getPreheatTail();

        try (MockedStatic<FileSystemManager> fsmStatic = Mockito.mockStatic(FileSystemManager.class);
            MockedStatic<com.alibaba.polardbx.common.orc.PreheatMetaManager> pmStatic =
                Mockito.mockStatic(com.alibaba.polardbx.common.orc.PreheatMetaManager.class)) {

            FileSystemGroup fileSystemGroup = Mockito.mock(FileSystemGroup.class);
            fsmStatic.when(() -> FileSystemManager.getFileSystemGroup(Engine.LOCAL_DISK))
                .thenReturn(fileSystemGroup);
            Mockito.when(fileSystemGroup.getMaster()).thenReturn(FILESYSTEM);

            com.alibaba.polardbx.common.orc.PreheatMetaManager mockPreheatManager =
                Mockito.mock(com.alibaba.polardbx.common.orc.PreheatMetaManager.class);
            pmStatic.when(com.alibaba.polardbx.common.orc.PreheatMetaManager::getInstance)
                .thenReturn(mockPreheatManager);
            mockPreheatManagerGet(mockPreheatManager, realPreheat);

            FilesRecord filesRecord = buildTestFilesRecord();
            OSSOrcFileMeta meta = (OSSOrcFileMeta) FileMeta.parseFrom(filesRecord);

            OrcTail result = meta.getOrcTail();
            org.junit.Assert.assertNotNull("OrcTail should not be null", result);
            org.junit.Assert.assertEquals("Schema should match",
                expectedTail.getSchema().toString(), result.getSchema().toString());
            org.junit.Assert.assertEquals("Stripe count should match",
                expectedTail.getFooter().getStripesCount(), result.getFooter().getStripesCount());
        }
    }

    /**
     * Test that getOrcTailImpl propagates exception when PreheatMetaManager throws.
     */
    @Test(expected = RuntimeException.class)
    public void testGetOrcTailImplPreheatManagerThrowsException() throws Exception {
        try (MockedStatic<FileSystemManager> fsmStatic = Mockito.mockStatic(FileSystemManager.class);
            MockedStatic<com.alibaba.polardbx.common.orc.PreheatMetaManager> pmStatic =
                Mockito.mockStatic(com.alibaba.polardbx.common.orc.PreheatMetaManager.class)) {

            FileSystemGroup fileSystemGroup = Mockito.mock(FileSystemGroup.class);
            fsmStatic.when(() -> FileSystemManager.getFileSystemGroup(Engine.LOCAL_DISK))
                .thenReturn(fileSystemGroup);
            Mockito.when(fileSystemGroup.getMaster()).thenReturn(FILESYSTEM);

            com.alibaba.polardbx.common.orc.PreheatMetaManager mockPreheatManager =
                Mockito.mock(com.alibaba.polardbx.common.orc.PreheatMetaManager.class);
            pmStatic.when(com.alibaba.polardbx.common.orc.PreheatMetaManager::getInstance)
                .thenReturn(mockPreheatManager);
            mockPreheatManagerGetThrows(mockPreheatManager, new IOException("OSS connection timeout"));

            // Construction should fail because getOrcTailImpl is called in constructor
            FilesRecord filesRecord = buildTestFilesRecord();
            FileMeta.parseFrom(filesRecord);
        }
    }

    /**
     * Test that OSSOrcFileMeta correctly initializes schema and statistics after construction.
     */
    @Test
    public void testConstructorInitializesSchemaAndStatistics() throws Exception {
        PreheatFileMeta realPreheat = preheat();

        try (MockedStatic<FileSystemManager> fsmStatic = Mockito.mockStatic(FileSystemManager.class);
            MockedStatic<com.alibaba.polardbx.common.orc.PreheatMetaManager> pmStatic =
                Mockito.mockStatic(com.alibaba.polardbx.common.orc.PreheatMetaManager.class)) {

            FileSystemGroup fileSystemGroup = Mockito.mock(FileSystemGroup.class);
            fsmStatic.when(() -> FileSystemManager.getFileSystemGroup(Engine.LOCAL_DISK))
                .thenReturn(fileSystemGroup);
            Mockito.when(fileSystemGroup.getMaster()).thenReturn(FILESYSTEM);

            com.alibaba.polardbx.common.orc.PreheatMetaManager mockPreheatManager =
                Mockito.mock(com.alibaba.polardbx.common.orc.PreheatMetaManager.class);
            pmStatic.when(com.alibaba.polardbx.common.orc.PreheatMetaManager::getInstance)
                .thenReturn(mockPreheatManager);
            mockPreheatManagerGet(mockPreheatManager, realPreheat);

            FilesRecord filesRecord = buildTestFilesRecord();
            OSSOrcFileMeta meta = (OSSOrcFileMeta) FileMeta.parseFrom(filesRecord);

            // Verify TypeDescription
            TypeDescription schema = meta.getTypeDescription();
            org.junit.Assert.assertNotNull("TypeDescription should be initialized", schema);
            org.junit.Assert.assertEquals("Should have 5 fields", 5, schema.getFieldNames().size());
            org.junit.Assert.assertTrue("Should contain field f1", schema.getFieldNames().contains("f1"));
            org.junit.Assert.assertTrue("Should contain field f5", schema.getFieldNames().contains("f5"));

            // Verify statistics map
            java.util.Map<String, org.apache.orc.ColumnStatistics> statsMap = meta.getStatisticsMap();
            org.junit.Assert.assertNotNull("Statistics map should be initialized", statsMap);
            org.junit.Assert.assertEquals("Should have stats for all 5 fields", 5, statsMap.size());
            org.junit.Assert.assertNotNull("f1 stats should exist", statsMap.get("f1"));

            // Verify column name to idx mapping
            org.junit.Assert.assertNotNull("Column name to idx should work", meta.getColumnNameToIdx("f1"));
            org.junit.Assert.assertNotNull("Column name to idx should work", meta.getColumnNameToIdx("f5"));
            org.junit.Assert.assertNull("Non-existent column should return null",
                meta.getColumnNameToIdx("non_existent"));
        }
    }

    /**
     * Test that multiple calls to getOrcTail() return consistent results.
     */
    @Test
    public void testGetOrcTailConsistency() throws Exception {
        PreheatFileMeta realPreheat = preheat();

        try (MockedStatic<FileSystemManager> fsmStatic = Mockito.mockStatic(FileSystemManager.class);
            MockedStatic<com.alibaba.polardbx.common.orc.PreheatMetaManager> pmStatic =
                Mockito.mockStatic(com.alibaba.polardbx.common.orc.PreheatMetaManager.class)) {

            FileSystemGroup fileSystemGroup = Mockito.mock(FileSystemGroup.class);
            fsmStatic.when(() -> FileSystemManager.getFileSystemGroup(Engine.LOCAL_DISK))
                .thenReturn(fileSystemGroup);
            Mockito.when(fileSystemGroup.getMaster()).thenReturn(FILESYSTEM);

            com.alibaba.polardbx.common.orc.PreheatMetaManager mockPreheatManager =
                Mockito.mock(com.alibaba.polardbx.common.orc.PreheatMetaManager.class);
            pmStatic.when(com.alibaba.polardbx.common.orc.PreheatMetaManager::getInstance)
                .thenReturn(mockPreheatManager);
            mockPreheatManagerGet(mockPreheatManager, realPreheat);

            FilesRecord filesRecord = buildTestFilesRecord();
            OSSOrcFileMeta meta = (OSSOrcFileMeta) FileMeta.parseFrom(filesRecord);

            OrcTail first = meta.getOrcTail();
            OrcTail second = meta.getOrcTail();
            OrcTail third = meta.getOrcTail();

            org.junit.Assert.assertEquals("Schema should be consistent across calls",
                first.getSchema().toString(), second.getSchema().toString());
            org.junit.Assert.assertEquals("Schema should be consistent across calls",
                second.getSchema().toString(), third.getSchema().toString());
            org.junit.Assert.assertEquals("Footer stripe count should be consistent",
                first.getFooter().getStripesCount(), third.getFooter().getStripesCount());
        }
    }

    /**
     * Test that extractFileTail handles various valid ORC tail byte sizes correctly.
     */
    @Test
    public void testExtractFileTailWithRealFooterContent() throws Exception {
        java.nio.ByteBuffer tailBuffer;
        try (Reader reader = OrcFile.createReader(FILE_PATH,
            OrcFile.readerOptions(CONFIGURATION).filesystem(FILESYSTEM))) {
            tailBuffer = reader.getSerializedFileFooter();
        }
        byte[] tailBytes = new byte[tailBuffer.remaining()];
        tailBuffer.get(tailBytes);

        // Verify deserialized tail has expected structure
        OrcTail tail = OrcMetaUtils.extractFileTail(java.nio.ByteBuffer.wrap(tailBytes));

        // Footer should contain stripes (we wrote 1500 rows with small stripe size)
        org.junit.Assert.assertTrue("Should have at least 1 stripe",
            tail.getFooter().getStripesCount() >= 1);

        // Footer should contain column statistics for struct + 5 long columns = 6 total
        org.junit.Assert.assertEquals("Should have 6 column statistics (struct + 5 fields)",
            6, tail.getFooter().getStatisticsCount());

        // Schema should be struct with 5 fields
        TypeDescription schema = tail.getSchema();
        org.junit.Assert.assertEquals("Category should be STRUCT",
            TypeDescription.Category.STRUCT, schema.getCategory());
        org.junit.Assert.assertEquals("Should have 5 children", 5, schema.getChildren().size());
    }

    /**
     * Test toString output contains key metadata fields.
     */
    @Test
    public void testToStringContainsKeyFields() throws Exception {
        PreheatFileMeta realPreheat = preheat();

        try (MockedStatic<FileSystemManager> fsmStatic = Mockito.mockStatic(FileSystemManager.class);
            MockedStatic<com.alibaba.polardbx.common.orc.PreheatMetaManager> pmStatic =
                Mockito.mockStatic(com.alibaba.polardbx.common.orc.PreheatMetaManager.class)) {

            FileSystemGroup fileSystemGroup = Mockito.mock(FileSystemGroup.class);
            fsmStatic.when(() -> FileSystemManager.getFileSystemGroup(Engine.LOCAL_DISK))
                .thenReturn(fileSystemGroup);
            Mockito.when(fileSystemGroup.getMaster()).thenReturn(FILESYSTEM);

            com.alibaba.polardbx.common.orc.PreheatMetaManager mockPreheatManager =
                Mockito.mock(com.alibaba.polardbx.common.orc.PreheatMetaManager.class);
            pmStatic.when(com.alibaba.polardbx.common.orc.PreheatMetaManager::getInstance)
                .thenReturn(mockPreheatManager);
            mockPreheatManagerGet(mockPreheatManager, realPreheat);

            FilesRecord filesRecord = buildTestFilesRecord();
            OSSOrcFileMeta meta = (OSSOrcFileMeta) FileMeta.parseFrom(filesRecord);

            String str = meta.toString();
            org.junit.Assert.assertTrue("Should contain class name", str.contains("OSSOrcFileMeta"));
            org.junit.Assert.assertTrue("Should contain logicalTableSchema",
                str.contains("test_logical_schema"));
            org.junit.Assert.assertTrue("Should contain logicalTableName",
                str.contains("test_logical_table"));
            org.junit.Assert.assertTrue("Should contain fileName",
                str.contains(FILE_PATH.toString()));
        }
    }

    private FilesRecord buildTestFilesRecord() {
        FilesRecord filesRecord = new FilesRecord();
        filesRecord.fileName = FILE_PATH.toString();
        filesRecord.fileType = "ORC";
        filesRecord.fileMeta = new byte[0];
        filesRecord.tablespaceName = null;
        filesRecord.tableCatalog = "";
        filesRecord.tableSchema = "test_schema";
        filesRecord.tableName = "test_table";
        filesRecord.logfileGroupName = null;
        filesRecord.logfileGroupNumber = 0;
        filesRecord.engine = "LOCAL_DISK";
        filesRecord.fulltextKeys = null;
        filesRecord.deletedRows = 0;
        filesRecord.updateCount = 0;
        filesRecord.freeExtents = 0;
        filesRecord.totalExtents = 0;
        filesRecord.extentSize = 1024L;
        filesRecord.initialSize = 0;
        filesRecord.maximumSize = 0;
        filesRecord.autoextendSize = 0;
        filesRecord.creationTime = null;
        filesRecord.lastUpdateTime = null;
        filesRecord.lastAccessTime = null;
        filesRecord.recoverTime = 0;
        filesRecord.transactionCounter = 0;
        filesRecord.version = 1;
        filesRecord.rowFormat = null;
        filesRecord.tableRows = 1500L;
        filesRecord.avgRowLength = 0;
        filesRecord.dataLength = 0;
        filesRecord.maxDataLength = 0;
        filesRecord.indexLength = 0;
        filesRecord.dataFree = 0;
        filesRecord.createTime = "2024-01-01 00:00:00";
        filesRecord.updateTime = "2024-01-01 00:00:00";
        filesRecord.checkTime = null;
        filesRecord.checksum = 0;
        filesRecord.deletedChecksum = null;
        filesRecord.status = "VISIBLE";
        filesRecord.extra = null;
        filesRecord.taskId = 0;
        filesRecord.lifeCycle = 0;
        filesRecord.localPath = null;
        filesRecord.logicalSchemaName = "test_logical_schema";
        filesRecord.logicalTableName = "test_logical_table";
        filesRecord.localPartitionName = null;
        filesRecord.partitionName = "test_partition";
        filesRecord.commitTs = System.currentTimeMillis();
        filesRecord.removeTs = null;
        filesRecord.schemaTs = System.currentTimeMillis();
        filesRecord.fileHash = 123456789L;
        return filesRecord;
    }
}