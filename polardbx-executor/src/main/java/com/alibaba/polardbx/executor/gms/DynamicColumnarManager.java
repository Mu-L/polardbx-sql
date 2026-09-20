/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.oss.ColumnarPartitionPrunedSnapshot;
import com.alibaba.polardbx.common.oss.OSSFileType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.schemaevolution.ColumnMetaWithTs;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.columnar.AutoFlashbackCSVFileReader;
import com.alibaba.polardbx.executor.columnar.CsvDataIterator;
import com.alibaba.polardbx.executor.columnar.FlashbackCsvDataIterator;
import com.alibaba.polardbx.executor.columnar.SimpleCSVFileReader;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.columnar.FlashbackColumnarManager;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarFileMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarFileMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.htrace.shaded.fasterxml.jackson.databind.util.EmptyIterator;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.RoaringBitmap;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The instructions of Dynamic-columnar-manager:
 * 0. (Inner) Periodically fetch the newest tso from meta db.
 * 1. Find columnar-partition-snapshot of certain tso.
 * 2. Get file meta from file-meta-cache and columnar-snapshot-cache use findFileNames method.
 * 3. Use oss read option to generate orc-task / csv-task
 * 4. Use getDeleteBitMapOf method to generate delete bitmap for each task
 */
public class DynamicColumnarManager extends AbstractLifecycle implements ColumnarManager {
    private static final Logger LOGGER = LoggerFactory.getLogger("mpp_log");

    private static final DynamicColumnarManager INSTANCE = new DynamicColumnarManager();

    public static final int MAXIMUM_FILE_META_COUNT = 1 << 18;
    public static final int MAXIMUM_SIZE_OF_SNAPSHOT_CACHE = 1 << 16;
    public static final int MAXIMUM_SIZE_OF_POSITION_CACHE_VERSION = 1 << 14;

    private FileVersionStorage versionStorage;
    private final Queue<String> filesToBePurged = new LinkedBlockingQueue<>();
    private final Object minTsoLock = new Object();
    private final Object latestTsoLock = new Object();
    private final AtomicLong purgeCount = new AtomicLong(0);

    /**
     * columnar snapshot cache
     */
    Cache<String, FlashbackColumnarManager> fcmCache;
    Cache<String, FlashbackDeleteBitmapManager> fdbmCache;
    Cache<Pair<String, Long>, List<Chunk>> csvSnapshotCache;

    /**
     * Cache all file-meta by its file name.
     */
    private LoadingCache<String, FileMeta> fileMetaCache;
    /**
     * Cache all Multi-version snapshot by schema name and table id.
     */
    private LoadingCache<Pair<String, Long>, MultiVersionColumnarSnapshot> snapshotCache;
    /**
     * columnar schema of each tso
     */
    private MultiVersionColumnarSchema columnarSchema;
    /**
     * Cache mapping: {partition-info, file-id} -> file-name
     */
    private LoadingCache<Pair<PartitionId, Integer>, Optional<String>> fileIdMapping;
    /**
     * cache append file last record: tso -> {file-name -> records}
     */
    private LoadingCache<Long, Map<String, List<ColumnarAppendedFilesRecord>>> appendFileRecordCache;
    /**
     * cache min compaction tso: checkpoint tso -> min compaction tso
     */
    private LoadingCache<Long, Long> minCompactionTsoCache;
    private LoadingCache<String, Integer> appendFileMaxLengthCache;

    private ColumnarVersionChainPruner columnarVersionChainPruner;

    private AtomicLong appendFileAccessCounter = new AtomicLong();
    private volatile Long minTso;
    private volatile Long latestTso;

    // Memory size count for file meta cache.
    private AtomicLong fileMetaCacheMemorySize = new AtomicLong(0);

    public DynamicColumnarManager() {
    }

    @Override
    public List<Object[]> dumpMemoryUsage() {
        DecimalFormat decimalFormat = new DecimalFormat("0.000000");
        List<Object[]> results = new ArrayList<>();
        Object[] result;

        // for csv cache.
        result = new Object[7];
        long csvCacheEntries = versionStorage.getCsvCacheSize();
        result[0] = "CSV CACHE".getBytes();
        result[1] = versionStorage.getCsvCacheSizeInBytes();
        result[2] = -1;
        result[3] = -1;

        result[4] = csvCacheEntries;
        result[5] = MAXIMUM_FILE_META_COUNT;
        result[6] = decimalFormat.format(csvCacheEntries * 1.0d / MAXIMUM_FILE_META_COUNT);
        results.add(result);

        // for del cache.
        result = new Object[7];
        long delCacheEntries = versionStorage.getDelCacheSize();
        result[0] = "DEL CACHE".getBytes();
        result[1] = versionStorage.getDelCacheSizeInBytes();
        result[2] = -1;
        result[3] = -1;

        result[4] = delCacheEntries;
        result[5] = -1;
        result[6] = -1;
        results.add(result);

        // for file cache.
        result = new Object[7];
        long fileMetaCacheEntries = fileMetaCache.size();
        result[0] = "FILE META CACHE".getBytes();
        result[1] = fileMetaCacheMemorySize.get();
        result[2] = -1;
        result[3] = -1;

        result[4] = fileMetaCacheEntries;
        result[5] = MAXIMUM_FILE_META_COUNT;
        result[6] = decimalFormat.format(fileMetaCacheEntries * 1.0d / MAXIMUM_FILE_META_COUNT);
        results.add(result);

        return results;
    }

    public static DynamicColumnarManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    public static List<ColumnarAppendedFilesRecord> getLatestAppendFileRecord(String fileName, Long tso) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarAppendedFilesAccessor columnarAppendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            columnarAppendedFilesAccessor.setConnection(connection);
            return columnarAppendedFilesAccessor.queryByFileNameAndMaxTso(fileName, tso);

        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, e,
                "fail to fetch append file record with file: " + fileName + " and tso: " + tso);
        }
    }

    public static int getMaxLength(String fileName) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(connection);
            List<FilesRecord> filesRecords = filesAccessor.queryColumnarByFileName(fileName);
            return (int) filesRecords.get(0).extentSize;
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, e,
                "fail to fetch max length: " + fileName);
        }
    }

    public FileVersionStorage getVersionStorage() {
        return versionStorage;
    }

    /**
     * This method is for unit test ONLY
     */
    public void injectForTest(FileVersionStorage versionStorage,
                              MultiVersionColumnarSchema multiVersionColumnarSchema,
                              AtomicLong appendFileAccessCounter,
                              LoadingCache<Long, Map<String, List<ColumnarAppendedFilesRecord>>> appendFileRecordCache,
                              LoadingCache<String, Integer> appendFileMaxLengthCache) {
        this.versionStorage = versionStorage;
        if (versionStorage != null) {
            this.versionStorage.open();
        }
        this.columnarSchema = multiVersionColumnarSchema;
        this.appendFileAccessCounter = appendFileAccessCounter;
        this.appendFileRecordCache = appendFileRecordCache;
        this.appendFileMaxLengthCache = appendFileMaxLengthCache;
    }

    @Override
    protected void doInit() {
        this.versionStorage = new FileVersionStorage(this);
        this.versionStorage.open();
        this.columnarSchema = new MultiVersionColumnarSchema(this);

        // Build file meta cache
        this.fileMetaCache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_FILE_META_COUNT)
            .removalListener(new RemovalListener<String, FileMeta>() {

                @Override
                public void onRemoval(RemovalNotification<String, FileMeta> notification) {
                    fileMetaCacheMemorySize.addAndGet(-(FastMemoryCounter.sizeOf(notification.getKey())
                        + FastMemoryCounter.sizeOf(notification.getValue())));
                }
            })
            .build(
                new CacheLoader<String, FileMeta>() {
                    @Override
                    public FileMeta load(@NotNull String fileName) {
                        List<FilesRecord> filesRecords = null;
                        try (Connection connection = MetaDbUtil.getConnection()) {
                            FilesAccessor filesAccessor = new FilesAccessor();
                            filesAccessor.setConnection(connection);

                            // query meta db && filter table files.
                            filesRecords = filesAccessor
                                .queryColumnarByFileName(fileName)
                                .stream()
                                .filter(filesRecord -> OSSFileType.of(filesRecord.fileType) == OSSFileType.TABLE_FILE)
                                .collect(Collectors.toList());
                        } catch (SQLException e) {
                            // ignore.
                        }

                        if (!filesRecords.isEmpty()) {
                            FileMeta fileMeta = FileMeta.parseSimpleFileMetaFrom(filesRecords.get(0));

                            // fill with column meta.
                            List<ColumnMeta> columnMetas =
                                getColumnMetas(fileMeta.getSchemaTs(),
                                    Long.parseLong(fileMeta.getLogicalTableName()));
                            fileMeta.initColumnMetas(ColumnarStoreUtils.IMPLICIT_COLUMN_CNT, columnMetas);

                            fileMetaCacheMemorySize.addAndGet(
                                FastMemoryCounter.sizeOf(fileName) + FastMemoryCounter.sizeOf(fileMeta));

                            return fileMeta;
                        }

                        return null;
                    }
                }
            );

        final DynamicColumnarManager self = this;
        // Build snapshot cache
        this.snapshotCache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_SIZE_OF_SNAPSHOT_CACHE)
            .build(new CacheLoader<Pair<String, Long>, MultiVersionColumnarSnapshot>() {
                @Override
                public MultiVersionColumnarSnapshot load(@NotNull Pair<String, Long> schemaAndTableId) {
                    return new MultiVersionColumnarSnapshot(
                        self, schemaAndTableId.getKey(), schemaAndTableId.getValue()
                    );
                }
            });

        // Build file-id mapping cache
        this.fileIdMapping = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_FILE_META_COUNT)
            .build(new CacheLoader<Pair<PartitionId, Integer>, Optional<String>>() {
                @Override
                public Optional<String> load(@NotNull Pair<PartitionId, Integer> key) throws Exception {
                    Integer columnarFileId = key.getValue();
                    PartitionId partitionId = key.getKey();
                    String logicalSchema = partitionId.getLogicalSchema();
                    String tableId = String.valueOf(partitionId.getTableId());
                    String partName = partitionId.getPartName();

                    List<ColumnarFileMappingRecord> records;
                    try (Connection connection = MetaDbUtil.getConnection()) {
                        ColumnarFileMappingAccessor accessor = new ColumnarFileMappingAccessor();
                        accessor.setConnection(connection);

                        records = accessor.queryByFileId(
                            logicalSchema, tableId, partName, columnarFileId
                        );
                    }

                    if (records != null && !records.isEmpty()) {
                        ColumnarFileMappingRecord record = records.get(0);
                        return Optional.of(record.getFileName());
                    }

                    return Optional.empty();
                }
            });

        this.appendFileRecordCache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_SIZE_OF_POSITION_CACHE_VERSION)
            .build(new CacheLoader<Long, Map<String, List<ColumnarAppendedFilesRecord>>>() {
                @Override
                public Map<String, List<ColumnarAppendedFilesRecord>> load(@NotNull Long tso) {
                    return Maps.newConcurrentMap();
                }
            });

        this.appendFileMaxLengthCache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_FILE_META_COUNT)
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(new CacheLoader<String, Integer>() {
                @Override
                public Integer load(@NotNull String fileName) {
                    return getMaxLength(fileName);
                }
            });

        this.minCompactionTsoCache = CacheBuilder.newBuilder()
            .maximumSize(256)
            .build(new CacheLoader<Long, Long>() {
                @Override
                public Long load(@NotNull Long tso) throws SQLException {
                    try (Connection connection = MetaDbUtil.getConnection()) {
                        ColumnarCheckpointsAccessor accessor = new ColumnarCheckpointsAccessor();
                        accessor.setConnection(connection);

                        List<ColumnarCheckpointsRecord> checkpointsRecords = accessor.queryValidCheckpointByTso(tso);
                        if (checkpointsRecords != null && !checkpointsRecords.isEmpty()) {
                            return checkpointsRecords.get(0).minCompactionTso;
                        }

                        return 0L;
                    }
                }
            });

        initSnapshotCache(DynamicConfig.getInstance().getColumnarSnapshotCacheTtlMs());

        this.columnarVersionChainPruner = new ColumnarVersionChainPruner();
        LOGGER.info("Columnar Manager of has been initialized");
    }

    @Override
    public void purge(long tso) {
        // Update inner state of version chain
        if (minTso != null && minTso >= tso) {
            return;
        }

        synchronized (minTsoLock) {
            if (minTso != null && minTso >= tso) {
                return;
            }

            // update min tso before physical purge to make it safe
            minTso = tso;
            // this could collect all files which should be purged
            snapshotCache.asMap().values().forEach(snapshot -> snapshot.purge(tso));

            for (String fileName = nextPurgedFile(); fileName != null; fileName = nextPurgedFile()) {
                ColumnarFileType columnarFileType = FileSystemUtils.getFileType(fileName);
                fileMetaCache.invalidate(fileName);

                if (columnarFileType.isDeltaFile()) {
                    versionStorage.purgeByFile(fileName);
                }
            }

            versionStorage.purge(tso);

            List<Long> versionsToPurge = appendFileRecordCache.asMap().keySet().stream().filter(
                t -> t < tso
            ).collect(Collectors.toList());
            appendFileRecordCache.invalidateAll(versionsToPurge);

            purgeCount.incrementAndGet();
            // TODO(siyun): purge when CCI is dropped

            // columnar fetches purge signal by SHOW COLUMNAR OFFSET
        }
    }

    public void putPurgedFile(String fileName) {
        filesToBePurged.add(fileName);
    }

    private String nextPurgedFile() {
        return filesToBePurged.poll();
    }

    @Override
    public Pair<List<FileMeta>, List<FileMeta>> findFiles(long tso, String logicalSchema, String logicalTable,
                                                          String partName, TableMeta tableMeta) {
        if (tso == Long.MIN_VALUE) {
            return Pair.of(ImmutableList.of(), ImmutableList.of());
        }
        try {
            Long tableId = getTableId(tso, logicalSchema, logicalTable, tableMeta);
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot = snapshotCache.get(
                Pair.of(logicalSchema, tableId)
            ).generateSnapshot(partName, tso);

            List<FileMeta> csvFiles = new ArrayList<>();
            List<FileMeta> orcFiles = new ArrayList<>();

            // fetch orc file metas
            snapshot.getOrcFiles().stream().map(
                this::fileMetaOf
            ).forEach(orcFiles::add);

            // fetch csv file metas
            snapshot.getCsvFiles().stream().map(
                this::fileMetaOf
            ).forEach(csvFiles::add);

            return Pair.of(orcFiles, csvFiles);
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to generate columnar snapshot of tso: %d", tso));
        }
    }

    public Long getLowerTsoByTableId(String logicalSchema, Long tableId, long tso) {
        Integer offsetDays = columnarVersionChainPruner.getPrunerMap().get(Pair.of(logicalSchema, tableId));
        if (offsetDays == null) {
            return null;
        }

        return ColumnarTransactionUtils.getTsoBeforeSeconds(tso, offsetDays.longValue() * 24 * 3600);
    }

    @Override
    public Pair<List<String>, List<String>> findFileNames(long tso, String logicalSchema, String logicalTable,
                                                          String partName, TableMeta tableMeta) {
        if (tso == Long.MIN_VALUE) {
            return Pair.of(ImmutableList.of(), ImmutableList.of());
        }
        try {
            Long tableId = getTableId(tso, logicalSchema, logicalTable, tableMeta);
            Long lowerTso = getLowerTsoByTableId(logicalSchema, tableId, tso);
            MultiVersionColumnarSnapshot mvSnapshot = snapshotCache.get(Pair.of(logicalSchema, tableId));
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot =
                lowerTso == null ? mvSnapshot.generateSnapshot(partName, tso) :
                    mvSnapshot.generateSnapshot(partName, lowerTso, tso);
            return Pair.of(snapshot.getOrcFiles(), snapshot.getCsvFiles());
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to generate columnar snapshot of tso: %d", tso));
        }
    }

    public Map<String, ColumnarPartitionPrunedSnapshot> findFileNames(long tso,
                                                                      String logicalSchema,
                                                                      String logicalTable,
                                                                      SortedMap<Long, Set<String>> partitionResult,
                                                                      TableMeta tableMeta) {
        if (tso == Long.MIN_VALUE) {
            return new HashMap<>();
        }
        try {
            Long tableId = getTableId(tso, logicalSchema, logicalTable, tableMeta);
            return snapshotCache.get(
                Pair.of(logicalSchema, tableId)
            ).generateSnapshot(partitionResult, tso);
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to generate columnar snapshot of tso: %d", tso));
        }
    }

    public ConcurrentNavigableMap<Long, PartitionInfo> getPartitionInfos(long tso, String logicalSchema,
                                                                         String logicalTable,
                                                                         TableMeta tableMeta) {
        try {
            Long tableId = getTableId(tso, logicalSchema, logicalTable, tableMeta);
            long schemaTso = snapshotCache.get(Pair.of(logicalSchema, tableId)).getLatestSchemaTso(tso);
            return columnarSchema.getPartitionInfos(schemaTso, tableId);
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to generate columnar partition info of tso: %d", tso));
        }
    }

    public List<ColumnarTableMeta> getColumnarTableMeta(long tso, String logicalSchema, String logicalTable) {
        try {
            List<ColumnarTableMeta> columnarTableMetas = new ArrayList<>();
            List<Long> tableIds = getTableIds(tso, logicalSchema, logicalTable);
            for (Long tableId : tableIds) {
                columnarTableMetas.add(columnarSchema.getColumnarTableMetaByTso(tableId, tso));
            }
            return columnarTableMetas;
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to generate columnar table meta info of tso: %d", tso));
        }
    }

    public Pair<Long, List<String>> delFileNames(String logicalSchema, String logicalTable, String partName) {
        try {
            Pair<Long, MultiVersionColumnarSnapshot.ColumnarSnapshot> minTsoAndSnapshot = snapshotCache.get(
                Pair.of(logicalSchema, Long.valueOf(logicalTable))
            ).generateMinSnapshot(partName);

            return Pair.of(minTsoAndSnapshot.getKey(), minTsoAndSnapshot.getValue().getDelFiles());
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                "Failed to generate columnar snapshot of minTso");
        }
    }

    private List<ColumnarAppendedFilesRecord> getOrLoadAppendFilesRecord(long tso, String csvFileName)
        throws ExecutionException {
        Map<String, List<ColumnarAppendedFilesRecord>> recordMap = appendFileRecordCache.get(tso);
        return recordMap.computeIfAbsent(csvFileName, k -> getLatestAppendFileRecord(csvFileName, tso));
    }

    @Override
    public Iterator<Chunk> csvData(long tso, String csvFileName, ExecutionContext ec) {
        appendFileAccessCounter.getAndIncrement();
        try {
            if (versionStorage.isCsvPositionLoaded(csvFileName, tso)) {
                return versionStorage.csvData(tso, tso, csvFileName, ec).iterator();
            }
            List<ColumnarAppendedFilesRecord> appendedFilesRecords = getOrLoadAppendFilesRecord(tso, csvFileName);
            if (appendedFilesRecords == null || appendedFilesRecords.isEmpty()) {
                return new EmptyIterator<>();
            } else {
                Preconditions.checkArgument(appendedFilesRecords.size() == 1);
                // TODO(siyun): async IO
                return versionStorage.csvData(appendedFilesRecords.get(0).checkpointTso, tso, csvFileName, ec)
                    .iterator();
            }
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_CSV_FILE, t,
                String.format("Failed to load csv file, filename: %s, tso: %d", csvFileName, tso));
        }
    }

    @Override
    public Iterator<Chunk> csvData(String csvFileName, long position) {
        FileMeta fileMeta = fileMetaOf(csvFileName);
        Engine engine = fileMeta.getEngine();
        List<ColumnMeta> columnMetas = fileMeta.getColumnMetas();

        // Create and return the chunk iterator
        return new CsvDataIterator(new SimpleCSVFileReader(), csvFileName, position, columnMetas, engine);
    }

    @Override
    public Iterator<Chunk> flashbackCsvData(long tso, String csvFileName) {
        if (DynamicConfig.getInstance().enableColumnarSnapshotCache()) {
            try {
                return csvSnapshotCache.get(Pair.of(csvFileName, tso), () -> buildCsvSnapshotChunk(tso, csvFileName))
                    .iterator();
            } catch (ExecutionException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                    String.format("Failed to generate columnar snapshot of tso: %d", tso));
            }
        } else {
            int maxLength = getMaxLength(csvFileName, tso);
            if (maxLength == 0) {
                // short path for zero length file
                return new EmptyIterator<>();
            }
            FileMeta fileMeta = fileMetaOf(csvFileName);
            Engine engine = fileMeta.getEngine();
            List<ColumnMeta> columnMetas = fileMeta.getColumnMetas();
            return new FlashbackCsvDataIterator(new AutoFlashbackCSVFileReader(),
                csvFileName, tso, maxLength, columnMetas, engine);
        }
    }

    private List<Chunk> buildCsvSnapshotChunk(long tso, String csvFileName) {
        int maxLength = getMaxLength(csvFileName, tso);
        if (maxLength == 0) {
            // short path for zero length file
            return ImmutableList.of();
        }

        FileMeta fileMeta = fileMetaOf(csvFileName);
        Engine engine = fileMeta.getEngine();
        List<ColumnMeta> columnMetas = fileMeta.getColumnMetas();

        try (AutoFlashbackCSVFileReader reader = new AutoFlashbackCSVFileReader()) {
            reader.open(new ExecutionContext(), columnMetas, FileVersionStorage.CSV_CHUNK_LIMIT, engine,
                csvFileName, tso, maxLength);

            List<Chunk> chunkList = new ArrayList<>();
            Chunk chunk;
            while ((chunk = reader.next()) != null) {
                chunkList.add(chunk);
            }
            return chunkList;
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_CSV_FILE, t,
                String.format("Failed to open csv file reader, file name: %s, tso: %d", csvFileName, tso));
        }
    }

    @Override
    public int fillSelection(String fileName, long tso, int[] selection, LongBlock positionBlock) {
        return versionStorage.fillSelection(fileName, tso, selection, positionBlock);
    }

    @Override
    public int fillSelection(String fileName, long tso, int[] selection, LongColumnVector longColumnVector,
                             int batchSize) {
        return versionStorage.fillSelection(fileName, tso, selection, longColumnVector, batchSize);
    }

    @Override
    public Optional<String> fileNameOf(String logicalSchema, long tableId, String partName, int columnarFileId) {
        PartitionId partitionId = PartitionId.of(partName, logicalSchema, tableId);
        try {
            return fileIdMapping.get(Pair.of(partitionId, columnarFileId));
        } catch (ExecutionException e) {
            fileIdMapping.invalidate(Pair.of(partitionId, columnarFileId));
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SCHEMA, e.getCause(),
                "Failed to fetch file id of partition: " + e.getCause().getMessage());
        }
    }

    @Override
    public FileMeta fileMetaOf(String fileName) {
        try {
            return fileMetaCache.get(fileName);
        } catch (ExecutionException e) {
            fileMetaCache.invalidate(fileName);
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SCHEMA, e.getCause(),
                "Failed to fetch file meta of file, file name: " + fileName);
        }
    }

    @Override
    public @NotNull
    List<Long> getColumnFieldIdList(long versionId, long tableId) {
        return columnarSchema.getColumnFieldIdList(versionId, tableId);
    }

    @Override
    public @NotNull
    List<ColumnMeta> getColumnMetas(long schemaTso, String logicalSchema, String logicalTable,
                                    TableMeta tableMeta) {
        return getColumnMetas(schemaTso, getTableId(schemaTso, logicalSchema, logicalTable, tableMeta));
    }

    @Override
    public @NotNull
    List<ColumnMeta> getColumnMetas(long schemaTso, long tableId) {
        return columnarSchema.getColumnMetas(schemaTso, tableId);
    }

    @Override
    public @NotNull
    Map<Long, Integer> getColumnIndex(long schemaTso, long tableId) {
        return columnarSchema.getColumnIndexMap(schemaTso, tableId);
    }

    @Override
    public @NotNull
    ColumnMetaWithTs getInitColumnMeta(long tableId, long fieldId) {
        return columnarSchema.getInitColumnMeta(tableId, fieldId);
    }

    @Override
    public int[] getPrimaryKeyColumns(String fileName) {
        FileMeta fileMeta = fileMetaOf(fileName);
        long tableId = Long.parseLong(fileMeta.getLogicalTableName());
        long schemaTso = fileMeta.getSchemaTs();
        return columnarSchema.getPrimaryKeyColumns(schemaTso, tableId);
    }

    @Override
    public RoaringBitmap getDeleteBitMapOf(long tso, String fileName) {
        FileMeta fileMeta = fileMetaOf(fileName);
        return versionStorage.getDeleteBitMap(fileMeta, tso);
    }

    @Override
    public RoaringBitmap getDeleteBitMapOf(long tso, String fileName, Boolean cacheOverride) {
        FileMeta fileMeta = fileMetaOf(fileName);
        return versionStorage.getDeleteBitMap(fileMeta, tso, cacheOverride);
    }

    @Override
    public long latestTso() {
        if (latestTso != null) {
            return latestTso;
        }
        // Fetch the latest tso
        synchronized (latestTsoLock) {
            if (latestTso != null) {
                return latestTso;
            }
            int tsoUpdateDelay = InstConfUtil.getInt(ConnectionParams.COLUMNAR_TSO_UPDATE_DELAY);
            Long gmsLatestTso;
            if (tsoUpdateDelay > 0) {
                gmsLatestTso = ColumnarTransactionUtils.getLatestTsoFromGmsWithDelay(
                    1000L * tsoUpdateDelay // convert milliseconds to microseconds
                );
            } else {
                gmsLatestTso = ColumnarTransactionUtils.getLatestTsoFromGms();
            }
            latestTso = gmsLatestTso != null ? gmsLatestTso : Long.MIN_VALUE;
            return latestTso;
        }
    }

    @Override
    public void setLatestTso(long tso) {
        if (latestTso == null || latestTso < tso) {
            synchronized (latestTsoLock) {
                if (latestTso == null || latestTso < tso) {
                    latestTso = tso;
                }
            }
        }
    }

    public Long getTableId(long tso, String logicalSchema, String logicalTable, TableMeta tableMeta) {
        try {
            return columnarSchema.getTableId(tso, logicalSchema, logicalTable, tableMeta);
        } catch (ExecutionException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SCHEMA, e.getCause(),
                String.format("Failed to fetch table id, tso: %d, schema name: %s, table name: %s",
                    tso, logicalSchema, logicalTable));
        }
    }

    public List<Long> getTableIds(long tso, String logicalSchema, String logicalTable) {
        return columnarSchema.getTableIds(tso, logicalSchema, logicalTable);
    }

    public List<byte[][]> generatePacket() {
        return versionStorage.generatePacket();
    }

    @Override
    protected void doDestroy() {
        versionStorage.close();
    }

    @Override
    public List<Integer> getPhysicalColumnIndexes(long tso, String tableName, List<Integer> columnIndexes) {
        // TODO(siyun): NO MOCK
        return columnIndexes.stream().map(index -> index + ColumnarStoreUtils.IMPLICIT_COLUMN_CNT)
            .collect(Collectors.toList());
    }

    @Override
    public Map<Long, Integer> getPhysicalColumnIndexes(String fileName) {
        FileMeta fileMeta = fileMetaOf(fileName);
        long tableId = Long.parseLong(fileMeta.getLogicalTableName());
        long schemaTso = fileMeta.getSchemaTs();
        return columnarSchema.getColumnIndexMap(schemaTso, tableId);
    }

    /**
     * 获取当前缓存的下水位线，每分钟更新一次，用于优化增量文件的加载和 purge
     *
     * @return 下水位线的一个下界
     */
    @NotNull
    public Long getMinTso() {
        if (minTso == null) {
            synchronized (minTsoLock) {
                if (minTso == null) {
                    minTso = ColumnarTransactionUtils.getMinColumnarSnapshotTime();
                }
            }
        }

        return minTso;
    }

    public int getMaxLength(String fileName, long tso) {
        try {
            List<ColumnarAppendedFilesRecord> appendedFilesRecords = getOrLoadAppendFilesRecord(tso, fileName);
            if (appendedFilesRecords == null || appendedFilesRecords.isEmpty()) {
                // 2 cases are possible here:
                // 1. the file has already finished appending
                // 2. the file just created and not appending, whose max length is 0
                // For case 2, we don't want to cache the value 0, so we manually handle the caching logic
                Integer cachedValue = appendFileMaxLengthCache.getIfPresent(fileName);
                if (cachedValue != null) {
                    return cachedValue;
                }

                // Load the value directly from the source without using the cache loader
                // to avoid automatic caching of 0 values
                int value = getMaxLength(fileName);

                // Only cache the value if it's not 0
                if (value != 0) {
                    appendFileMaxLengthCache.put(fileName, value);
                }

                return value;
            } else {
                ColumnarAppendedFilesRecord record = appendedFilesRecords.get(0);
                return (int) (record.appendOffset + record.appendLength);
            }
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e);
        }
    }

    public void resetCsvCacheSize(int newCacheSize) {
        versionStorage.resetCsvCacheSize(newCacheSize);
    }

    public long getMinCompactionTso(Long tso) {
        try {
            return minCompactionTsoCache.get(tso);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * approximate number of entries in this cache
     */
    public long getLoadedAppendFileCount() {
        return appendFileRecordCache.size() * snapshotCache.size();
    }

    public long getPurgeCount() {
        return purgeCount.get();
    }

    /**
     * size of loaded version
     */
    public long getLoadedVersionCount() {
        return appendFileRecordCache.size();
    }

    public long getAppendFileAccessCount() {
        return appendFileAccessCounter.get();
    }

    /**
     * Get file meta cache memory size in bytes
     */
    public long getFileMetaCacheMemorySize() {
        return fileMetaCacheMemorySize.get();
    }

    /**
     * Get file meta cache entry count
     */
    public long getFileMetaCacheCount() {
        return fileMetaCache != null ? fileMetaCache.size() : 0;
    }

    @Override
    public void reload() {
        reload(ReloadType.ALL);
    }

    @Override
    public void reload(ReloadType type) {
        synchronized (minTsoLock) {
            minTso = null;
            synchronized (latestTsoLock) {
                latestTso = null;
                if (type == ReloadType.ALL || type == ReloadType.SNAPSHOT_ONLY) {
                    try {
                        snapshotCache.invalidateAll();
                        fileMetaCache.invalidateAll();
                        fileIdMapping.invalidateAll();
                        appendFileRecordCache.invalidateAll();
                        minCompactionTsoCache.invalidateAll();
                        filesToBePurged.clear();
                    } catch (Throwable t) {
                        // ignore
                    }
                }

                if (type == ReloadType.ALL || type == ReloadType.SCHEMA_ONLY) {
                    try {
                        this.columnarSchema = new MultiVersionColumnarSchema(this);
                    } catch (Throwable t) {
                        // ignore
                    }
                }

                if (type == ReloadType.ALL || type == ReloadType.CACHE_ONLY) {
                    try {
                        this.versionStorage = new FileVersionStorage(this);
                        this.versionStorage.open();
                    } catch (Throwable t) {
                        // ignore
                    }
                }
            }
        }

        LOGGER.info("Columnar Manager of has been reloaded");
    }

    private void initSnapshotCache(int cacheTtlMs) {
        fcmCache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_FILE_META_COUNT)
            .expireAfterAccess(cacheTtlMs, TimeUnit.MILLISECONDS)
            .build();
        fdbmCache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_FILE_META_COUNT)
            .expireAfterAccess(cacheTtlMs, TimeUnit.MILLISECONDS)
            .build();
        csvSnapshotCache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_FILE_META_COUNT)
            .expireAfterAccess(cacheTtlMs, TimeUnit.MILLISECONDS)
            .build();
    }

    @Override
    public FlashbackColumnarManager getFlashbackColumnarManager(long flashbackTso, String logicalSchema,
                                                                String logicalTable, boolean autoPosition) {
        if (DynamicConfig.getInstance().enableColumnarSnapshotCache()) {
            String key = logicalSchema + "." + logicalTable + "." + flashbackTso + "." + autoPosition;
            try {
                return fcmCache.get(key,
                    () -> new FlashbackColumnarManager(flashbackTso, logicalSchema, logicalTable, autoPosition));
            } catch (ExecutionException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                    String.format("Failed to generate columnar snapshot of tso: %d", flashbackTso));
            }
        } else {
            return new FlashbackColumnarManager(flashbackTso, logicalSchema, logicalTable, autoPosition);
        }
    }

    @Override
    public FlashbackDeleteBitmapManager getFlashbackDeleteBitmapManager(long flashbackTso, String logicalSchema,
                                                                        String logicalTable, String partName,
                                                                        List<Pair<String, Long>> delPositions) {
        if (DynamicConfig.getInstance().enableColumnarSnapshotCache()) {
            String key = logicalSchema + "." + logicalTable + "." + partName + "." + flashbackTso;
            try {
                return fdbmCache.get(key,
                    () -> new FlashbackDeleteBitmapManager(flashbackTso, logicalSchema, logicalTable, partName,
                        delPositions));
            } catch (ExecutionException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                    String.format("Failed to generate columnar snapshot of tso: %d", flashbackTso));
            }
        } else {
            return FlashbackDeleteBitmapManager.getInstance(flashbackTso, logicalSchema, logicalTable, partName,
                delPositions);
        }
    }

    synchronized public void resetSnapshotCacheTtlMs(int cacheTtlMs) {
        initSnapshotCache(cacheTtlMs);
    }

    synchronized public void reloadColumnarVersionChainPruner(String prunerString) {
        columnarVersionChainPruner.reload(prunerString);
    }

}