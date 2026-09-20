/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.cache;

import com.alibaba.polardbx.cache.external.IdNameProvider;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.cache.CacheFileMappingAccessor;
import com.alibaba.polardbx.gms.metadb.cache.CacheFileMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Provider implementation for mapping file names to file IDs.
 * Uses CacheFileMappingAccessor to query cache_file_mapping table and maintains a Guava cache for performance.
 */
public class FileIdNameProvider implements IdNameProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileIdNameProvider.class);

    private static final String PKIDX_LOG_SUFFIX = ".pkidx.log";

    /**
     * Cache for file name to file ID mappings.
     * Maximum 4096 entries, expire after 300 seconds of no access.
     */
    private final Cache<String, Long> fileIdCache;

    /**
     * Create a new FileIdNameProvider with default cache configuration.
     */
    public FileIdNameProvider() {
        this(4096, 300, SECONDS);
    }

    /**
     * Create a new FileIdNameProvider with custom cache configuration.
     *
     * @param maxCacheSize Maximum number of entries in cache
     * @param expireAfterAccessSeconds Expiration time in seconds after last access
     * @param timeUnit Time unit for expiration
     */
    public FileIdNameProvider(long maxCacheSize, long expireAfterAccessSeconds, TimeUnit timeUnit) {
        this.fileIdCache = CacheBuilder.newBuilder()
            .maximumSize(maxCacheSize)
            .expireAfterAccess(expireAfterAccessSeconds, timeUnit)
            .build();
    }

    /**
     * Get the file ID corresponding to the given file name.
     *
     * @param fileName the file name to look up
     * @return the file ID associated with the name, 0 means invalid ID
     */
    @Override
    public long getId(String fileName) {
        if (fileName == null || fileName.isEmpty()) {
            return 0L;
        }

        // Try to get from cache first
        Long cachedId = fileIdCache.getIfPresent(fileName);
        if (cachedId != null) {
            return cachedId;
        }

        // Cache miss, query from database
        long fileId = queryFileIdFromMetaDb(fileName);

        // Store in cache if valid
        if (fileId > 0) {
            fileIdCache.put(fileName, fileId);
        }

        return fileId;
    }

    /**
     * Get or create file ID from cache_file_mapping table.
     * <p>
     * Optimized flow:
     * 1. getByFileName — try cache_file_mapping first (single SELECT)
     * 2. If found AND schema info populated → return id directly (fast path)
     * 3. If found but schema info is null → enrich from reference table, return id
     * 4. If not found → query reference table for schema info → insertIgnoreAndGet → return id
     * <p>
     * Reference table routing: .PkIdx.log files → columnar_appended_files, others → files
     *
     * @param fileName the file name to query or create mapping for
     * @return the file ID (id from cache_file_mapping), guaranteed to be valid (> 0)
     */
    private long queryFileIdFromMetaDb(String fileName) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            accessor.setConnection(metaDbConn);

            // Step 1: Try to get existing mapping
            CacheFileMappingRecord record = accessor.getByFileName(fileName);

            if (record != null) {
                // Step 2: Found — check if schema info needs enrichment
                if (record.logicalSchema == null || record.logicalSchema.isEmpty()) {
                    enrichSchemaInfo(metaDbConn, accessor, record, fileName);
                }
                LOGGER.debug("Got file ID " + record.id + " for file name: " + fileName);
                return record.id;
            }

            // Step 3: Not found — query schema info from appropriate reference table
            String[] schemaInfo = querySchemaInfo(metaDbConn, fileName);

            // Step 4: INSERT IGNORE directly (skip initial SELECT since Step 1 already confirmed not exists)
            long fileId = accessor.insertIgnoreAndGet(
                fileName, schemaInfo[0], schemaInfo[1], schemaInfo[2], schemaInfo[3],
                null, 0, false
            );

            LOGGER.debug("Created file ID " + fileId + " for file name: " + fileName);
            return fileId;
        } catch (Exception e) {
            LOGGER.error("Error getting file ID for: " + fileName);
            throw GeneralUtil.nestedException(e);
        }
    }

    private static boolean isPkIdxLogFile(String fileName) {
        return fileName != null && fileName.toLowerCase().endsWith(PKIDX_LOG_SUFFIX);
    }

    /**
     * Query schema info (logicalSchema, logicalTable, partName, engine) from the appropriate
     * reference table based on file suffix.
     *
     * @return String[4]: {logicalSchema, logicalTable, partName, engine}, elements may be null
     */
    private String[] querySchemaInfo(Connection conn, String fileName) {
        String[] info = new String[4];
        if (isPkIdxLogFile(fileName)) {
            ColumnarAppendedFilesRecord cafRecord = queryColumnarAppendedRecord(conn, fileName);
            if (cafRecord != null) {
                info[0] = cafRecord.logicalSchema;
                info[1] = cafRecord.logicalTable;
                info[2] = cafRecord.partName;
                info[3] = cafRecord.engine;
            }
        } else {
            FilesRecord filesRecord = queryFilesRecord(conn, fileName);
            if (filesRecord != null) {
                info[0] = filesRecord.logicalSchemaName;
                info[1] = filesRecord.logicalTableName;
                info[2] = filesRecord.partitionName;
                info[3] = filesRecord.engine;
            }
        }
        return info;
    }

    /**
     * Query files system table by pure file name.
     *
     * @return the first matching FilesRecord, or null if not found
     */
    private FilesRecord queryFilesRecord(Connection conn, String fileName) {
        try {
            FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(conn);
            List<FilesRecord> records = filesAccessor.queryByFileName(fileName);
            return records.isEmpty() ? null : records.get(0);
        } catch (Exception e) {
            LOGGER.warn("Failed to query files table for: " + fileName);
            return null;
        }
    }

    /**
     * Query columnar_appended_files table by file name.
     *
     * @return the last appended record, or null if not found
     */
    private ColumnarAppendedFilesRecord queryColumnarAppendedRecord(Connection conn, String fileName) {
        try {
            ColumnarAppendedFilesAccessor accessor = new ColumnarAppendedFilesAccessor();
            accessor.setConnection(conn);
            List<ColumnarAppendedFilesRecord> records = accessor.queryFileLastAppendedRecord(fileName);
            return records.isEmpty() ? null : records.get(0);
        } catch (Exception e) {
            LOGGER.warn("Failed to query columnar_appended_files for: " + fileName);
            return null;
        }
    }

    /**
     * Enrich cache_file_mapping record with schema info from the appropriate reference table.
     * Uses columnar_appended_files for .PkIdx.log files, files table for others.
     * Failure does not affect the core flow — only logs a warning.
     */
    private void enrichSchemaInfo(Connection conn, CacheFileMappingAccessor cacheAccessor,
                                  CacheFileMappingRecord record, String cacheFileName) {
        try {
            String[] info = querySchemaInfo(conn, cacheFileName);
            if (info[0] != null) {
                cacheAccessor.updateSchemaInfo(record.id, info[0], info[1], info[2], info[3]);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to enrich cache_file_mapping for: " + cacheFileName);
        }
    }

    /**
     * Invalidate cache entry for the given file name.
     *
     * @param fileName the file name to invalidate
     */
    public void invalidate(String fileName) {
        if (fileName != null) {
            fileIdCache.invalidate(fileName);
        }
    }

    /**
     * Clear all entries from the cache.
     */
    public void invalidateAll() {
        fileIdCache.invalidateAll();
    }

    /**
     * Get the current size of the cache.
     *
     * @return the number of entries in the cache
     */
    public long getCacheSize() {
        return fileIdCache.size();
    }

    /**
     * Get cache statistics.
     *
     * @return cache stats as string
     */
    public String getCacheStats() {
        return fileIdCache.stats().toString();
    }
}
