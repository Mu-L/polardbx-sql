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

package com.alibaba.polardbx.gms.metadb.cache;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.CACHE_FILE_MAPPING;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_APPENDED_FILES;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.FILES;

/**
 * Accessor for cache_file_mapping table.
 * Provides mapping from file_name to id (as file_id).
 */
public class CacheFileMappingAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheFileMappingAccessor.class);
    private static final String CACHE_FILE_MAPPING_TABLE = wrap(CACHE_FILE_MAPPING);

    // Insert ignore to ensure unique mapping, id is auto-generated as file_id
    private static final String INSERT_IGNORE_MAPPING = "insert ignore into " + CACHE_FILE_MAPPING_TABLE
        + " (`file_name`, `logical_schema`, `logical_table`, `part_name`, `engine`, `meta`, `version`, `locked`) "
        + "values (?, ?, ?, ?, ?, ?, ?, ?)";

    // Get by file_name (unique)
    private static final String SELECT_BY_FILE_NAME = "select * from " + CACHE_FILE_MAPPING_TABLE
        + " where `file_name` = ?";

    // Get by id (which is the file_id)
    private static final String SELECT_BY_ID = "select * from " + CACHE_FILE_MAPPING_TABLE
        + " where `id` = ?";

    // Delete by logical_schema and logical_table for cleanup
    private static final String DELETE_BY_SCHEMA_TABLE = "delete from " + CACHE_FILE_MAPPING_TABLE
        + " where `logical_schema` = ? and `logical_table` = ?";

    // Delete by logical_schema, logical_table and part_name for cleanup
    private static final String DELETE_BY_SCHEMA_TABLE_PART = "delete from " + CACHE_FILE_MAPPING_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `part_name` = ?";

    // Delete by logical_schema for cleanup
    private static final String DELETE_BY_SCHEMA = "delete from " + CACHE_FILE_MAPPING_TABLE
        + " where `logical_schema` = ?";

    // Delete by engine for cleanup
    private static final String DELETE_BY_ENGINE = "delete from " + CACHE_FILE_MAPPING_TABLE
        + " where `engine` = ?";

    // Delete by id
    private static final String DELETE_BY_ID = "delete from " + CACHE_FILE_MAPPING_TABLE
        + " where `id` = ?";

    // Select all records
    private static final String SELECT_ALL = "select * from " + CACHE_FILE_MAPPING_TABLE;

    // Update meta by id
    private static final String UPDATE_META_BY_ID = "update " + CACHE_FILE_MAPPING_TABLE
        + " set `meta` = ?, `version` = `version` + 1 where `id` = ?";

    // Update locked by id
    private static final String UPDATE_LOCKED_BY_ID = "update " + CACHE_FILE_MAPPING_TABLE
        + " set `locked` = ? where `id` = ?";

    // Update meta with optimistic locking (CAS on version)
    private static final String UPDATE_META_BY_ID_AND_VERSION = "update " + CACHE_FILE_MAPPING_TABLE
        + " set `meta` = ?, `version` = `version` + 1 where `id` = ? and `version` = ?";

    // Update schema info by id (back-fill from reference table)
    private static final String UPDATE_SCHEMA_INFO_BY_ID = "update " + CACHE_FILE_MAPPING_TABLE
        + " set `logical_schema` = ?, `logical_table` = ?, `part_name` = ?, `engine` = ? where `id` = ?";

    private static final String FILES_TABLE = wrap(FILES);
    private static final String COLUMNAR_APPENDED_FILES_TABLE = wrap(COLUMNAR_APPENDED_FILES);

    /**
     * Insert or get mapping for a file. Uses INSERT IGNORE to ensure uniqueness.
     * Logic:
     * 1. First query if mapping exists
     * 2. If exists, return the existing id
     * 3. If not exists, INSERT IGNORE
     * 4. If insert succeeds, return the auto-generated id
     * 5. If insert fails (concurrent insert succeeded), query again
     *
     * @return the id (file_id) for the given file name
     */
    public long insertOrGetMapping(String fileName, String logicalSchema, String logicalTable, String partName,
                                   String engine) {
        return insertOrGetMapping(fileName, logicalSchema, logicalTable, partName, engine, null, 0, false);
    }

    /**
     * Insert or get mapping for a file with all fields. Uses INSERT IGNORE to ensure uniqueness.
     * Includes an initial SELECT check before attempting insert.
     *
     * @return the id (file_id) for the given file name
     */
    public long insertOrGetMapping(String fileName, String logicalSchema, String logicalTable, String partName,
                                   String engine, byte[] meta, long version, boolean locked) {
        // First query if already exists
        CacheFileMappingRecord existingRecord = getByFileName(fileName);
        if (existingRecord != null) {
            return existingRecord.id;
        }
        // Not exists, do INSERT IGNORE
        return insertIgnoreAndGet(fileName, logicalSchema, logicalTable, partName, engine, meta, version, locked);
    }

    /**
     * Directly INSERT IGNORE and return id. Skips initial SELECT — use when caller has already
     * confirmed the record does not exist (e.g. after a prior getByFileName check).
     * <p>
     * If INSERT IGNORE is ignored (concurrent insert), falls back to SELECT.
     *
     * @return the id (file_id) for the given file name
     */
    public long insertIgnoreAndGet(String fileName, String logicalSchema, String logicalTable, String partName,
                                   String engine, byte[] meta, long version, boolean locked) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(8);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, fileName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, partName);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, engine);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setBytes, meta);
            MetaDbUtil.setParameter(7, params, ParameterMethod.setLong, version);
            MetaDbUtil.setParameter(8, params, ParameterMethod.setBoolean, locked);

            List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(1);
            paramsBatch.add(params);

            Long insertedId = MetaDbUtil.insertAndReturnLastInsertId(INSERT_IGNORE_MAPPING, paramsBatch, connection);
            if (insertedId != null) {
                return insertedId;
            }

            // INSERT was ignored (concurrent insert succeeded), query to get the id
            CacheFileMappingRecord record = getByFileName(fileName);
            if (record != null) {
                return record.id;
            }

            throw new RuntimeException("Failed to get mapping for file: " + fileName);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Get mapping record by file name (unique).
     */
    public CacheFileMappingRecord getByFileName(String fileName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, fileName);

            List<CacheFileMappingRecord> records =
                MetaDbUtil.query(SELECT_BY_FILE_NAME, params, CacheFileMappingRecord.class, connection);
            return records.isEmpty() ? null : records.get(0);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Get mapping record by id (file_id).
     */
    public CacheFileMappingRecord getById(long id) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, id);

            List<CacheFileMappingRecord> records =
                MetaDbUtil.query(SELECT_BY_ID, params, CacheFileMappingRecord.class, connection);
            return records.isEmpty() ? null : records.get(0);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Get all mapping records.
     */
    public List<CacheFileMappingRecord> getAll() {
        try {
            return MetaDbUtil.query(SELECT_ALL, CacheFileMappingRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Delete mappings by logical schema and table for cleanup.
     *
     * @return number of deleted records
     */
    public int deleteBySchemaTable(String logicalSchema, String logicalTable) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);

            return MetaDbUtil.delete(DELETE_BY_SCHEMA_TABLE, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Delete mappings by logical schema, table and partition for cleanup.
     *
     * @return number of deleted records
     */
    public int deleteBySchemaTablePart(String logicalSchema, String logicalTable, String partName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partName);

            return MetaDbUtil.delete(DELETE_BY_SCHEMA_TABLE_PART, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Delete mappings by logical schema for cleanup.
     *
     * @return number of deleted records
     */
    public int deleteBySchema(String logicalSchema) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);

            return MetaDbUtil.delete(DELETE_BY_SCHEMA, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Delete mappings by engine type for cleanup.
     *
     * @return number of deleted records
     */
    public int deleteByEngine(String engine) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, engine);

            return MetaDbUtil.delete(DELETE_BY_ENGINE, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Delete mapping by id.
     *
     * @return true if deleted, false if not found
     */
    public boolean deleteById(long id) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, id);

            int deleted = MetaDbUtil.delete(DELETE_BY_ID, params, connection);
            return deleted > 0;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Update meta data by id, auto-increments version.
     *
     * @return number of updated records
     */
    public int updateMetaById(long id, byte[] meta) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setBytes, meta);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, id);

            return MetaDbUtil.update(UPDATE_META_BY_ID, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Update meta data by id with optimistic locking (CAS on version).
     *
     * @return number of updated records (0 if version mismatch)
     */
    public int updateMetaByIdAndVersion(long id, byte[] meta, long expectedVersion) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setBytes, meta);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, id);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, expectedVersion);

            return MetaDbUtil.update(UPDATE_META_BY_ID_AND_VERSION, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Update locked status by id.
     *
     * @return number of updated records
     */
    public int updateLockedById(long id, boolean locked) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setBoolean, locked);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, id);

            return MetaDbUtil.update(UPDATE_LOCKED_BY_ID, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Update schema info for a cache_file_mapping record.
     * Called when the record was created with null schema info and later enriched
     * from a reference table (files or columnar_appended_files).
     *
     * @param id record id
     * @param logicalSchema logical schema name
     * @param logicalTable logical table name
     * @param partName partition name
     * @param engine storage engine
     * @return number of rows updated
     */
    public int updateSchemaInfo(long id, String logicalSchema, String logicalTable,
                                String partName, String engine) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, engine);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setLong, id);

            return MetaDbUtil.update(UPDATE_SCHEMA_INFO_BY_ID, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Scan cache_file_mapping with LEFT JOIN to files table, returning cfm.id and f.file_id.
     * Uses cursor-based pagination (id > minIdExclusive) with LIMIT.
     * Does NOT filter ref id IS NULL in SQL — caller determines orphans.
     *
     * @param minIdExclusive scan records with id > this value
     * @param limit max records per batch
     * @param suffixes file suffixes to filter (e.g. ".orc", ".csv")
     * @return list of [cfm_id, ref_id] pairs; ref_id is null if not found in reference table
     */
    public List<Long[]> scanWithFileCheck(long minIdExclusive, int limit, String[] suffixes) {
        return scanWithReferenceCheck(minIdExclusive, limit, suffixes, FILES_TABLE, "file_id");
    }

    /**
     * Scan cache_file_mapping with LEFT JOIN to columnar_appended_files table.
     * Same logic as {@link #scanWithFileCheck} but against a different reference table.
     */
    public List<Long[]> scanWithColumnarAppendedFileCheck(long minIdExclusive, int limit, String[] suffixes) {
        return scanWithReferenceCheck(minIdExclusive, limit, suffixes, COLUMNAR_APPENDED_FILES_TABLE, "id");
    }

    /**
     * Generic scan: LEFT JOIN cache_file_mapping against a reference table on file_name,
     * returning cfm.id and the reference table's id column.
     *
     * @param minIdExclusive cursor position (exclusive)
     * @param limit max records per batch
     * @param suffixes file suffix filters
     * @param refTable wrapped reference table name
     * @param refIdColumn id column name in the reference table
     * @return list of [cfm_id, ref_id] pairs; ref_id is null if not found
     */
    private List<Long[]> scanWithReferenceCheck(long minIdExclusive, int limit, String[] suffixes,
                                                String refTable, String refIdColumn) {
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT cfm.`id`, ref.`").append(refIdColumn).append("` FROM ")
                .append(CACHE_FILE_MAPPING_TABLE).append(" cfm");
            sql.append(" LEFT JOIN ").append(refTable).append(" ref ON cfm.`file_name` = ref.`file_name`");
            sql.append(" WHERE cfm.`id` > ?");

            if (suffixes != null && suffixes.length > 0) {
                sql.append(" AND (");
                for (int i = 0; i < suffixes.length; i++) {
                    if (i > 0) {
                        sql.append(" OR ");
                    }
                    sql.append("cfm.`file_name` LIKE ?");
                }
                sql.append(")");
            }

            sql.append(" ORDER BY cfm.`id` LIMIT ?");

            try (java.sql.PreparedStatement ps = connection.prepareStatement(sql.toString())) {
                int idx = 1;
                ps.setLong(idx++, minIdExclusive);
                if (suffixes != null) {
                    for (String suffix : suffixes) {
                        ps.setString(idx++, "%" + suffix);
                    }
                }
                ps.setInt(idx, limit);

                List<Long[]> results = new ArrayList<>();
                try (java.sql.ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long cfmId = rs.getLong(1);
                        Long refId = rs.getLong(2);
                        if (rs.wasNull()) {
                            refId = null;
                        }
                        results.add(new Long[] {cfmId, refId});
                    }
                }
                return results;
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Batch delete records by id list.
     *
     * @param ids list of ids to delete
     * @return number of deleted records
     */
    public int deleteByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("DELETE FROM ").append(CACHE_FILE_MAPPING_TABLE);
            sql.append(" WHERE `id` IN (");
            for (int i = 0; i < ids.size(); i++) {
                if (i > 0) {
                    sql.append(",");
                }
                sql.append("?");
            }
            sql.append(")");

            final Map<Integer, ParameterContext> params = new HashMap<>(ids.size());
            for (int i = 0; i < ids.size(); i++) {
                MetaDbUtil.setParameter(i + 1, params, ParameterMethod.setLong, ids.get(i));
            }

            return MetaDbUtil.delete(sql.toString(), params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
