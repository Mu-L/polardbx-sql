package com.alibaba.polardbx.gms.metadb.chain;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.POLARDBX_GLOBAL_CHAIN;

public class GlobalChainAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalChainAccessor.class);

    private static final String INSERT_GLOBAL_CHAIN =
        String.format(
            "insert into `%s` (`trace_id`, `ip`, `port`, `user`, `schema_name`, `table_name`, `op_hash`, `block_hash`, `extra`, `tso`) "
                + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            POLARDBX_GLOBAL_CHAIN);

    private static final String QUERY_OP_HASH_BY_SCHEMA_AND_TABLE = String.format(
        "select `op_hash` from %s where schema_name = ? and table_name = ? and tso < ? "
            + "order by block_id",
        POLARDBX_GLOBAL_CHAIN
    );

    private static final String QUERY_OP_HASH_BY_SCHEMA_AND_TABLE_AND_ID = String.format(
        "select `op_hash` from %s where schema_name = ? and table_name = ? and block_id <= ? order by block_id for update",
        POLARDBX_GLOBAL_CHAIN
    );

    private static final String QUERY_OP_HASH = String.format(
        "select * from %s where tso < ? and (extra != 'ARCHIVE' or extra is null) order by `block_id`",
        POLARDBX_GLOBAL_CHAIN
    );

    private static final String QUERY_LAST_ARCHIVE_BLOCK_HASH = String.format(
        "select block_hash from %s where extra = 'ARCHIVE' order by block_id desc limit 1",
        POLARDBX_GLOBAL_CHAIN
    );

    private static final String QUERY_BY_LAST_RECORD = "select * from " + POLARDBX_GLOBAL_CHAIN
        + " order by `block_id` desc limit 1 ";

    private static final String QUERY_BY_LAST_RECORD_FOR_EACH_TABLE = "select schema_name, table_name, max(block_id) as block_id from "
        + POLARDBX_GLOBAL_CHAIN + " group by schema_name,table_name FOR UPDATE";

    private static final String DELETE_BY_BLOCK_ID = "delete from " + POLARDBX_GLOBAL_CHAIN
        + " where `block_id` < ? and `block_id` not in (%s) ";

    private static final String UPDATE_ARCHIVE_BY_BLOCK_ID = "update " + POLARDBX_GLOBAL_CHAIN
        + " set extra = 'ARCHIVE',  op_hash = ? where `block_id` = ? ";

    private static final String UPDATE_DELETE_BY_SCHEMA_AND_TABLE = "update " + POLARDBX_GLOBAL_CHAIN
        + " set schema_name = null, table_name = null "
        + " where `schema_name` = ? and `table_name` = ?";

    private static final String UPDATE_DELETE_BY_SCHEMA = "update " + POLARDBX_GLOBAL_CHAIN
        + " set schema_name = null, table_name = null "
        + " where `schema_name` = ?";

    public int[] insertRows(List<GlobalChainRecord> records) {

        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (GlobalChainRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_GLOBAL_CHAIN, paramsBatch);
            return MetaDbUtil.insert(INSERT_GLOBAL_CHAIN, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + POLARDBX_GLOBAL_CHAIN, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                POLARDBX_GLOBAL_CHAIN,
                e.getMessage());
        }
    }

    public List<GlobalChainRecord> queryLastRecord() {
        try {
            return MetaDbUtil.query(QUERY_BY_LAST_RECORD, null, GlobalChainRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the table " + POLARDBX_GLOBAL_CHAIN, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                POLARDBX_GLOBAL_CHAIN, e.getMessage());
        }
    }

    public List<GlobalChainSimpleRecord> queryLastRecordForEachTable() {
        try {
            return MetaDbUtil.query(QUERY_BY_LAST_RECORD_FOR_EACH_TABLE, null, GlobalChainSimpleRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the table " + POLARDBX_GLOBAL_CHAIN, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                POLARDBX_GLOBAL_CHAIN, e.getMessage());
        }
    }

    public void processGlobalChain(String schemaName, String tableName, long tso, Consumer<String> processor)
        throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(QUERY_OP_HASH_BY_SCHEMA_AND_TABLE)) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            stmt.setLong(3, tso);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    processor.accept(rs.getString(1));
                }
            }
        }
    }

    public void processGlobalChainById(String schemaName, String tableName, long blockId, Consumer<String> processor)
        throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(QUERY_OP_HASH_BY_SCHEMA_AND_TABLE_AND_ID)) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            stmt.setLong(3, blockId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    processor.accept(rs.getString(1));
                }
            }
        }
    }

    public void processGlobalChain(long tso, AtomicBoolean fail, Consumer<GlobalChainRecord> processor)
        throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(QUERY_OP_HASH)) {
            stmt.setLong(1, tso);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    processor.accept(new GlobalChainRecord().fill(rs));
                    if (fail.get()) {
                        break;
                    }
                }
            }
        }
    }

    public int deleteByBlockId(List<GlobalChainSimpleRecord> records, long maxBlockId) {
        try {
            final String sql = String.format(DELETE_BY_BLOCK_ID,
                records.stream().map(r -> String.valueOf(r.blockId)).collect(Collectors.joining(",")));
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, maxBlockId);
            DdlMetaLogUtil.logSql(sql, params);
            return MetaDbUtil.delete(sql, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + POLARDBX_GLOBAL_CHAIN, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                POLARDBX_GLOBAL_CHAIN,
                e.getMessage());
        }
    }

    public int updateArchiveByBlockId(List<GlobalChainSimpleRecord> records) {
        try {
            int affectRows = 0;
            for (GlobalChainSimpleRecord record : records) {
                int i = 0;
                Map<Integer, ParameterContext> params = new HashMap<>(2);
                MetaDbUtil.setParameter(++i, params, ParameterMethod.setString, record.opHash);
                MetaDbUtil.setParameter(++i, params, ParameterMethod.setLong, record.blockId);
                DdlMetaLogUtil.logSql(UPDATE_ARCHIVE_BY_BLOCK_ID, params);
                affectRows += MetaDbUtil.update(UPDATE_ARCHIVE_BY_BLOCK_ID, params, connection);
            }
            return affectRows;
        } catch (Exception e) {
            LOGGER.error("Failed to update from system table " + POLARDBX_GLOBAL_CHAIN, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                POLARDBX_GLOBAL_CHAIN,
                e.getMessage());
        }
    }

    public int updateDeleteBySchemaAndTable(String schemaName, String tableName) {
        try {
            int i = 0;
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(++i, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(++i, params, ParameterMethod.setString, tableName);
            DdlMetaLogUtil.logSql(UPDATE_DELETE_BY_SCHEMA_AND_TABLE, params);
            return MetaDbUtil.update(UPDATE_DELETE_BY_SCHEMA_AND_TABLE, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to update from system table " + POLARDBX_GLOBAL_CHAIN, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                POLARDBX_GLOBAL_CHAIN,
                e.getMessage());
        }
    }

    public int updateDeleteBySchema(String schemaName) {
        try {
            int i = 0;
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(++i, params, ParameterMethod.setString, schemaName);
            DdlMetaLogUtil.logSql(UPDATE_DELETE_BY_SCHEMA, params);
            return MetaDbUtil.update(UPDATE_DELETE_BY_SCHEMA, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to update from system table " + POLARDBX_GLOBAL_CHAIN, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                POLARDBX_GLOBAL_CHAIN,
                e.getMessage());
        }
    }

    public String queryArchiveBlockHashById() {
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(QUERY_LAST_ARCHIVE_BLOCK_HASH);
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        } catch (Exception e) {
            LOGGER.error("Failed to query from system table " + POLARDBX_GLOBAL_CHAIN, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                POLARDBX_GLOBAL_CHAIN,
                e.getMessage());
        }
    }
}
