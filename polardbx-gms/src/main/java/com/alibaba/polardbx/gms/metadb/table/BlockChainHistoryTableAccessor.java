package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlockChainHistoryTableAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("BlockChainHistoryTableAccessor");

    private static final String INSERT_RECORD = "insert into " + " `%s`.`%s` "
        + "(`trace_id`, `start_time`, `rec_num`, `op_type`, `hash_ins`, `hash_del`, `block_hash`, `long_pk`, `bytes_pk`, `tso`, `extra`) "
            + "values (?,?,?,?,?,?,?,?,?,?,?)";

    private static final String QUERY_BY_TRACE_ID = "select * from " + " `%s`.`%s` "
            + "where `trace_id` = ? order by `rec_num` ";

    private static final String QUERY_BY_TRACE_ID_LAST_REC = "select * from " + " `%s`.`%s` "
            + "where `trace_id` = ? order by `rec_num` desc limit 1 ";

    private static final String QUERY_BY_LAST_RECORD = "select * from " + " `%s`.`%s` "
        + " order by `block_id` desc limit 1 ";

    /**
     * 找到最后一个trace id的第0行记录，方便清理时保留最后一个trace id的所有记录
     */
    private static final String QUERY_BY_LAST_TRACE_ID_RECORD = "select * from " + " `%s`.`%s` "
        + " where rec_num = 0 order by `block_id` desc limit 1 ";

    private static final String DELETE_BY_ID = "delete from `%s`.`%s` "
        + " where `block_id` < ? ";

    private static final String UPDATE_BY_TRACE_ID = "update `%s`.`%s` "
            + " set extra = ? where `trace_id` = ? ";

    /**
     * Insert new checkpoints
     */
    public int[] insertRows(String schemaName, String tableName, List<BlockChainHistoryTableRecord> records) {
        String sql = String.format(INSERT_RECORD, schemaName, tableName);

        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (BlockChainHistoryTableRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(sql, paramsBatch);
            return MetaDbUtil.insert(sql, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + schemaName + "." + tableName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                    schemaName + "." + tableName,
                    e.getMessage());
        }
    }

    public List<BlockChainHistoryTableRecord> queryByTraceId(String schemaName, String tableName, String traceId) {
        String sql = String.format(QUERY_BY_TRACE_ID, schemaName, tableName);
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, traceId);

            return MetaDbUtil.query(sql, params, BlockChainHistoryTableRecord.class,
                    connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the table " + schemaName + "." + tableName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                    schemaName + "." + tableName, e.getMessage());
        }
    }

    public List<BlockChainHistoryTableRecord> queryByTraceIdLastRec(String schemaName, String tableName, String traceId) {
        String sql = String.format(QUERY_BY_TRACE_ID_LAST_REC, schemaName, tableName);
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, traceId);

            return MetaDbUtil.query(sql, params, BlockChainHistoryTableRecord.class,
                    connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the table " + schemaName + "." + tableName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                    schemaName + "." + tableName, e.getMessage());
        }
    }

    public List<BlockChainHistoryTableRecord> queryLastRecord(String schemaName, String tableName) {
        String sql = String.format(QUERY_BY_LAST_RECORD, schemaName, tableName);
        try {
            return MetaDbUtil.query(sql, null, BlockChainHistoryTableRecord.class,
                    connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the table " + schemaName + "." + tableName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                    schemaName + "." + tableName, e.getMessage());
        }
    }

    public List<BlockChainHistoryTableRecord> queryLastTraceIdRecord(String schemaName, String tableName) {
        String sql = String.format(QUERY_BY_LAST_TRACE_ID_RECORD, schemaName, tableName);
        try {
            return MetaDbUtil.query(sql, null, BlockChainHistoryTableRecord.class,
                    connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the table " + schemaName + "." + tableName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                    schemaName + "." + tableName, e.getMessage());
        }
    }

    public int deleteById(String schemaName, String tableName, long id) {
        String sql = String.format(DELETE_BY_ID, schemaName, tableName);
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, id);

            DdlMetaLogUtil.logSql(sql, params);
            return MetaDbUtil.delete(sql, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete the table " + schemaName + "." + tableName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                    schemaName + "." + tableName, e.getMessage());
        }
    }

    public int updateByTraceId(String schemaName, String tableName, String traceId, String extra) {
        String sql = String.format(UPDATE_BY_TRACE_ID, schemaName, tableName);
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, extra);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, traceId);

            DdlMetaLogUtil.logSql(sql, params);
            return MetaDbUtil.update(sql, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to update the table " + schemaName + "." + tableName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                    schemaName + "." + tableName, e.getMessage());
        }
    }

}
