package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ReadWriteLockWaitingAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadWriteLockWaitingAccessor.class);

    public static final String READ_WRITE_LOCK_WAITING_TABLE = wrap(GmsSystemTables.READ_WRITE_LOCK_WAITING);

    private static final String INSERT_DATA =
        "insert into " + READ_WRITE_LOCK_WAITING_TABLE
            + "(`schema_name`, `owner`, `resource`, `type`, `queue_seq`) values (?, ?, ?, ?, ?)";

    private static final String SELECT_FULL =
        "select `schema_name`, `owner`, `resource`, `type`, `queue_seq`, `gmt_created`, `gmt_modified` ";

    private static final String FROM_TABLE = " from " + READ_WRITE_LOCK_WAITING_TABLE;

    private static final String WHERE_OWNER = " where `owner` = ?";

    private static final String WHERE_OWNER_RESOURCE = " where `owner` = ? and `resource` = ?";

    private static final String WHERE_RESOURCE = " where `resource` = ?";

    private static final String WHERE_SCHEMA_NAME = " where `schema_name` = ?";

    private static final String FOR_UPDATE = " for update";

    private static final String SELECT_BY_OWNER_RESOURCE = SELECT_FULL + FROM_TABLE + WHERE_OWNER_RESOURCE;

    private static final String SELECT_BY_RESOURCE_FOR_UPDATE =
        SELECT_FULL + FROM_TABLE + WHERE_RESOURCE + " order by `queue_seq` asc" + FOR_UPDATE;

    private static final String SELECT_ALL = SELECT_FULL + FROM_TABLE + " order by `resource` asc, `queue_seq` asc";

    private static final String SELECT_MAX_QUEUE_SEQ =
        "select max(`queue_seq`) as `max_queue_seq`" + FROM_TABLE + WHERE_RESOURCE;

    private static final String UPDATE_TYPE_AND_QUEUE_SEQ =
        "update " + READ_WRITE_LOCK_WAITING_TABLE + " set `type` = ?, `queue_seq` = ?" + WHERE_OWNER_RESOURCE;

    private static final String DELETE_BY_OWNER =
        "delete t from " + READ_WRITE_LOCK_WAITING_TABLE + " as t force index(`uk_owner_resource`)"
            + WHERE_OWNER;

    private static final String DELETE_BY_OWNER_RESOURCE =
        "delete from " + READ_WRITE_LOCK_WAITING_TABLE + WHERE_OWNER_RESOURCE;

    public int insert(ReadWriteLockWaitingRecord record) {
        try {
            return MetaDbUtil.insert(INSERT_DATA, record.buildParams(), connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert into " + READ_WRITE_LOCK_WAITING_TABLE, "insert into", e);
        }
    }

    public Optional<ReadWriteLockWaitingRecord> queryByOwnerAndResource(String owner, String resource) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(16);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, owner);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, resource);
            List<ReadWriteLockWaitingRecord> records =
                MetaDbUtil.query(SELECT_BY_OWNER_RESOURCE, params, ReadWriteLockWaitingRecord.class, connection);
            if (CollectionUtils.isNotEmpty(records)) {
                return Optional.of(records.get(0));
            }
            return Optional.empty();
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + READ_WRITE_LOCK_WAITING_TABLE, "query from", e);
        }
    }

    public List<ReadWriteLockWaitingRecord> queryByResourceForUpdate(String resource) {
        try {
            Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {resource});
            return MetaDbUtil.query(SELECT_BY_RESOURCE_FOR_UPDATE, params, ReadWriteLockWaitingRecord.class,
                connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + READ_WRITE_LOCK_WAITING_TABLE, "query from", e);
        }
    }

    public List<ReadWriteLockWaitingRecord> queryAll() {
        try {
            List<ReadWriteLockWaitingRecord> records =
                MetaDbUtil.query(SELECT_ALL, ReadWriteLockWaitingRecord.class, connection);
            if (records != null) {
                return records;
            }
            return java.util.Collections.emptyList();
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + READ_WRITE_LOCK_WAITING_TABLE, "query from", e);
        }
    }

    public long nextQueueSeq(String resource) {
        try (PreparedStatement ps = connection.prepareStatement(SELECT_MAX_QUEUE_SEQ)) {
            ps.setString(1, resource);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return 1L;
                }
                long maxQueueSeq = rs.getLong("max_queue_seq");
                return rs.wasNull() ? 1L : maxQueueSeq + 1L;
            }
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + READ_WRITE_LOCK_WAITING_TABLE, "query from", e);
        }
    }

    public int updateTypeAndQueueSeq(String owner, String resource, String type, long queueSeq) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(16);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, type);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, queueSeq);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, owner);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, resource);
            return MetaDbUtil.update(UPDATE_TYPE_AND_QUEUE_SEQ, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + READ_WRITE_LOCK_WAITING_TABLE, "update", e);
        }
    }

    public int deleteByOwner(String owner) {
        try {
            Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {owner});
            return MetaDbUtil.delete(DELETE_BY_OWNER, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + READ_WRITE_LOCK_WAITING_TABLE, "delete from", e);
        }
    }

    public int deleteByOwnerAndResource(String owner, String resource) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(16);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, owner);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, resource);
            return MetaDbUtil.delete(DELETE_BY_OWNER_RESOURCE, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + READ_WRITE_LOCK_WAITING_TABLE, "delete from", e);
        }
    }

    public int deleteByOwnerAndResources(String owner, Set<String> resources) {
        if (CollectionUtils.isEmpty(resources)) {
            return 0;
        }
        int count = 0;
        for (String resource : resources) {
            count += deleteByOwnerAndResource(owner, resource);
        }
        return count;
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            READ_WRITE_LOCK_WAITING_TABLE, e.getMessage());
    }
}
