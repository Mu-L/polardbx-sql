package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Record in expand_partition_tasks system table.
 * Tracks the progress of ALTER TABLE ... EXPAND PARTITIONS TO N
 */
public class ExpandPartitionTaskRecord implements SystemTableRecord {

    // Status constants
    public static final int STATUS_RUNNING = 0;
    public static final int STATUS_COMPLETED = 1;
    public static final int STATUS_CANCELLED = 2;
    public static final int STATUS_FAILED = 3;
    public static final int STATUS_PAUSED = 4;

    public long id;
    public String schemaName;
    public String tableName;
    public long tableId;
    public long initialPartitionCount;
    public long targetPartitionCount;
    /**
     * JSON: {"initialPartitionCount":C0,"targetPartitionCount":N,"expandFactor":k,"items":[{"name":"p1","factor":k,"status":"PENDING"},...]}
     */
    public String planJson;
    public int status;
    public long completedPartitions;
    public long pendingPartitions;
    public long ddlJobId;
    public String extraInfo;
    public java.sql.Timestamp gmtCreated;
    public java.sql.Timestamp gmtModified;

    @Override
    public ExpandPartitionTaskRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.schemaName = rs.getString("schema_name");
        this.tableName = rs.getString("table_name");
        this.tableId = rs.getLong("table_id");
        this.initialPartitionCount = rs.getLong("initial_partition_count");
        this.targetPartitionCount = rs.getLong("target_partition_count");
        this.planJson = rs.getString("plan_json");
        this.status = rs.getInt("status");
        this.completedPartitions = rs.getLong("completed_partitions");
        this.pendingPartitions = rs.getLong("pending_partitions");
        this.ddlJobId = rs.getLong("ddl_job_id");
        this.extraInfo = rs.getString("extra_info");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(12);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.tableId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.initialPartitionCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.targetPartitionCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.planJson);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.completedPartitions);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.pendingPartitions);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.ddlJobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extraInfo);
        return params;
    }
}
