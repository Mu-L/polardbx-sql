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
 * Represents the metadata for DDL physical lock statistics.
 * Records lock information during DDL operations such as split and other DDL types.
 *
 * @author luoyanxin
 */
public class DdlPhysicalLockStatRecord implements SystemTableRecord {

    public static final int STATE_LOCKED = 1;
    public static final int STATE_UNLOCKED = 0;
    public static final int DDL_TYPE_SPLIT_PARTITION = 0;
    public static final int DDL_TYPE_OTHERS = 1;

    /**
     * Auto-increment primary key.
     */
    private long id;

    /**
     * Job ID associated with the DDL operation.
     */
    private long jobId;

    /**
     * DDL type: 0 for split, 1 for other DDL operations.
     */
    private int ddlType;

    /**
     * Database schema where the table belongs.
     */
    private String tableSchema;

    /**
     * Name of the table being locked.
     */
    private String tableName;

    /**
     * Physical database name.
     */
    private String physicalDb;

    /**
     * Physical table name in the storage system.
     */
    private String physicalTable;

    /**
     * Total lock duration in milliseconds.
     * This field can be updated multiple times as locks may occur in multiple time periods.
     */
    private Long lockDurationMs;

    /**
     * Number of rows backfilled during the operation.
     */
    private long rowCount;

    /**
     * Start time of the lock operation.
     */
    private long startTime;

    /**
     * Current lock start time, used to track the latest lock period.
     */
    private long curLockStartTime;

    /**
     * End time of the lock operation.
     */
    private long endTime;

    /**
     * Lock state: 0 for writable, 1 for readonly.
     */
    private int state;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public int getDdlType() {
        return ddlType;
    }

    public void setDdlType(int ddlType) {
        this.ddlType = ddlType;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPhysicalDb() {
        return physicalDb;
    }

    public void setPhysicalDb(String physicalDb) {
        this.physicalDb = physicalDb;
    }

    public String getPhysicalTable() {
        return physicalTable;
    }

    public void setPhysicalTable(String physicalTable) {
        this.physicalTable = physicalTable;
    }

    public Long getLockDurationMs() {
        return lockDurationMs;
    }

    public void setLockDurationMs(Long lockDurationMs) {
        this.lockDurationMs = lockDurationMs;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getCurLockStartTime() {
        return curLockStartTime;
    }

    public void setCurLockStartTime(long curLockStartTime) {
        this.curLockStartTime = curLockStartTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    @Override
    public DdlPhysicalLockStatRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.jobId = rs.getLong("job_id");
        this.ddlType = rs.getInt("ddl_type");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.physicalDb = rs.getString("physical_db");
        this.physicalTable = rs.getString("physical_table");
        this.lockDurationMs = rs.getLong("lock_duration_ms");
        if (rs.wasNull()) {
            this.lockDurationMs = 0L;
        }
        this.rowCount = rs.getLong("row_count");
        this.startTime = rs.getLong("start_time");
        this.curLockStartTime = rs.getLong("cur_lock_start_time");
        this.endTime = rs.getLong("end_time");
        this.state = rs.getInt("state");
        return this;
    }

    /**
     * Build parameters for insert operation.
     * Note: id is auto-increment, so it's not included in the parameter list.
     */
    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.jobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.ddlType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.physicalDb);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.physicalTable);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.lockDurationMs);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.rowCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.startTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.curLockStartTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.endTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.state);
        return params;
    }
}
