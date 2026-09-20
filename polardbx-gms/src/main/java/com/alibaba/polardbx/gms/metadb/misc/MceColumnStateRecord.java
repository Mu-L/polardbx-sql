package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * One row per (job, task, column, physical partition) recording the transient MCE migration
 * state plus resumable backfill checkpoint. The logical control row is retained through
 * EXTERNALIZED and removed by the final content-column cutover transaction.
 */
@Data
public class MceColumnStateRecord implements SystemTableRecord {

    private static final int MAX_PARTITION_NAME_LENGTH = 64;

    // ---- state values (column-level MCE state machine) ----
    public static final int STATE_NONE = 0;
    public static final int STATE_DUAL_WRITE = 1;
    public static final int STATE_READ_ADDR = 2;
    public static final int STATE_EXTERNALIZED = 3;

    // ---- status values (task lifecycle) ----
    public static final int STATUS_INIT = 0;
    public static final int STATUS_RUNNING = 1;
    public static final int STATUS_SUCCESS = 2;
    public static final int STATUS_FAILED = 3;

    private long id;
    private long jobId;
    private long taskId;
    private String tableSchema;
    private String tableName;
    private String columnName;
    private String addrColumnName;
    private int state;
    private int status;
    private String physicalDb;
    private String physicalTable;
    private String partitionName;
    private String lastPk;
    private String maxPk;
    private String pkType;
    private long processedRows;
    private long totalRows;
    private String startTime;
    private String endTime;
    private String errorMessage;
    private String extra;

    @Override
    public MceColumnStateRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.jobId = rs.getLong("job_id");
        this.taskId = rs.getLong("task_id");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.columnName = rs.getString("column_name");
        this.addrColumnName = rs.getString("addr_column_name");
        this.state = rs.getInt("state");
        this.status = rs.getInt("status");
        this.physicalDb = rs.getString("physical_db");
        this.physicalTable = rs.getString("physical_table");
        this.partitionName = rs.getString("partition_name");
        this.lastPk = rs.getString("last_pk");
        this.maxPk = rs.getString("max_pk");
        this.pkType = rs.getString("pk_type");
        this.processedRows = rs.getLong("processed_rows");
        this.totalRows = rs.getLong("total_rows");
        this.startTime = rs.getString("start_time");
        this.endTime = rs.getString("end_time");
        this.errorMessage = rs.getString("error_message");
        this.extra = rs.getString("extra");
        return this;
    }

    /**
     * Build params for INSERT. Column order must match INSERT_DATA in MceColumnStateAccessor
     * (id is auto-increment, excluded here; start/end time use DB defaults, excluded here).
     */
    public Map<Integer, ParameterContext> buildInsertParams() {
        validateForInsert();
        Map<Integer, ParameterContext> params = new HashMap<>(20);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.jobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.taskId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.columnName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.addrColumnName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.state);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString,
            this.physicalDb == null ? "" : this.physicalDb);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString,
            this.physicalTable == null ? "" : this.physicalTable);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString,
            this.partitionName == null ? "" : this.partitionName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.lastPk);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.maxPk);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.pkType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.processedRows);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.totalRows);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.errorMessage);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extra);
        return params;
    }

    private void validateForInsert() {
        if (isBlank(tableSchema) || isBlank(tableName) || isBlank(columnName) || isBlank(addrColumnName)) {
            throw new IllegalArgumentException("MCE state record identity fields must not be blank");
        }
        if (state < STATE_NONE || state > STATE_EXTERNALIZED) {
            throw new IllegalArgumentException("Invalid persistent MCE state: " + state);
        }
        if (status < STATUS_INIT || status > STATUS_FAILED) {
            throw new IllegalArgumentException("Invalid MCE checkpoint status: " + status);
        }
        if (isWhitespaceOnly(physicalDb) || isWhitespaceOnly(physicalTable) || isWhitespaceOnly(partitionName)) {
            throw new IllegalArgumentException("MCE physical identity must not contain whitespace-only fields");
        }
        if (partitionName != null && partitionName.length() > MAX_PARTITION_NAME_LENGTH) {
            throw new IllegalArgumentException(
                "MCE checkpoint partition name exceeds " + MAX_PARTITION_NAME_LENGTH + " characters: "
                    + partitionName);
        }
        boolean emptyDb = isBlank(physicalDb);
        boolean emptyTable = isBlank(physicalTable);
        boolean emptyPartition = isBlank(partitionName);
        if (!(emptyDb == emptyTable && emptyTable == emptyPartition)) {
            throw new IllegalArgumentException("MCE physical identity must be either fully empty or fully specified");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static boolean isWhitespaceOnly(String value) {
        return value != null && !value.isEmpty() && value.trim().isEmpty();
    }
}
