package com.alibaba.polardbx.gms.scheduler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ColumnarWarmupRecord implements SystemTableRecord {
    private long taskId;
    private String createTime;
    private String updateTime;
    private String instanceId;
    private String schemaName;
    private String cronExpression;
    private String sqlDef;
    private int status;

    @Override
    public ColumnarWarmupRecord fill(ResultSet rs) throws SQLException {
        this.taskId = rs.getLong("task_id");
        this.createTime = rs.getString("create_time");
        this.updateTime = rs.getString("update_time");
        this.instanceId = rs.getString("instance_id");
        this.schemaName = rs.getString("schema_name");
        this.cronExpression = rs.getString("cron_expression");
        this.sqlDef = rs.getString("sql_def");
        this.status = rs.getInt("status");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>();
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.instanceId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.cronExpression);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.sqlDef);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.status);
        return params;
    }

    public long getTaskId() {
        return taskId;
    }

    public ColumnarWarmupRecord setTaskId(long taskId) {
        this.taskId = taskId;
        return this;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public ColumnarWarmupRecord setInstanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public ColumnarWarmupRecord setSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public String getCreateTime() {
        return createTime;
    }

    public ColumnarWarmupRecord setCreateTime(String createTime) {
        this.createTime = createTime;
        return this;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public ColumnarWarmupRecord setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
        return this;
    }

    public String getSqlDef() {
        return sqlDef;
    }

    public ColumnarWarmupRecord setSqlDef(String sqlDef) {
        this.sqlDef = sqlDef;
        return this;
    }

    public int getStatus() {
        return status;
    }

    public ColumnarWarmupRecord setStatus(int status) {
        this.status = status;
        return this;
    }

    @Override
    public String toString() {
        return "ColumnarWarmupRecord{" +
            "taskId=" + taskId +
            ", createTime='" + createTime + '\'' +
            ", updateTime='" + updateTime + '\'' +
            ", instanceId='" + instanceId + '\'' +
            ", schemaName='" + schemaName + '\'' +
            ", cronExpression='" + cronExpression + '\'' +
            ", sqlDef='" + sqlDef + '\'' +
            ", status=" + status +
            '}';
    }
}
