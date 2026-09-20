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
 * @author wumu
 */
@Data
public class OmcRecord implements SystemTableRecord {
    // job id
    private long jobId;
    // task id
    private long taskId;
    // changeset id
    private long changesetId;
    // 逻辑库名
    private String tableSchema;
    // 逻辑表名
    private String tableName;
    // 存储节点 ID
    private String storageInstId;
    // 物理库名
    private String physicalDb;
    // 物理表名
    private String physicalTable;
    // 物理表 DDL
    private String physicalDdlSql;
    // 总行数
    private long totalRowCount;
    // 是否回滚, 0 false, 1 true
    private boolean rollback;
    // 物理表 space id
    private long spaceId;
    // 状态
    private int status;
    // OMC状态
    private int omcStatus;
    // 开始时间
    private String startTime;
    // 结束时间
    private String endTime;
    // 切换耗时
    private long cutOverTime;
    // 生成列映射
    private String generatedColumnMap;
    // 额外信息
    private String extra;

    @Override
    public OmcRecord fill(ResultSet rs) throws SQLException {
        this.jobId = rs.getLong("job_id");
        this.taskId = rs.getLong("task_id");
        this.changesetId = rs.getLong("changeset_id");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.storageInstId = rs.getString("storage_inst_id");
        this.physicalDb = rs.getString("physical_db");
        this.physicalTable = rs.getString("physical_table");
        this.physicalDdlSql = rs.getString("physical_ddl_sql");
        this.totalRowCount = rs.getLong("total_row_count");
        this.rollback = rs.getBoolean("rollback");
        this.spaceId = rs.getLong("space_id");
        this.status = rs.getInt("status");
        this.omcStatus = rs.getInt("omc_status");
        this.startTime = rs.getString("start_time");
        this.endTime = rs.getString("end_time");
        this.cutOverTime = rs.getLong("cut_over_time");
        this.generatedColumnMap = rs.getString("generated_column_map");
        this.extra = rs.getString("extra");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(18);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.jobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.taskId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.changesetId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.storageInstId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.physicalDb);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.physicalTable);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.physicalDdlSql);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.totalRowCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setBoolean, this.rollback);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.spaceId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.omcStatus);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.startTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.endTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.cutOverTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.generatedColumnMap);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extra);
        return params;
    }
}
