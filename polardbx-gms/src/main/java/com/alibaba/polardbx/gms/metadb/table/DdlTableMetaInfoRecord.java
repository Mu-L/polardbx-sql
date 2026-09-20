package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Data
public class DdlTableMetaInfoRecord implements SystemTableRecord {

    public String schemaName;
    public String tableName;
    public String ddlStmt;
    public String ddlType;
    public Long jobId;
    public Long sourceJobId;
    public String tableMetaInfo;

    public DdlTableMetaInfoRecord() {
    }

    public DdlTableMetaInfoRecord(String schemaName, String tableName, String ddlStmt, String ddlType, Long jobId,
                                  Long sourceJobId, String tableMetaInfo) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.ddlStmt = ddlStmt;
        this.ddlType = ddlType;
        this.jobId = jobId;
        this.sourceJobId = sourceJobId;
        this.tableMetaInfo = tableMetaInfo;
    }

    @Override
    public DdlTableMetaInfoRecord fill(ResultSet rs) throws SQLException {
        this.schemaName = rs.getString("schema_name");
        this.tableName = rs.getString("table_name");
        this.ddlStmt = rs.getString("ddl_stmt");
        this.ddlType = rs.getString("ddl_type");
        this.jobId = rs.getLong("job_id");
        this.sourceJobId = rs.getLong("source_job_id");
        this.tableMetaInfo = rs.getString("table_meta_info");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>();
        int index = params.size();
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.ddlStmt);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.ddlType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.jobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.sourceJobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableMetaInfo);
        return params;
    }
}
