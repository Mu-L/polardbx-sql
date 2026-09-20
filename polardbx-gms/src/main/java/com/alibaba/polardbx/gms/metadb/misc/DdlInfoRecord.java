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
 * @author wumu
 */
public class DdlInfoRecord implements SystemTableRecord {

    public DdlInfoRecord() {
    }

    public long jobId;
    public String ddlType;
    public String schemaName;
    public String objectName;
    public String state;
    public String result;
    public String ddlStmt;
    public long gmtCreated;
    public long gmtModified;

    @Override
    public DdlInfoRecord fill(ResultSet rs) throws SQLException {
        this.jobId = rs.getLong("job_id");
        this.ddlType = rs.getString("ddl_type");
        this.schemaName = rs.getString("schema_name");
        this.objectName = rs.getString("object_name");
        this.state = rs.getString("state");
        this.result = rs.getString("result");
        this.ddlStmt = rs.getString("ddl_stmt");
        this.gmtCreated = rs.getLong("gmt_created");
        this.gmtModified = rs.getLong("gmt_modified");
        return this;
    }
}
