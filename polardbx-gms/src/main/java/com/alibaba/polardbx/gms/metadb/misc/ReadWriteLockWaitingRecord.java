package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class ReadWriteLockWaitingRecord implements SystemTableRecord {

    public String schemaName;
    public String owner;
    public String resource;
    public String type;
    public long queueSeq;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;

    @Override
    public ReadWriteLockWaitingRecord fill(ResultSet rs) throws SQLException {
        this.schemaName = rs.getString("schema_name");
        this.owner = rs.getString("owner");
        this.resource = rs.getString("resource");
        this.type = rs.getString("type");
        this.queueSeq = rs.getLong("queue_seq");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.owner);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.resource);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.type);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.queueSeq);
        return params;
    }
}
