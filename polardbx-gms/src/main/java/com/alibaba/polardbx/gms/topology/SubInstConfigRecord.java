package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * 子实例配置记录类
 *
 * @author assistant
 */
public class SubInstConfigRecord implements SystemTableRecord {
    public long id;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public String instId;
    public String subInstId;
    public String paramKey;
    public String paramVal;

    @Override
    public SubInstConfigRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.instId = rs.getString("inst_id");
        this.subInstId = rs.getString("sub_inst_id");
        this.paramKey = rs.getString("param_key");
        this.paramVal = rs.getString("param_val");
        return this;
    }
}