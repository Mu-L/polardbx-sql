package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 * @author luoyanxin.pt
 */
public class DdlTaskBarrierRecord implements SystemTableRecord {

    public DdlTaskBarrierRecord() {
    }

    public String tableSchema;
    public String barrierName;
    public long ref_cnt;
    public Date gmtCreated;
    public Date gmtModified;
    public long lastUpdatedTaskId;

    @Override
    public DdlTaskBarrierRecord fill(ResultSet rs) throws SQLException {
        this.tableSchema = rs.getString("table_schema");
        this.barrierName = rs.getString("barrier_name");
        this.ref_cnt = rs.getLong("ref_cnt");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.lastUpdatedTaskId = rs.getLong("last_updated_task_id");
        return this;
    }
}
