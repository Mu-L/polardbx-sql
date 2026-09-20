package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SubClusterRecord implements SystemTableRecord {
    public long id;

    public String instId;

    public String node;
    public String subCluster;

    // for mock
    public SubClusterRecord(long id, String instId, String node, String subCluster) {
        this.id = id;
        this.instId = instId;
        this.node = node;
        this.subCluster = subCluster;
    }

    public SubClusterRecord() {

    }

    @Override
    public SubClusterRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.instId = rs.getString("inst_id");
        this.node = rs.getString("node");
        this.subCluster = rs.getString("sub_cluster");
        return this;
    }
}
