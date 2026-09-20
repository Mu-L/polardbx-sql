package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LoadWeightRecord implements SystemTableRecord {
    public long id;

    public String instId;

    public String node;
    public String loadWeight;

    // for mock
    public LoadWeightRecord(long id, String instId, String node, String loadWeight) {
        this.id = id;
        this.instId = instId;
        this.node = node;
        this.loadWeight = loadWeight;
    }

    public LoadWeightRecord() {

    }

    @Override
    public LoadWeightRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.instId = rs.getString("inst_id");
        this.node = rs.getString("node");
        this.loadWeight = rs.getString("load_weight");
        return this;
    }
}
