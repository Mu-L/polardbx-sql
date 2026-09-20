package com.alibaba.polardbx.gms.metadb.ccl;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import lombok.ToString;

import java.sql.ResultSet;
import java.sql.SQLException;

@ToString
public class DnCclRecord implements SystemTableRecord {

    public String storageId;
    public String inst;
    public String id;
    public String type;
    public String schema;
    public String table;
    public String state;
    public String order;
    public String concurrencyCount;
    public String matched;
    public String running;
    public String waiting;
    public String keywords;

    @Override
    public DnCclRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getString("id");
        this.type = rs.getString("type");
        this.schema = rs.getString("schema");
        this.table = rs.getString("table");
        this.schema = rs.getString("schema");
        this.state = rs.getString("state");
        this.order = rs.getString("order");
        this.concurrencyCount = rs.getString("concurrency_count");
        this.matched = rs.getString("matched");
        this.running = rs.getString("running");
        // compatible with dn typo
        this.waiting = rs.getString("waitting");
        this.keywords = rs.getString("keywords");
        return this;
    }

}
