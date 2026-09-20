package com.alibaba.polardbx.gms.metadb.chain;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class GlobalChainSimpleRecord implements SystemTableRecord {
    public long blockId;
    public String schemaName;
    public String tableName;
    public String opHash;

    @Override
    public GlobalChainSimpleRecord fill(ResultSet rs) throws SQLException {
        this.blockId = rs.getLong("block_id");
        this.schemaName = rs.getString("schema_name");
        this.tableName = rs.getString("table_name");
        return this;
    }
}
