package com.alibaba.polardbx.gms.partition;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Objects;

/**
 * @author chenghui.lch
 */
public class TablePartitionDeltaRecord extends TablePartitionRecord {

    public Integer refCount;

    @Override
    public TablePartitionDeltaRecord fill(ResultSet rs) throws SQLException {
        super.fill(rs);
        this.refCount = rs.getInt("ref_count");
        return this;
    }
}
