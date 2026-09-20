package com.alibaba.polardbx.gms.partition;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author luoyanxin
 */
public class TablePartitionArchiveRecord extends TablePartitionRecord {

    public long task_id;

    @Override
    public TablePartitionArchiveRecord fill(ResultSet rs) throws SQLException {
        super.fill(rs);
        this.task_id = rs.getInt("task_id");
        return this;
    }
}
