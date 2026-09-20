package com.alibaba.polardbx.gms.tablegroup;

import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PartitionGroupArchiveRecord extends PartitionGroupRecord {
    public Long task_Id;

    @Override
    public PartitionGroupArchiveRecord fill(ResultSet rs) throws SQLException {
        this.task_Id = rs.getLong("task_id");
        super.fill(rs);
        return this;
    }
}
