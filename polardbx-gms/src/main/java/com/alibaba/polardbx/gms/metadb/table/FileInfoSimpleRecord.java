package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * files系统表的精简列record，只包含文件名和长度，
 * 用于快照文件列表等只关心文件名和长度的大结果集查询，避免拉取files表的全部列
 *
 * @author lijiu
 */
@Data
public class FileInfoSimpleRecord implements SystemTableRecord {

    public String fileName;
    public long extentSize;

    @Override
    public FileInfoSimpleRecord fill(ResultSet rs) throws SQLException {
        this.fileName = rs.getString("file_name");
        this.extentSize = rs.getLong("extent_size");
        return this;
    }
}
