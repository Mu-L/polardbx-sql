package com.alibaba.polardbx.gms.metadb.misc;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import lombok.Getter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
/**
 * @author luoyanxin.pt
 */
@Getter
public class RollbackTaskConfigRecord implements SystemTableRecord {
    //type
    public final static int DN_REBUILT = 0;
    public RollbackTaskConfigRecord() {
    }
    long id;
    long root_job_id;
    long task_id;
    int type;
    String old_value;
    String new_value;
    @Override
    public RollbackTaskConfigRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.root_job_id = rs.getLong("root_job_id");
        this.task_id = rs.getLong("task_id");
        this.type = rs.getInt("type");
        this.old_value = rs.getString("old_value");
        this.new_value = rs.getString("new_value");
        return this;
    }
}