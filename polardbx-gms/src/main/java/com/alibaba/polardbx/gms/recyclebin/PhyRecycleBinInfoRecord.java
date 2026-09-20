package com.alibaba.polardbx.gms.recyclebin;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PhyRecycleBinInfoRecord implements SystemTableRecord {

    public static final Integer NORM_DLL_TYPE = 0;
    public static final Integer REBALANCE_TYPE = 1;
    public static final Integer STATUS_INIT = 0;
    public static final Integer STATUS_RENAME = 1;
    public static final Integer STATUS_DROP = 2;
    public static final String PHY_DB_NAME = "__pxc_phy_recycle_bin__";
    public static final String GROUP_NAME = PHY_DB_NAME + "_group";

    @Override
    public PhyRecycleBinInfoRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.jobId = rs.getLong("job_id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.storageInstId = rs.getString("storage_inst_id");
        this.originDbName = rs.getString("origin_db_name");
        this.originTbName = rs.getString("origin_tb_name");
        this.curDbName = rs.getString("cur_db_name");
        this.curTbName = rs.getString("cur_tb_name");
        this.type = rs.getInt("type");
        this.status = rs.getInt("status");
        this.extras = rs.getString("extras");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;

        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.jobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.storageInstId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.originDbName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.originTbName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.curDbName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.curTbName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.type);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extras);
        return params;
    }

    private Long id;
    private Long jobId;
    private String storageInstId;
    private Date gmtCreated;
    private Date gmtModified;
    private String originDbName;
    private String originTbName;
    private String curDbName;
    private String curTbName;
    private Integer type;
    private Integer status;
    private String extras;

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public Date getGmtCreated() {
        return gmtCreated;
    }

    public void setGmtCreated(Date gmtCreated) {
        this.gmtCreated = gmtCreated;
    }

    public Date getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(Date gmtModified) {
        this.gmtModified = gmtModified;
    }

    public String getStorageInstId() {
        return storageInstId;
    }

    public void setStorageInstId(String storageInstId) {
        this.storageInstId = storageInstId;
    }

    public String getOriginDbName() {
        return originDbName;
    }

    public void setOriginDbName(String originDbName) {
        this.originDbName = originDbName;
    }

    public String getOriginTbName() {
        return originTbName;
    }

    public void setOriginTbName(String originTbName) {
        this.originTbName = originTbName;
    }

    public String getCurDbName() {
        return curDbName;
    }

    public void setCurDbName(String curDbName) {
        this.curDbName = curDbName;
    }

    public String getCurTbName() {
        return curTbName;
    }

    public void setCurTbName(String curTbName) {
        this.curTbName = curTbName;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getExtras() {
        return extras;
    }

    public void setExtras(String extras) {
        this.extras = extras;
    }
}
