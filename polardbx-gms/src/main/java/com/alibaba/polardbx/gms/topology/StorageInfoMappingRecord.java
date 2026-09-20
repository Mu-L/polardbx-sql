package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author chenghui.lch
 */
public class StorageInfoMappingRecord implements SystemTableRecord {
    public long id;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public String upstreamInstId;
    public String upstreamStorageInstId;
    public String instId;
    public String storageInstId;

    @Override
    public StorageInfoMappingRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.upstreamInstId = rs.getString("upstream_inst_id");
        this.upstreamStorageInstId = rs.getString("upstream_storage_inst_id");
        this.storageInstId = rs.getString("storage_inst_id");
        this.instId = rs.getString("inst_id");
        this.storageInstId = rs.getString("storage_inst_id");
        return this;
    }

    public StorageInfoMappingRecord copy() {
        StorageInfoMappingRecord newRecord = new StorageInfoMappingRecord();
        newRecord.id = this.id;
        newRecord.gmtCreated = this.gmtCreated;
        newRecord.gmtModified = this.gmtModified;

        newRecord.upstreamInstId = this.upstreamInstId;
        newRecord.upstreamStorageInstId = this.upstreamStorageInstId;
        newRecord.instId = this.instId;
        newRecord.storageInstId = this.storageInstId;
        return newRecord;

    }

    public long getId() {
        return id;
    }

    public Timestamp getGmtCreated() {
        return gmtCreated;
    }

    public Timestamp getGmtModified() {
        return gmtModified;
    }

    public String getUpstreamInstId() {
        return upstreamInstId;
    }

    public String getUpstreamStorageInstId() {
        return upstreamStorageInstId;
    }

    public String getInstId() {
        return instId;
    }

    public String getStorageInstId() {
        return storageInstId;
    }

    @Override
    public String toString() {
        return "StorageInfoMappingRecord{" +
            "id=" + id +
            ", gmtCreated=" + gmtCreated +
            ", gmtModified=" + gmtModified +
            ", upstreamInstId='" + upstreamInstId + '\'' +
            ", upstreamStorageInstId='" + upstreamStorageInstId + '\'' +
            ", instId='" + instId + '\'' +
            ", storageInstId='" + storageInstId + '\'' +
            '}';
    }
}
