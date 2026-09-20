package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Record mapping for ext_staging_meta table.
 *
 * <p>Schema (v1): seq → DN mapping. The mapping is immutable for a given seq.
 */
@Data
public class ExtStagingMetaRecord implements SystemTableRecord {
    private int seqId;
    private String ownerCn;
    private String status;
    /**
     * storageInstId of the DN that hosts this seq's physical staging table.
     */
    private String dnId;
    /**
     * physical DB name on DN where the staging table lives.
     */
    private String phyDb;
    private long rowCount;
    private Timestamp gmtCreated;
    private Timestamp gmtModified;

    // Explicit accessors keep this cross-module MetaDB record usable even when Lombok annotation
    // processing is disabled by an incremental compiler invocation.
    public int getSeqId() {
        return seqId;
    }

    public String getOwnerCn() {
        return ownerCn;
    }

    public String getStatus() {
        return status;
    }

    public String getDnId() {
        return dnId;
    }

    public String getPhyDb() {
        return phyDb;
    }

    public long getRowCount() {
        return rowCount;
    }

    public Timestamp getGmtCreated() {
        return gmtCreated;
    }

    public Timestamp getGmtModified() {
        return gmtModified;
    }

    @Override
    public ExtStagingMetaRecord fill(ResultSet rs) throws SQLException {
        this.seqId = rs.getInt("seq_id");
        this.ownerCn = rs.getString("owner_cn");
        this.status = rs.getString("status");
        this.dnId = rs.getString("dn_id");
        this.phyDb = rs.getString("phy_db");
        this.rowCount = rs.getLong("row_count");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        return this;
    }
}
