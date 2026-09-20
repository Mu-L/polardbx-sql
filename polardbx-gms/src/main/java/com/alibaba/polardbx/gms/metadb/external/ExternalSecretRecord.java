package com.alibaba.polardbx.gms.metadb.external;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExternalSecretRecord implements SystemTableRecord {
    public String name;
    public String type;
    public byte[] encryptedKv;

    @Override
    public ExternalSecretRecord fill(ResultSet rs) throws SQLException {
        this.name = rs.getString("name");
        this.type = rs.getString("type");
        this.encryptedKv = rs.getBytes("encrypted_kv");
        return this;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public byte[] getEncryptedKv() {
        return encryptedKv;
    }
}
