package com.alibaba.polardbx.gms.metadb.external;

import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class ExternalCatalogInfoRecord implements SystemTableRecord {
    public String name;
    public String connector;
    public byte[] properties;
    public String secretName;
    public String comment;

    @Override
    public ExternalCatalogInfoRecord fill(ResultSet rs) throws SQLException {
        this.name = rs.getString("name");
        this.connector = rs.getString("connector");
        this.properties = rs.getBytes("properties");
        this.secretName = rs.getString("secret_name");
        this.comment = rs.getString("comment");
        return this;
    }

    public String getName() {
        return name;
    }

    public String getConnector() {
        return connector;
    }

    public byte[] getProperties() {
        return properties;
    }

    public String getSecretName() {
        return secretName;
    }

    public String getComment() {
        return comment;
    }

    public ExternalCatalogInfo toInfo() {
        Map<String, String> props = ExternalCredentialEncryptor.decryptToMap(properties);
        return new ExternalCatalogInfo(name, connector, props, secretName, comment);
    }
}
