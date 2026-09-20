package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import org.apache.calcite.rel.core.DDL;

import java.util.Map;

public class LogicalExternalCatalogDdl extends BaseDdlOperation {

    private final DdlType ddlType;
    private final String catalogName;
    private final boolean ifNotExists;
    private final boolean ifExists;
    private final String connector;
    private final Map<String, String> properties;
    private final String secretName;
    private final String comment;
    private final String dbName;
    private final String tableName;

    public LogicalExternalCatalogDdl(DDL ddl, DdlType ddlType, String catalogName,
                                     boolean ifNotExists, boolean ifExists,
                                     String connector, Map<String, String> properties,
                                     String secretName, String comment,
                                     String dbName, String tableName) {
        super(ddl);
        this.ddlType = ddlType;
        this.catalogName = catalogName;
        this.ifNotExists = ifNotExists;
        this.ifExists = ifExists;
        this.connector = connector;
        this.properties = properties;
        this.secretName = secretName;
        this.comment = comment;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    @Override
    public DdlType getDdlType() {
        return ddlType;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public String getConnector() {
        return connector;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getSecretName() {
        return secretName;
    }

    public String getComment() {
        return comment;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName == null ? catalogName : tableName;
    }

    public static LogicalExternalCatalogDdl create(DDL ddl, DdlType ddlType, String catalogName,
                                                   boolean ifNotExists, boolean ifExists,
                                                   String connector, Map<String, String> properties,
                                                   String secretName, String comment,
                                                   String dbName, String tableName) {
        return new LogicalExternalCatalogDdl(ddl, ddlType, catalogName, ifNotExists, ifExists,
            connector, properties, secretName, comment, dbName, tableName);
    }
}
