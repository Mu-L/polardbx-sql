package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import org.apache.calcite.rel.core.DDL;

import java.util.Map;

public class LogicalSecretDdl extends BaseDdlOperation {

    private final DdlType ddlType;
    private final String secretName;
    private final boolean ifNotExists;
    private final boolean ifExists;
    private final Map<String, String> properties;

    public LogicalSecretDdl(DDL ddl, DdlType ddlType, String secretName,
                            boolean ifNotExists, boolean ifExists,
                            Map<String, String> properties) {
        super(ddl);
        this.ddlType = ddlType;
        this.secretName = secretName;
        this.ifNotExists = ifNotExists;
        this.ifExists = ifExists;
        this.properties = properties;
    }

    @Override
    public DdlType getDdlType() {
        return ddlType;
    }

    public String getSecretName() {
        return secretName;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public static LogicalSecretDdl create(DDL ddl, DdlType ddlType, String secretName,
                                          boolean ifNotExists, boolean ifExists,
                                          Map<String, String> properties) {
        return new LogicalSecretDdl(ddl, ddlType, secretName, ifNotExists, ifExists,
            properties);
    }
}
