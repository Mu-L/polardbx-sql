package com.alibaba.polardbx.gms.metadb.external;

import java.util.Collections;
import java.util.Map;

public class ExternalCatalogInfo {
    private final String name;
    private final String connector;
    private final Map<String, String> properties;
    private final String secretName;
    private final String comment;

    public ExternalCatalogInfo(String name, String connector, Map<String, String> properties,
                               String secretName, String comment) {
        this.name = name;
        this.connector = connector;
        this.properties = Collections.unmodifiableMap(properties);
        this.secretName = secretName;
        this.comment = comment;
    }

    public String getName() {
        return name;
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
}
