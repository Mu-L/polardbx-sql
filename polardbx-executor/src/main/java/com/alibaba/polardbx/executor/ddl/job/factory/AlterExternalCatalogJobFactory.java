package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;

import java.util.Set;

public class AlterExternalCatalogJobFactory extends DdlJobFactory {

    private final String catalogName;
    private final String secretName;
    private final String propertiesJson;
    private final String comment;

    public AlterExternalCatalogJobFactory(String catalogName, String secretName, String propertiesJson,
                                          String comment) {
        this.catalogName = catalogName.toLowerCase();
        this.secretName = secretName;
        this.propertiesJson = propertiesJson;
        this.comment = comment;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob job = new ExecutableDdlJob();

        return job;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(ExternalCatalogConstants.CATALOG_RESOURCE_PREFIX + catalogName.toLowerCase());
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}
