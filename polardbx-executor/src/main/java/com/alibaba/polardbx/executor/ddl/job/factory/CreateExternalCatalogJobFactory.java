package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.executor.ddl.job.task.external.CreateExternalCatalogAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.external.ExternalCatalogValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.external.ExternalCatalogSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.alibaba.polardbx.gms.metadb.external.ExternalNameValidator;
import com.google.common.collect.Lists;

import java.util.Map;
import java.util.Set;

public class CreateExternalCatalogJobFactory extends DdlJobFactory {

    private final String catalogName;
    private final String connector;
    private final Map<String, String> properties;
    private final String secretName;
    private final String comment;

    public CreateExternalCatalogJobFactory(String catalogName, String connector,
                                           Map<String, String> properties, String secretName,
                                           String comment) {
        this.catalogName = catalogName.toLowerCase();
        this.connector = connector;
        this.properties = properties;
        this.secretName = secretName != null ? secretName.toLowerCase() : null;
        this.comment = comment;
    }

    @Override
    protected void validate() {
        ExternalNameValidator.validateCatalogName(catalogName);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        byte[] encrypted = ExternalCredentialEncryptor.encryptMap(properties);
        ExecutableDdlJob job = new ExecutableDdlJob();
        job.addSequentialTasks(Lists.newArrayList(
            new ExternalCatalogValidateTask(catalogName, connector, encrypted, secretName),
            new CreateExternalCatalogAddMetaTask(catalogName, connector, encrypted, secretName, comment),
            new ExternalCatalogSyncTask(catalogName, ExternalCatalogSyncTask.SyncAction.ADD)
        ));
        return job;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(ExternalCatalogConstants.CATALOG_RESOURCE_PREFIX + catalogName.toLowerCase());
    }

    @Override
    protected void sharedResources(Set<String> resources) {
        if (secretName != null && !secretName.isEmpty()) {
            resources.add(ExternalCatalogConstants.SECRET_RESOURCE_PREFIX + secretName.toLowerCase());
        }
    }
}
