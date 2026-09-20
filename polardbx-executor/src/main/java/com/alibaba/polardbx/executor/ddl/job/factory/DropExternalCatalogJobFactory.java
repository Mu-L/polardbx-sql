package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.external.CheckExternalCatalogExistenceTask;
import com.alibaba.polardbx.executor.ddl.job.task.external.DropExternalCatalogRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.external.ExternalCatalogSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.google.common.collect.Lists;

import java.util.Set;

public class DropExternalCatalogJobFactory extends DdlJobFactory {

    private final String catalogName;
    private final boolean ifExists;

    public DropExternalCatalogJobFactory(String catalogName, boolean ifExists) {
        this.catalogName = catalogName.toLowerCase();
        this.ifExists = ifExists;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob job = new ExecutableDdlJob();
        job.addSequentialTasks(Lists.newArrayList(
            new CheckExternalCatalogExistenceTask(catalogName, ifExists),
            new DropExternalCatalogRemoveMetaTask(catalogName),
            new ExternalCatalogSyncTask(catalogName, ExternalCatalogSyncTask.SyncAction.REMOVE)
        ));
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
