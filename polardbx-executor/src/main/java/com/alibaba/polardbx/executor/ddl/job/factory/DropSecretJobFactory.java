package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.external.DropSecretRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.external.DropSecretValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.external.SecretSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.google.common.collect.Lists;

import java.util.Set;

public class DropSecretJobFactory extends DdlJobFactory {

    private final String secretName;
    private final boolean ifExists;

    public DropSecretJobFactory(String secretName, boolean ifExists) {
        this.secretName = secretName.toLowerCase();
        this.ifExists = ifExists;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob job = new ExecutableDdlJob();
        job.addSequentialTasks(Lists.newArrayList(
            new DropSecretValidateTask(secretName, ifExists),
            new DropSecretRemoveMetaTask(secretName),
            new SecretSyncTask(secretName, SecretSyncTask.SecretOpType.REMOVE)
        ));
        return job;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(ExternalCatalogConstants.SECRET_RESOURCE_PREFIX + secretName.toLowerCase());
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}
