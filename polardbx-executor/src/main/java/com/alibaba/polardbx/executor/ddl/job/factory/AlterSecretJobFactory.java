package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.executor.ddl.job.task.external.AlterSecretUpdateMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.external.AlterSecretValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.external.SecretSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.alibaba.polardbx.gms.metadb.external.ExternalNameValidator;
import com.google.common.collect.Lists;

import java.util.Map;
import java.util.Set;

public class AlterSecretJobFactory extends DdlJobFactory {

    private final String secretName;
    private final Map<String, String> newRawKv;

    public AlterSecretJobFactory(String secretName, Map<String, String> newRawKv) {
        this.secretName = secretName.toLowerCase();
        this.newRawKv = newRawKv;
    }

    @Override
    protected void validate() {
        ExternalNameValidator.validateSecretName(secretName);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        byte[] encrypted = ExternalCredentialEncryptor.encryptMap(newRawKv);
        ExecutableDdlJob job = new ExecutableDdlJob();
        job.addSequentialTasks(Lists.newArrayList(
            new AlterSecretValidateTask(secretName),
            new AlterSecretUpdateMetaTask(secretName, encrypted),
            new SecretSyncTask(secretName, SecretSyncTask.SecretOpType.UPDATE)
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
