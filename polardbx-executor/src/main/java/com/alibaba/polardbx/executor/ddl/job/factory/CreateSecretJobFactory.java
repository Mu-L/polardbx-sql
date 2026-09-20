package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.executor.ddl.job.task.external.CreateSecretAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.external.CreateSecretValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.external.SecretSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.alibaba.polardbx.gms.metadb.external.ExternalNameValidator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CreateSecretJobFactory extends DdlJobFactory {

    private final String secretName;
    private final String type;
    private final Map<String, String> rawKv;

    public CreateSecretJobFactory(String secretName, String type,
                                  Map<String, String> rawKv) {
        this.secretName = secretName.toLowerCase();
        this.type = type;
        this.rawKv = rawKv;
    }

    @Override
    protected void validate() {
        ExternalNameValidator.validateSecretName(secretName);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        byte[] encrypted = ExternalCredentialEncryptor.encryptMap(rawKv);
        ExecutableDdlJob job = new ExecutableDdlJob();
        List<DdlTask> tasks = new ArrayList<>();
        tasks.add(new CreateSecretValidateTask(secretName));
        tasks.add(new CreateSecretAddMetaTask(secretName, type, encrypted));
        tasks.add(new SecretSyncTask(secretName, SecretSyncTask.SecretOpType.ADD));
        job.addSequentialTasks(tasks);
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
