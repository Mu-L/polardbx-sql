package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.secret.SecretManager;

import java.sql.Connection;

public class SecretSyncAction implements ISyncAction {

    private String secretName;
    private String action;

    public SecretSyncAction() {
    }

    public SecretSyncAction(String secretName, String action) {
        this.secretName = secretName;
        this.action = action;
    }

    @Override
    public ResultCursor sync() {
        switch (action) {
        case "ADD":
            reloadSecret();
            break;
        case "UPDATE":
            reloadSecret();
            break;
        case "REMOVE":
            SecretManager.getInstance().remove(secretName);
            break;
        }
        SecretManager.getInstance().invalidateDependentCatalogSchemas(secretName);
        return null;
    }

    private void reloadSecret() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExternalSecretAccessor accessor = new ExternalSecretAccessor();
            accessor.setConnection(conn);
            ExternalSecretRecord record = accessor.selectByName(secretName);
            if (record != null) {
                SecretManager.getInstance().register(
                    record.name, record.type,
                    record.encryptedKv);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public String getSecretName() {
        return secretName;
    }

    public void setSecretName(String secretName) {
        this.secretName = secretName;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}
