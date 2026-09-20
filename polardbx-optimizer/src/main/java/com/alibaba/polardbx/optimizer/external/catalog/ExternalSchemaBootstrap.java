package com.alibaba.polardbx.optimizer.external.catalog;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.metadb.external.ExternalNameValidator;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ExternalSchemaManager;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.secret.SecretManager;

import java.util.ArrayList;

public class ExternalSchemaBootstrap {

    public static OptimizerContext tryBootstrap(String schemaNameLowerCase) {
        String[] parts = ExternalNameValidator.splitSchemaName(schemaNameLowerCase);
        if (parts == null) {
            return null;
        }
        String catalogName = parts[0];
        String dbName = parts[1];

        ExternalCatalogInfo info = ExternalCatalogManager.getInstance().get(catalogName);
        if (info == null) {
            return null;
        }
        ExternalNameValidator.validateExternalDbName(dbName);

        String secretName = info.getSecretName();
        long versionBefore = SecretManager.getInstance().getGeneration(secretName);

        SecretBundle secret = SecretBundle.EMPTY;
        if (secretName != null && !secretName.isEmpty()) {
            secret = SecretManager.getInstance().resolve(secretName, info.getProperties());
        }
        ConnectorDescriptor descriptor = ConnectorRegistry.getInstance().get(info.getConnector());
        boolean cache = descriptor.isThreadSafe();
        ConnectorMetadata metadata = null;
        boolean ownedByManager = false;

        try {
            metadata = descriptor.createMetadata(info.getProperties(), secret);
            if (!metadata.databaseExists(dbName.toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_DATABASE,
                    "Unknown database '" + dbName + "' in external catalog '" + catalogName + "'");
            }

            // Re-check secret generation: createMetadata is a slow network op during which the
            // secret may have been rotated/revoked. If so, the resolved bundle is stale; throw
            // to abort this bootstrap — getContext will not cache a context built on a stale
            // secret, and the next request re-bootstraps with the fresh one.
            if (secretName != null && !secretName.isEmpty()) {
                long versionAfter = SecretManager.getInstance().getGeneration(secretName);
                if (versionAfter != versionBefore) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                        "Secret '" + secretName + "' was rotated during bootstrap (version "
                            + versionBefore + " -> " + versionAfter + "), please retry");
                }
            }
            ExternalSchemaManager sm = new ExternalSchemaManager(catalogName, dbName,
                info.getConnector(), info.getProperties(), secret, secretName, versionBefore, cache, metadata);
            OptimizerContext ctx = new OptimizerContext(schemaNameLowerCase);
            ctx.setSchemaManager(sm);

            Matrix matrix = new Matrix();
            matrix.setGroups(new ArrayList<>());
            ctx.setMatrix(matrix);
            ctx.setFinishInit(true);

            OptimizerContext existing = OptimizerContext.loadExternalContext(ctx);
            // Only a shared handle is retained by the manager; a per-lookup connector
            // ignores the one passed in, so this bootstrap still owns it.
            ownedByManager = cache;
            if (existing != null) {
                sm.close();
                return existing;
            }
            return ctx;
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, e,
                "Failed to access external catalog '" + catalogName + "': " + e.getMessage());
        } finally {
            if (!ownedByManager) {
                ConnectorMetadata.closeQuietly(metadata);
            }
        }
    }
}
