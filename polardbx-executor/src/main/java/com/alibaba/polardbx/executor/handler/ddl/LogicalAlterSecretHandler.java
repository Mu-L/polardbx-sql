package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterSecretJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalSecretDdl;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import com.alibaba.polardbx.optimizer.secret.SecretMaskUtils;
import com.alibaba.polardbx.optimizer.secret.SecretTypeRegistry;
import com.taobao.tddl.common.privilege.PrivilegePoint;

import java.sql.Connection;
import java.util.Map;

public class LogicalAlterSecretHandler extends LogicalCommonDdlHandler {

    public LogicalAlterSecretHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected String getObjectName(BaseDdlOperation logicalDdlPlan) {
        return ((LogicalSecretDdl) logicalDdlPlan).getSecretName();
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        executionContext.getDdlContext().setDdlStmt(
            SecretMaskUtils.mask(executionContext.getDdlContext().getDdlStmt()));
        PolarPrivilegeUtils.checkInstancePrivilege(PrivilegePoint.ALTER, executionContext);
        LogicalSecretDdl ddl = (LogicalSecretDdl) logicalDdlPlan;
        Map<String, String> props = ddl.getProperties();
        String secretName = ddl.getSecretName();

        // Get existing type (immutable)
        SecretManager.SecretInfo existing = SecretManager.getInstance().getInfo(secretName);
        String existingType;
        if (existing != null) {
            existingType = existing.type;
        } else {
            // Fallback to GMS if not in cache
            try (Connection conn = MetaDbUtil.getConnection()) {
                ExternalSecretAccessor accessor = new ExternalSecretAccessor(conn);
                ExternalSecretRecord record = accessor.selectByName(secretName);
                existingType = record != null ? record.getType() : null;
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        }

        // Validate type immutability
        String propsType = props.get("type");
        if (propsType != null && existingType != null
            && !propsType.equalsIgnoreCase(existingType)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Cannot change secret type from '" + existingType + "' to '" + propsType
                    + "'. Drop and recreate the secret instead.");
        }

        // Use existing type for validation
        String typeForValidation = existingType != null ? existingType : propsType;
        if (typeForValidation != null) {
            if (typeForValidation.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                    "'type' is required. Available types: "
                        + SecretTypeRegistry.getInstance().registeredTypes());
            }
            PropertyDefinition def = SecretTypeRegistry.getInstance().get(typeForValidation);
            if (def == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                    "Unknown secret type: '" + typeForValidation + "'. Available types: "
                        + SecretTypeRegistry.getInstance().registeredTypes());
            }
            def.validateAsSecret(props);
        }

        return new AlterSecretJobFactory(secretName, props).create();
    }
}
