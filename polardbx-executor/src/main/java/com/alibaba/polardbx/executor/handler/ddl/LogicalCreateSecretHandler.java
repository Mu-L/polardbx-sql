package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateSecretJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalSecretDdl;
import com.alibaba.polardbx.optimizer.secret.SecretMaskUtils;
import com.alibaba.polardbx.optimizer.secret.SecretTypeRegistry;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.rel.RelNode;

import java.sql.Connection;
import java.util.Map;

public class LogicalCreateSecretHandler extends LogicalCommonDdlHandler {

    public LogicalCreateSecretHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected String getObjectName(BaseDdlOperation logicalDdlPlan) {
        return ((LogicalSecretDdl) logicalDdlPlan).getSecretName();
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        PolarPrivilegeUtils.checkInstancePrivilege(PrivilegePoint.CREATE, executionContext);
        LogicalSecretDdl ddl = (LogicalSecretDdl) logicalPlan;
        if (ddl.isIfNotExists()) {
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                ExternalSecretAccessor accessor = new ExternalSecretAccessor(metaDbConn);
                if (accessor.selectByName(ddl.getSecretName()) != null) {
                    return new AffectRowCursor(0);
                }
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        }
        return super.handle(logicalPlan, executionContext);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        executionContext.getDdlContext().setDdlStmt(
            SecretMaskUtils.mask(executionContext.getDdlContext().getDdlStmt()));
        LogicalSecretDdl ddl = (LogicalSecretDdl) logicalDdlPlan;
        Map<String, String> props = ddl.getProperties();
        String type = props.get("type");

        // Validate type is specified and registered, check required/unknown keys
        if (type == null || type.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "'type' is required. Available types: "
                    + SecretTypeRegistry.getInstance().registeredTypes());
        }

        PropertyDefinition def = SecretTypeRegistry.getInstance().get(type);
        if (def == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Unknown secret type: '" + type + "'. Available types: "
                    + SecretTypeRegistry.getInstance().registeredTypes());
        }

        def.validateAsSecret(props);

        return new CreateSecretJobFactory(
            ddl.getSecretName(),
            type,
            props
        ).create();
    }
}
