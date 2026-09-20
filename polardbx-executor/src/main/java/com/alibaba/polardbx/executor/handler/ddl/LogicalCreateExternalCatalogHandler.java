package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateExternalCatalogJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalExternalCatalogDdl;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.rel.RelNode;

import java.sql.Connection;

public class LogicalCreateExternalCatalogHandler extends LogicalCommonDdlHandler {

    public LogicalCreateExternalCatalogHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected String getObjectName(BaseDdlOperation logicalDdlPlan) {
        return ((LogicalExternalCatalogDdl) logicalDdlPlan).getCatalogName();
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        PolarPrivilegeUtils.checkInstancePrivilege(PrivilegePoint.CREATE, executionContext);
        LogicalExternalCatalogDdl ddl = (LogicalExternalCatalogDdl) logicalPlan;
        if (ddl.isIfNotExists()) {
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor(metaDbConn);
                if (accessor.selectByName(ddl.getCatalogName()) != null) {
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
        LogicalExternalCatalogDdl ddl = (LogicalExternalCatalogDdl) logicalDdlPlan;
        return new CreateExternalCatalogJobFactory(
            ddl.getCatalogName(),
            ddl.getConnector(),
            ddl.getProperties(),
            ddl.getSecretName(),
            ddl.getComment()
        ).create();
    }
}
