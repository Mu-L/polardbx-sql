package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.ddl.job.factory.DropExternalCatalogJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalExternalCatalogDdl;
import com.taobao.tddl.common.privilege.PrivilegePoint;

public class LogicalDropExternalCatalogHandler extends LogicalCommonDdlHandler {

    public LogicalDropExternalCatalogHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected String getObjectName(BaseDdlOperation logicalDdlPlan) {
        return ((LogicalExternalCatalogDdl) logicalDdlPlan).getCatalogName();
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        PolarPrivilegeUtils.checkInstancePrivilege(PrivilegePoint.DROP, executionContext);
        LogicalExternalCatalogDdl ddl = (LogicalExternalCatalogDdl) logicalDdlPlan;
        return new DropExternalCatalogJobFactory(ddl.getCatalogName(), ddl.isIfExists()).create();
    }
}
