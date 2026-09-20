package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.ddl.job.factory.DropSecretJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalSecretDdl;
import com.taobao.tddl.common.privilege.PrivilegePoint;

public class LogicalDropSecretHandler extends LogicalCommonDdlHandler {

    public LogicalDropSecretHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected String getObjectName(BaseDdlOperation logicalDdlPlan) {
        return ((LogicalSecretDdl) logicalDdlPlan).getSecretName();
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        PolarPrivilegeUtils.checkInstancePrivilege(PrivilegePoint.DROP, executionContext);
        LogicalSecretDdl ddl = (LogicalSecretDdl) logicalDdlPlan;
        return new DropSecretJobFactory(ddl.getSecretName(), ddl.isIfExists()).create();
    }
}
