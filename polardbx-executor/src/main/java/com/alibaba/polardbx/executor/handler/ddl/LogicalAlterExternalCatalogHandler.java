package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterExternalCatalogJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalExternalCatalogDdl;
import com.taobao.tddl.common.privilege.PrivilegePoint;

import java.util.Map;

public class LogicalAlterExternalCatalogHandler extends LogicalCommonDdlHandler {

    public LogicalAlterExternalCatalogHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected String getObjectName(BaseDdlOperation logicalDdlPlan) {
        return ((LogicalExternalCatalogDdl) logicalDdlPlan).getCatalogName();
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        PolarPrivilegeUtils.checkInstancePrivilege(PrivilegePoint.ALTER, executionContext);
        LogicalExternalCatalogDdl ddl = (LogicalExternalCatalogDdl) logicalDdlPlan;
        Map<String, String> props = ddl.getProperties();
        String propertiesJson = (props != null && !props.isEmpty()) ? JSON.toJSONString(props) : null;

        return new AlterExternalCatalogJobFactory(
            ddl.getCatalogName(),
            ddl.getSecretName(),
            propertiesJson,
            ddl.getComment()
        ).create();
    }
}
