package com.alibaba.polardbx.executor.ddl.newengine.job;

import com.alibaba.polardbx.executor.gsi.BackfillParameterManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wumu
 */
public abstract class OnlineDdlJobFactory extends DdlJobFactory {
    private final ExecutionContext executionContext;
    private final OnlineDdlInfo.DdlAlgorithm algorithm;

    public OnlineDdlJobFactory(ExecutionContext executionContext, OnlineDdlInfo.DdlAlgorithm algorithm) {
        this.executionContext = executionContext;
        this.algorithm = algorithm;
    }

    @Override
    public List<String> getPerfParameter() {
        return new ArrayList<>();
    }

    @Override
    protected void updateOnlineDdlInfo(OnlineDdlInfo onlineDdlInfo) {
        DdlContext ddlContext = executionContext.getDdlContext();
        if (!ddlContext.isExplainOnlineDdlAdvisor() && !ddlContext.isExplainOnlineDdl()) {
            return;
        }

        onlineDdlInfo.setOnlineDdlType(OnlineDdlInfo.DdlType.ONLINE_DDL);
        onlineDdlInfo.setOnlineDdlAlgorithm(algorithm);
        onlineDdlInfo.setAdviceOnlineDdlSql(String.format("%s", executionContext.getOriginSql()));
    }
}
