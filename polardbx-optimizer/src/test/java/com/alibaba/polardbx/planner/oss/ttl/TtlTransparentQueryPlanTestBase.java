package com.alibaba.polardbx.planner.oss.ttl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.ttl.query.TtlQueryType;
import com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.Before;

import java.util.Collections;

public abstract class TtlTransparentQueryPlanTestBase extends ParameterizedTestCommon {

    public TtlTransparentQueryPlanTestBase(String caseName, int sqlIndex, String sql, String expectedPlan,
                                           String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
        super.initBasePlannerTestEnv();
    }

    @Before
    public void before() {
        OptimizerContext op = getContextByAppName(getAppName());
        for (TableMeta tableMeta : op.getLatestSchemaManager().getAllTables()) {
            if (tableMeta.getTtlDefinitionInfo() != null) {
                ExtraFieldJSON extra = tableMeta.getTtlDefinitionInfo().getTtlInfoRecord().getExtra();
                extra.setArcBound("2020-01-01 00:00:00");
                extra.setTtlRefColValueList(Collections.singletonList("1000"));
            }
        }
    }

    @Override
    public ExecutionPlan afterOptimize(String testSql, ExecutionContext ec, ExecutionPlan executionPlan) {
        if (ec.getParamManager().getBoolean(ConnectionParams.ENABLE_TRANSPARENT_TTL)) {
            TtlQueryType ttlQueryType = PlannerContext.getPlannerContext(executionPlan.getPlan()).getTtlQueryType();
            executionPlan =
                Planner.getInstance()
                    .buildTtlQueryPlan(executionPlan, ec.getSqlParameterized(), ec, null, false, ttlQueryType);
        }
        return executionPlan;
    }
}
