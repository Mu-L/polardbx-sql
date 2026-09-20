package com.alibaba.polardbx.planner.oss;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.deepage.DeepPageCache;
import com.alibaba.polardbx.optimizer.deepage.DeepPageUtil;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeepPagePlanTest extends ParameterizedTestCommon {

    private Map<PlanCache.CacheKey, DeepPageCache> deepPageCacheMap = new HashMap<>();

    private final int OFFSET = 1000;

    private final int PARAM = 0;

    public DeepPagePlanTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    public void beforeOptimize(ExecutionContext ec) {
        ec.getParamManager().getProps().put(ConnectionProperties.ENABLE_DEEP_PAGE_OPTIMIZER, "true");
        ec.setDeepPageCacheMap(deepPageCacheMap);
        deepPageCacheMap.clear();
    }

    /**
     * 增加offset为1000的deepPageCache
     */
    @Override
    public ExecutionPlan afterOptimize(String testSql, ExecutionContext ec, ExecutionPlan executionPlan) {
        ec.setOriginSql(testSql);
        DeepPageCache deepPageCache = DeepPageUtil.tryCreateDeepPageCache(executionPlan, ec);
        if (deepPageCache == null) {
            return executionPlan;
        }
        int size = deepPageCache.getOrderByColIndexes().size();
        List<Object> deepPageParams = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            deepPageParams.add(PARAM);
        }
        deepPageCache.updateCacheOffsetAndParams(OFFSET, deepPageParams);
        ExecutionPlan deepPageExecutionPlan = DeepPageUtil.tryCreateDeepPageExecutionPlan(executionPlan, ec);
        return deepPageExecutionPlan == null ? executionPlan : deepPageExecutionPlan;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(DeepPagePlanTest.class);
    }
}
