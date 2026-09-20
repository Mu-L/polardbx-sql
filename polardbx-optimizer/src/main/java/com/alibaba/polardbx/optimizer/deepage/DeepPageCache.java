package com.alibaba.polardbx.optimizer.deepage;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.google.common.base.Objects;
import org.apache.calcite.rel.RelNode;

import java.util.List;

public class DeepPageCache {

    RelNode deepPagePlan;
    boolean isOptimized = false;
    final List<Integer> orderByColIndexes;

    //start from 1
    final int offsetParamIndex;
    final int fetchParamIndex;
    final List<Object> sqlParams;

    //偏移量和参数的获取以及更新应该具有原子性
    long deepPageOffset;

    List<Object> deepPageParams;

    public DeepPageCache(RelNode deepPagePlan, boolean isOptimized, List<Integer> orderByColIndexes,
                         int offsetParamIndex, int fetchParamIndex, List<Object> sqlParams) {
        this.deepPagePlan = deepPagePlan;
        this.isOptimized = isOptimized;
        this.offsetParamIndex = offsetParamIndex;
        this.fetchParamIndex = fetchParamIndex;
        this.orderByColIndexes = orderByColIndexes;
        this.sqlParams = sqlParams;
        this.deepPageOffset = 0L;
        this.deepPageParams = null;
    }

    public synchronized void updateCacheOffsetAndParams(long deepPageOffset, List<Object> deepPageParams) {
        this.deepPageOffset = deepPageOffset;
        this.deepPageParams = deepPageParams;
    }

    public synchronized Pair<Long, List<Object>> getDeepPageOffsetAndParams() {
        return Pair.of(deepPageOffset, deepPageParams);
    }

    public RelNode getDeepPagePlan() {
        return deepPagePlan;
    }

    public List<Integer> getOrderByColIndexes() {
        return orderByColIndexes;
    }

    public int getOffsetParamIndex() {
        return offsetParamIndex;
    }

    public int getFetchParamIndex() {
        return fetchParamIndex;
    }

    public List<Object> getSqlParams() {
        return sqlParams;
    }

    public void setOptimizedDeepPagePlan(RelNode deepPagePlan) {
        this.deepPagePlan = deepPagePlan;
        this.isOptimized = true;
    }
}
