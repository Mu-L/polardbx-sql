package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.BooleanConfigParam;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.htaprouting.OptimizerType;
import com.alibaba.polardbx.optimizer.htaprouting.PlanType;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingType;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.apache.calcite.rel.logical.LogicalValues;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mockStatic;

public class PlanCacheTest extends PlanTestCommon {
//    private static String schemaName = "tddl";

    private static String[] sqls = {
        "select * from t_shard_id1",
        "select * from t_shard_id1 limit 1",
        "select * from t_shard_id1 order by id",
        "select * from t_shard_id1 order by id limit 1",
        "select * from t_shard_id1 where name='a'",
        "select * from t_shard_id1 where name like '%s'",
        "select * from t_shard_id1 where name='a' limit 10",
    };

    private static String[] inSqls = {
        "select * from t_shard_id1",
        "select * from t_shard_id1 limit 1",
        "select * from t_shard_id1 where name in ('a')",
        "select * from t_shard_id1 where name in ('a', 'b')",
        "select * from t_shard_id1 where name in ('a', 'c', 'e')",
        "select * from t_shard_id1 where name in ('a', 'd', 'e', 'd')",
        "select * from t_shard_id1 where name in ('a', 'b') limit 1",
        "select * from t_shard_id1 where name in ('a') limit 1",
        "select * from t_shard_id1 where name in ('a', 'c', 'e') limit 1",
        "select * from t_shard_id1 where name in ('a', 'j', 'f', 'o') limit 1",
    };

    public PlanCacheTest(String caseName, String targetEnvFile) {
        super(caseName, targetEnvFile);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return ImmutableList.of(
            new Object[] {"PlanCacheTest", "/com/alibaba/polardbx/planner/planmanagement/PlanManagementPlanTest"});
    }

    @Override
    public void testSql() {

    }

    @Test
    public void testExpireTime() {
        // default config test, 12H
        testExpireTimeEqualsConfig(12 * 3600 * 1000);

        // manual set config test
        int manualExpireTime = 110 * 1000;
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.PLAN_CACHE_EXPIRE_TIME, manualExpireTime + "");
        testExpireTimeEqualsConfig(manualExpireTime);
    }

    private void testExpireTimeEqualsConfig(int expectedTime) {
        int expireTime = DynamicConfig.getInstance().planCacheExpireTime();
        PlanCache planCache = new PlanCache(1);
        assert expireTime == planCache.getPlanCacheExpireTime();
        assert expireTime == expectedTime;
    }

    @Test
    public void testUpperBound() throws ExecutionException {
        int maxSize = 3;
        PlanCache planCache = new PlanCache(maxSize);
        for (String sql : sqls) {
            ExecutionContext executionContext = new ExecutionContext(this.appName);
            SqlParameterized sqlParameterized =
                SqlParameterizeUtils.parameterize(ByteString.from(sql), null, executionContext, false);
            planCache.get(this.appName, sqlParameterized, executionContext, false);
        }
        if (planCache.getCache().size() > maxSize) {
            Assert.fail("plan cache max size over limit");
        }
    }

    @Test
    public void testInSqlBound() throws ExecutionException {
        int maxSize = 3;
        PlanCache planCache = new PlanCache(maxSize);
        for (String sql : inSqls) {
            ExecutionContext executionContext = new ExecutionContext(this.appName);
            SqlParameterized sqlParameterized =
                SqlParameterizeUtils.parameterize(ByteString.from(sql), null, executionContext, false);
            planCache.get(this.appName, sqlParameterized, executionContext, false);
        }
        if (planCache.getCache().size() > maxSize) {
            Assert.fail("plan cache in sql size over limit:" + planCache.getCache().size());
        }
    }

    @Test
    public void testInSqlBound2() throws ExecutionException {
        int maxSize = 50;
        PlanCache planCache = new PlanCache(maxSize);
        for (String sql : inSqls) {
            ExecutionContext executionContext = new ExecutionContext(this.appName);
            SqlParameterized sqlParameterized =
                SqlParameterizeUtils.parameterize(ByteString.from(sql), null, executionContext, false);
            planCache.get(this.appName, sqlParameterized, executionContext, false);
        }
        if (planCache.getCache().size() > inSqls.length) {
            Assert.fail("plan cache in sql size over limit:" + planCache.getCache().size());
        }
    }

    @Test
    public void testError() {
        PlanCache planCache = new PlanCache(10);
        String sql = "select * from table_not_exists";
        ExecutionContext executionContext = new ExecutionContext(this.appName);
        SqlParameterized sqlParameterized =
            SqlParameterizeUtils.parameterize(ByteString.from(sql), null, executionContext, false);
        try {
            planCache.get(this.appName, sqlParameterized, executionContext, false);
            Assert.fail("should throw error");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e instanceof TddlRuntimeException &&
                e.getMessage()
                    .contains("ERR-CODE: [PXC-4006][ERR_TABLE_NOT_EXIST] Table 'table_not_exists' doesn't exist"));
        }
    }

    @Test
    public void testColumnarCacheSwitch() throws Exception {
        OptimizerType[] optimizerTypes = new OptimizerType[] {OptimizerType.COLUMNAR, OptimizerType.MPP};
        boolean[] values = new boolean[] {true, false};

        for (OptimizerType cachedOptimizerType : optimizerTypes) {
            for (boolean cachedColumnarPlanCache : values) {
                for (OptimizerType generatedOptimizerType : optimizerTypes) {
                    for (boolean useColumnarPlanCache : values) {
                        columnarPlanCacheSwitch(cachedOptimizerType, cachedColumnarPlanCache, generatedOptimizerType,
                            useColumnarPlanCache);
                    }
                }
            }
        }
    }

    void columnarPlanCacheSwitch(OptimizerType cachedOptimizerType, boolean cacheColumnarPlanCache,
                                 OptimizerType generatedOptimizerType,
                                 boolean useColumnarPlanCache) throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        boolean config = DynamicConfig.getInstance().colPlanCache();
        ExecutionContext executionContext = new ExecutionContext("test");
        LogicalValues va0 = Mockito.mock(LogicalValues.class);
        LogicalValues va1 = Mockito.mock(LogicalValues.class);
        boolean useColumnar = generatedOptimizerType == OptimizerType.COLUMNAR;
        try (MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class)) {
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.ENABLE_COLUMNAR_PLAN_CACHE, String.valueOf(useColumnarPlanCache));
            PlanCache planCache = Mockito.mock(PlanCache.class);
            Mockito.doNothing().when(planCache).invalidateByCacheKey(any());

            SqlParameterized sqlParameterized = new SqlParameterized("", Lists.newArrayList());
            Field tablesField = sqlParameterized.getClass().getDeclaredField("tables");
            tablesField.setAccessible(true);
            tablesField.set(sqlParameterized, Sets.newHashSet());

            // cached plan
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(va0)).thenAnswer(
                invocation -> {
                    PlannerContext pc = Mockito.mock(PlannerContext.class);
                    Mockito.when(pc.isColumnarOptimizer()).thenReturn(cachedOptimizerType == OptimizerType.COLUMNAR);
                    Mockito.when(pc.getOptimizerType()).thenReturn(cachedOptimizerType);
                    Mockito.when(pc.getPlanType())
                        .thenReturn(cachedOptimizerType == OptimizerType.COLUMNAR ? PlanType.COLUMNAR : PlanType.ROW);
                    Mockito.when(pc.isUseColumnarPlanCache()).thenReturn(cacheColumnarPlanCache);
                    return pc;
                });

            // new plan
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(va1)).thenAnswer(
                invocation -> {
                    PlannerContext pc = Mockito.mock(PlannerContext.class);
                    Mockito.when(pc.isColumnarOptimizer()).thenReturn(generatedOptimizerType == OptimizerType.COLUMNAR);
                    Mockito.when(pc.getOptimizerType()).thenReturn(generatedOptimizerType);
                    Mockito.when(pc.getPlanType())
                        .thenReturn(
                            generatedOptimizerType == OptimizerType.COLUMNAR ? PlanType.COLUMNAR : PlanType.ROW);
                    Mockito.when(pc.isUseColumnarPlanCache()).thenReturn(executionContext.isColumnarPlanCache());
                    return pc;
                });
            Mockito.when(planCache.getCacheLoader(any(), any(), any(), any(), anyBoolean(), any(), any())).thenAnswer(
                (Answer<Callable<ExecutionPlan>>) invocation -> () -> {
                    ExecutionPlan plan = Mockito.mock(ExecutionPlan.class);
                    Mockito.when(plan.getPlan()).thenReturn(va1);
                    return plan;
                }
            );

            AtomicInteger count = new AtomicInteger(0);
            Mockito.when(planCache.getPlanWithLoader(any(), any(), any())).thenAnswer(
                (Answer<ExecutionPlan>) invocation -> {
                    count.getAndIncrement();
                    if (count.get() == 1) {
                        ExecutionPlan plan = Mockito.mock(ExecutionPlan.class);
                        Mockito.when(plan.getPlan()).thenReturn(va0);
                        return plan;
                    }
                    Object[] args = invocation.getArguments();
                    Callable<ExecutionPlan> loader = (Callable<ExecutionPlan>) args[1];
                    return loader.call();
                });

            Mockito.when(planCache.savePlanCachedKey(any(), any(), any(), any())).thenAnswer(
                (Answer<ExecutionPlan>) invocation -> (ExecutionPlan) invocation.getArguments()[1]);

            Mockito.when(planCache.checkColumnarDisable(any(), any(), any(), any(), any())).thenReturn(null);
            doCallRealMethod().when(planCache).checkColumnarPlanCache(any(), any(), any(), any(), any(), any());
            doCallRealMethod().when(planCache).getFromCache(any(), any(), any(), any(), anyBoolean());
            ExecutionPlan plan = planCache.getFromCache("test", sqlParameterized, null, executionContext, false);

            // row plan cached
            if (OptimizerType.COLUMNAR != cachedOptimizerType) {
                // plan must be row
                Assert.assertFalse(PlannerContext.getPlannerContext(plan.getPlan()).isColumnarOptimizer());
                Assert.assertTrue(count.get() == 1);
                return;
            }

            // cacheColumnar == true
            if (cacheColumnarPlanCache == useColumnarPlanCache) {
                Assert.assertTrue(count.get() == 1);
            }

            if (cacheColumnarPlanCache && useColumnar && useColumnarPlanCache) {
                Assert.assertTrue(
                    PlanType.containColumnar(PlannerContext.getPlannerContext(plan.getPlan()).getPlanType()));
                Assert.assertTrue(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnarPlanCache());
                Assert.assertTrue(count.get() == 1);
            }
            if (cacheColumnarPlanCache && useColumnar && !useColumnarPlanCache) {
                Assert.assertTrue(plan == null);
                Assert.assertTrue(count.get() == 2);
            }
            if (cacheColumnarPlanCache && !useColumnar && useColumnarPlanCache) {
                Assert.assertTrue(
                    PlanType.containColumnar(PlannerContext.getPlannerContext(plan.getPlan()).getPlanType()));
                Assert.assertTrue(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnarPlanCache());
                Assert.assertTrue(count.get() == 1);
            }
            if (cacheColumnarPlanCache && !useColumnar && !useColumnarPlanCache) {
                Assert.assertFalse(
                    PlanType.containColumnar(PlannerContext.getPlannerContext(plan.getPlan()).getPlanType()));
                Assert.assertFalse(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnarPlanCache());
                Assert.assertTrue(count.get() == 2);
            }
            if (!cacheColumnarPlanCache && useColumnar && useColumnarPlanCache) {
                Assert.assertTrue(
                    PlanType.containColumnar(PlannerContext.getPlannerContext(plan.getPlan()).getPlanType()));
                Assert.assertTrue(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnarPlanCache());
                Assert.assertTrue(count.get() == 2);
            }
            if (!cacheColumnarPlanCache && useColumnar && !useColumnarPlanCache) {
                Assert.assertTrue(plan == null);
                Assert.assertTrue(count.get() == 1);
            }
            if (!cacheColumnarPlanCache && !useColumnar && useColumnarPlanCache) {
                Assert.assertFalse(
                    PlanType.containColumnar(PlannerContext.getPlannerContext(plan.getPlan()).getPlanType()));
                Assert.assertTrue(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnarPlanCache());
                Assert.assertTrue(count.get() == 2);
            }
            if (!cacheColumnarPlanCache && !useColumnar && !useColumnarPlanCache) {
                Assert.assertTrue(plan == null);
                Assert.assertTrue(count.get() == 1);
            }
        } finally {
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.ENABLE_COLUMNAR_PLAN_CACHE, String.valueOf(config));
        }
    }

    @Test
    public void testColumnarSwitch() throws Exception {
        OptimizerType[] optimizerTypes = new OptimizerType[] {OptimizerType.COLUMNAR, OptimizerType.MPP};
        boolean[] values = new boolean[] {true, false};
        for (OptimizerType cachedOptimizerType : optimizerTypes) {
            for (boolean enableColumnarOptimizer : values) {
                columnarSwitch(cachedOptimizerType, enableColumnarOptimizer);
            }
        }
    }

    void columnarSwitch(OptimizerType optimizerType, boolean enableColumnarOptimizer) throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        ExecutionContext executionContext = new ExecutionContext("test");
        LogicalValues va0 = Mockito.mock(LogicalValues.class);
        LogicalValues va1 = Mockito.mock(LogicalValues.class);
        try (MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class)) {
            DynamicConfig.getInstance().existColumnarNodes(true);

            ParamManager pm = Mockito.mock(ParamManager.class);
            executionContext.setParamManager(pm);
            Mockito.when(pm.getBoolean(any())).thenAnswer(
                invocation -> {
                    BooleanConfigParam param = invocation.getArgument(0);
                    if (param == ConnectionParams.ENABLE_COLUMNAR_OPTIMIZER) {
                        return enableColumnarOptimizer;
                    }
                    if (param == ConnectionParams.ENABLE_COLUMNAR_OPTIMIZER_WITH_COLUMNAR) {
                        return enableColumnarOptimizer;
                    }
                    return false;
                }
            );
            PlanCache planCache = Mockito.mock(PlanCache.class);
            Mockito.doNothing().when(planCache).invalidateByCacheKey(any());

            SqlParameterized sqlParameterized = new SqlParameterized("", Lists.newArrayList());
            Field tablesField = sqlParameterized.getClass().getDeclaredField("tables");
            tablesField.setAccessible(true);
            tablesField.set(sqlParameterized, Sets.newHashSet());

            // cached plan
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(va0)).thenAnswer(
                invocation -> {
                    PlannerContext pc = Mockito.mock(PlannerContext.class);
                    Mockito.when(pc.isColumnarOptimizer()).thenReturn(optimizerType == OptimizerType.COLUMNAR);
                    Mockito.when(pc.getOptimizerType()).thenReturn(optimizerType);
                    Mockito.when(pc.getPlanType())
                        .thenReturn(optimizerType == OptimizerType.COLUMNAR ? PlanType.COLUMNAR : PlanType.ROW);
                    Mockito.when(pc.isUseColumnarPlanCache()).thenReturn(true);
                    return pc;
                });

            // new plan
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(va1)).thenAnswer(
                invocation -> {
                    PlannerContext pc = Mockito.mock(PlannerContext.class);
                    Mockito.when(pc.getPlanType())
                        .thenReturn(enableColumnarOptimizer ? PlanType.COLUMNAR : PlanType.ROW);
                    Mockito.when(pc.isUseColumnarPlanCache()).thenReturn(true);
                    return pc;
                });
            Mockito.when(planCache.getCacheLoader(any(), any(), any(), any(), anyBoolean(), any(), any())).thenAnswer(
                (Answer<Callable<ExecutionPlan>>) invocation -> () -> {
                    ExecutionPlan plan = Mockito.mock(ExecutionPlan.class);
                    Mockito.when(plan.getPlan()).thenReturn(va1);
                    return plan;
                }
            );

            AtomicInteger count = new AtomicInteger(0);
            Mockito.when(planCache.getPlanWithLoader(any(), any(), any())).thenAnswer(
                (Answer<ExecutionPlan>) invocation -> {
                    count.getAndIncrement();
                    if (count.get() == 1) {
                        ExecutionPlan plan = Mockito.mock(ExecutionPlan.class);
                        Mockito.when(plan.getPlan()).thenReturn(va0);
                        return plan;
                    }
                    Object[] args = invocation.getArguments();
                    Callable<ExecutionPlan> loader = (Callable<ExecutionPlan>) args[1];
                    return loader.call();
                });

            Mockito.when(planCache.savePlanCachedKey(any(), any(), any(), any())).thenAnswer(
                (Answer<ExecutionPlan>) invocation -> (ExecutionPlan) invocation.getArguments()[1]);

            Mockito.when(planCache.checkColumnarPlanCache(any(), any(), any(), any(), any(), any())).thenAnswer(
                invocation -> invocation.getArgument(5));

            doCallRealMethod().when(planCache).checkColumnarDisable(any(), any(), any(), any(), any());
            doCallRealMethod().when(planCache).getFromCache(any(), any(), any(), any(), anyBoolean());
            ExecutionPlan plan = planCache.getFromCache("test", sqlParameterized, null, executionContext, false);

            // row plan cached
            if (OptimizerType.COLUMNAR != optimizerType) {
                // plan must be row
                Assert.assertFalse(
                    PlanType.containColumnar(PlannerContext.getPlannerContext(plan.getPlan()).getPlanType()));
                Assert.assertTrue(count.get() == 1);
                return;
            }

            // cacheColumnar == true
            if (enableColumnarOptimizer) {
                Assert.assertTrue(
                    PlanType.containColumnar(PlannerContext.getPlannerContext(plan.getPlan()).getPlanType()));
                Assert.assertTrue(count.get() == 1);
            } else {
                Assert.assertFalse(
                    PlanType.containColumnar(PlannerContext.getPlannerContext(plan.getPlan()).getPlanType()));
                Assert.assertTrue(count.get() == 2);
            }
        } finally {
            DynamicConfig.getInstance().existColumnarNodes(false);
        }
    }

    @Test
    public void testCacheKeyRoutingType() {
        SqlParameterized sqlParameterized = new SqlParameterized("ge", Lists.newArrayList());
        PlanCache.CacheKey cacheKey1 = new PlanCache.CacheKey("hello", sqlParameterized,
            null, null, true, true, true, RoutingType.ROW, 0L, false, false, null);
        PlanCache.CacheKey cacheKey2 = new PlanCache.CacheKey("hello", sqlParameterized,
            null, null, true, true, true, RoutingType.HTAP, 0L, false, false, null);
        PlanCache.CacheKey cacheKey3 = new PlanCache.CacheKey("hello", sqlParameterized,
            null, null, true, true, true, null, 0L, false, false, null);
        PlanCache.CacheKey cacheKey4 = new PlanCache.CacheKey("hello", sqlParameterized,
            null, null, true, true, true, null, 0L, false, false, null);
        Assert.assertTrue(cacheKey1.hashCode() != cacheKey2.hashCode());
        Assert.assertTrue(cacheKey1.hashCode() != cacheKey3.hashCode());
        Assert.assertTrue(cacheKey3.hashCode() == cacheKey4.hashCode());
    }

    /**
     * 测试通过tempID失效缓存的功能.
     */
    @Test
    public void testInvalidateByTempId() throws ExecutionException {
        long maxCapacity = sqls.length;
        PlanCache cacheInstance = new PlanCache(maxCapacity);

        int baselineId = -1;
        SqlParameterized selectedSqlParameterized = null;

        for (String currentSql : sqls) {
            ExecutionContext context = new ExecutionContext(appName);
            SqlParameterized parameterizedSql =
                SqlParameterizeUtils.parameterize(ByteString.from(currentSql), null, context, false);

            cacheInstance.get(appName, parameterizedSql, context, false);

            if (baselineId == -1) {
                baselineId = parameterizedSql.getSql().hashCode();  // 记录初始基准标识符
                selectedSqlParameterized = parameterizedSql;  // 保存选定的参数化的SQL语句
            }
        }

        assert baselineId != -1 && selectedSqlParameterized != null : "初始化失败";  // 添加断言信息

        // 测试tempID失效缓存功能
        String temporaryIdentifier = TStringUtil.int2FixedLenHexStr(baselineId);

        ExecutionContext executionContext = new ExecutionContext(appName);
        ExecutionPlan cachedPlan = cacheInstance.get(appName, selectedSqlParameterized, executionContext, false);
        assert cachedPlan != null && cachedPlan.isHitCache() : "预期命中缓存";

        cacheInstance.invalidateByTempId(appName, temporaryIdentifier);

        cachedPlan = cacheInstance.get(appName, selectedSqlParameterized, executionContext, false);
        assert cachedPlan != null && !cachedPlan.isHitCache() : "预期未命中缓存";
    }

    @Test
    public void testGetWithNullPlanReturnsNullWithoutReadingPlannerContext() throws Exception {
        ExecutionContext executionContext = new ExecutionContext(this.appName);
        SqlParameterized sqlParameterized =
            new SqlParameterized("select /*+TDDL:cmd_extra()*/ 1", Lists.newArrayList());
        ExecutionPlan executionPlan = Mockito.mock(ExecutionPlan.class);
        Mockito.when(executionPlan.getPlan()).thenReturn(null);
        PlanCache planCache = Mockito.spy(new PlanCache(1));
        Mockito.doReturn(executionPlan).when(planCache)
            .getFromCache(any(), any(), any(), any(), anyBoolean());

        try (MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class)) {
            ExecutionPlan result = planCache.get(this.appName, sqlParameterized, executionContext, false);

            Assert.assertTrue(result == null);
            plannerContextMockedStatic.verifyNoInteractions();
        }
    }

}
