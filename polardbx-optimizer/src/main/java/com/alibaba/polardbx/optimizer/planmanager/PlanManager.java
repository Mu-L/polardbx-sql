/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.external.ExternalNameValidator;
import com.alibaba.polardbx.gms.metadb.table.BaselineInfoAccessor;
import com.alibaba.polardbx.gms.metadb.table.BaselineInfoRecord;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleInfo;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.node.MPPQueryMonitor;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.SyncUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlaceHolderExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.PostPlanner;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.htaprouting.HtapTrace;
import com.alibaba.polardbx.optimizer.htaprouting.PlanType;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadType;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadUtil;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.parametric.BaseParametricQueryAdvisor;
import com.alibaba.polardbx.optimizer.planmanager.parametric.Point;
import com.alibaba.polardbx.optimizer.utils.DrdsRexFolder;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.stats.metric.FeatureStats;
import com.google.common.cache.CacheStats;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionParams.MAX_BASELINE_SYNC_BYTE_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.MAX_BASELINE_SYNC_PLAN_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.SPM_ENABLE_PQO;
import static com.alibaba.polardbx.common.properties.ConnectionParams.SPM_RECENTLY_EXECUTED_PERIOD;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogPattern.LOAD_DATA;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_BASELINE_UPDATE_ERR;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_DELETE_ERR;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_LOADING_ERR;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_PERSIST_ERR;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_PLAN_BUILD_ERR;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_PLAN_INIT_ERR;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_SERIALIZATION_ERR;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_TRY_UPDATE_PLAN_ERR;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_VIEW_INVALIDATE_ERR;
import static com.alibaba.polardbx.optimizer.planmanager.BaselineInfo.EXTEND_POINT_SET;
import static com.alibaba.polardbx.optimizer.planmanager.BaselineInfo.hotEvolution;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManager.PLAN_SOURCE.PLAN_CACHE;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManager.PLAN_SOURCE.SPM_FIX;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManager.PLAN_SOURCE.SPM_NEW_BUILD;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManager.PLAN_SOURCE.SPM_PQO;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.PlanBuildPath.BASELINE_FIX_UPDATE;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.PlanBuildPath.BASELINE_NEW_ACC;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.PlanBuildPath.BASELINE_NEW_UNACC;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.PlanBuildPath.FEEDBACK;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.getRexNodeTableMap;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.loggerSpm;
import static com.alibaba.polardbx.optimizer.view.VirtualViewType.PLAN_CACHE_CAPACITY;
import static com.alibaba.polardbx.optimizer.view.VirtualViewType.SPM;
import static com.alibaba.polardbx.stats.metric.FeatureStatsItem.HOT_EVOLVE_PLAN_NUM;
import static com.alibaba.polardbx.stats.metric.FeatureStatsItem.NEW_BASELINE_NUM;
import static org.apache.calcite.util.Litmus.IGNORE;

/**
 * @author jilong.ljl
 */
public class PlanManager extends AbstractLifecycle implements BaselineManageable, PlanManageable, ModuleInfo {

    private static final Logger logger = LoggerFactory.getLogger(PlanManager.class);

    public static final int ERROR_TABLES_HASH_CODE = -1;

    public static final double MINOR_TOLERANCE_RATIO = 0.6D;
    public static final double MAX_TOLERANCE_RATIO = 1.4D;

    // schema -> parameterizedSql -> BaselineInfo
    private Map<String, Map<String, BaselineInfo>> baselineMap = Maps.newConcurrentMap();

    public static IBaselineSyncController baselineSyncController;

    private static final PlanManager PLAN_MANAGER = new PlanManager();

    private PlanManager() {
        init();
    }

    public static PlanManager getInstance() {
        return PLAN_MANAGER;
    }

    @Override
    protected void doInit() {
        if (ConfigDataMode.isFastMock()) {
            return;
        }
        long start = System.currentTimeMillis();
        loadBaseLineInfoAndPlanInfo();
        initParametricInfo();
        ModuleLogInfo.getInstance()
            .logRecord(
                Module.SPM,
                LogPattern.START_OVER,
                new String[] {Module.SPM.name()},
                LogLevel.NORMAL);
        long end = System.currentTimeMillis();
        logger.warn("PlanManager init consuming " + (end - start) / 1000.0 + " seconds");
    }

    private void initParametricInfo() {
        for (Map<String, BaselineInfo> bMap : baselineMap.values()) {
            for (BaselineInfo baselineInfo : bMap.values()) {
                Map<String, Object> extendMap = JSON.parseObject(baselineInfo.getExtend());
                List<Map<String, Object>> rawPoint =
                    (List<Map<String, Object>>) Optional
                        .ofNullable(extendMap)
                        .map(m -> m.get(EXTEND_POINT_SET))
                        .orElse(null);
                if (rawPoint != null) {
                    Set<Point> points = PlanManagerUtil.jsonToPoints(baselineInfo.getParameterSql(), rawPoint);
                    baselineInfo.setPointSet(points);
                }
            }
        }
    }

    public ExecutionPlan choosePlanForPrepare(SqlParameterized sqlParameterized,
                                              SqlNodeList sqlNodeList,
                                              ExecutionContext executionContext) {
        ExecutionPlan plan = null;
        try {
            String schema = executionContext.getSchemaName();
            plan = PlanCache.getInstance()
                .getForPrepare(schema, sqlParameterized, executionContext, executionContext.isTestMode());
        } catch (ExecutionException e) {
            OptimizerAlertUtil.spmAlert(SPM_PLAN_BUILD_ERR, null, e);
            logger.error(e);
        }
        if (plan == null) {
            // force building one plan when the preparing query has not been executed
            plan = Planner.getInstance().doBuildPlan(sqlNodeList, executionContext, sqlParameterized,
                sqlParameterized.getForPrepare());
        }
        plan.setUsePostPlanner(false);
        return plan;
    }

    @Override
    public ExecutionPlan choosePlan(SqlParameterized sqlParameterized, ExecutionContext executionContext) {
        String schemaName = executionContext.getSchemaName();
        // plan cache
        ExecutionPlan executionPlan;
        try {
            executionPlan = PlanCache.getInstance()
                .get(schemaName, sqlParameterized, executionContext, executionContext.isTestMode());
        } catch (ExecutionException e) {
            OptimizerAlertUtil.spmAlert(SPM_PLAN_BUILD_ERR, null, e);
            logger.error(e);
            return Planner.getInstance().doBuildPlan(sqlParameterized, executionContext);
        }

        if (executionPlan == null) {
            return Planner.getInstance().doBuildPlan(sqlParameterized, executionContext);
        }

        executionContext.setPlanSource(PLAN_CACHE);

        if (!(executionPlan instanceof PlaceHolderExecutionPlan)) {
            executionPlan = executionPlan.copy(executionPlan.getPlan());
        }
        executionPlan.setUsePostPlanner(
            PostPlanner.usePostPlanner(executionContext.getExplain(),
                executionContext.isUseHint()));
        executionPlan.setExplain(executionContext.getExplain() != null);

        if (!DynamicConfig.getInstance().enableUseView() && PlannerContext.getPlannerContext(
            executionPlan.getPlan()).isHasView()) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW,
                "view is not enabled, check variable ENABLE_USE_VIEW");
        }

        if (!PlanManagerUtil.useBaseline(schemaName, executionPlan, sqlParameterized, executionContext)
            || executionContext.isTestMode()) {
            return executionPlan;
        }

        try {
            RelNode rel = doChoosePlan(executionPlan.getPlan(), executionPlan.getAst(), sqlParameterized,
                executionPlan.isExplain(), executionContext);

            final ExecutionPlan resultPlan = executionPlan.copy(rel);
            if (rel != executionPlan.getPlan()) {
                // Skip post planner for plan with pushdown hint
                final BaselineInfo baselineInfo =
                    baselineMap.get(executionContext.getSchemaName().toLowerCase(Locale.ROOT))
                        .get(sqlParameterized.getSql());
                // TODO Move this flag to the planner context to completely avoid concurrency issues.
                if (baselineInfo != null) {
                    resultPlan.setUsePostPlanner(baselineInfo.isUsePostPlanner());
                }
            }

            return resultPlan;
        } catch (Throwable e) {
            OptimizerAlertUtil.spmAlert(SPM_PLAN_BUILD_ERR, null, e);
            logger.error("Plan Management Error", e);
            return executionPlan;
        }
    }

    private RelNode doChoosePlan(RelNode plan, SqlNode ast, SqlParameterized sqlParameterized, boolean isExplain,
                                 ExecutionContext executionContext) {
        String schema = executionContext.getSchemaName();
        String parameterizedSql = sqlParameterized.getSql();
        String traceId = executionContext.getTraceId();
        if (plan == null || ast == null || parameterizedSql == null || traceId == null || StringUtils.isEmpty(schema)) {
            return plan;
        }

        schema = schema.toLowerCase(Locale.ROOT);

        final int maxBaselineInfoSqlLength = InstConfUtil.getInt(ConnectionParams.SPM_MAX_BASELINE_INFO_SQL_LENGTH);
        if (parameterizedSql.length() > maxBaselineInfoSqlLength) {
            return plan;
        }

        if (!baselineMap.containsKey(schema)) {
            baselineMap.put(schema, Maps.newConcurrentMap());
        }
        BaselineInfo baselineInfo = baselineMap.get(schema).get(parameterizedSql);

        boolean skipPostOpt = PlannerContext.getPlannerContext(plan).isSkipPostOpt();
        /*
           change plan context parameters with current sql parameters.
           so every plan deserialized from json could calculate cost with the right parameters.
         */
        PlannerContext.getPlannerContext(plan).setParams(executionContext.getParams());

        Result result;
        if (baselineInfo != null) {
            result = selectPlan(baselineInfo, plan, sqlParameterized, isExplain, executionContext);
        } else { // capture
            String planJsonString = PlanManagerUtil.relNodeToJson(plan);
            final int maxPlanInfoSqlLength = InstConfUtil.getInt(ConnectionParams.SPM_MAX_PLAN_INFO_PLAN_LENGTH);
            if (planJsonString.length() > maxPlanInfoSqlLength) {
                // If the plan is too much complex, refuse capture it.
                return plan;
            }
            result =
                capturePlan(schema, plan, sqlParameterized, ast, planJsonString, traceId, isExplain, executionContext);
        }

        if (result.source != null) {
            executionContext.setPlanSource(result.source);
        }

        if (result.plan != null && ConfigDataMode.isMasterMode() && CBOUtil.planAllColumnar(result.plan)) {
            // columnar query limiter.
            long maximumQPS = executionContext.getParamManager().getLong(ConnectionParams.COLUMNAR_CLUSTER_MAXIMUM_QPS);
            long maximumConcurrency =
                executionContext.getParamManager().getLong(ConnectionParams.COLUMNAR_CLUSTER_MAXIMUM_CONCURRENCY);

            if (maximumQPS > 0 || maximumConcurrency > 0) {
                // check concurrency of MPP query
                long concurrency = MPPQueryMonitor.getInstance().getQueryConcurrency();
                if (maximumConcurrency > 0 && concurrency > maximumConcurrency) {
                    // use logicview.
                    return plan;
                }

                long windowPeriod =
                    executionContext.getParamManager().getLong(ConnectionParams.COLUMNAR_QPS_WINDOW_PERIOD);
                double qps = MPPQueryMonitor.getInstance().calculateQPS(windowPeriod);
                if (maximumQPS > 0 && qps > maximumQPS) {
                    // use logicview.
                    return plan;
                }
            }
        }

        if (result.plan == null) {
            logger.warn("result plan is null");
            return plan;
        }

        if (result.planInfo != null) {
            if (!result.planInfo.isFixed()) {
                PlannerContext.getPlannerContext(result.plan).setSkipPostOpt(skipPostOpt);
            }
            if (result.planInfo.getOrigin() == null) {
                // for origin is null
                RelMetadataQuery mq = result.plan.getCluster().getMetadataQuery();
                synchronized (mq) {
                    PlannerContext.getPlannerContext(result.plan)
                        .setWorkloadType(WorkloadUtil.determineWorkloadType(result.plan, mq));
                }
            } else {
                PlannerContext.getPlannerContext(result.plan).setWorkloadType(
                    PlanManagerUtil.getWorkloadType(result.planInfo));
            }
        }
        if (PLAN_SOURCE.isSpm(result.source)) {
            HtapTrace.traceFromSpm(executionContext, PlannerContext.getPlannerContext(result.plan));
        }
        PlannerContext.getPlannerContext(result.plan).setPlanInfo(result.planInfo);
        PlannerContext.getPlannerContext(result.plan).setBaselineInfo(result.baselineInfo);
        if (result.planInfo != null) {
            PlannerContext.getPlannerContext(result.plan).setTtlQueryType(result.planInfo.getTtlQueryType());
        }
        return result.plan;
    }

    /**
     * make sure all plans cached were
     */
    public void invalidateCache() {
        PlanCache.getInstance().invalidate();
    }

    public void invalidateCacheBySchema(String schema) {
        PlanCache.getInstance().invalidateBySchema(schema);
    }

    public void persistBaseline() {
        String instId = ServerInstIdManager.getInstance().getInstId();
        try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(true)) {
            for (Map.Entry<String, Map<String, BaselineInfo>> e : baselineMap.entrySet()) {
                String schema = e.getKey();
                if (StringUtils.isEmpty(schema)) {
                    continue;
                }
                schema = schema.toLowerCase(Locale.ROOT);
                for (BaselineInfo baselineInfo : e.getValue().values()) {
                    baselineInfoAccessor.persist(schema, baselineInfo.buildBaselineRecord(schema, instId),
                        baselineInfo.buildPlanRecord(schema, instId), false);
                }
            }
        } catch (Exception e) {
            OptimizerAlertUtil.spmAlert(SPM_PERSIST_ERR, null, e);
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SPM,
                    UNEXPECTED,
                    new String[] {"BASELINE PERSIST", e.getMessage()},
                    CRITICAL,
                    e);
        }
    }

    @Override
    public void forceLoadAll() {
        if (InstConfUtil.getBool(ConnectionParams.ENABLE_SPM)) {
            loadBaseLineInfoAndPlanInfo();
        }
    }

    @Override
    public void feedBack(ExecutionPlan executionPlan, Throwable ex,
                         Map<RelNode, RuntimeStatisticsSketch> runtimeStatisticsMap,
                         ExecutionContext executionContext) {
        PlanCache.getInstance().feedBack(executionPlan, executionContext, ex);
        String schema = executionContext.getSchemaName();
        if (StringUtils.isEmpty(schema)) {
            return;
        }
        schema = schema.toLowerCase(Locale.ROOT);
        RelNode plan = executionPlan.getPlan();
        PlannerContext plannerContext = PlannerContext.getPlannerContext(plan);
        if (ex == null) {
            final double[] runtimeRowCount = new double[1];
            // sum the row count handled by this query
            runtimeStatisticsMap.forEach((key, value) -> {
                if (key instanceof TableScan) {
                    // only count fetched rows
                    runtimeRowCount[0] += value.getRowCount();
                }
            });

            String parameterizedSql = executionPlan.getCacheKey().getParameterizedSql();
            final int minorTole = InstConfUtil.getInt(ConnectionParams.MINOR_TOLERANCE_VALUE);
            Map<String, BaselineInfo> baselineSchemaMap = baselineMap.get(schema);
            if (baselineSchemaMap == null) {
                return;
            }
            if (plannerContext.getExpectedRowcount() == -1) {
                // record first
                plannerContext.setExpectedRowcount((long) runtimeRowCount[0]);
            } else if (plannerContext.getBaselineInfo() == null &&
                baselineSizeCheck(schema) &&
                !baselineSchemaMap.containsKey(parameterizedSql) &&
                !PlanManagerUtil.isTolerated(plannerContext.getExpectedRowcount(), runtimeRowCount[0], minorTole) &&
                PlanManagerUtil.baselineSupported(plan)) {
                // move stmt into spm from plan cache
                BaselineInfo baselineInfo =
                    createBaselineInfo(parameterizedSql, executionPlan.getAst(), executionContext);
                baselineInfo = addBaselineInfo(schema, parameterizedSql, baselineInfo);
                String planJsonString = PlanManagerUtil.relNodeToJson(plan);
                PlanInfo planInfo =
                    createPlanInfo(schema, planJsonString, plan, baselineInfo.getId(), executionContext.getTraceId(),
                        PlanManagerUtil.getPlanOrigin(plan), executionPlan.getAst(), executionContext);
                baselineInfo.addAcceptedPlan(planInfo);
                baselineSchemaMap.put(parameterizedSql, baselineInfo);
            }

            // pqo feedback
            final boolean enablePQO = InstConfUtil.getBool(SPM_ENABLE_PQO);
            if (enablePQO && parameterizedSql != null && baselineSchemaMap.containsKey(parameterizedSql)) {
                BaseParametricQueryAdvisor.getInstance()
                    .feedBack(plannerContext, baselineSchemaMap.get(parameterizedSql), plannerContext.getPlanInfo(),
                        executionContext, runtimeStatisticsMap);
            }
        }
    }

    /**
     * invalidate plan by schema
     */
    public synchronized void invalidateSchema(String schema) {
        if (StringUtils.isEmpty(schema)) {
            return;
        }
        schema = schema.toLowerCase();
        PlanCache.getInstance().invalidateBySchema(schema);

        // check and mark
        // schema -> Set [sql]
        Map<String, Set<Integer>> toRemove = Maps.newHashMap();
        for (Map.Entry<String, Map<String, BaselineInfo>> entry : baselineMap.entrySet()) {
            String currentSchema = entry.getKey().toLowerCase();
            Map<String, BaselineInfo> baselineInfoMap = entry.getValue();

            if (currentSchema.equalsIgnoreCase(schema)) {
                toRemove.put(schema,
                    baselineInfoMap.values().stream().map(BaselineInfo::getId).collect(Collectors.toSet()));
                continue;
            }
            if (baselineInfoMap == null) {
                continue;
            }
            for (Map.Entry<String, BaselineInfo> entryTmp : baselineInfoMap.entrySet()) {
                BaselineInfo baselineInfo = entryTmp.getValue();
                Set<Pair<String, String>> schemaTables = baselineInfo.getTableSet();
                if (schemaTables == null) {
                    continue;
                }
                for (Pair<String, String> pair : schemaTables) {
                    String schemaTmp = pair.getKey();
                    if (null == schemaTmp) {
                        schemaTmp = currentSchema;
                    } else {
                        schemaTmp = schemaTmp.toLowerCase();
                    }
                    if (schema.equalsIgnoreCase(schemaTmp)) {
                        Set<Integer> removeSet = toRemove.get(schema);
                        if (removeSet == null) {
                            removeSet = Sets.newHashSet();
                            toRemove.put(currentSchema, removeSet);
                        }
                        removeSet.add(baselineInfo.getId());
                    }
                }
            }
        }

        removeBaseline(toRemove, true);
    }

    /**
     * remove baseline
     *
     * @param toRemove schema-> sql set
     * @param isForce is from drop database/table
     */
    private void removeBaseline(Map<String, Set<Integer>> toRemove, boolean isForce) {
        if (toRemove == null || toRemove.isEmpty()) {
            return;
        }
        // remove
        for (Map.Entry<String, Set<Integer>> entry : toRemove.entrySet()) {
            String removeSchema = entry.getKey().toLowerCase();
            if (StringUtils.isEmpty(removeSchema)) {
                continue;
            }
            removeSchema = removeSchema.toLowerCase();
            Map<String, BaselineInfo> schemaMap = baselineMap.get(removeSchema);
            if (schemaMap == null) {
                continue;
            }
            for (Integer bid : entry.getValue()) {
                if (bid == null) {
                    continue;
                }
                if (isForce) {
                    ModuleLogInfo.getInstance().logRecord(Module.SPM, LogPattern.PROCESSING,
                        new String[] {"SPM removed baseline by force", removeSchema + "," + bid},
                        LogLevel.NORMAL);
                    this.deleteBaseline(removeSchema, bid);
                } else {
                    this.deleteBaselineUnfixed(removeSchema, bid);
                }
            }
            if (schemaMap.isEmpty()) {
                baselineMap.remove(removeSchema);
            }
        }
    }

    public synchronized void invalidateTable(String schema, String table) {
        invalidateTable(schema, table, false);
    }

    public synchronized void invalidateTable(String schema, String table, boolean isForce) {
        if (StringUtils.isEmpty(schema) || StringUtils.isEmpty(table)) {
            return;
        }
        schema = schema.toLowerCase();
        table = table.toLowerCase();
        PlanCache.getInstance().invalidateByTable(schema, table);

        // check and mark
        // schema -> Set [baseline id]
        Map<String, Set<Integer>> toRemove = Maps.newHashMap();
        for (Map.Entry<String, Map<String, BaselineInfo>> entry : baselineMap.entrySet()) {
            String currentSchema = entry.getKey().toLowerCase();
            Map<String, BaselineInfo> baselineInfoMap = entry.getValue();
            if (baselineInfoMap == null) {
                continue;
            }
            for (BaselineInfo baselineInfo : baselineInfoMap.values()) {
                Set<Pair<String, String>> schemaTables = baselineInfo.getTableSet();
                if (schemaTables == null) {
                    continue;
                }
                for (Pair<String, String> pair : schemaTables) {
                    // might be null
                    String cmpSchema = pair.getKey();
                    // should not be null
                    String cmpTable = pair.getValue().toLowerCase();

                    if (cmpSchema == null) {
                        cmpSchema = currentSchema;
                    } else {
                        cmpSchema = cmpSchema.toLowerCase();
                    }
                    if (OptimizerContext.getContext(cmpSchema) == null) {
                        continue;
                    }

                    handleView(schema, table, baselineInfo, cmpSchema, cmpTable, toRemove);

                    if (schema.equalsIgnoreCase(cmpSchema) && cmpTable.equalsIgnoreCase(table)) {
                        Set<Integer> removeSet = toRemove.get(schema);
                        if (removeSet == null) {
                            removeSet = Sets.newHashSet();
                            toRemove.put(schema, removeSet);
                        }
                        removeSet.add(baselineInfo.getId());
                    }
                }
            }
        }

        // remove
        removeBaseline(toRemove, isForce);
    }

    /**
     * Handles the reference relationship of tables in views and checks whether specific baseline information needs to be removed.
     *
     * @param targetSchema The current database schema name
     * @param targetTable The table name
     * @param baselineInfo The baseline info object
     * @param comparisonSchema The comparison database schema name
     * @param comparisonTable The comparison table name
     * @param removalCandidates A map containing sets of baseline id that need to be removed as candidates
     */
    public static void handleView(String targetSchema, String targetTable, BaselineInfo baselineInfo,
                                  String comparisonSchema, String comparisonTable,
                                  Map<String, Set<Integer>> removalCandidates) {
        try {

            // Get the view definition of the comparison table
            SystemTableView.Row viewRow = OptimizerContext.getContext(comparisonSchema)
                .getViewManager()
                .select(comparisonTable);

            if (viewRow != null) {
                String viewDefinition = viewRow.getViewDefinition();

                // Create an execution context and set parameters
                ExecutionContext executionContext = new ExecutionContext(comparisonSchema);
                executionContext.setParams(new Parameters());

                // Plan the query execution plan
                ExecutionPlan executionPlan = Planner.getInstance().plan(viewDefinition, executionContext);

                // Obtain all referenced tables
                Set<Pair<String, String>> tableSet = executionPlan.getTableSet();
                for (Pair<String, String> table : tableSet) {
                    String referencedSchema = table.getKey();
                    String referencedTableId = table.getValue();
                    if (TStringUtil.isEmpty(referencedSchema)) {
                        referencedSchema = comparisonSchema;
                    }

                    // If it references the specified table, record the baseline info for potential removal
                    if (referencedSchema.equalsIgnoreCase(targetSchema) &&
                        referencedTableId.equalsIgnoreCase(targetTable)) {
                        Set<Integer> candidateRemovalSet = removalCandidates.get(referencedSchema);

                        if (candidateRemovalSet == null) {
                            candidateRemovalSet = Sets.newHashSet();
                            removalCandidates.put(referencedSchema, candidateRemovalSet);
                        }

                        candidateRemovalSet.add(baselineInfo.getId());
                    }
                }
            }
        } catch (Exception e) {
            OptimizerAlertUtil.spmAlert(SPM_VIEW_INVALIDATE_ERR, null, e);
            // Add exception handling to ensure the program does not terminate due to unexpected issues
            loggerSpm.error("Error processing view definition: " + comparisonSchema + "," + comparisonTable, e);
        }
    }

    public static String getBaselineAsJson(Map<String, Map<String, BaselineInfo>> baselineMap) {
        try {
            JSONObject baselineMapJson = new JSONObject();
            Map<String, Map<String, String>> jsonMap = Maps.newHashMap();
            long fullLength = 0L;
            boolean simpleMode = false;
            int planUpperbound = InstConfUtil.getInt(MAX_BASELINE_SYNC_PLAN_SIZE);
            int planSize = 0;
            for (Map.Entry<String, Map<String, BaselineInfo>> e : baselineMap.entrySet()) {
                planSize += e.getValue().values().stream()
                    .mapToInt(b -> b.getAcceptedPlans().size() + b.getUnacceptedPlans().size()).sum();
            }
            if (planSize > planUpperbound) {
                simpleMode = true;
            }
            for (Map.Entry<String, Map<String, BaselineInfo>> entry : baselineMap.entrySet()) {
                String schema = entry.getKey();
                Map<String, String> sMap = Maps.newHashMap();
                jsonMap.put(schema, sMap);
                for (Map.Entry<String, BaselineInfo> e : entry.getValue().entrySet()) {
                    String sql = e.getKey();
                    BaselineInfo b = e.getValue();
                    if (b.isDirty()) {
                        continue;
                    }
                    String bStr = BaselineInfo.serializeToJson(b, simpleMode);
                    if (fullLength > InstConfUtil.getLong(MAX_BASELINE_SYNC_BYTE_SIZE)) {
                        break;
                    }
                    sMap.put(sql, bStr);
                    fullLength += bStr.length();
                }
            }
            baselineMapJson.putAll(jsonMap);
            return baselineMapJson.toJSONString();
        } catch (Throwable t) {
            OptimizerAlertUtil.spmAlert(SPM_SERIALIZATION_ERR, null, t);
            loggerSpm.error("get baseline as json error", t);
            throw t;
        }
    }

    public static String getGrayPlanAsJson(Map<String, Map<String, BaselineInfo>> baselineMap) {
        try {
            JSONObject baselineMapJson = new JSONObject();
            Map<String, Map<String, String>> jsonMap = Maps.newHashMap();
            long fullLength = 0L;
            for (Map.Entry<String, Map<String, BaselineInfo>> entry : baselineMap.entrySet()) {
                String schema = entry.getKey();
                Map<String, String> sMap = Maps.newHashMap();
                jsonMap.put(schema, sMap);
                for (Map.Entry<String, BaselineInfo> e : entry.getValue().entrySet()) {
                    String sql = e.getKey();
                    BaselineInfo b = e.getValue();
                    if (b.isDirty() || !b.grayStatus()) {
                        continue;
                    }
                    String bStr = BaselineInfo.serializeToJsonForShow(b);
                    if (fullLength > InstConfUtil.getLong(MAX_BASELINE_SYNC_BYTE_SIZE)) {
                        break;
                    }
                    sMap.put(sql, bStr);
                    fullLength += bStr.length();
                }
            }
            baselineMapJson.putAll(jsonMap);
            return baselineMapJson.toJSONString();
        } catch (Throwable t) {
            OptimizerAlertUtil.spmAlert(SPM_SERIALIZATION_ERR, null, t);
            loggerSpm.error("get baseline as json error", t);
            throw t;
        }
    }

    public static Map<String, Map<String, JSONObject>> getBaselineForShowFromJson(String json) {
        try {
            JSONObject baselineInfoJson = JSON.parseObject(json);

            Map<String, Map<String, JSONObject>> rsMap = Maps.newConcurrentMap();
            for (Map.Entry<String, Object> entry : baselineInfoJson.entrySet()) {
                String schema = entry.getKey();
                Map<String, String> v = (Map<String, String>) entry.getValue();
                if (v == null || v.size() == 0) {
                    continue;
                }
                Map<String, JSONObject> baselineInfoMap = Maps.newConcurrentMap();
                rsMap.put(schema, baselineInfoMap);
                for (Map.Entry<String, String> eb : v.entrySet()) {
                    String sql = eb.getKey();
                    String baseline = eb.getValue();
                    JSONObject bJson = JSON.parseObject(baseline);

                    baselineInfoMap.put(sql, bJson);
                }
            }
            return rsMap;
        } catch (Throwable t) {
            OptimizerAlertUtil.spmAlert(SPM_SERIALIZATION_ERR, null, t);
            loggerSpm.error("get baseline as json error", t);
            throw t;
        }
    }

    public static Map<String, Map<String, BaselineInfo>> getBaselineFromJson(String json) {
        try {
            JSONObject baselineInfoJson = JSON.parseObject(json);

            Map<String, Map<String, BaselineInfo>> rsMap = Maps.newConcurrentMap();
            for (Map.Entry<String, Object> entry : baselineInfoJson.entrySet()) {
                String schema = entry.getKey();
                Map<String, String> v = (Map<String, String>) entry.getValue();
                if (v == null || v.size() == 0) {
                    continue;
                }
                Map<String, BaselineInfo> baselineInfoMap = Maps.newConcurrentMap();
                rsMap.put(schema, baselineInfoMap);
                for (Map.Entry<String, String> eb : v.entrySet()) {
                    String sql = eb.getKey();
                    String baseline = eb.getValue();
                    BaselineInfo baselineInfo = BaselineInfo.deserializeFromJson(baseline);
                    if (baselineInfo != null) {
                        baselineInfoMap.put(sql, baselineInfo);
                    }
                }
            }
            return rsMap;
        } catch (Throwable t) {
            OptimizerAlertUtil.spmAlert(SPM_SERIALIZATION_ERR, null, t);
            loggerSpm.error("get baseline as json error", t);
            throw t;
        }
    }

    public String getFullBaselineJson() {
        return PlanManager.getBaselineAsJson(baselineMap);
    }

    public String getGrayPlan() {
        return PlanManager.getGrayPlanAsJson(baselineMap);
    }

    public Map<String, BaselineInfo> getBaselineMap(String schema) {
        if (StringUtils.isEmpty(schema)) {
            throw GeneralUtil.nestedException("empty schema name");
        }
        schema = schema.toLowerCase(Locale.ROOT);
        // TODO check if schema exists really
        Map<String, BaselineInfo> baselineInfoMap = baselineMap.get(schema);
        if (baselineInfoMap == null) {
            baselineInfoMap = Maps.newConcurrentMap();
            baselineMap.put(schema, baselineInfoMap);
        }
        return baselineInfoMap;
    }

    public Integer getBaselineIdBySql(String schema, String sql) {
        BaselineInfo baselineInfo = getBaselineBySql(schema, sql);
        return baselineInfo == null ? null : baselineInfo.getId();
    }

    @Override
    public String state() {
        return ModuleInfo.buildStateByArgs(
            ConnectionParams.ENABLE_SPM,
            ConnectionParams.SPM_ENABLE_PQO,
            ConnectionParams.ENABLE_SPM_EVOLUTION_BY_TIME,
            ConnectionParams.ENABLE_SPM_BACKGROUND_TASK
        );
    }

    @Override
    public String status(long since) {
        return "";
    }

    @Override
    public String resources() {
        StringBuilder s = new StringBuilder();
        s.append("plan cache size:" + PlanCache.getInstance().getCacheKeyCount() + ";");
        for (Map.Entry<String, Map<String, BaselineInfo>> e : baselineMap.entrySet()) {
            String schema = e.getKey();
            s.append(schema + " baseline size:" + e.getValue().size() + ";");
        }

        return s.toString();
    }

    @Override
    public String scheduleJobs() {
        if (!LeaderStatusBridge.getInstance().hasLeadership()) {
            return "";
        }
        // schedule job resources
        return baselineSyncController.scheduledJobsInfo();
    }

    @Override
    public String workload() {
        CacheStats cs = PlanCache.getInstance().getCache().stats();
        return "plan cache workload:" + cs.toString();
    }

    @Override
    public String views() {
        return SPM + "," + PLAN_CACHE + "," + PLAN_CACHE_CAPACITY;
    }

    public static class Result {
        public final BaselineInfo baselineInfo;
        public final PlanInfo planInfo;
        public final RelNode plan;
        public PLAN_SOURCE source;

        public Result(BaselineInfo baselineInfo, PlanInfo planInfo, RelNode plan, PLAN_SOURCE source) {
            this.baselineInfo = baselineInfo;
            this.planInfo = planInfo;
            this.plan = plan;
            this.source = source;
        }
    }

    public boolean baselineSizeCheck(String schema) {
        if (StringUtils.isEmpty(schema)) {
            return false;
        }
        schema = schema.toLowerCase(Locale.ROOT);
        Map<String, BaselineInfo> baselineInfoMap = baselineMap.get(schema);

        if (baselineInfoMap != null) {
            final int maxBaselineSize = InstConfUtil.getInt(ConnectionParams.SPM_MAX_BASELINE_SIZE);

            return baselineInfoMap.size() < maxBaselineSize;
        }
        return true;
    }

    private Result capturePlan(String schema,
                               RelNode plan,
                               SqlParameterized parameterizedSql,
                               SqlNode ast,
                               String planJsonString,
                               String traceId,
                               boolean isExplain,
                               ExecutionContext executionContext) {
        // only master can capture
        if (ConfigDataMode.isMasterMode() &&
            baselineSizeCheck(schema) &&
            !isExplain &&
            isRepeatableSql(schema, parameterizedSql, executionContext) &&
            PlanManagerUtil.baselineSupported(plan) &&
            !PlanManagerUtil.containsExternalTable(plan)) {
            final Set<Pair<String, String>> schemaTables = PlanManagerUtil.getTableSetFromAst(ast);
            int tablesVersion = PlanManagerUtil.computeTablesVersion(schemaTables, schema,
                PlannerContext.getPlannerContext(plan).getExecutionContext());
            if (tablesVersion != ERROR_TABLES_HASH_CODE) {
                FeatureStats.getInstance().increment(NEW_BASELINE_NUM);
                BaselineInfo baselineInfo = new BaselineInfo(parameterizedSql.getSql(), schemaTables);
                PlanInfo resultPlanInfo = new PlanInfo(planJsonString, baselineInfo.getId(),
                    simpleCostValue(plan), traceId, PlanManagerUtil.getPlanOrigin(plan), tablesVersion,
                    PlannerContext.getPlannerContext(plan).getTtlQueryType());
                baselineInfo.addAcceptedPlan(resultPlanInfo);
                // we don't add baseline to baselineMap immediately until finishing its execution
                return new Result(baselineInfo, resultPlanInfo, plan, SPM_NEW_BUILD);
            }
        }
        return new Result(null, null, plan, null);
    }

    protected Result selectPlan(BaselineInfo baselineInfo,
                                RelNode plan,
                                SqlParameterized sqlParameterized,
                                boolean isExplain, ExecutionContext ec) {
        String schema = ec.getSchemaName();
        Map<Integer, PlanInfo> acceptedPlans = baselineInfo.getAcceptedPlans();
        Map<Integer, PlanInfo> unacceptedPlans = baselineInfo.getUnacceptedPlans();

        assert !acceptedPlans.isEmpty();

        PlanInfo resultPlanInfo;
        RelNode resultPlan;

        /*
         * Generate plan with rebuildAtLoad flag
         */
        int cHashCode = PlanManagerUtil.computeTablesVersion(baselineInfo.getTableSet(), schema, ec);
        if (baselineInfo.isRebuildAtLoad()) {
            return handleRebuildAtLoadPlan(baselineInfo, plan, sqlParameterized, ec, cHashCode);
        }

        /*
          try fixed plan first
         */
        Collection<PlanInfo> fixPlans = baselineInfo.getFixPlans();
        if (fixPlans != null && !fixPlans.isEmpty()) {
            PlanInfo fixPlan = findMinCostPlan(fixPlans, ec, null, cHashCode, false, true);
            if (fixPlan != null) {
                PLAN_SOURCE planSource = SPM_FIX;
                if (fixPlan.isInGrayStatus()) {
                    planSource = PLAN_SOURCE.SPM_FIX_GRAY;
                    ec.setGrayWorkload(true);
                }

                if (fixPlan.getTablesHashCode() != cHashCode &&
                    !StringUtils.isEmpty(fixPlan.getFixHint())) {
                    // in case of repeated updates
                    synchronized (fixPlan) {
                        if (fixPlan.getTablesHashCode() != cHashCode) {
                            planSource =
                                tryUpdatePlan(baselineInfo, fixPlan, sqlParameterized, cHashCode,
                                    ec);
                        }
                    }
                }
                resultPlan = fixPlan.getPlan(schema, ec);
                fixPlan.prepareExecutorArgs(ec);
                return new Result(baselineInfo, fixPlan, resultPlan, planSource);
            }
        }

        /*
           use CBO to find best plan from baseline accepted plans
         */
        Point point;
        PlannerContext plannerContext = PlannerContext.getPlannerContext(plan);
        if (plannerContext.getExprMap() == null && InstConfUtil.getBool(SPM_ENABLE_PQO)) {
            plannerContext.setExprMap(getRexNodeTableMap(plan));
        }
        final int maxPqoParamsSize = InstConfUtil.getInt(ConnectionParams.SPM_MAX_PQO_PARAMS_SIZE);
        final boolean enablePQO = InstConfUtil.getBool(ConnectionParams.SPM_ENABLE_PQO);
        final boolean enableEvo = InstConfUtil.getBool(ConnectionParams.ENABLE_SPM_EVOLUTION_BY_TIME);
        if (!enableEvo &&
            enablePQO &&
            plannerContext.getExprMap() != null && // 参数推导模块已推导出有效的表达式-> tbl 映射
            plannerContext.getExprMap().size() > 0 &&
            sqlParameterized.getParameters().size() < maxPqoParamsSize) {// stmt 参数的数量小于 pqo 模块设定参数的最大值
            // 进入 PQO
            Pair<Point, PlanInfo> planInfoPair = BaseParametricQueryAdvisor.getInstance()
                .advise(sqlParameterized.getSql(), acceptedPlans.values(), sqlParameterized.getParameters(),
                    baselineInfo.getPointSet(), plannerContext, cHashCode);
            point = planInfoPair.getKey();
            ec.setPoint(point);
            if (planInfoPair.getValue() != null) {
                return new Result(baselineInfo, planInfoPair.getValue(),
                    planInfoPair.getValue().getPlan(schema, ec), SPM_PQO);
            } else {
                if (isExplain) {
                    return new Result(baselineInfo, null, plan, PLAN_CACHE);
                }
                // 为该参数空间生成一个新的 plan
                Result result = buildNewPlan(baselineInfo, sqlParameterized, ec, cHashCode);
                point.setPlanId(result.planInfo.getId());
                return result;
            }
        } else {
            // only leader can try unaccepted plan
            if (unacceptedPlans.size() > 0 &&
                LeaderStatusBridge.getInstance().hasLeadership() &&
                !isExplain && enableEvo) {
                // use unaccepted plan first if enableEvo
                PlanInfo unacceptedPlan =
                    findMinCostPlan(unacceptedPlans.values(), ec, null, cHashCode, false, false);
                if (unacceptedPlan != null) {
                    return new Result(baselineInfo, unacceptedPlan, unacceptedPlan.getPlan(schema, ec),
                        PLAN_SOURCE.SPM_UNACCEPTED);
                }
            }

            // find best plan from baseline accepted plans
            resultPlanInfo = findMinCostPlan(acceptedPlans.values(), ec, null, cHashCode,
                baselineInfo.isHotEvolution(), false);
            if (resultPlanInfo != null) {
                // min cost plan
                return new Result(baselineInfo, resultPlanInfo, resultPlanInfo.getPlan(schema, ec),
                    PLAN_SOURCE.SPM_ACCEPT);
            } else {
                if (isExplain) {
                    return new Result(baselineInfo, null, plan, PLAN_CACHE);
                }
                // new plan
                return buildNewPlan(baselineInfo, sqlParameterized, ec, cHashCode);
            }
        }
    }

    @NotNull
    public Result handleRebuildAtLoadPlan(BaselineInfo baselineInfo,
                                          RelNode plan,
                                          SqlParameterized sqlParameterized,
                                          ExecutionContext ec,
                                          int cHashCode) {
        PlanInfo resultPlanInfo;
        RelNode resultPlan;
        Supplier<PlanInfo> supplier = () -> {
            final String hint = Optional.ofNullable(baselineInfo.getHint()).orElse("");
            final RelNode retPlan = Planner.getInstance()
                .plan(hint + " " + sqlParameterized.getSql(), ec)
                .getPlan();
            int tablesVersion =
                PlanManagerUtil.computeTablesVersion(baselineInfo.getTableSet(), ec.getSchemaName(), ec);

            return new PlanInfo(retPlan, baselineInfo.getId(), simpleCostValue(retPlan), ec.getTraceId(),
                PlanManagerUtil.getPlanOrigin(plan), tablesVersion);
        };
        resultPlanInfo = baselineInfo.computeRebuiltAtLoadPlanIfNotExists(supplier);

        PLAN_SOURCE planSource = SPM_FIX;
        if (resultPlanInfo.getTablesHashCode() != cHashCode) {
            // in case of repeated updates
            synchronized (baselineInfo.getRebuildAtLoadPlan()) {
                baselineInfo.resetRebuildAtLoadPlanIfMismatched(cHashCode);
                resultPlanInfo = baselineInfo.computeRebuiltAtLoadPlanIfNotExists(supplier);
            }
        }

        resultPlan = resultPlanInfo.getPlan(ec.getSchemaName(), ec);

        return new Result(baselineInfo, resultPlanInfo, resultPlan, planSource);
    }

    /**
     * update fixed plan by record hint
     */
    public static PLAN_SOURCE tryUpdatePlan(BaselineInfo baselineInfo,
                                            PlanInfo planInfo,
                                            SqlParameterized sqlParameterized,
                                            int currentHashCode,
                                            ExecutionContext executionContext) {
        try {
            RelNode oldPlan = planInfo.getPlan(executionContext.getSchemaName(), executionContext);
            ExecutionContext executionContextCopy = executionContext.copy();
            // Change context:
            // - Before: executionContext.copy() carries over executingPreparedStmt/preparedStmtCache from the
            //   original request. When the original request came from a server-side PreparedStatement
            //   (COM_STMT_EXECUTE), Planner.plan() detected isExecutingPreparedStmt()==true and reused the
            //   cached SqlParameterized/AST built during COM_STMT_PREPARE, which never contains the fix hint,
            //   so fix hint SQL passed below was never re-parsed and the local index hint was silently dropped.
            // - Path impact: clearing it forces this rebuild to go through the normal parse path
            //   (planAfterProcessing) so the fix hint is applied; execution of the original prepared statement
            //   itself is unaffected since only the copy is modified.
            // - Capability regression: None; rebuild always re-parses fixHint + sql regardless of protocol.
            executionContextCopy.clearPreparedStmt();
            executionContextCopy.setUseHint(true);
            ExecutionPlan newPlan =
                Planner.getInstance()
                    .plan(planInfo.getFixHint() + " " + sqlParameterized.getSql(), executionContextCopy);
            PlanManagerUtil.logPlanBuild(BASELINE_FIX_UPDATE, baselineInfo.getId(), executionContext,
                newPlan.getPlan());// if row type changed(select * xxx), meaning plan should be rebuilt
        RelDataType oldRowType = oldPlan.getRowType();
        RelDataType newRowType = newPlan.getPlan().getRowType();

        // plan needed to be rebuild in two cases:
        // 1: row type changed
        // 2: old plan is not valid anymore
        if (!oldRowType.getFullTypeString().equalsIgnoreCase(newRowType.getFullTypeString())) {
            planInfo.resetPlan(newPlan.getPlan(), executionContext);
                planInfo.setTablesHashCode(currentHashCode);
                loggerSpm.warn(
                    "fix plan being rebuilt for row type change, fix hint:" + planInfo.getFixHint()
                        + ", stmt:" + sqlParameterized.getSql()
                        + ", old row type:" + oldRowType.getFullTypeString()
                        + ", new row type:" + newRowType.getFullTypeString()
                        + ", bid=" + planInfo.getBaselineId()
                        + ", pid=" + planInfo.getId()
                );
                return PLAN_SOURCE.SPM_FIX_PLAN_UPDATE_FOR_ROW_TYPE;
            } else if (!oldPlan.isValid(IGNORE, null)) {
                planInfo.resetPlan(newPlan.getPlan(), executionContext);
                planInfo.setTablesHashCode(currentHashCode);
                loggerSpm.warn(
                    "fix plan being rebuilt for plan valid check, fix hint:" + planInfo.getFixHint()
                        + ", stmt:" + sqlParameterized.getSql()
                        + ", old plan:" + oldPlan.getDigest()
                        + ", bid=" + planInfo.getBaselineId()
                        + ", pid=" + planInfo.getId()
                );
                return PLAN_SOURCE.SPM_FIX_PLAN_UPDATE_FOR_INVALID;
            } else {
                planInfo.resetPlan(newPlan.getPlan(), executionContext);
                planInfo.setTablesHashCode(currentHashCode);
                loggerSpm.warn("fix plan reset ddl hashcode, fix hint:" + planInfo.getFixHint()
                    + ", stmt:" + sqlParameterized.getSql()
                    + ", tables:" + newPlan.getTableSet()
                    + ", bid=" + planInfo.getBaselineId()
                    + ", pid=" + planInfo.getId()
                );
                return PLAN_SOURCE.SPM_FIX_DDL_HASHCODE_UPDATE;
            }
        } catch (Throwable t) {
            OptimizerAlertUtil.spmAlert(SPM_TRY_UPDATE_PLAN_ERR, null, t);
            loggerSpm.error("tryUpdatePlan error", t);
            throw t;
        }
    }

    public Result buildNewPlan(BaselineInfo baselineInfo,
                               SqlParameterized sqlParameterized,
                               ExecutionContext executionContext,
                               int currentHashCode) {
        try {
            ExecutionPlan planWithContext = Planner.getInstance().doBuildPlan(sqlParameterized, executionContext);
            RelNode p = planWithContext.getPlan();
            if (PlanManagerUtil.containsExternalTable(p)) {
                throw new TddlRuntimeException(ErrorCode.ERR_BASELINE, "SPM is not allowed on external catalog schema");
            }
            PlanInfo resultPlanInfo =
                new PlanInfo(PlanManagerUtil.relNodeToJson(p), baselineInfo.getId(), simpleCostValue(p),
                    executionContext.getTraceId()
                    , PlanManagerUtil.getPlanOrigin(p), currentHashCode, executionContext.getTtlQueryType());
            resultPlanInfo.resetPlan(p, executionContext);
            baselineInfo.getAcceptedPlans().put(resultPlanInfo.getId(), resultPlanInfo);

            PlanManagerUtil.logPlanBuild(BASELINE_NEW_ACC, baselineInfo.getId(), executionContext, p);
            return new Result(baselineInfo, resultPlanInfo,
                resultPlanInfo.getPlan(executionContext.getSchemaName(), executionContext),
                SPM_NEW_BUILD);
        } catch (Throwable t) {
            OptimizerAlertUtil.spmAlert(SPM_PLAN_BUILD_ERR, null, t);
            loggerSpm.error("buildNewPlan error", t);
            throw t;
        }
    }

    private void buildNewPlanToUnacceptedPlan(BaselineInfo baselineInfo, ExecutionContext executionContext) {
        if (baselineInfo.isRebuildAtLoad()) {
            return;
        }
        // plan num exceed upper bound
        if (baselineInfo.getUnacceptedPlans().size() >= InstConfUtil.getInt(
            ConnectionParams.SPM_MAX_UNACCEPTED_PLAN_SIZE_PER_BASELINE)) {
            return;
        }

        // any fix plan
        if (baselineInfo.getFixPlans().size() > 0) {
            return;
        }

        logger.info("build new plan to unaccepted plan cluster, sql:" + baselineInfo.getParameterSql());
        SqlParameterized sqlParameterized =
            new SqlParameterized(baselineInfo.getParameterSql(),
                executionContext.getParams().getCurrentParameter());
        ExecutionPlan planWithContext = Planner.getInstance().doBuildPlan(sqlParameterized, executionContext);
        RelNode plan = planWithContext.getPlan();
        if (PlanManagerUtil.containsExternalTable(plan)) {
            throw new TddlRuntimeException(ErrorCode.ERR_BASELINE,
                "SPM is not allowed on external catalog schema");
        }
        int tablesVersion = PlanManagerUtil
            .computeTablesVersion(baselineInfo.getTableSet(), executionContext.getSchemaName(), executionContext);
        PlanInfo resultPlanInfo =
            new PlanInfo(plan, baselineInfo.getId(), simpleCostValue(plan),
                executionContext.getTraceId()
                , PlanManagerUtil.getPlanOrigin(plan), tablesVersion);
        baselineInfo.getUnacceptedPlans().put(resultPlanInfo.getId(), resultPlanInfo);
        PlanManagerUtil.logPlanBuild(BASELINE_NEW_UNACC, baselineInfo.getId(), executionContext, plan);
    }

    /**
     * judge if a plan executed recently.(default one week)
     */
    public static boolean isRecentlyExecuted(PlanInfo planInfo) {
        if (planInfo.getLastExecuteTime() == null) {
            return false;
        }
        long recentTimePeriod = InstConfUtil.getLong(SPM_RECENTLY_EXECUTED_PERIOD);

        return System.currentTimeMillis() - planInfo.getLastExecuteTime() * 1000 <= recentTimePeriod;
    }

    /**
     * check whether the plan can be chosen
     *
     * @param node root of plan
     * @param ec executionContext
     * @param planInfo plan info of current plan
     * @return false if it's a columnar plan and can't be chosen
     */
    public boolean canChooseColumnarPlan(RelNode node, ExecutionContext ec, PlanInfo planInfo) {
        if (ec.getRoutingType() != null) {
            // columnar plan can't be chosen in trans, except on columnar readonly instance
            if (PlanType.containColumnar(PlannerContext.getPlannerContext(node).getPlanType())
                && !ec.isAutoCommit() && !ConfigDataMode.isColumnarMode()) {
                return false;
            }
            if (planInfo.isFixed()) {
                return true;
            }
            return (PlanType.compatibleWithRoutingType(PlannerContext.getPlannerContext(node).getPlanType(),
                ec.getRoutingType()));
        }

        // not columnar plan
        if (!PlanType.containColumnar(PlannerContext.getPlannerContext(node).getPlanType())) {
            return true;
        }
        // not auto commit, columnar plan can't be chosen, except on columnar readonly instance
        if (!ec.isAutoCommit() && !ConfigDataMode.isColumnarMode()) {
            return false;
        }
        // fixed plan can be chosen
        if (planInfo.isFixed()) {
            return true;
        }
        return OptimizerUtils.enableColumnarOptimizer(ec.getParamManager());
    }

    public boolean canChooseTtlQueryPlan(RelNode node, ExecutionContext ec, PlanInfo planInfo) {
        if (planInfo.getTtlQueryType() != null) {
            if (ec.getTtlQueryType() != null) {
                return ec.getTtlQueryType() == planInfo.getTtlQueryType() && planInfo.isFixed();
            } else {
                return planInfo.isFixed();
            }
        }
        return true;
    }

    public PlanInfo findMinCostPlan(Collection<PlanInfo> plans, ExecutionContext executionContext,
                                    Collection<Integer> toBeRemovedPlan, int tablesHashCode, boolean hotEvolution,
                                    boolean fixPath) {
        String schema = executionContext.getSchemaName();
        if (plans.size() == 0) {
            return null;
        } else if (plans.size() == 1) {
            PlanInfo planInfo = plans.iterator().next();
            if (planInfo.isFixed() && !fixPath) {
                return null;
            }
            if (fixPath && !planInfo.isGrayWorkload()) {
                if (planInfo.isInGrayStatus()) {
                    executionContext.setGrayWorkload(true);
                }
                return null;
            }
            if (planInfo != null && (planInfo.isFixed() || planInfo.getTablesHashCode() == tablesHashCode)) {
                RelNode rel = planInfo.getPlan(schema, executionContext);
                if (!canChooseColumnarPlan(rel, executionContext, planInfo) || !exprCheck(planInfo, executionContext)
                    || !canChooseTtlQueryPlan(rel, executionContext, planInfo)) {
                    return null;
                }
                return planInfo;
            } else {
                return null;
            }
        }
        RelOptCost bestCost = null;
        PlanInfo basePlan = null;
        int chosenPlanCount = 0;
        for (PlanInfo planInfo : plans) {
            if (planInfo.isFixed() && !fixPath) {
                continue;
            }
            if (!planInfo.isFixed() && planInfo.getTablesHashCode() != tablesHashCode) {
                continue;
            }
            if (fixPath && !planInfo.isGrayWorkload()) {
                if (planInfo.isInGrayStatus()) {
                    executionContext.setGrayWorkload(true);
                }
                continue;
            }
            try {
                // getPlan must befor exprCheck
                RelNode rel = planInfo.getPlan(schema, executionContext);
                if (!exprCheck(planInfo, executionContext)) {
                    continue;
                }
                if (!canChooseColumnarPlan(rel, executionContext, planInfo)) {
                    continue;
                }
                if (!canChooseTtlQueryPlan(rel, executionContext, planInfo)) {
                    continue;
                }

            } catch (Throwable e) {
                OptimizerAlertUtil.spmAlert(SPM_PLAN_INIT_ERR, null, e);
                loggerSpm.error("Plan Management Error", e);
                if (toBeRemovedPlan != null) {
                    toBeRemovedPlan.add(planInfo.getId());
                }
                continue;
            }

            // delay calculating acceptedPlanInfo cost
            if (bestCost == null) {
                bestCost = planInfo.getCumulativeCost(executionContext);
                basePlan = planInfo;
                continue;
            }
            RelOptCost cost = planInfo.getCumulativeCost(executionContext);
            if (cost.isLt(bestCost)) {
                bestCost = cost;
                basePlan = planInfo;
            }

            if (hotEvolution && planInfo.getChooseCount() > 1) {
                chosenPlanCount++;
            }
        }
        // more than one plan was chosen
        if (chosenPlanCount > 1) {
            FeatureStats.getInstance().increment(HOT_EVOLVE_PLAN_NUM);
        }

        return basePlan;
    }

    private boolean exprCheck(PlanInfo planInfo, ExecutionContext executionContext) {
        if (planInfo.getExpr() == null) {
            return true;
        }
        return (Long) DrdsRexFolder.fold(planInfo.getExpr(), executionContext) == 1L;
    }

    @Override
    public void doEvolution(String schema, BaselineInfo baselineInfo, PlanInfo planInfo, long lastExecuteUnixTime,
                            double executeTimeMs, ExecutionContext ec, Throwable ex) {
        if (StringUtils.isEmpty(schema) || ec.getExplain() != null) {
            return;
        }
        schema = schema.toLowerCase(Locale.ROOT);
        Map<String, BaselineInfo> baselineInfoMap = baselineMap.get(schema);
        if (ex != null) {
            // something error, maybe: user kill the sql or plan externalization is not compatible or bug
            int errorCount = planInfo.incrementAndGetErrorCount();
            loggerSpm.error("plan Management error : planInfo execute error, " +
                "BaselineInfoId = " + baselineInfo.getId() + ", planInfoId = " + planInfo.getId() +
                ",errorCount = " + errorCount + ",executeTimeMs = " + executeTimeMs, ex);
            final int maxPlanInfoErrorCount = InstConfUtil.getInt(ConnectionParams.SPM_MAX_PLAN_INFO_ERROR_COUNT);
            if (errorCount >= maxPlanInfoErrorCount) {
                // mark fixed plan need updated instead of delete it
                if (planInfo.isFixed()) {
                    planInfo.setTablesHashCode(PlanInfo.REBUILD_PLAN_HASH_CODE);
                    return;
                }
                // planInfo errorCount larger than SPM_MAX_PLAN_INFO_ERROR_COUNT
                // if planInfo is the only acceptedPlan. notifyDeleteBaseline
                baselineInfo.removeUnacceptedPlan(planInfo.getId());
                if (baselineInfoMap != null &&
                    baselineInfo.getAcceptedPlans().containsKey(planInfo.getId()) &&
                    baselineInfo.getAcceptedPlans().size() == 1) {
                    baselineInfoMap.remove(baselineInfo.getParameterSql());
                } else {
                    baselineInfo.removeAcceptedPlan(planInfo.getId());
                }
            }
            return;
        }
        baselineInfo = addBaselineInfo(schema, baselineInfo.getParameterSql(), baselineInfo);
        if (baselineInfo == null) {
            return;
        }
        /*
          try to build new plan if Estimate time diffed too much (1 second)
         */
        boolean enableEvoByTime = InstConfUtil.getBool(ConnectionParams.ENABLE_SPM_EVOLUTION_BY_TIME);
        int dt = InstConfUtil.getInt(ConnectionParams.SPM_DIFF_ESTIMATE_TIME);
        if (enableEvoByTime &&
            LeaderStatusBridge.getInstance().hasLeadership() &&
            (executeTimeMs / 1e3) - planInfo.getEstimateExecutionTime() > dt / 1000D) {
            buildNewPlanToUnacceptedPlan(baselineInfo, ec);
        }

        /*
          try change plan from unaccepted plan to accepted plan
         */
        planInfo.addChooseCount();
        planInfo.setLastExecuteTime(lastExecuteUnixTime);
        planInfo.updateEstimateExecutionTime(executeTimeMs);
        Map<Integer, PlanInfo> acceptedPlans = baselineInfo.getAcceptedPlans();
        Map<Integer, PlanInfo> unacceptedPlans = baselineInfo.getUnacceptedPlans();
        final int maxAcceptedPlanSize =
            InstConfUtil.getInt(ConnectionParams.SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE);
        if (unacceptedPlans.containsKey(planInfo.getId())) {
            for (PlanInfo acceptedPlanInfo : acceptedPlans.values()) {
                if (planInfo.getEstimateExecutionTime() < acceptedPlanInfo.getEstimateExecutionTime()) {
                    /* evolution succeed */
                    baselineInfo.removeUnacceptedPlan(planInfo.getId());
                    if (acceptedPlans.size() >= maxAcceptedPlanSize) {
                        baselineInfo.removeAcceptedPlan(acceptedPlanInfo.getId());
                    }
                    baselineInfo.addAcceptedPlan(planInfo);
                    LoggerUtil.logSpm(schema, "plan evolution:" + planInfo.getId());
                    break;
                }
            }
        }
    }

    /**
     * add baselineInfo to baselineMap
     *
     * @param schema schema of sql
     * @param parameterSql parameterized sql
     * @param baselineInfo baselineInfo to be added
     * @return baselineInfo of parameterized sql in baselineMap
     */
    @Override
    public BaselineInfo addBaselineInfo(String schema, String parameterSql, BaselineInfo baselineInfo) {
        if (SystemDbHelper.isDBBuildIn(schema)) {
            return null;
        }
        if (ExternalNameValidator.isExternalSchema(schema)) {
            return null;
        }
        if (StringUtils.isEmpty(schema)) {
            throw GeneralUtil.nestedException("empty schema name");
        }
        if (baselineInfo.isDirty()) {
            return null;
        }
        schema = schema.toLowerCase(Locale.ROOT);
        if (!baselineSizeCheck(schema)) {
            return null;
        }
        Map<String, BaselineInfo> baselineInfoMap =
            baselineMap.computeIfAbsent(schema, k -> Maps.newConcurrentMap());
        baselineInfoMap.putIfAbsent(parameterSql, baselineInfo);
        return baselineInfoMap.get(parameterSql);
    }

    private boolean isRepeatableSql(String schema, SqlParameterized parameterizedSql, ExecutionContext ec) {
        PlanCache.CacheKey cacheKey = PlanCache.getCacheKey(schema, parameterizedSql, ec, false);
        return PlanCache.getInstance().getCache().getIfPresent(cacheKey) != null;
    }

    /**
     * only callback by baselineSyncController
     */
    @Override
    public void updateBaseline(Map<String, List<String>> bMap) {
        String instId = ServerInstIdManager.getInstance().getInstId();
        try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(true)) {
            for (Map.Entry<String, List<String>> entry : bMap.entrySet()) {
                String schema = entry.getKey();

                if (StringUtils.isEmpty(schema)) {
                    continue;
                }
                schema = schema.toLowerCase(Locale.ROOT);
                Map<String, BaselineInfo> baselineInfoMap =
                    baselineMap.computeIfAbsent(schema, k -> Maps.newConcurrentMap());
                for (String bJson : entry.getValue()) {
                    BaselineInfo b = BaselineInfo.deserializeFromJson(bJson);
                    if (b.isDirty()) {
                        continue;
                    }
                    // persist
                    if (SyncUtil.isNodeWithSmallestId()) {
                        // persist baseline
                        baselineInfoAccessor.persist(schema, b.buildBaselineRecord(schema, instId),
                            b.buildPlanRecord(schema, instId), false);
                    }

                    checkVersionAndAddBaseline(PlannerUtils.OPTIMIZER_VERSION, b, baselineInfoMap);
                }
            }
        } catch (Exception e) {
            OptimizerAlertUtil.spmAlert(SPM_BASELINE_UPDATE_ERR, null, e);
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SPM,
                    UNEXPECTED,
                    new String[] {"BASELINE SYNC", e.getMessage()},
                    CRITICAL,
                    e);
        }

    }

    /**
     * Checks if the plan information versions are compatible with the current version and adds the baseline information to the map.
     *
     * @param optimizerVersion The current version number.
     * @param b The baseline information object.
     * @param baselineInfoMap The map that stores baseline information.
     */
    public static void checkVersionAndAddBaseline(int optimizerVersion,
                                                  BaselineInfo b,
                                                  Map<String, BaselineInfo> baselineInfoMap) {
        if (b == null || b.isDirty()) {
            return;
        }
        // check if plan info version compatible
        List<Integer> planInfoIdsToIgnore = new ArrayList<>();

        // Iterate over all accepted plans.
        for (PlanInfo planInfo : b.getAcceptedPlans().values()) {

            // If the current version is not compatible with the plan version, add its ID to the ignore list.
            if (!PlanManagerUtil.isVersionCompatible(optimizerVersion, planInfo.getVersion())) {
                planInfoIdsToIgnore.add(planInfo.getId());
            }
        }
        planInfoIdsToIgnore.forEach(b::removeAcceptedPlan);

        // If there are still compatible plans left, add the baseline information to the map.
        if (b.getAcceptedPlans().size() > 0 || b.isRebuildAtLoad()) {
            baselineInfoMap.put(b.getParameterSql(), b);
        }
    }

    public void notifyUpdatePlanSync(
        ExecutionPlan originPlan,
        Integer bid,
        WorkloadType feedBackWorkload,
        ExecutionContext newExecutionContext) {
        if (InstConfUtil.getBool(ConnectionParams.ENABLE_SPM)) {
            try {
                String schema = newExecutionContext.getSchemaName();
                newExecutionContext.setWorkloadType(null);
                newExecutionContext.setEnableFeedBackWorkload(true);

                Map<String, Object> extraCmd = new HashMap<>(newExecutionContext.getExtraCmds());
                extraCmd.put(ConnectionProperties.WORKLOAD_TYPE, feedBackWorkload.name());
                newExecutionContext.setExtraCmds(extraCmd);
                ExecutionPlan targetExecutionPlan =
                    Planner.getInstance().plan(newExecutionContext.getOriginSql(), newExecutionContext);

                if (targetExecutionPlan != null) {
                    RelNode targetPlan = targetExecutionPlan.getPlan();
                    if (PlanManagerUtil.containsExternalTable(targetPlan)) {
                        // Defensive guard: caller (LogicalBaselineHandler) rejects external tables up front.
                        loggerSpm.warn("skip plan feedback: SPM is not allowed on external catalog schema");
                        return;
                    }
                    PlannerContext targetContext = PlannerContext.getPlannerContext(targetPlan);
                    if (feedBackWorkload == targetContext.getWorkloadType()) {
                        if (originPlan.getCacheKey() != null) {
                            targetExecutionPlan.saveCacheState(
                                originPlan.getTableSet(), originPlan.getTableSetHashCode(),
                                originPlan.getCacheKey(), originPlan.getTableMetaSnapshots());
                            targetExecutionPlan
                                .setPrivilegeVerifyItems(originPlan.getPrivilegeVerifyItems());
                            PlanCache.getInstance().putCachePlan(originPlan.getCacheKey(), targetExecutionPlan);
                        }
                        if (bid != null) {
                            SqlNode ast = targetExecutionPlan.getAst();
                            String planJsonString = PlanManagerUtil.relNodeToJson(targetPlan);
                            PlanInfo planInfo =
                                createPlanInfo(schema,
                                    planJsonString, targetPlan, bid,
                                    newExecutionContext.getTraceId(),
                                    PlanManagerUtil.getPlanOrigin(targetPlan),
                                    ast, newExecutionContext);
                            planInfo.setAccepted(true);
                            BaselineInfo baselineInfo = getBaselineById(schema, bid);
                            if (baselineInfo != null) {
                                baselineSyncController.updateBaselineSync(schema, baselineInfo);
                            }
                            PlanManagerUtil.logPlanBuild(FEEDBACK, bid, newExecutionContext, targetPlan);
                        }
                        logger.info("Feedback the workload for " + newExecutionContext.getTraceId());
                    }
                }
            } catch (Throwable t) {
                OptimizerAlertUtil.spmAlert(SPM_TRY_UPDATE_PLAN_ERR, null, t);
                loggerSpm.error("notifyUpdatePlanAsync failed!", t);
            }
        }
    }

    public synchronized void loadBaseLineInfoAndPlanInfo() {
        final int maxSize = InstConfUtil.getInt(ConnectionParams.SPM_MAX_BASELINE_SIZE);
        int baselineSize = 0;
        int planSize = 0;
        int fixPlanSize = 0;
        int hintBaselineNum = 0;
        List<BaselineInfoRecord> baselineInfoRecords;
        // get baseline info from metadb
        try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(true)) {
            baselineInfoAccessor.planMigration();
            baselineInfoRecords = baselineInfoAccessor.loadBaselineData(0L);
        } catch (Exception e) {
            OptimizerAlertUtil.spmAlert(SPM_LOADING_ERR, null, e);
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SPM,
                    UNEXPECTED,
                    new String[] {"BASELINE LOAD", e.getMessage()},
                    CRITICAL,
                    e);
            return;
        }
        Map<String, Map<String, BaselineInfo>> baselineMapTmp = Maps.newConcurrentMap();
        // transform baseline info records to baseline map
        for (BaselineInfoRecord baselineInfoRecord : baselineInfoRecords) {
            BaselineInfo baselineInfo;
            try {

                if (!baselineMapTmp.containsKey(baselineInfoRecord.getSchemaName())) {
                    Map<String, BaselineInfo> newSchemaMap = Maps.newConcurrentMap();
                    baselineMapTmp.put(baselineInfoRecord.getSchemaName(), newSchemaMap);
                }

                if (baselineMapTmp.get(baselineInfoRecord.getSchemaName()).containsKey(baselineInfoRecord.getSql())) {
                    baselineInfo =
                        baselineMapTmp.get(baselineInfoRecord.getSchemaName()).get(baselineInfoRecord.getSql());
                    if (baselineInfo.isRebuildAtLoad()) {
                        hintBaselineNum++;
                        continue;
                    }
                } else {
                    boolean hotEvolution = hotEvolution(baselineInfoRecord.getExtendField());
                    if (!hotEvolution && !baselineInfoRecord.fixed && baselineSize > maxSize) {
                        continue;
                    }
                    // check version
                    int version = baselineInfoRecord.getVersion();
                    boolean isCompatible = PlanManagerUtil.isVersionCompatible(PlannerUtils.OPTIMIZER_VERSION, version);
                    if (!isCompatible) {
                        continue;
                    }

                    // sql -> baseline
                    baselineInfo = new BaselineInfo(baselineInfoRecord.getSql(),
                        BaselineInfo.deserializeTableSet(baselineInfoRecord.getTableSet()));
                    baselineInfo.setExtend(baselineInfoRecord.getExtendField());
                    baselineMapTmp.get(baselineInfoRecord.getSchemaName())
                        .put(baselineInfo.getParameterSql(), baselineInfo);
                    baselineSize++;
                }
                if (baselineInfo.isRebuildAtLoad()) {
                    continue;
                }
                if (baselineMapTmp.get(baselineInfoRecord.getSchemaName()).containsKey(baselineInfoRecord.getSql())) {
                    baselineInfo =
                        baselineMapTmp.get(baselineInfoRecord.getSchemaName()).get(baselineInfoRecord.getSql());
                    if (baselineInfo.isRebuildAtLoad()) {
                        hintBaselineNum++;
                        continue;
                    }
                } else {
                    boolean hotEvolution = hotEvolution(baselineInfoRecord.getExtendField());
                    if (!hotEvolution && !baselineInfoRecord.fixed && baselineSize > maxSize) {
                        continue;
                    }
                    // sql -> baseline
                    baselineInfo = new BaselineInfo(baselineInfoRecord.getSql(),
                        BaselineInfo.deserializeTableSet(baselineInfoRecord.getTableSet()));
                    baselineInfo.setExtend(baselineInfoRecord.getExtendField());
                    baselineMapTmp.get(baselineInfoRecord.getSchemaName())
                        .put(baselineInfo.getParameterSql(), baselineInfo);
                    baselineSize++;
                }
                if (baselineInfo.isRebuildAtLoad()) {
                    continue;
                }

                if (StringUtils.isEmpty(baselineInfoRecord.getPlan())) {
                    baselineMapTmp.get(baselineInfoRecord.getSchemaName()).remove(baselineInfoRecord.getSql());
                    continue;
                }

                PlanInfo planInfo =
                    new PlanInfo(baselineInfo.getId(),
                        baselineInfoRecord.getPlan(),
                        baselineInfoRecord.getCreateTime(),
                        baselineInfoRecord.getLastExecuteTime(),
                        baselineInfoRecord.getChooseCount(),
                        baselineInfoRecord.getCost(),
                        baselineInfoRecord.getEstimateExecutionTime(),
                        true,
                        baselineInfoRecord.isFixed(),
                        baselineInfoRecord.getTraceId(),
                        baselineInfoRecord.getOrigin(),
                        baselineInfoRecord.getPlanExtend(),
                        baselineInfoRecord.getTablesHashCode(),
                        baselineInfoRecord.getVersion());
                planSize++;
                if (baselineInfoRecord.isFixed()) {
                    fixPlanSize++;
                }
                baselineInfo.addAcceptedPlan(planInfo);
            } catch (Throwable e) {
                OptimizerAlertUtil.spmAlert(SPM_LOADING_ERR, null, e);
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SPM,
                        UNEXPECTED,
                        new String[] {
                            "baseline plan deserialize failed:" + baselineInfoRecord.getSql(),
                            e.getMessage()},
                        CRITICAL,
                        e
                    );
            }
        }
        baselineMap = baselineMapTmp;

        ModuleLogInfo.getInstance()
            .logRecord(
                Module.SPM,
                LOAD_DATA,
                new String[] {
                    "baseline:" + baselineSize + ",plan:" + planSize + ",fixed:" + fixPlanSize + ", hint baseline:"
                        + hintBaselineNum},
                LogLevel.NORMAL
            );
    }

    @Override
    public boolean checkBaselineHashCodeValid(BaselineInfo baselineInfo, PlanInfo planInfo) {
        return false;
    }

    /**
     * Set the gray percentage for a specific plan.
     *
     * @param schema db name
     * @param baselineId the baseline ID
     * @param planId the plan ID
     * @param grayRatio the new gray ratio to set
     * @return the previous gray percentage of the plan;
     * 0 if the plan was previously in non-gray state;
     * -1 if the corresponding plan was not found
     */
    @Override
    public int grayPlan(String schema, int baselineId, int planId, int grayRatio) {
        BaselineInfo baselineInfo = getBaselineById(schema, baselineId);
        for (PlanInfo planInfo : baselineInfo.getFixPlans()) {
            if (planInfo.getId() == planId) {
                int oldRatio = planInfo.getGrayPercentage();
                planInfo.setGrayPercentage(grayRatio);
                if (oldRatio <= 0) {
                    return 0;
                } else {
                    return oldRatio;
                }
            }
        }
        return -1;
    }

    @Override
    public void persistPlanExtendsToMeta(String instId, String schema, int baselineId, int planId) {
        BaselineInfo baselineInfo = getBaselineById(schema, baselineId);
        if (baselineInfo == null) {
            return;
        }
        String extendsField = null;
        for (PlanInfo planInfo : baselineInfo.getFixPlans()) {
            if (planInfo.getId() == planId) {
                extendsField = planInfo.encodeExtend();
            }
        }
        if (extendsField == null) {
            return;
        }
        try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(true)) {
            baselineInfoAccessor.updateExtendField(instId, schema, baselineId, planId, extendsField);
        } catch (Exception e) {
            OptimizerAlertUtil.spmAlert(SPM_PERSIST_ERR, null, e);
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SPM,
                    UNEXPECTED,
                    new String[] {"BASELINE PERSIST PLAN EXTENDS", e.getMessage()},
                    CRITICAL,
                    e);
        }
    }

    private double simpleCostValue(RelNode plan) {
        /* simple cost, we do not use synchronized and parameters to get precise value */
        RelMetadataQuery mq = plan.getCluster().getMetadataQuery();
        synchronized (mq) {
            RelOptCost cost = mq.getCumulativeCost(plan);
            return cost.getCpu()
                + CostModelWeight.INSTANCE.getIoWeight() * cost.getIo()
                + CostModelWeight.INSTANCE.getNetWeight() * cost.getNet();
        }
    }

    public void invalidateCache(ExecutionPlan executionPlan, Throwable ex) {
        PlanCache.getInstance().feedBack(executionPlan, null, ex);
    }

    @Override
    public BaselineInfo createBaselineInfo(String parameterizedSql, SqlNode ast, ExecutionContext ec) {
        final Set<Pair<String, String>> schemaTables = PlanManagerUtil.getTableSetFromAst(ast);
        return new BaselineInfo(parameterizedSql, schemaTables);
    }

    @Override
    public PlanInfo createPlanInfo(String schema, String planJsonString, RelNode plan, int baselineId,
                                   String traceId,
                                   String origin, SqlNode ast,
                                   ExecutionContext executionContext) {
        final Set<Pair<String, String>> schemaTables = PlanManagerUtil.getTableSetFromAst(ast);
        int tablesVersion = PlanManagerUtil.computeTablesVersion(schemaTables, schema, executionContext);
        return new PlanInfo(planJsonString, baselineId, simpleCostValue(plan), traceId, origin, tablesVersion,
            PlannerContext.getPlannerContext(plan).getTtlQueryType());
    }

    /**
     * only callback by baselineSyncController
     */
    @Override
    public void deleteBaseline(String schema, Integer baselineId) {
        if (StringUtils.isEmpty(schema) || baselineId == null) {
            return;
        }
        deleteBaseline(schema, baselineId, baselineMap);
    }

    /**
     * delete unfixed plans and reset fixed plan
     */
    @Override
    public void deleteBaselineUnfixPlan(String schema, Integer baselineId) {
        if (StringUtils.isEmpty(schema) || baselineId == null) {
            return;
        }
        deleteBaseline(schema, baselineId, baselineMap);
    }

    public static void deleteBaseline(String schema, Integer baselineId,
                                      Map<String, Map<String, BaselineInfo>> baselineMap) {
        schema = schema.toLowerCase();
        Map<String, BaselineInfo> bMap = baselineMap.get(schema);

        if (bMap == null) {
            handleMetaDB(schema, baselineId);
            return;
        }

        String key = null;
        for (Map.Entry<String, BaselineInfo> entry : bMap.entrySet()) {
            if (entry.getValue().getId() == baselineId) {
                key = entry.getKey();
                break;
            }
        }
        if (key == null) {
            handleMetaDB(schema, baselineId);
            return;
        }

        BaselineInfo target = bMap.remove(key);
        if (target == null) {
            ModuleLogInfo.getInstance().logInfo(Module.SPM, LogPattern.CHECK_FAIL,
                new String[] {"baseline delete check ", schema + "," + baselineId});
        } else {
            target.setDirty(true);
        }
        handleMetaDB(schema, baselineId);
    }

    private static void handleMetaDB(String schema, Integer baselineId) {
        if (baselineId == null) {
            OptimizerAlertUtil.spmAlert(SPM_DELETE_ERR, null, null);
            return;
        }
        if (SyncUtil.isNodeWithSmallestId()) {
            try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(true)) {
                baselineInfoAccessor.deleteBaseline(schema, baselineId);
            } catch (Exception e) {
                OptimizerAlertUtil.spmAlert(SPM_DELETE_ERR, null, e);
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SPM,
                        UNEXPECTED,
                        new String[] {"BASELINE DELETE", e.getMessage()},
                        CRITICAL,
                        e);
            }
        }
    }

    /**
     * Gets the baseline information by ID.
     *
     * @param schema The schema name.
     * @param baselineId The baseline ID.
     * @return The BaselineInfo object if found; otherwise, returns null.
     */
    protected BaselineInfo getBaselineById(String schema, Integer baselineId) {
        // Check if input parameters are empty or invalid
        if (StringUtils.isEmpty(schema) || baselineId == null) {
            return null;
        }

        schema = schema.toLowerCase();

        // Get the map of baselines for the given schema
        Map<String, BaselineInfo> bMap = baselineMap.get(schema);

        // If there are no baselines for this schema, return null
        if (bMap == null) {
            return null;
        }

        // Iterate through the entries in the map and find the matching baseline ID
        for (Map.Entry<String, BaselineInfo> entry : bMap.entrySet()) {
            if (entry.getValue().getId() == baselineId) {
                return entry.getValue();
            }
        }

        // If no match was found, return null
        return null;
    }

    protected BaselineInfo getBaselineBySql(String schema, String normalizedSql) {
        // Check if input parameters are empty or invalid
        if (StringUtils.isEmpty(schema) || StringUtils.isEmpty(normalizedSql)) {
            return null;
        }
        schema = schema.toLowerCase();

        // Get the map of baselines for the given schema
        Map<String, BaselineInfo> bMap = baselineMap.get(schema);

        // If there are no baselines for this schema, return null
        if (bMap == null) {
            return null;
        }
        return bMap.get(normalizedSql);
    }

    /**
     * delete unfix plan
     */
    public void deleteBaselineUnfixed(String schema, Integer baselineId) {
        schema = schema.toLowerCase();
        BaselineInfo target = getBaselineById(schema, baselineId);
        Set<Integer> pIds = null;
        if (target == null) {
            ModuleLogInfo.getInstance().logInfo(Module.SPM, LogPattern.CHECK_FAIL,
                new String[] {"baseline delete check ", schema + "," + baselineId});
        } else {
            if (target.isRebuildAtLoad()) {
                target.resetRebuildAtLoadPlanByForce();
                ModuleLogInfo.getInstance().logInfo(Module.SPM, LogPattern.PROCESSING,
                    new String[] {"baseline delete rebuild at load ", schema + "," + baselineId});
            } else {
                pIds = target.clearAllPlans();
                ModuleLogInfo.getInstance().logInfo(Module.SPM, LogPattern.PROCESSING,
                    new String[] {"baseline delete all plan ", schema + "," + baselineId + "," + pIds});
                if (target.getFixPlans().isEmpty()) {
                    deleteBaseline(schema, baselineId);
                    return;
                }
            }
        }

        if (SyncUtil.isNodeWithSmallestId() && pIds != null) {
            try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(true)) {
                String instId = ServerInstIdManager.getInstance().getInstId();
                baselineInfoAccessor.deletePlans(instId, schema, baselineId, pIds);
            } catch (Exception e) {
                OptimizerAlertUtil.spmAlert(SPM_DELETE_ERR, null, e);
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SPM,
                        UNEXPECTED,
                        new String[] {"BASELINE DELETE", e.getMessage()},
                        CRITICAL,
                        e);
            }
        }
    }

    /**
     * only callback by baselineSyncController
     */
    @Override
    public void deleteBaselinePlan(String schema, Integer baselineId, Integer planInfoId) {
        if (StringUtils.isEmpty(schema) || baselineId == null || planInfoId == null) {
            return;
        }

        deleteBaselinePlan(schema, baselineId, planInfoId, baselineMap);
    }

    public static void deleteBaselinePlan(String schema, Integer baselineId, int planInfoId,
                                          Map<String, Map<String, BaselineInfo>> baselineMap) {
        schema = schema.toLowerCase();
        Map<String, BaselineInfo> map = baselineMap.get(schema);
        if (map == null) {
            return;
        }
        BaselineInfo baselineInfo = null;
        String key = null;
        for (Map.Entry<String, BaselineInfo> entry : map.entrySet()) {
            if (entry.getValue().getId() == baselineId) {
                key = entry.getKey();
                baselineInfo = entry.getValue();
                break;
            }
        }
        if (baselineInfo == null) {
            return;
        }
        baselineInfo.removeAcceptedPlan(planInfoId);
        baselineInfo.removeUnacceptedPlan(planInfoId);
        boolean needRemoveBaseline = false;
        if (baselineInfo.getAcceptedPlans().isEmpty()) {
            map.remove(key);
            needRemoveBaseline = true;
        }

        if (SyncUtil.isNodeWithSmallestId()) {
            try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(true)) {
                if (needRemoveBaseline) {
                    baselineInfoAccessor.deleteBaseline(schema, baselineId);
                } else {
                    String instId = ServerInstIdManager.getInstance().getInstId();
                    baselineInfoAccessor.deletePlan(instId, schema, baselineId, planInfoId);
                }
            } catch (Exception e) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SPM,
                        UNEXPECTED,
                        new String[] {"BASELINE DELETE:" + schema + "," + baselineId, e.getMessage()},
                        CRITICAL,
                        e);
            }
        }
    }

    @Override
    public void deleteBaselineEvolved(String schema) {
        if (StringUtils.isEmpty(schema)) {
            return;
        }
        schema = schema.toLowerCase(Locale.ROOT);
        if (baselineMap.get(schema) == null) {
            return;
        }

        if (SyncUtil.isNodeWithSmallestId()) {
            try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(true)) {
                for (BaselineInfo baselineInfo : baselineMap.get(schema).values()) {
                    if (baselineInfo.isHotEvolution()) {
                        baselineInfoAccessor.deleteBaseline(schema, baselineInfo.getId());
                    }
                }
            } catch (Exception e) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SPM,
                        UNEXPECTED,
                        new String[] {"BASELINE DELETE_EVOLVED:" + schema, e.getMessage()},
                        CRITICAL,
                        e);
            }
        }

        baselineMap.get(schema).entrySet().removeIf(entry -> entry.getValue().isHotEvolution());
    }

    @Override
    public void deleteBaselineUnfixed(String schema) {
        if (StringUtils.isEmpty(schema)) {
            return;
        }
        schema = schema.toLowerCase(Locale.ROOT);
        if (baselineMap.get(schema) == null) {
            return;
        }

        if (SyncUtil.isNodeWithSmallestId()) {
            try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(true)) {
                for (BaselineInfo baselineInfo : baselineMap.get(schema).values()) {
                    if (!PlanManagerUtil.isBaselineCustomized(baselineInfo)) {
                        baselineInfoAccessor.deleteBaseline(schema, baselineInfo.getId());
                    }
                }
            } catch (Exception e) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SPM,
                        UNEXPECTED,
                        new String[] {"BASELINE DELETE_UNFIXED:" + schema, e.getMessage()},
                        CRITICAL,
                        e);
            }
        }

        baselineMap.get(schema).entrySet().removeIf(entry -> !PlanManagerUtil.isBaselineCustomized(entry.getValue()));
    }

    public enum PLAN_SOURCE {
        PLAN_CACHE, SPM_FIX, SPM_FIX_GRAY, SPM_PQO, SPM_ACCEPT, SPM_UNACCEPTED, SPM_NEW_BUILD,
        SPM_FIX_PLAN_UPDATE_FOR_ROW_TYPE,
        SPM_FIX_PLAN_UPDATE_FOR_INVALID, SPM_FIX_DDL_HASHCODE_UPDATE, DEEP_PAGE_OPTIMIZER;

        public static boolean isSpm(PLAN_SOURCE planSource) {
            if (planSource == null) {
                return false;
            }
            return !(planSource == PLAN_CACHE || planSource == DEEP_PAGE_OPTIMIZER);
        }
    }

}