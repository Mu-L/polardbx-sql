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
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.TDDLHint;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.htaprouting.PlanType;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.ttl.query.TtlQueryType;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.util.JsonBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_BASELINE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_DIRECT_PLAN;
import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl.TINY;
import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.SPM_PLAN_COST_ERR;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.OPTIMIZER_VERSION;

public class PlanInfo {

    public static int INVAILD_HASH_CODE = -1;
    public static int REBUILD_PLAN_HASH_CODE = -2;

    private int id;

    private int baselineId;

    private byte[] compressPlanByteArray;

    private int chooseCount = 0;

    private boolean accepted;

    private boolean fixed;

    private double cost;

    private String traceId;

    private String origin;

    private String extend;

    private long createTime; // unix time

    private Long lastExecuteTime; // unix time, default null

    // estimation of execution time, in ms
    private double estimateExecutionTime = -1;

    private RelNode plan = null;

    private int tablesHashCode;

    private int version;

    private AtomicInteger errorCount = new AtomicInteger(0);

    /**
     * params from extend
     */
    private String fixHint;

    private Map<String, String> hintArgs = new HashMap<>();

    private SqlNode exprNode;

    private RexNode expr;

    /**
     * Gray percentage, value range 0-100, indicates the percentage of this plan in gray status
     */
    private int grayPercentage = -1;

    private int lastTenAvgRt = -1;

    /**
     * Maximum number of execution time records to keep
     */
    private static final int MAX_EXECUTION_TIME_RECORDS = 10;

    /**
     * Recent execution time records (ring buffer, lock-free implementation)
     * Initialized to -1 to indicate empty slots
     */
    private final double[] recentExecutionTimes;

    /**
     * Write position index (lock-free implementation)
     */
    private final AtomicInteger writeIndex = new AtomicInteger(0);

    {
        recentExecutionTimes = new double[MAX_EXECUTION_TIME_RECORDS];
        Arrays.fill(recentExecutionTimes, -1.0);
    }

    private TtlQueryType ttlQueryType;

    private enum IGNORE_HINT_NAME {
        NODE, SCAN, DIRECT, TDDL;

        public static boolean contains(String name) {
            for (IGNORE_HINT_NAME ignoreHintName : IGNORE_HINT_NAME.values()) {
                if (ignoreHintName.name().equalsIgnoreCase(name)) {
                    return true;
                }
            }
            return false;
        }
    }

    private PlanInfo() {
    }

    public PlanInfo(String planJsonString, int baselineId, double cost, String traceId, String origin,
                    int tablesVersion) {
        this(planJsonString, baselineId, cost, traceId, origin, tablesVersion, null);
    }

    public PlanInfo(String planJsonString, int baselineId, double cost, String traceId, String origin,
                    int tablesVersion, TtlQueryType ttlQueryType) {
        this.compressPlanByteArray = PlanManagerUtil.compressPlan(planJsonString);
        this.id = planJsonString.hashCode();
        this.baselineId = baselineId;
        this.createTime = unixTimeStamp();
        this.cost = cost;
        this.traceId = traceId;
        this.origin = origin;
        this.tablesHashCode = tablesVersion;
        this.version = OPTIMIZER_VERSION;
        this.grayPercentage = 0;
        this.ttlQueryType = ttlQueryType;
    }

    public PlanInfo(RelNode plan, int baselineId, double cost, String traceId, String origin,
                    int tablesHashCode) {
        String planJson = PlanManagerUtil.relNodeToJson(plan);
        this.compressPlanByteArray = PlanManagerUtil.compressPlan(planJson);
        this.plan = plan;
        this.id = planJson.hashCode();
        this.baselineId = baselineId;
        this.createTime = unixTimeStamp();
        this.cost = cost;
        this.traceId = traceId;
        this.origin = origin;
        this.tablesHashCode = tablesHashCode;
        this.version = OPTIMIZER_VERSION;
        this.grayPercentage = 0;
        this.ttlQueryType = PlannerContext.getPlannerContext(plan).getTtlQueryType();
    }

    public PlanInfo(int baselineId, String planJsonString, long createTime, Long lastExecuteTime, int chooseCount,
                    double cost, double estimateExecutionTime, boolean accepted, boolean fixed, String traceId,
                    String origin, String extend, int tablesHashCode, int version) {
        this.compressPlanByteArray = PlanManagerUtil.compressPlan(planJsonString);
        this.id = planJsonString.hashCode();
        this.baselineId = baselineId;
        this.createTime = createTime;
        this.lastExecuteTime = lastExecuteTime;
        this.chooseCount = chooseCount;
        this.cost = cost;
        this.estimateExecutionTime = estimateExecutionTime;
        this.accepted = accepted;
        this.fixed = fixed;
        this.traceId = traceId;
        this.origin = origin;
        this.extend = extend;
        this.tablesHashCode = tablesHashCode;
        this.version = version;
        this.grayPercentage = 0;

        decodeExtend();
    }

    @Override
    public boolean equals(Object planInfo) {
        if (!(planInfo instanceof PlanInfo)) {
            return false;
        }
        PlanInfo p = (PlanInfo) planInfo;
        return this.id == p.getId() &&
            this.baselineId == p.getBaselineId() &&
            this.fixed == p.fixed &&
            this.accepted == p.accepted &&
            (Objects.equals(this.extend, p.extend)) &&
            this.tablesHashCode == p.tablesHashCode &&
            this.grayPercentage == p.grayPercentage;
    }

    public void decodeExtend() {
        if (extend == null || "".equals(extend)) {
            return;
        }
        Map<String, Object> extendMap = (Map<String, Object>) JSON.parseObject(extend);
        Object tmpFixHint = extendMap.get("FIX_HINT");
        if (tmpFixHint != null) {
            fixHint = tmpFixHint.toString();
            parseExecutorArgs();
        }

        try {
            Object tmpExpr = extendMap.get("EXPR");
            if (tmpExpr instanceof String) {
                exprNode = buildExpr(tmpExpr.toString());
            }
            Object tmpGray = extendMap.get("GRAY_PERCENTAGE");
            if (tmpGray instanceof Integer) {
                grayPercentage = (Integer) tmpGray;
            }
        } catch (Exception e) {
            // this should be caught by loading alert SPM_LOADING_ERR
            throw new RuntimeException(e);
        }

        try {
            Object tmpTtlQueryType = extendMap.get("TTL_QUERY_TYPE");
            if (tmpTtlQueryType != null) {
                ttlQueryType = TtlQueryType.valueOf(tmpTtlQueryType.toString());
            }
        } catch (Exception e) {
            // this should be caught by loading alert SPM_LOADING_ERR
            throw new RuntimeException(e);
        }

    }

    public static Map<String, String> decodeExtendForShow(String extend) {
        if (extend == null || "".equals(extend)) {
            return new HashMap<>();
        }

        Map<String, String> result = new HashMap<>();
        try {
            Map<String, Object> extendMap = (Map<String, Object>) JSON.parseObject(extend);

            Object tmpFixHint = extendMap.get("FIX_HINT");
            if (tmpFixHint != null) {
                result.put("FIX_HINT", tmpFixHint.toString());
            }

            Object tmpExpr = extendMap.get("EXPR");
            if (tmpExpr != null) {
                result.put("EXPR", tmpExpr.toString());
            }

            Object tmpGray = extendMap.get("GRAY_PERCENTAGE");
            if (tmpGray != null) {
                result.put("GRAY_PERCENTAGE", tmpGray.toString());
            }

            Object tmpTtlQueryType = extendMap.get("TTL_QUERY_TYPE");
            if (tmpTtlQueryType != null) {
                result.put("TTL_QUERY_TYPE", tmpTtlQueryType.toString());
            }
        } catch (Exception e) {
            return new HashMap<>();
        }

        return result;
    }

    /**
     * Builds a SQL expression node.
     *
     * @param exprStr The String representation of the expression
     * @return The constructed SqlNode
     */
    public static SqlNode buildExpr(String exprStr) {
        if (StringUtils.isEmpty(exprStr)) {
            return null;
        }

        // Parameterize the expression string using SqlParameterizeUtils utility class
        SQLExpr sqlExpr = SqlParameterizeUtils.parameterizeExpr(exprStr);

        // Convert the SQL expression to a SqlNode
        return FastSqlConstructUtils.convertToSqlNode(sqlExpr, new ContextParameters(false), new ExecutionContext());
    }

    public String encodeExtend() {
        final JsonBuilder jsonBuilder = new JsonBuilder();
        Map<String, Object> extendMap = Maps.newHashMap();
        if (!StringUtils.isEmpty(fixHint)) {
            extendMap.put("FIX_HINT", fixHint);
        }
        if (exprNode != null) {
            extendMap.put("EXPR", exprNode.toSqlString(MysqlSqlDialect.DEFAULT).getSql());
        }
        if (grayPercentage > 0) {
            extendMap.put("GRAY_PERCENTAGE", grayPercentage);
        }
        if (ttlQueryType != null) {
            extendMap.put("TTL_QUERY_TYPE", ttlQueryType.name());
        }
        if (extendMap.isEmpty()) {
            return "";
        }

        return jsonBuilder.toJsonString(extendMap);
    }

    public int getId() {
        return id;
    }

    public int getBaselineId() {
        return baselineId;
    }

    public String getTraceId() {
        return traceId;
    }

    public String getPlanJsonString() {
        return PlanManagerUtil.uncompressPlan(compressPlanByteArray);
    }

    public void addChooseCount() {
        chooseCount++;
    }

    public void addChooseCount(int value) {
        chooseCount += value;
        if (chooseCount < 0) {
            chooseCount = 0;
        }
    }

    public int getChooseCount() {
        return chooseCount;
    }

    public boolean isAccepted() {
        return accepted;
    }

    public void setAccepted(boolean accepted) {
        this.accepted = accepted;
    }

    public void setFixed(boolean fixed) {
        this.fixed = fixed;
    }

    public boolean isFixed() {
        return fixed;
    }

    public double getCost() {
        return cost;
    }

    public long getCreateTime() {
        return createTime;
    }

    public Long getLastExecuteTime() {
        return lastExecuteTime;
    }

    public void setLastExecuteTime(long lastExecuteTime) {
        this.lastExecuteTime = lastExecuteTime;
    }

    public double getEstimateExecutionTime() {
        return estimateExecutionTime;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public void updateEstimateExecutionTime(double lastExecutionTimeInMs) {
        if (estimateExecutionTime == -1) {
            estimateExecutionTime = lastExecutionTimeInMs;
        } else {
            estimateExecutionTime = estimateExecutionTime * 0.8 + lastExecutionTimeInMs * 0.2;
        }
        recordExecutionTime(lastExecutionTimeInMs);
    }

    public void resetPlan(RelNode newPlan, ExecutionContext ec) {
        String planJson = PlanManagerUtil.relNodeToJson(newPlan);
        this.compressPlanByteArray = PlanManagerUtil.compressPlan(planJson);
        preparePlan(newPlan, ec);
        this.plan = newPlan;
        this.id = planJson.hashCode();
    }

    public RelNode getPlan(String schema, ExecutionContext ec) {
        if (plan == null) {
            if (StringUtils.isEmpty(schema) || ec == null) {
                return null;
            }
            synchronized (this) {
                if (plan == null) {
                    SqlConverter sqlConverter = SqlConverter.getInstance(schema, ec);
                    RelOptCluster cluster = sqlConverter.createRelOptCluster();
                    RelOptSchema relOptSchema = sqlConverter.getCatalog();

                    plan = PlanManagerUtil.jsonToRelNode(getPlanJsonString(), cluster, relOptSchema);
                    preparePlan(plan, ec);
                }
            }
        }
        return plan;
    }

    public void preparePlan(RelNode plan, ExecutionContext ec) {
        PlannerContext pc = PlannerContext.getPlannerContext(plan);
        // clear plannerContext cost cache
        pc.setCost(null);
        // use select default, todo use real sql kind
        pc.setSqlKind(SqlKind.SELECT);
        // set schedule type based on
        pc.setPlanType(PlanType.determinePlanType(plan, null, PlannerContext.getPlannerContext(plan)));
        pc.getCteContext().reCollect(plan);
        if (this.isFixed()) {
            pc.setSkipPostOpt(true);
            pc.getParamManager().getProps().put(ENABLE_DIRECT_PLAN, "false");
            prepareExecutorArgs(ec);
        }
        try {
            if (exprNode != null) {
                // try to build rexNode && check if it is valid
                Map<String, RexNode> rexNodeTableMap = PlanManagerUtil.getRexNodeTableNameMap(plan);
                expr = PlanManagerUtil.buildRexNode(exprNode, ec.getSchemaName(), rexNodeTableMap, plan, ec);
            }
        } catch (Exception e) {
            throw new TddlRuntimeException(ERR_BASELINE, e,
                "not support baseline fix expr:" + expr + ", " + e.getMessage());
        }
    }

    public boolean inited() {
        return plan != null;
    }

    public RelOptCost getCumulativeCost(ExecutionContext ec) {
        if (isInGrayStatus()) {
            return TINY;
        }
        RelNode plan = getPlan(ec.getSchemaName(), ec);
        RelOptCluster cluster = plan.getCluster();
        Parameters parameters = ec.getParams();
        if (DynamicConfig.getInstance().isEnableMQCacheByThread()) {
            try {
                RelMetadataQuery.THREAD_PARAMETERS.set(parameters);
                return cluster.getMetadataQuery().getCumulativeCost(plan);
            } catch (Throwable t) {
                ModuleLogInfo.getInstance()
                    .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"spm get plan cost", t.getMessage()},
                        LogLevel.CRITICAL);
                OptimizerAlertUtil.spmAlert(SPM_PLAN_COST_ERR, ec, t);
                throw new TddlRuntimeException(ErrorCode.ERR_PLAN_COST, t.getMessage());
            } finally {
                RelMetadataQuery.THREAD_PARAMETERS.remove();
                cluster.getMetadataQuery().clearThreadCache();
            }
        } else {
            synchronized (this) {
                PlannerContext.getPlannerContext(plan).setParams(parameters);
                return cluster.getMetadataQuery().getCumulativeCost(plan);
            }
        }
    }

    public int incrementAndGetErrorCount() {
        return errorCount.incrementAndGet();
    }

    public void zeroErrorCount() {
        errorCount.set(0);
    }

    public static String serializeToJson(PlanInfo planInfo) {
        JSONObject planInfoJson = new JSONObject();
        planInfoJson.put("id", planInfo.getId());
        planInfoJson.put("baselineId", planInfo.getBaselineId());
        planInfoJson.put("traceId", planInfo.getTraceId());
        planInfoJson.put("planJsonString", planInfo.getPlanJsonString());
        planInfoJson.put("hashcode", planInfo.getTablesHashCode());
        planInfoJson.put("chooseCount", planInfo.getChooseCount());
        planInfoJson.put("cost", planInfo.getCost());
        planInfoJson.put("fixed", planInfo.isFixed());
        planInfoJson.put("accepted", planInfo.isAccepted());
        planInfoJson.put("createTime", planInfo.getCreateTime());
        planInfoJson.put("lastExecuteTime", planInfo.getLastExecuteTime());
        planInfoJson.put("estimateExecutionTime", planInfo.getEstimateExecutionTime());
        planInfoJson.put("origin", planInfo.getOrigin());
        planInfoJson.put("extend", planInfo.encodeExtend());
        planInfoJson.put("version", planInfo.getVersion());
        planInfoJson.put("grayPercentage", planInfo.getGrayPercentage());
        planInfoJson.put("lastTenAvgRt", planInfo.getAverageExecutionTime());
        return planInfoJson.toJSONString();
    }

    public static PlanInfo deserializeFromJson(String json) {
        JSONObject planInfoJson = JSON.parseObject(json);
        PlanInfo planInfo = new PlanInfo();
        planInfo.id = planInfoJson.getIntValue("id");
        planInfo.baselineId = planInfoJson.getIntValue("baselineId");
        planInfo.traceId = planInfoJson.getString("traceId");
        planInfo.compressPlanByteArray = PlanManagerUtil.compressPlan(planInfoJson.getString("planJsonString"));
        planInfo.chooseCount = planInfoJson.getIntValue("chooseCount");
        planInfo.cost = planInfoJson.getDoubleValue("cost");
        planInfo.fixed = planInfoJson.getBooleanValue("fixed");
        planInfo.accepted = planInfoJson.getBooleanValue("accepted");
        planInfo.createTime = planInfoJson.getLongValue("createTime");
        planInfo.lastExecuteTime = planInfoJson.getLong("lastExecuteTime");
        planInfo.estimateExecutionTime = planInfoJson.getDoubleValue("estimateExecutionTime");
        planInfo.origin = planInfoJson.getString("origin");
        planInfo.extend = planInfoJson.getString("extend");
        planInfo.version = planInfoJson.getIntValue("version");
        planInfo.grayPercentage = planInfoJson.getIntValue("grayPercentage");
        planInfo.lastTenAvgRt = planInfoJson.getIntValue("lastTenAvgRt");
        planInfo.decodeExtend();
        try {
            planInfo.tablesHashCode = planInfoJson.getInteger("hashcode");
        } catch (Throwable t) {
            planInfo.tablesHashCode = INVAILD_HASH_CODE;
        }

        return planInfo;
    }

    public static String serializeToJsonForShow(PlanInfo planInfo) {
        RelNode plan = planInfo.getPlan(null, null);
        String planSimple = "";
        if (plan != null) {
            planSimple = RelOptUtil.dumpPlan("",
                planInfo.getPlan(null, null),
                SqlExplainFormat.TEXT,
                SqlExplainLevel.NO_ATTRIBUTES);
        }
        JSONObject planInfoJson = new JSONObject();
        planInfoJson.put("id", planInfo.getId());
        planInfoJson.put("baselineId", planInfo.getBaselineId());
        planInfoJson.put("planExplain", planSimple);
        planInfoJson.put("chooseCount", planInfo.getChooseCount());
        planInfoJson.put("fixed", planInfo.isFixed());
        planInfoJson.put("createTime", planInfo.getCreateTime());
        planInfoJson.put("lastExecuteTime", planInfo.getLastExecuteTime());
        planInfoJson.put("extend", planInfo.encodeExtend());
        planInfoJson.put("grayPercentage", planInfo.getGrayPercentage());
        planInfoJson.put("lastTenAvgRt", planInfo.getAverageExecutionTime());
        return planInfoJson.toJSONString();
    }

    public static Map<String, String> deserializeFromJsonForShow(String json) {
        JSONObject planInfoJson = JSON.parseObject(json);
        Map<String, String> result = new HashMap<>();

        result.put("id", String.valueOf(planInfoJson.getIntValue("id")));
        result.put("baselineId", String.valueOf(planInfoJson.getIntValue("baselineId")));
        result.put("planExplain", planInfoJson.getString("planExplain"));
        result.put("chooseCount", String.valueOf(planInfoJson.getIntValue("chooseCount")));
        result.put("fixed", String.valueOf(planInfoJson.getBooleanValue("fixed")));
        result.put("createTime", String.valueOf(planInfoJson.getLongValue("createTime")));
        result.put("lastExecuteTime", String.valueOf(planInfoJson.getLong("lastExecuteTime")));
        result.put("extend", planInfoJson.getString("extend"));
        result.put("grayPercentage", String.valueOf(planInfoJson.getIntValue("grayPercentage")));
        result.put("lastTenAvgRt", String.valueOf(planInfoJson.getDoubleValue("lastTenAvgRt")));

        return result;
    }

    public int getTablesHashCode() {
        return tablesHashCode;
    }

    public void setTablesHashCode(int tablesHashCode) {
        this.tablesHashCode = tablesHashCode;
    }

    public String getExtend() {
        return extend;
    }

    public void setExtend(String extend) {
        this.extend = extend;
    }

    public String getFixHint() {
        return fixHint;
    }

    public void setFixHint(String fixHint) {
        this.fixHint = fixHint;
        parseExecutorArgs();
    }

    /**
     * Parses executor parameters from hints.
     */
    public void parseExecutorArgs() {
        // If the hint starts with "/*" and ends with "*/", parse its content
        if (fixHint.startsWith("/*") && fixHint.endsWith("*/")) {
            // Remove leading and trailing "/*" and "*/"
            String tddlText = fixHint.substring(2, fixHint.length() - 2);
            TDDLHint tddlHint;
            try {
                tddlHint = new TDDLHint(tddlText);
            } catch (Exception e) {
                ModuleLogInfo.getInstance()
                    .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"spm parse hint", e.getMessage()},
                        WARNING);
                return;
            }

            // Iterate through all functions
            for (TDDLHint.Function function : tddlHint.getFunctions()) {
                List<TDDLHint.Argument> arguments = function.getArguments();

                // ignore function
                if (IGNORE_HINT_NAME.contains(function.getName())) {
                    continue;
                }

                // try cmd_extra
                if (function.getName().equalsIgnoreCase("cmd_extra")) {
                    for (TDDLHint.Argument argument : arguments) {
                        if (argument == null) {
                            continue;
                        }
                        if (argument.getName() == null) {
                            continue;
                        }
                        if (argument.getValue() == null) {
                            continue;
                        }

                        hintArgs.put(argument.getName().toString().toUpperCase(), argument.getValue().toString());
                    }
                    continue;
                }

                // Only support one argument at this time
                if (arguments.size() == 1) {
                    TDDLHint.Argument argument = arguments.get(0);
                    if (argument == null) {
                        continue;
                    }
                    SQLExpr val = arguments.get(0).getValue();
                    if (val == null) {
                        continue;
                    }
                    String argumentValue = val.toString();

                    // Put the function name and its argument value into the map
                    hintArgs.put(function.getName().toUpperCase(), argumentValue);
                }
            }
        }
    }

    public void prepareExecutorArgs(ExecutionContext ec) {
        if (ec == null) {
            return;
        }
        ParamManager pm = ec.getParamManager();
        if (pm == null) {
            return;
        }
        Map<String, String> props = pm.getProps();
        if (props == null) {
            return;
        }
        props.putAll(hintArgs);
    }

    public Map<String, String> getHintArgs() {
        return hintArgs;
    }

    // only for ut test
    public void setId(int id) {
        this.id = id;
    }

    public static Integer genPlanId(ExecutionPlan plan) {
        if (plan == null || plan.getPlan() == null) {
            return null;
        }
        try {
            return PlanManagerUtil.relNodeToJson(plan.getPlan()).hashCode();
        } catch (Throwable t) {
            try {
                ModuleLogInfo.getInstance()
                    .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"PLAN TO JSON", t.getMessage()},
                        WARNING);
            } catch (Throwable ignore) {
            }
            return null;
        }

    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public RexNode getExpr() {
        return expr;
    }

    public void setExpr(RexNode expr) {
        this.expr = expr;
    }

    public SqlNode getExprNode() {
        return exprNode;
    }

    public void setExprNode(SqlNode exprNode) {
        this.exprNode = exprNode;
    }

    public int getGrayPercentage() {
        return grayPercentage;
    }

    public void setGrayPercentage(int grayPercentage) {
        if (grayPercentage < 0 || grayPercentage > 100) {
            throw new IllegalArgumentException("grayPercentage must be between 0 and 100");
        }
        this.grayPercentage = grayPercentage;
    }

    public int getLastTenAvgRt() {
        return lastTenAvgRt;
    }

    public void setLastTenAvgRt(int lastTenAvgRt) {
        this.lastTenAvgRt = lastTenAvgRt;
    }

    /**
     * Determines if currently in gray status
     *
     * @return true indicates in gray status, false indicates not in gray status
     */
    public boolean isInGrayStatus() {
        return grayPercentage > 0 && grayPercentage < 100;
    }

    /**
     * Uses random number to determine if gray percentage is valid
     * If gray percentage is 0, returns false; if 100, returns true
     * If between 0-100, determines by random number whether it falls within gray range
     *
     * @return true indicates gray is valid (should use gray logic), false indicates gray is invalid
     */
    public boolean isGrayWorkload() {
        if (!isInGrayStatus()) {
            return true;
        }
        // Generate random number between 0-99, check if less than gray percentage
        int random = ThreadLocalRandom.current().nextInt(100);
        return random < grayPercentage;
    }

    /**
     * Record an execution RT (Response Time) - lock-free version
     * Keeps the most recent records using a ring buffer approach
     * Note: Does not guarantee precision; reads may get partially stale data
     *
     * @param executeTimeMs execution time in milliseconds
     */
    public void recordExecutionTime(double executeTimeMs) {
        int index = writeIndex.getAndIncrement() % MAX_EXECUTION_TIME_RECORDS;
        recentExecutionTimes[index] = executeTimeMs;
    }

    /**
     * Get the average of recent execution RTs - lock-free version
     *
     * @return average value, returns 0.0 if no records exist
     */
    public double getAverageExecutionTime() {
        double sum = 0.0;
        int count = 0;

        for (int i = 0; i < MAX_EXECUTION_TIME_RECORDS; i++) {
            double time = recentExecutionTimes[i];
            if (time >= 0) {
                sum += time;
                count++;
            }
        }

        return count > 0 ? sum / count : 0.0;
    }

    public TtlQueryType getTtlQueryType() {
        return ttlQueryType;
    }

    public void setTtlQueryType(TtlQueryType ttlQueryType) {
        this.ttlQueryType = ttlQueryType;
    }
}
