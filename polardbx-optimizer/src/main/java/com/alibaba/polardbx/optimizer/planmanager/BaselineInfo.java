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
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.table.BaselineInfoRecord;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.optimizer.planmanager.parametric.Point;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.JsonBuilder;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.common.properties.ConnectionParams.SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.SPM_OLD_PLAN_CHOOSE_COUNT_LEVEL;
import static com.alibaba.polardbx.common.properties.ConnectionParams.SPM_RECENTLY_EXECUTED_PERIOD;
import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;
import static com.alibaba.polardbx.gms.module.LogPattern.REMOVE;
import static com.alibaba.polardbx.gms.module.Module.SPM;
import static com.alibaba.polardbx.optimizer.planmanager.PlanInfo.REBUILD_PLAN_HASH_CODE;

/**
 * @author jilong.ljl
 */
public class BaselineInfo {
    public static final String EXTEND_POINT_SET = "POINT_SET";
    public static final String EXTEND_HINT = "HINT";
    public static final String EXTEND_USE_POST_PLANNER = "USE_POST_PLANNER";
    public static final String EXTEND_REBUILD_AT_LOAD = "REBUILD_AT_LOAD";
    public static final String EXTEND_HOT_EVOLVED = "HOT_EVOLVED";
    private int id;

    private String parameterSql;

    private Set<Pair<String, String>> tableSet;

    // planInfoId -> PlanInfo
    private Map<Integer, PlanInfo> acceptedPlans = new ConcurrentHashMap<>();

    // planInfoId -> PlanInfo
    private Map<Integer, PlanInfo> unacceptedPlans = new ConcurrentHashMap<>();

    // Cached PlanInfo for plan with pushdown hint
    private volatile PlanInfo rebuildAtLoadPlan;

    // use for evolution fail plan, just use hashCode to save memory
    private Set<Integer> evolutionFailPlanHashSet = ConcurrentHashMap.newKeySet();

    private volatile boolean dirty = false;

    // whether the baseline used to evolve hot gsi
    private volatile boolean hotEvolution = false;

    private String extend;

    // --- params from extend ---

    private Set<Point> pointSet = Sets.newHashSet();
    private String hint = "";
    private boolean usePostPlanner = true;
    /**
     * For plan with pushdown hint, we should rebuild plan from parameterized sql.
     * Cause some field in plan cannot be serialized, e.g.
     * {@link com.alibaba.polardbx.optimizer.core.rel.LogicalView#sqlTemplateHintCache},
     * {@link com.alibaba.polardbx.optimizer.core.rel.LogicalView#targetTablesHintCache},
     * {@link com.alibaba.polardbx.optimizer.core.rel.LogicalView#comparativeHintCache},
     */
    private boolean rebuildAtLoad = false;

    private BaselineInfo() {
    }

    public BaselineInfo(String parameterSql, Set<Pair<String, String>> tableSet) {
        this.parameterSql = parameterSql;
        this.id = parameterSql.hashCode();
        this.tableSet = tableSet;
    }

    public int getId() {
        return id;
    }

    public Set<Pair<String, String>> getTableSet() {
        return tableSet;
    }

    public PlanInfo addAcceptedPlan(PlanInfo planInfo) {
        planInfo.setAccepted(true);
        return acceptedPlans.put(planInfo.getId(), planInfo);
    }

    public void addUnacceptedPlan(PlanInfo planInfo) {
        planInfo.setAccepted(false);
        unacceptedPlans.put(planInfo.getId(), planInfo);
    }

    public void removeAcceptedPlan(Integer planInfoId) {
        acceptedPlans.remove(planInfoId);
    }

    public void removeUnacceptedPlan(Integer planInfoId) {
        unacceptedPlans.remove(planInfoId);
    }

    public Map<Integer, PlanInfo> getAcceptedPlans() {
        return acceptedPlans;
    }

    public Map<Integer, PlanInfo> getUnacceptedPlans() {
        return unacceptedPlans;
    }

    public String getParameterSql() {
        return parameterSql;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isHotEvolution() {
        return hotEvolution;
    }

    public void setHotEvolution(boolean hotEvolution) {
        this.hotEvolution = hotEvolution;
    }

    public boolean canHotEvolution() {
        return (!hotEvolution)
            && (!rebuildAtLoad)
            && (!dirty)
            && (getFixPlans().isEmpty());
    }

    public void addEvolutionFailPlan(int planInfoId) {
        evolutionFailPlanHashSet.add(planInfoId);
    }

    public boolean evolutionFailPlanHashSetContain(int planInfoId) {
        return evolutionFailPlanHashSet.contains(planInfoId);
    }

    public boolean tooLongNoUsed() {
        for (PlanInfo planInfo : acceptedPlans.values()) {
            long lastTime =
                planInfo.getLastExecuteTime() != null ? planInfo.getLastExecuteTime() : planInfo.getCreateTime();
            if (unixTimeStamp() - lastTime < 7 * 24 * 60 * 60) {
                return false;
            }
        }
        // a week no used
        return true;
    }

    public static String serializeToJsonForShow(BaselineInfo baselineInfo) {
        JSONObject baselineInfoJson = new JSONObject();
        baselineInfoJson.put("id", baselineInfo.getId());
        baselineInfoJson.put("parameterSql", baselineInfo.getParameterSql());
        baselineInfoJson.put("tableSet", serializeTableSet(baselineInfo.getTableSet()));
        Map<String, String> acceptedPlansMap = new HashMap<>();
        for (Map.Entry<Integer, PlanInfo> entry : baselineInfo.getAcceptedPlans().entrySet()) {
            Integer planInfoId = entry.getKey();
            PlanInfo planInfo = entry.getValue();
            acceptedPlansMap.put(planInfoId.toString(), PlanInfo.serializeToJsonForShow(planInfo));
        }
        baselineInfoJson.put("acceptedPlans", acceptedPlansMap);
        baselineInfoJson.put("extend", baselineInfo.encodeExtend());
        baselineInfoJson.put("dirty", baselineInfo.isDirty());
        return baselineInfoJson.toJSONString();
    }

    public static String serializeToJson(BaselineInfo baselineInfo, boolean simpleMode) {
        JSONObject baselineInfoJson = new JSONObject();
        baselineInfoJson.put("id", baselineInfo.getId());
        baselineInfoJson.put("parameterSql", baselineInfo.getParameterSql());
        baselineInfoJson.put("tableSet", serializeTableSet(baselineInfo.getTableSet()));

        Map<String, String> acceptedPlansMap = new HashMap<>();
        for (Map.Entry<Integer, PlanInfo> entry : baselineInfo.getAcceptedPlans().entrySet()) {
            Integer planInfoId = entry.getKey();
            PlanInfo planInfo = entry.getValue();
            RelNode planRel = planInfo.getPlan(null, null);
            if (planRel != null && !PlanManagerUtil.baselineSupported(planRel)) {
                continue;
            }
            if (simpleMode) {
                if (planInfo.getChooseCount() > InstConfUtil.getInt(SPM_OLD_PLAN_CHOOSE_COUNT_LEVEL)) {
                    acceptedPlansMap.put(planInfoId.toString(), PlanInfo.serializeToJson(planInfo));
                }
            } else {
                acceptedPlansMap.put(planInfoId.toString(), PlanInfo.serializeToJson(planInfo));
            }
        }
        baselineInfoJson.put("acceptedPlans", acceptedPlansMap);

        Map<String, String> unacceptedPlansMap = new HashMap<>();
        if (!simpleMode) {
            for (Map.Entry<Integer, PlanInfo> entry : baselineInfo.getUnacceptedPlans().entrySet()) {
                Integer planInfoId = entry.getKey();
                PlanInfo planInfo = entry.getValue();
                unacceptedPlansMap.put(planInfoId.toString(), PlanInfo.serializeToJson(planInfo));
            }
        }

        baselineInfoJson.put("unacceptedPlans", unacceptedPlansMap);
        baselineInfoJson.put("extend", baselineInfo.encodeExtend());
        baselineInfoJson.put("dirty", baselineInfo.isDirty());

        return baselineInfoJson.toJSONString();
    }

    public static BaselineInfo deserializeFromJson(String json) {
        try {
            JSONObject baselineInfoJson = JSON.parseObject(json);
            BaselineInfo baselineInfo = new BaselineInfo();
            baselineInfo.id = baselineInfoJson.getIntValue("id");
            baselineInfo.parameterSql = baselineInfoJson.getString("parameterSql");
            baselineInfo.tableSet = deserializeTableSet(baselineInfoJson.getString("tableSet"));

            Map<Integer, PlanInfo> acceptedPlans = new ConcurrentHashMap<>();
            JSONObject acceptedPlansJsonObject = baselineInfoJson.getJSONObject("acceptedPlans");
            for (Map.Entry<String, Object> entry : acceptedPlansJsonObject.entrySet()) {
                acceptedPlans.put(Integer.valueOf(entry.getKey()),
                    PlanInfo.deserializeFromJson(acceptedPlansJsonObject.getString(entry.getKey())));
            }
            baselineInfo.acceptedPlans = acceptedPlans;

            Map<Integer, PlanInfo> unacceptedPlans = new ConcurrentHashMap<>();
            JSONObject unacceptedPlansJsonObject = baselineInfoJson.getJSONObject("unacceptedPlans");
            for (Map.Entry<String, Object> entry : unacceptedPlansJsonObject.entrySet()) {
                unacceptedPlans.put(Integer.valueOf(entry.getKey()),
                    PlanInfo.deserializeFromJson(unacceptedPlansJsonObject.getString(entry.getKey())));
            }
            baselineInfo.unacceptedPlans = unacceptedPlans;
            baselineInfo.extend = baselineInfoJson.getString("extend");
            Boolean dirty = baselineInfoJson.getBoolean("dirty");
            if (Boolean.TRUE.equals(dirty)) {
                baselineInfo.setDirty(true);
            }
            baselineInfo.decodeExtend();
            return baselineInfo;
        } catch (Throwable t) {
            OptimizerAlertUtil.spmAlert(OptimizerAlertType.SPM_SERIALIZATION_ERR, null, t);
            return null;
        }
    }

    public static String serializeTableSet(Set<Pair<String, String>> tableSet) {
        return JSON.toJSONString(tableSet);
    }

    public static Set<Pair<String, String>> deserializeTableSet(String jsonString) {
        Set<Pair<String, String>> tableSet = new HashSet<>();
        JSONArray jsonArray = JSON.parseArray(jsonString);
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject o = jsonArray.getJSONObject(i);
            tableSet.add(Pair.of(o.getString("key"), o.getString("value")));
        }
        return tableSet;
    }

    public String getExtend() {
        if (extend == null) {
            return "";
        }
        return extend;
    }

    public void setExtend(String extend) {
        this.extend = extend;
        decodeExtend();
    }

    public String getHint() {
        if (hint == null) {
            return "";
        }
        return hint;
    }

    public void setHint(String hint) {
        this.hint = hint;
    }

    public boolean isUsePostPlanner() {
        return usePostPlanner;
    }

    public void setUsePostPlanner(boolean usePostPlanner) {
        this.usePostPlanner = usePostPlanner;
    }

    public boolean isRebuildAtLoad() {
        return rebuildAtLoad;
    }

    public void setRebuildAtLoad(boolean rebuildAtLoad) {
        this.rebuildAtLoad = rebuildAtLoad;
    }

    public PlanInfo computeRebuiltAtLoadPlanIfNotExists(Supplier<PlanInfo> planInfoSupplier) {
        if (null == rebuildAtLoadPlan) {
            synchronized (this) {
                if (null == rebuildAtLoadPlan) {
                    rebuildAtLoadPlan = planInfoSupplier.get();
                }
            }
        }

        return rebuildAtLoadPlan;
    }

    /**
     * Resets the rebuild at load plan if the given hash does not match.
     *
     * @param providedHash The hash value to compare against.
     */
    public void resetRebuildAtLoadPlanIfMismatched(int providedHash) {
        // If the rebuild at load plan is already null, just return
        if (rebuildAtLoadPlan == null) {
            return;
        }

        int currentTableHashCode = rebuildAtLoadPlan.getTablesHashCode();

        // If the current hash code does not match the provided hash, clear the rebuild at load plan
        if (currentTableHashCode != providedHash) {
            rebuildAtLoadPlan = null;
        }
    }

    public void resetRebuildAtLoadPlanByForce() {
        rebuildAtLoadPlan = null;
    }

    public PlanInfo getRebuildAtLoadPlan() {
        return rebuildAtLoadPlan;
    }

    public PlanInfo getPlan(int planId) {
        PlanInfo planInfo = acceptedPlans.get(planId);
        if (planInfo != null) {
            return planInfo;
        }
        planInfo = unacceptedPlans.get(planId);
        return planInfo;
    }

    public boolean grayStatus() {
        for (PlanInfo planInfo : acceptedPlans.values()) {
            if (planInfo.isInGrayStatus()) {
                return true;
            }
        }
        return false;
    }

    public List<PlanInfo> getPlans() {
        List<PlanInfo> planInfos = Lists.newArrayList();
        planInfos.addAll(getAcceptedPlans().values());
        planInfos.addAll(getUnacceptedPlans().values());
        return planInfos;
    }

    public Set<Point> getPointSet() {
        return pointSet;
    }

    public void setPointSet(Set<Point> pointSet) {
        this.pointSet = pointSet;
    }

    public Collection<PlanInfo> getFixPlans() {
        Collection<PlanInfo> fixPlans = Sets.newHashSet();
        for (PlanInfo planInfo : acceptedPlans.values()) {
            if (planInfo.isFixed()) {
                fixPlans.add(planInfo);
            }
        }
        return fixPlans;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BaselineInfo)) {
            return false;
        }
        BaselineInfo other = (BaselineInfo) obj;
        return
            this.id == other.id &&
                this.parameterSql.equals(other.parameterSql) &&
                this.dirty == other.dirty &&
                this.hotEvolution == other.hotEvolution &&
                this.getExtend().equals(other.getExtend()) &&
                this.isRebuildAtLoad() == other.isRebuildAtLoad() &&
                this.getHint().equals(other.getHint()) &&
                this.getAcceptedPlans().equals(other.getAcceptedPlans());
    }

    public void merge(String schema, BaselineInfo t) {
        if (!parameterSql.equals(t.parameterSql)) {
            return;
        }
        if (t.isHotEvolution()) {
            this.hotEvolution = true;
        }

        /*
         * Change context:
         * - Before: merging a duplicate planId only OR'd the fixed flag and summed chooseCount;
         *   TABLES_HASHCODE of whichever PlanInfo happened to be inserted first was kept as-is.
         *   That was fine while a plan's tablesHashCode never changed once a planId existed.
         * - Path impact: after a DDL, PlanManager.tryUpdatePlan() rebuilds a fixed plan and bumps
         *   its in-memory tablesHashCode on whichever CN first serves the fix-hint SQL; the planId
         *   stays the same because the rebuilt plan JSON is identical. Other CNs report the same
         *   planId with the old tablesHashCode. Without reconciling here, BASELINE_SYNC merge result
         *   depends on iteration order over per-CN replicas and can discard the fresh hashcode,
         *   causing repeated BASELINE_FIX_UPDATE every sync cycle. Non-fixed plan merging and
         *   different-planId fixed plans in the same baseline are unaffected.
         * - Capability regression: None; when no replica's tablesHashCode matches the current table
         *   version we keep the previous value untouched rather than guessing, so a genuinely stale
         *   baseline still falls back to on-demand rebuild instead of being marked fresh.
         */
        int currentHashCode = PlanManagerUtil.computeTablesVersion(tableSet, schema, null);

        // only merge acceptedPlans
        int maxAcceptedPlans = InstConfUtil.getInt(SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE);
        for (PlanInfo planInfo : t.getAcceptedPlans().values()) {
            if (acceptedPlans.containsKey(planInfo.getId())) {
                PlanInfo merge = acceptedPlans.get(planInfo.getId());

                // merge fixed plan
                if (planInfo.isFixed()) {
                    merge.setFixed(true);
                }
                merge.addChooseCount(planInfo.getChooseCount());

                // reconcile stale TABLES_HASHCODE across per-CN replicas of the same fixed planId
                if (merge.isFixed() && merge.getTablesHashCode() != planInfo.getTablesHashCode()
                    && planInfo.getTablesHashCode() == currentHashCode) {
                    merge.setTablesHashCode(currentHashCode);
                }
                continue;
            } else {
                acceptedPlans.put(planInfo.getId(), planInfo);
            }
        }

        List<Integer> toRemoveList = Lists.newArrayList();
        // remove all unfixed plan when fix num exceeded maxAcceptedPlans num
        if (acceptedPlans.values().stream().filter(p -> p.isFixed()).count() > maxAcceptedPlans) {
            for (PlanInfo p : acceptedPlans.values()) {
                if (!p.isFixed()) {
                    toRemoveList.add(p.getId());
                }
            }
            toRemoveList.forEach(pid -> acceptedPlans.remove(pid));
        } else {
            // remove plan expired[1 week] or was unable to match the table version
            for (PlanInfo p : acceptedPlans.values()) {
                if (isNeedRemove(schema, p)) {
                    ModuleLogInfo.getInstance()
                        .logInfo(SPM, REMOVE, new String[] {
                            "BASELINE REMOVE", schema + "," + id + "," + p.getId() + "," + p.getLastExecuteTime()});
                    toRemoveList.add(p.getId());
                }
            }
            toRemoveList.forEach(pid -> acceptedPlans.remove(pid));

            // Remove accepted plan until the size fits SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE
            while (acceptedPlans.size() >= maxAcceptedPlans) {
                int minChooseCount = Integer.MAX_VALUE;
                int toRemove = -1;
                for (PlanInfo p : acceptedPlans.values()) {
                    if (p.getChooseCount() <= minChooseCount && !p.isFixed()) {
                        minChooseCount = p.getChooseCount();
                        toRemove = p.getId();
                    }
                }
                acceptedPlans.remove(toRemove);
            }
        }

        // merge point
        Set<Point> newPoints = new HashSet<>();

        Stream<Point> pointSetStream = pointSet.stream()
            .filter(point -> point != null)
            .filter(point -> acceptedPlans.containsKey(point.getPlanId()));

        Stream<Point> targetPointSetStream = t.getPointSet().stream()
            .filter(point -> point != null)
            .filter(point -> acceptedPlans.containsKey(point.getPlanId()));

        newPoints.addAll(Stream.concat(pointSetStream, targetPointSetStream).collect(Collectors.toList()));

        pointSet = newPoints;
    }

    /**
     * remove plans with a mismatching tbl version or is not used recently
     *
     * @param schema baseline schema
     * @param p plan
     */
    private boolean isNeedRemove(String schema, PlanInfo p) {
        if (p == null || p.isFixed()) {
            return false;
        }

        // Change context:
        // - Before: getPlan(null, null) was passed directly to PlanManagerUtil.baselineSupported(), which
        //   returns false for a null RelNode. During baseline sync merge a PlanInfo deserialized from JSON
        //   or loaded from storage only carries compressPlanByteArray and cannot rebuild its RelNode without
        //   schema/ExecutionContext, so getPlan(null, null) returns null and every such accepted plan was
        //   wrongly judged unsupported and removed. In addition, isRecentlyExecuted() returns false for
        //   never-executed plans (lastExecuteTime null in memory, 0 after the storage roundtrip), so freshly
        //   added user baselines were wiped out by the same merge even though "never executed" is not
        //   "expired one week".
        // - Path impact: only the BaselineInfo.merge() cleanup branch (SPM sync merge). Materialized plans
        //   that are truly unsupported (e.g. BaseTableOperation), executed plans stale beyond
        //   SPM_RECENTLY_EXECUTED_PERIOD, and table-version mismatches are still removed. serializeToJson,
        //   PlanManager.evolve and baseline handler call sites are unchanged.
        // - Capability regression: None. Null plan means "not materialized", not "unsupported" (same
        //   null-skip pattern already used by serializeToJson). Never-executed plans fall back to createTime
        //   for the recent-window judgement, so fresh baselines are kept within the window and stale
        //   never-executed baselines still expire; only plans with both timestamps invalid are kept, which
        //   is the conservative choice over wrongly dropping user baselines.
        RelNode planRel = p.getPlan(null, null);
        if (planRel != null && !PlanManagerUtil.baselineSupported(planRel)) {
            return true;
        }

        // For never-executed plans (lastExecuteTime null in memory, 0 after the storage roundtrip)
        // fall back to createTime so they are kept within the recent window but still expire once
        // their creation time is beyond it, instead of living forever.
        Long lastExecuteTime = p.getLastExecuteTime();
        boolean isRecentlyUsed;
        if (lastExecuteTime != null && lastExecuteTime > 0) {
            isRecentlyUsed = PlanManager.isRecentlyExecuted(p);
        } else if (p.getCreateTime() > 0) {
            long recentTimePeriod = InstConfUtil.getLong(SPM_RECENTLY_EXECUTED_PERIOD);
            isRecentlyUsed = System.currentTimeMillis() - p.getCreateTime() * 1000 <= recentTimePeriod;
        } else {
            // both timestamps invalid: keep the plan rather than wrongly dropping it
            isRecentlyUsed = true;
        }
        int tblHashcode = PlanManagerUtil.computeTablesVersion(tableSet, schema, null);
        boolean isTableVersionMatch = p.getTablesHashCode() == tblHashcode;
        return !isRecentlyUsed || !isTableVersionMatch;
    }

    /**
     * Clear unaccepted and unfixed plans, and mark fixed plans for an on-demand rebuild.
     *
     * @return plan ids that should also be removed from metadb. Fixed plan ids must not be returned because the
     * fixed plan row is intentionally retained while its in-memory tables hash code is reset.
     */
    public Set<Integer> clearAllPlans() {
        Set<Integer> removeList = Sets.newHashSet();
        removeList.addAll(unacceptedPlans.keySet());
        unacceptedPlans.clear();

        List<PlanInfo> invalidatePlanInfo = new ArrayList<>();
        for (PlanInfo planInfo : acceptedPlans.values()) {
            if (!planInfo.isFixed()) {
                removeList.add(planInfo.getId());
                invalidatePlanInfo.add(planInfo);
            } else {
                planInfo.setTablesHashCode(REBUILD_PLAN_HASH_CODE);
            }
        }
        for (PlanInfo planInfo : invalidatePlanInfo) {
            removeAcceptedPlan(planInfo.getId());
        }
        return removeList;
    }

    private void decodeExtend() {
        if (extend == null || "".equals(extend)) {
            return;
        }
        Map<String, Object> extendMap = JSON.parseObject(extend);
        pointSet = PlanManagerUtil.jsonToPoints(
            parameterSql,
            (List<Map<String, Object>>) Optional
                .ofNullable(
                    extendMap.get(EXTEND_POINT_SET))
                .orElse(new ArrayList<>()));
        hint = extendMap.getOrDefault(EXTEND_HINT, "").toString();
        usePostPlanner = Boolean.parseBoolean(
            extendMap
                .getOrDefault(EXTEND_USE_POST_PLANNER, true)
                .toString());
        rebuildAtLoad = Boolean.parseBoolean(
            extendMap
                .getOrDefault(EXTEND_REBUILD_AT_LOAD, false)
                .toString());
        hotEvolution = Boolean.parseBoolean(
            extendMap
                .getOrDefault(EXTEND_HOT_EVOLVED, false)
                .toString());
    }

    public String encodeExtend() {
        Map<String, Object> extendMap = Maps.newHashMap();
        if (!StringUtils.isEmpty(hint)) {
            extendMap.put(EXTEND_POINT_SET, PlanManagerUtil.pointsTolist(pointSet));
            extendMap.put(EXTEND_HINT, hint);
            extendMap.put(EXTEND_USE_POST_PLANNER, usePostPlanner);
            extendMap.put(EXTEND_REBUILD_AT_LOAD, rebuildAtLoad);
        }
        if (hotEvolution) {
            extendMap.put(EXTEND_HOT_EVOLVED, hotEvolution);
        }
        if (extendMap.isEmpty()) {
            return "";
        }
        final JsonBuilder jsonBuilder = new JsonBuilder();
        return jsonBuilder.toJsonString(extendMap);
    }

    public static boolean hotEvolution(String extend) {
        if (StringUtils.isEmpty(extend)) {
            return false;
        }
        Map<String, Object> extendMap = JSON.parseObject(extend);
        return Boolean.parseBoolean(extendMap.getOrDefault(EXTEND_HOT_EVOLVED, false).toString());
    }

    public BaselineInfoRecord buildBaselineRecord(String schemaName, String instId) {
        BaselineInfoRecord baselineInfoRecord = new BaselineInfoRecord();
        baselineInfoRecord.setInstId(instId);
        baselineInfoRecord.setSchemaName(schemaName);
        baselineInfoRecord.setId(this.id);
        baselineInfoRecord.setSql(this.parameterSql);
        baselineInfoRecord.setTableSet(serializeTableSet(this.tableSet));
        baselineInfoRecord.setExtendField(this.encodeExtend());

        return baselineInfoRecord;
    }

    public List<BaselineInfoRecord> buildPlanRecord(String schemaName, String instId) {
        if (this.isRebuildAtLoad()) {
            return Collections.emptyList();
        }
        List<BaselineInfoRecord> rs = Lists.newArrayList();
        for (PlanInfo planInfo : acceptedPlans.values()) {
            BaselineInfoRecord baselineInfoRecord = new BaselineInfoRecord();
            baselineInfoRecord.setInstId(instId);
            baselineInfoRecord.setSchemaName(schemaName);
            baselineInfoRecord.setId(this.getId());

            baselineInfoRecord.setTablesHashCode(planInfo.getTablesHashCode());
            baselineInfoRecord.setPlanId(planInfo.getId());
            baselineInfoRecord.setPlan(planInfo.getPlanJsonString());
            // Change context:
            // - Before: only lastExecuteTime == null was mapped to -1 (stored as SQL NULL). After a
            //   baseline sync roundtrip a never-executed plan comes back with lastExecuteTime == 0
            //   (rs.getLong on a NULL timestamp yields 0), and persisting it wrote Timestamp(0) which
            //   metadb rejects ("Incorrect datetime value: '1970-01-01 08:00:00.0'"), breaking the whole
            //   baseline persist of the sync job.
            // - Path impact: only BaselineInfo.buildPlanRecord() used by baseline persist. 0 and null both
            //   mean "never executed" and are stored as NULL, identical to the existing -1 convention.
            // - Capability regression: None. A genuine epoch-0 execution time cannot exist.
            baselineInfoRecord.setLastExecuteTime(
                planInfo.getLastExecuteTime() == null || planInfo.getLastExecuteTime() <= 0
                    ? -1
                    : planInfo.getLastExecuteTime());
            baselineInfoRecord.setChooseCount(planInfo.getChooseCount());
            baselineInfoRecord.setCost(planInfo.getCost());
            baselineInfoRecord.setEstimateExecutionTime(planInfo.getEstimateExecutionTime());
            baselineInfoRecord.setFixed(planInfo.isFixed());
            baselineInfoRecord.setTraceId(planInfo.getTraceId());
            baselineInfoRecord.setCreateTime(planInfo.getCreateTime());
            baselineInfoRecord.setOrigin(planInfo.getOrigin());
            baselineInfoRecord.setPlanExtend(planInfo.getExtend());
            baselineInfoRecord.setVersion(planInfo.getVersion());
            rs.add(baselineInfoRecord);
        }
        return rs;
    }
}
