package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.BaselineQueryAllGraySyncAction;
import com.alibaba.polardbx.executor.sync.FetchPlanCacheByIdsSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.view.InformationSchemaGrayStatus;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.Maps;
import org.glassfish.jersey.internal.guava.Sets;

import java.text.DecimalFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handler for information_schema.spm_gray_status view
 * Displays gray release monitoring information for SPM plans
 *
 * @author fangwu
 */
public class InformationSchemaGrayStatusHandler extends BaseVirtualViewSubClassHandler {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.00");
    private static final String YES = "YES";
    private static final String NO = "NO";
    private static final String PLAN_CACHE_ORIGIN = "PLAN_CACHE";
    private static final int NO_GRAY_PERCENTAGE = -1;

    public InformationSchemaGrayStatusHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaGrayStatus;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        Map<String, Map<String, Map<String, JSONObject>>> grayBaselineMap = queryGrayBaseline();

        // Collect all baseline IDs from all compute nodes
        Set<Integer> allBaselineIds = Sets.newHashSet();

        // Process each compute node
        for (Map.Entry<String, Map<String, Map<String, JSONObject>>> nodeEntry : grayBaselineMap.entrySet()) {
            String cnNode = nodeEntry.getKey();
            Map<String, Map<String, JSONObject>> instBaseline = nodeEntry.getValue();

            // Collect baseline IDs and add gray plan rows
            Set<Integer> baselineIds = processGrayBaselines(cnNode, instBaseline, cursor);
            allBaselineIds.addAll(baselineIds);
        }

        // Fetch and add plan cache rows once for all baseline IDs
        if (!allBaselineIds.isEmpty()) {
            addPlanCacheRows(allBaselineIds, cursor);
        }

        return cursor;
    }

    /**
     * Process gray baselines for a compute node and collect baseline IDs
     *
     * @param cnNode compute node name
     * @param instBaseline baseline map for the node
     * @param cursor result cursor to add rows
     * @return set of baseline IDs
     */
    private Set<Integer> processGrayBaselines(String cnNode, Map<String, Map<String, JSONObject>> instBaseline,
                                              ArrayResultCursor cursor) {
        Set<Integer> baselineIds = Sets.newHashSet();

        // Process each schema
        for (Map.Entry<String, Map<String, JSONObject>> schemaEntry : instBaseline.entrySet()) {
            String schema = schemaEntry.getKey();
            Collection<JSONObject> baselineInfos = schemaEntry.getValue().values();

            if (baselineInfos.isEmpty()) {
                continue;
            }

            // Process each baseline
            for (JSONObject baselineInfo : baselineInfos) {
                Integer baselineId = baselineInfo.getIntValue("id");
                baselineIds.add(baselineId);

                String parameterSql = baselineInfo.getString("parameterSql");
                JSONObject acceptedPlans = baselineInfo.getJSONObject("acceptedPlans");

                // Process each accepted plan
                processAcceptedPlans(cnNode, schema, baselineId, parameterSql, acceptedPlans, cursor);
            }
        }

        return baselineIds;
    }

    /**
     * Process accepted plans for a baseline
     *
     * @param cnNode compute node name
     * @param schema schema name
     * @param baselineId baseline ID
     * @param parameterSql parameterized SQL
     * @param acceptedPlans accepted plans JSON object
     * @param cursor result cursor to add rows
     */
    private void processAcceptedPlans(String cnNode, String schema, Integer baselineId, String parameterSql,
                                      JSONObject acceptedPlans, ArrayResultCursor cursor) {
        for (Map.Entry<String, Object> planEntry : acceptedPlans.entrySet()) {
            String planId = planEntry.getKey();
            JSONObject planInfo = acceptedPlans.getJSONObject(planId);

            // Extract plan info
            long chooseCount = planInfo.getLongValue("chooseCount");
            int errorCount = planInfo.getIntValue("errorCount");
            double lastTenAvgRt = planInfo.getDoubleValue("lastTenAvgRt");
            int grayPercentage = planInfo.getIntValue("grayPercentage");
            boolean isGray = grayPercentage > 0;
            boolean fixed = planInfo.getBooleanValue("fixed");
            String planExplain = planInfo.getString("planExplain");

            // Decode extend info
            String extend = planInfo.getString("extend");
            Map<String, String> extendMap = PlanInfo.decodeExtendForShow(extend);
            String fixHint = extendMap.get("FIX_HINT");
            String fixExpr = extendMap.get("FIX_EXPR");

            // Determine plan origin
            String origin = determinePlanOrigin(isGray, fixed);

            // Add row to cursor
            addGrayPlanRow(cursor, cnNode, schema, baselineId, parameterSql, planId,
                grayPercentage, isGray, chooseCount, lastTenAvgRt, errorCount,
                origin, fixHint, fixExpr, planExplain);
        }
    }

    /**
     * Determine the origin of a plan based on its properties
     *
     * @param isGray whether the plan is in gray release
     * @param fixed whether the plan is fixed
     * @return plan origin string
     */
    private String determinePlanOrigin(boolean isGray, boolean fixed) {
        if (isGray) {
            return PlanManager.PLAN_SOURCE.SPM_FIX_GRAY.name();
        } else if (fixed) {
            return PlanManager.PLAN_SOURCE.SPM_FIX.name();
        } else {
            return PlanManager.PLAN_SOURCE.SPM_ACCEPT.name();
        }
    }

    /**
     * Add a gray plan row to the cursor
     */
    private void addGrayPlanRow(ArrayResultCursor cursor, String cnNode, String schema, Integer baselineId,
                                String parameterSql, String planId, int grayPercentage, boolean isGray,
                                long chooseCount, double lastTenAvgRt, int errorCount, String origin,
                                String fixHint, String fixExpr, String planExplain) {
        cursor.addRow(new Object[] {
            cnNode,
            schema,
            baselineId,
            PlanCache.CacheKey.getTemplateId(parameterSql),
            parameterSql,
            planId,
            grayPercentage,
            isGray ? YES : NO,
            chooseCount,
            lastTenAvgRt,
            errorCount,
            origin,
            fixHint,
            fixExpr,
            planExplain
        });
    }

    /**
     * Fetch plan cache data and add rows to cursor
     *
     * @param baselineIds set of baseline IDs to fetch
     * @param cursor result cursor to add rows
     */
    private void addPlanCacheRows(Set<Integer> baselineIds, ArrayResultCursor cursor) {
        List<List<Map<String, Object>>> results = SyncManagerHelper.syncIgnoreExceptions(
            new FetchPlanCacheByIdsSyncAction(baselineIds),
            SystemDbHelper.INFO_SCHEMA_DB_NAME,
            SyncScope.CURRENT_ONLY);

        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }

            for (Map<String, Object> row : nodeRows) {
                addPlanCacheRow(cursor, row);
            }
        }
    }

    /**
     * Add a plan cache row to the cursor
     */
    private void addPlanCacheRow(ArrayResultCursor cursor, Map<String, Object> row) {
        cursor.addRow(new Object[] {
            row.get("COMPUTE_NODE"),
            row.get("SCHEMA_NAME"),
            row.get("BASELINE_ID"),
            row.get("TEMP_ID"),
            row.get("STATEMENT"),
            row.get("PLAN_ID"),
            NO_GRAY_PERCENTAGE,
            NO,
            row.get("HIT_COUNT"),
            row.get("LAST_TEN_AVG_RT"),
            row.get("ERROR_COUNT"),
            PLAN_CACHE_ORIGIN,
            null,
            null,
            row.get("PLAN")
        });
    }

    protected Map<String, Map<String, Map<String, JSONObject>>> queryGrayBaseline() {
        List<List<Map<String, Object>>> results =
            GmsSyncManagerHelper.sync(new BaselineQueryAllGraySyncAction(), SystemDbHelper.DEFAULT_DB_NAME,
                SyncScope.CURRENT_ONLY);

        Map<String, Map<String, Map<String, JSONObject>>> instSchemaSqlBaselineMap = Maps.newConcurrentMap();
        // Node
        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }
            Map<String, Object> row = nodeRows.get(0);
            String cn = (String) row.get("COMPUTE_NODE");
            String baselines = (String) row.get("BASELINES");
            Map<String, Map<String, JSONObject>> temp = PlanManager.getBaselineForShowFromJson(baselines);

            instSchemaSqlBaselineMap.put(cn, temp);
        }
        return instSchemaSqlBaselineMap;
    }

}
