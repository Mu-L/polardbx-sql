package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlaceHolderExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.Map;
import java.util.Set;

/**
 * Sync action to fetch plan cache entries by template hash IDs.
 * This action retrieves execution plan details from the plan cache for specified template IDs.
 *
 * @author fangwu
 */
public class FetchPlanCacheByIdsSyncAction implements ISyncAction {

    private static final String TABLE_NAME = "PLAN_CACHE";
    private static final String COLUMN_COMPUTE_NODE = "COMPUTE_NODE";
    private static final String COLUMN_SCHEMA_NAME = "SCHEMA_NAME";
    private static final String COLUMN_BASELINE_ID = "BASELINE_ID";
    private static final String COLUMN_TEMP_ID = "TEMP_ID";
    private static final String COLUMN_STATEMENT = "STATEMENT";
    private static final String COLUMN_PLAN_ID = "PLAN_ID";
    private static final String COLUMN_HIT_COUNT = "HIT_COUNT";
    private static final String COLUMN_PLAN = "PLAN";
    private static final String COLUMN_LAST_TEN_AVG_RT = "LAST_TEN_AVG_RT";
    private static final String COLUMN_ERROR_COUNT = "ERROR_COUNT";

    private static final String PLAN_DIRECT = "DIRECT";
    private static final String PLAN_UNAVAILABLE = "UNAVAILABLE";
    private static final Long DEFAULT_COUNT = 0L;
    private static final Float DEFAULT_RT = 0.0f;

    private Set<Integer> ids;

    public FetchPlanCacheByIdsSyncAction(Set<Integer> ids) {
        this.ids = ids;
    }

    /**
     * Executes the sync action to fetch plan cache entries.
     *
     * @return ResultCursor containing plan cache information
     */
    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = createResultCursor();

        // Return empty result if IDs are null or empty
        if (ids == null || ids.isEmpty()) {
            return result;
        }

        // Get plan cache instance safely
        PlanCache planCache = PlanCache.getInstance();
        if (planCache == null || planCache.getCache() == null) {
            return result;
        }

        // Iterate through cache entries
        for (Map.Entry<PlanCache.CacheKey, ExecutionPlan> entry : planCache.getCache().asMap().entrySet()) {
            if (entry == null) {
                continue;
            }

            PlanCache.CacheKey cacheKey = entry.getKey();
            ExecutionPlan executionPlan = entry.getValue();

            // Skip if key or value is null
            if (cacheKey == null || executionPlan == null) {
                continue;
            }

            // Skip built-in database schemas
            String schema = cacheKey.getSchema();
            if (schema != null && SystemDbHelper.isDBBuildIn(schema)) {
                continue;
            }

            // Filter by template hash
            Integer templateHash = cacheKey.getTemplateHash();
            if (templateHash == null || !ids.contains(templateHash)) {
                continue;
            }

            // Build row data with null-safety
            Object[] rowData = buildRowData(cacheKey, executionPlan);
            result.addRow(rowData);
        }

        return result;
    }

    /**
     * Creates and initializes the result cursor with appropriate schema.
     *
     * @return Initialized ArrayResultCursor
     */
    private ArrayResultCursor createResultCursor() {
        ArrayResultCursor cursor = new ArrayResultCursor(TABLE_NAME);
        cursor.addColumn(COLUMN_COMPUTE_NODE, DataTypes.StringType);
        cursor.addColumn(COLUMN_SCHEMA_NAME, DataTypes.StringType);
        cursor.addColumn(COLUMN_BASELINE_ID, DataTypes.StringType);
        cursor.addColumn(COLUMN_TEMP_ID, DataTypes.StringType);
        cursor.addColumn(COLUMN_STATEMENT, DataTypes.StringType);
        cursor.addColumn(COLUMN_PLAN_ID, DataTypes.StringType);
        cursor.addColumn(COLUMN_HIT_COUNT, DataTypes.LongType);
        cursor.addColumn(COLUMN_PLAN, DataTypes.StringType);
        cursor.addColumn(COLUMN_LAST_TEN_AVG_RT, DataTypes.FloatType);
        cursor.addColumn(COLUMN_ERROR_COUNT, DataTypes.LongType);
        return cursor;
    }

    /**
     * Builds a row of data from cache key and execution plan with null-safety.
     *
     * @param cacheKey Cache key containing query information
     * @param executionPlan Execution plan with statistics
     * @return Array of row data
     */
    private Object[] buildRowData(PlanCache.CacheKey cacheKey, ExecutionPlan executionPlan) {
        String computeNode = TddlNode.getHost() + ":" + TddlNode.getPort();
        String schema = safeGetString(cacheKey.getSchema());
        Integer baselineId = cacheKey.getTemplateHash();
        String tempId = safeGetString(cacheKey.getTemplateId());
        String statement = safeGetString(cacheKey.getParameterizedSql());

        // Get plan ID safely
        Integer planId = getPlanId(executionPlan);

        // Get statistics safely
        Long hitCount = safeGetAtomicLong(executionPlan.getHitCount());
        String plan = getPlanString(executionPlan);
        Float avgRT = safeGetDouble(executionPlan.getAverageExecutionTime());
        Long errorCount = safeGetInt(executionPlan.getErrorCount());

        return new Object[] {
            computeNode,
            schema,
            baselineId,
            tempId,
            statement,
            planId,
            hitCount,
            plan,
            avgRT,
            errorCount
        };
    }

    /**
     * Gets plan ID from execution plan with null-safety.
     *
     * @param executionPlan Execution plan
     * @return Plan ID hash code, or null if plan is unavailable
     */
    private Integer getPlanId(ExecutionPlan executionPlan) {
        if (executionPlan == null) {
            return null;
        }

        try {
            RelNode relNode = executionPlan.getPlan();
            if (relNode == null) {
                return null;
            }
            return PlanManagerUtil.relNodeToJson(relNode).hashCode();
        } catch (Exception e) {
            // Return null if any exception occurs during plan ID generation
            return null;
        }
    }

    /**
     * Gets plan string representation with null-safety.
     *
     * @param executionPlan Execution plan
     * @return Plan string or "DIRECT"/"UNAVAILABLE"
     */
    private String getPlanString(ExecutionPlan executionPlan) {
        if (executionPlan == null) {
            return PLAN_UNAVAILABLE;
        }

        if (executionPlan == PlaceHolderExecutionPlan.INSTANCE) {
            return PLAN_DIRECT;
        }

        try {
            RelNode relNode = executionPlan.getPlan();
            if (relNode == null) {
                return PLAN_UNAVAILABLE;
            }
            return "\n" + RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT,
                SqlExplainLevel.NO_ATTRIBUTES);
        } catch (Exception e) {
            // Return UNAVAILABLE if any exception occurs during plan dump
            return PLAN_UNAVAILABLE;
        }
    }

    /**
     * Safely gets string value, returns empty string if null.
     *
     * @param value String value
     * @return Non-null string
     */
    private String safeGetString(String value) {
        return value != null ? value : "";
    }

    /**
     * Safely gets Long value from AtomicLong with default.
     *
     * @param value AtomicLong value
     * @return Non-null Long value
     */
    private Long safeGetAtomicLong(java.util.concurrent.atomic.AtomicLong value) {
        return value != null ? value.longValue() : DEFAULT_COUNT;
    }

    /**
     * Safely gets Float value from double with default.
     *
     * @param value double value
     * @return Non-null Float value
     */
    private Float safeGetDouble(double value) {
        return (float) value;
    }

    /**
     * Safely gets Long value from int.
     *
     * @param value int value
     * @return Non-null Long value
     */
    private Long safeGetInt(int value) {
        return (long) value;
    }

    public Set<Integer> getIds() {
        return ids;
    }

    public void setIds(Set<Integer> ids) {
        this.ids = ids;
    }
}

