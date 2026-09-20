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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.BASELINE_INFO;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.PLAN_INFO;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.SPM_BASELINE;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.SPM_PLAN;

/**
 * @author jilong.ljl
 */
public class BaselineInfoAccessor extends AbstractAccessor implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaselineInfoAccessor.class);

    private static final String BASELINE_INFO_TABLE = wrap(SPM_BASELINE);

    private static final String PLAN_INFO_TABLE = wrap(SPM_PLAN);

    private static final String LOAD_BASELINE =
        "SELECT BASELINE_INFO.SCHEMA_NAME, "
            + "BASELINE_INFO.ID, "
            + "BASELINE_INFO.SQL, "
            + "BASELINE_INFO.TABLE_SET, "
            + "BASELINE_INFO.EXTEND_FIELD,"
            + "PLAN_INFO.TABLES_HASHCODE, "
            + "PLAN_INFO.ID, PLAN_INFO.PLAN, "
            + "UNIX_TIMESTAMP(PLAN_INFO.LAST_EXECUTE_TIME), "
            + "PLAN_INFO.CHOOSE_COUNT, "
            + "PLAN_INFO.COST, "
            + "PLAN_INFO.ESTIMATE_EXECUTION_TIME, "
            + "PLAN_INFO.ACCEPTED, "
            + "PLAN_INFO.FIXED, "
            + "PLAN_INFO.TRACE_ID, "
            + "PLAN_INFO.ORIGIN, "
            + "PLAN_INFO.EXTEND_FIELD AS PLAN_EXTEND, "
            + "PLAN_INFO.VERSION AS VERSION, "
            + "UNIX_TIMESTAMP(PLAN_INFO.GMT_MODIFIED), "
            + "UNIX_TIMESTAMP(PLAN_INFO.GMT_CREATED) FROM " + BASELINE_INFO_TABLE + " AS BASELINE_INFO LEFT JOIN "
            + PLAN_INFO_TABLE + " AS PLAN_INFO ON "
            + "BASELINE_INFO.SCHEMA_NAME = PLAN_INFO.SCHEMA_NAME AND "
            + "BASELINE_INFO.INST_ID = PLAN_INFO.INST_ID AND "
            + "BASELINE_INFO.ID = PLAN_INFO.BASELINE_ID "
            + "WHERE UNIX_TIMESTAMP(BASELINE_INFO.GMT_MODIFIED) > ? AND "
            + "(PLAN_INFO.GMT_MODIFIED IS NULL OR UNIX_TIMESTAMP(PLAN_INFO.GMT_MODIFIED) > ?)"
            + " AND BASELINE_INFO.INST_ID=? ";
    private static final String MOVE_BASELINE_INFO_TO_SPM_BASELINE =
        "INSERT INTO " + BASELINE_INFO_TABLE
            + "(`ID`, `INST_ID`, `SCHEMA_NAME`, `GMT_MODIFIED`, `GMT_CREATED`, `SQL`, `TABLE_SET`, `EXTEND_FIELD`) "
            + "SELECT  `ID`, ?,`SCHEMA_NAME`, `GMT_MODIFIED`, `GMT_CREATED`, `SQL`, `TABLE_SET`, `EXTEND_FIELD` FROM "
            + BASELINE_INFO;

    private static final String MOVE_PLAN_INFO_TO_SPM_PLAN =
        "INSERT INTO " + PLAN_INFO_TABLE
            + "(`ID`, `INST_ID`, `SCHEMA_NAME`, `BASELINE_ID`, `GMT_MODIFIED`, `GMT_CREATED`, `LAST_EXECUTE_TIME`, `PLAN`, `PLAN_TYPE`, "
            + "`PLAN_ERROR`, `CHOOSE_COUNT`, `COST`, `ESTIMATE_EXECUTION_TIME`, `ACCEPTED`, `FIXED`, `TRACE_ID`, `ORIGIN`, "
            + "`ESTIMATE_OPTIMIZE_TIME`, `CPU`, `MEMORY`, `IO`, `NET`, `TABLES_HASHCODE`, `EXTEND_FIELD`) "
            + "SELECT `ID`, ?, `SCHEMA_NAME`, `BASELINE_ID`, `GMT_MODIFIED`, `GMT_CREATED`, `LAST_EXECUTE_TIME`, `PLAN`, `PLAN_TYPE`, "
            + "`PLAN_ERROR`, `CHOOSE_COUNT`, `COST`, `ESTIMATE_EXECUTION_TIME`, `ACCEPTED`, `FIXED`, `TRACE_ID`, `ORIGIN`, "
            + "`ESTIMATE_OPTIMIZE_TIME`, `CPU`, `MEMORY`, `IO`, `NET`, `TABLES_HASHCODE`, `EXTEND_FIELD` FROM "
            + PLAN_INFO;

    private static final String CHECK_BASELINE_INFO_NOT_EMPTY = "SELECT 1 FROM " + BASELINE_INFO + " LIMIT 1";

    private static final String CHECK_PLAN_INFO_NOT_EMPTY = "SELECT 1 FROM " + PLAN_INFO + " LIMIT 1";

    private static final String BASELINE_COUNT = "SELECT COUNT(1) FROM " + BASELINE_INFO_TABLE + "WHERE INST_ID=?";

    private static final String DELETE_BASELINE_BY_SCHEMA =
        "DELETE FROM " + BASELINE_INFO_TABLE + " WHERE SCHEMA_NAME=?";

    private static final String DELETE_PLAN_BY_SCHEMA = "DELETE FROM " + PLAN_INFO_TABLE + " WHERE SCHEMA_NAME=?";

    public static final String DELETE_PLAN =
        "DELETE FROM " + PLAN_INFO_TABLE + " WHERE SCHEMA_NAME = ? AND BASELINE_ID = ? AND ID = ? AND INST_ID=?";

    private static final String DELETE_BASELINE =
        "DELETE BASELINE_INFO, PLAN_INFO FROM " + BASELINE_INFO_TABLE + " AS BASELINE_INFO LEFT JOIN "
            + PLAN_INFO_TABLE + " AS PLAN_INFO ON "
            + "BASELINE_INFO.SCHEMA_NAME = PLAN_INFO.SCHEMA_NAME AND "
            + "BASELINE_INFO.INST_ID = PLAN_INFO.INST_ID AND "
            + "BASELINE_INFO.ID = PLAN_INFO.BASELINE_ID "
            + "WHERE BASELINE_INFO.SCHEMA_NAME  = ? AND BASELINE_INFO.ID = ? AND BASELINE_INFO.INST_ID=?";

    private static final String DELETE_BASELINE_BY_NOT_IN_INST =
        "DELETE BASELINE_INFO, PLAN_INFO FROM " + BASELINE_INFO_TABLE + " AS BASELINE_INFO LEFT JOIN "
            + PLAN_INFO_TABLE + " AS PLAN_INFO ON "
            + "BASELINE_INFO.SCHEMA_NAME = PLAN_INFO.SCHEMA_NAME AND "
            + "BASELINE_INFO.INST_ID = PLAN_INFO.INST_ID AND "
            + "BASELINE_INFO.ID = PLAN_INFO.BASELINE_ID "
            + "WHERE BASELINE_INFO.INST_ID NOT IN (%s) "
            + "AND PLAN_INFO.FIXED!=1";

    private static final String DELETE_BASELINE_BY_NOT_IN_SCHEMA =
        "DELETE BASELINE_INFO, PLAN_INFO FROM " + BASELINE_INFO_TABLE + " AS BASELINE_INFO LEFT JOIN "
            + PLAN_INFO_TABLE + " AS PLAN_INFO ON "
            + "BASELINE_INFO.SCHEMA_NAME = PLAN_INFO.SCHEMA_NAME AND "
            + "BASELINE_INFO.INST_ID = PLAN_INFO.INST_ID AND "
            + "BASELINE_INFO.ID = PLAN_INFO.BASELINE_ID "
            + "WHERE BASELINE_INFO.INST_ID ='%s' AND BASELINE_INFO.SCHEMA_NAME NOT IN (%s) "
            + "AND PLAN_INFO.FIXED!=1";

    private static final String DELETE_BASELINE_BY_NOT_IN_BASELINE_ID =
        "DELETE BASELINE_INFO, PLAN_INFO FROM " + BASELINE_INFO_TABLE + " AS BASELINE_INFO LEFT JOIN "
            + PLAN_INFO_TABLE + " AS PLAN_INFO ON "
            + "BASELINE_INFO.SCHEMA_NAME = PLAN_INFO.SCHEMA_NAME AND "
            + "BASELINE_INFO.INST_ID = PLAN_INFO.INST_ID AND "
            + "BASELINE_INFO.ID = PLAN_INFO.BASELINE_ID "
            + "WHERE BASELINE_INFO.INST_ID ='%s' AND BASELINE_INFO.SCHEMA_NAME ='%s' AND BASELINE_INFO.ID NOT IN(%s) "
            + "AND PLAN_INFO.FIXED!=1";

    private static final String DELETE_BASELINE_BY_INST_SCHEMA =
        "DELETE BASELINE_INFO, PLAN_INFO FROM " + BASELINE_INFO_TABLE + " AS BASELINE_INFO LEFT JOIN "
            + PLAN_INFO_TABLE + " AS PLAN_INFO ON "
            + "BASELINE_INFO.SCHEMA_NAME = PLAN_INFO.SCHEMA_NAME AND "
            + "BASELINE_INFO.INST_ID = PLAN_INFO.INST_ID AND "
            + "BASELINE_INFO.ID = PLAN_INFO.BASELINE_ID "
            + "WHERE BASELINE_INFO.INST_ID ='%s' AND BASELINE_INFO.SCHEMA_NAME ='%s' "
            + "AND PLAN_INFO.FIXED!=1";

    private static final String DELETE_DRIFT_PLAN =
        "DELETE FROM " + PLAN_INFO_TABLE + " WHERE BASELINE_ID NOT IN(SELECT ID FROM " + BASELINE_INFO_TABLE + ")";

    private static final String DELETE_DRIFT_BASELINE =
        "DELETE FROM " + BASELINE_INFO_TABLE + " WHERE ID NOT IN(SELECT BASELINE_ID FROM " + PLAN_INFO_TABLE
            + ") AND EXTEND_FIELD NOT LIKE '%REBUILD_AT_LOAD%'";

    private static final String REPLACE_BASELINE =
        "REPLACE INTO " + BASELINE_INFO_TABLE
            + "(`SCHEMA_NAME`,`INST_ID`, `ID`, `SQL`, `TABLE_SET`, `EXTEND_FIELD`) "
            + "VALUES (?, ?, ?, ?, ?, ?)";

    private static final String REPLACE_PLAN =
        "REPLACE INTO " + PLAN_INFO_TABLE
            + "(`INST_ID`, "
            + "`SCHEMA_NAME`, "
            + "`ID`, "
            + "`BASELINE_ID`, "
            + "`LAST_EXECUTE_TIME`, "
            + "`PLAN`, "
            + "`CHOOSE_COUNT`, "
            + "`COST`, "
            + "`ESTIMATE_EXECUTION_TIME`, "
            + "`ACCEPTED`, "
            + "`FIXED`, "
            + "`TRACE_ID`, "
            + "`ORIGIN`, "
            + "`TABLES_HASHCODE`, "
            + "`EXTEND_FIELD`, "
            + "`VERSION`)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    /*
     * Change context:
     * - Before: only LAST_EXECUTE_TIME/CHOOSE_COUNT were refreshed here, because this branch was
     *   originally meant for plan usage statistics only, on the assumption that a plan's shape
     *   (and therefore its TABLES_HASHCODE) never changes while its plan id stays the same.
     * - Path impact: fixed plans rebuilt in-memory by PlanManager.tryUpdatePlan() after a DDL bumps
     *   the table version keep the same plan id, so persist() takes this UPDATE_PLAN_STATS branch;
     *   without TABLES_HASHCODE here the new hashcode never reaches metadb.spm_plan, causing the
     *   hourly BASELINE_SYNC load to see the stale hashcode and rebuild the same fixed plan again.
     *   Non-fixed/normal plan persistence paths (REPLACE_PLAN, full insert) are unaffected.
     * - Capability regression: None; TABLES_HASHCODE is now kept in sync with the in-memory value
     *   on every persist(), matching the value already written by REPLACE_PLAN.
     */
    private static final String UPDATE_PLAN_STATS =
        "UPDATE " + PLAN_INFO_TABLE
            + " SET `LAST_EXECUTE_TIME` = ?, `CHOOSE_COUNT` = ?, `TABLES_HASHCODE` = ?"
            + " WHERE `INST_ID` = ? AND `SCHEMA_NAME` = ? AND `BASELINE_ID` = ? AND `ID` = ?";

    private static final String UPDATE_PLAN_EXTEND_FIELD =
        "UPDATE " + PLAN_INFO_TABLE
            + " SET `EXTEND_FIELD` = ?"
            + " WHERE `INST_ID` = ? AND `SCHEMA_NAME` = ? AND `BASELINE_ID` = ? AND `ID` = ?";

    private static final String GET_PLAN_IDS_BY_BASELINE_ID =
        "SELECT ID FROM " + PLAN_INFO_TABLE + " WHERE SCHEMA_NAME = ? AND BASELINE_ID=? AND INST_ID=?";

    public BaselineInfoAccessor(boolean withConn) {
        if (withConn) {
            setConnection(MetaDbUtil.getConnection());
        }
    }

    public List<BaselineInfoRecord> loadBaselineData(long sinceTime) {
        try {
            String instId = ServerInstIdManager.getInstance().getInstId();
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, sinceTime);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, sinceTime);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, instId);
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.PROCESS_END,
                    new String[] {"spm load", "since time:" + sinceTime},
                    LogLevel.NORMAL);
            return MetaDbUtil.query(LOAD_BASELINE, params, BaselineInfoRecord.class,
                connection);
        } catch (Exception e) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"loadBaselineData", e.getMessage()},
                    LogLevel.CRITICAL);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                BASELINE_INFO_TABLE,
                e.getMessage());
        }
    }

    public void deleteBySchema(String schemaName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);

            MetaDbUtil.delete(DELETE_BASELINE_BY_SCHEMA, params, connection);
            MetaDbUtil.delete(DELETE_PLAN_BY_SCHEMA, params, connection);
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.PROCESS_END,
                    new String[] {"spm delete by schema", schemaName},
                    LogLevel.NORMAL);
        } catch (Exception e) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"deleteBySchema", e.getMessage()},
                    LogLevel.CRITICAL);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                BASELINE_INFO_TABLE + "/" + PLAN_INFO_TABLE,
                e.getMessage());
        }
    }

    public void deletePlan(String instId, String schemaName, int baselineInfoId, int planInfoId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, baselineInfoId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setInt, planInfoId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, instId);

            MetaDbUtil.delete(DELETE_PLAN, params, connection);
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.PROCESS_END,
                    new String[] {"spm delete plan", schemaName + "," + baselineInfoId + "," + planInfoId},
                    LogLevel.NORMAL);
        } catch (Exception e) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"deletePlan", e.getMessage()},
                    LogLevel.CRITICAL);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                PLAN_INFO_TABLE,
                e.getMessage());
        }
    }

    public void deletePlans(String instId, String schemaName, int baselineInfoId, Set<Integer> planInfoIds) {
        try {
            for (Integer planInfoId : planInfoIds) {
                Map<Integer, ParameterContext> params = new HashMap<>(1);
                MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
                MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, baselineInfoId);
                MetaDbUtil.setParameter(3, params, ParameterMethod.setInt, planInfoId);
                MetaDbUtil.setParameter(4, params, ParameterMethod.setString, instId);
                MetaDbUtil.delete(DELETE_PLAN, params, connection);
            }
            String pIdsForLog = planInfoIds.stream().map(String::valueOf).collect(Collectors.joining(","));
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.PROCESS_END,
                    new String[] {
                        "spm delete plan", schemaName + "," + baselineInfoId + "," + pIdsForLog},
                    LogLevel.NORMAL);
        } catch (Exception e) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"deletePlan", e.getMessage()},
                    LogLevel.CRITICAL);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                PLAN_INFO_TABLE,
                e.getMessage());
        }
    }

    public void deleteBaseline(String schemaName, int baselineInfoId) {
        try {
            String instId = ServerInstIdManager.getInstance().getInstId();
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, baselineInfoId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, instId);

            MetaDbUtil.delete(DELETE_BASELINE, params, connection);
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.PROCESS_END,
                    new String[] {"spm delete baseline", schemaName + "," + baselineInfoId},
                    LogLevel.NORMAL);
        } catch (Exception e) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"deleteBaseline", e.getMessage()},
                    LogLevel.CRITICAL);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                BASELINE_INFO_TABLE + "/" + PLAN_INFO_TABLE,
                e.getMessage());
        }
    }

    /**
     * Insert baseline and plan info, only support one baseline once
     */
    public void persist(String schemaName, BaselineInfoRecord baseline, List<BaselineInfoRecord> plans,
                        boolean persistPlanStats) {
        try {
            MetaDbUtil.beginTransaction(connection);
            // insert baselines
            Map<Integer, ParameterContext> params = baseline.buildInsertParamsForBaseline();
            MetaDbUtil.insert(REPLACE_BASELINE, params, connection);

            List<Integer> planIdsCurrent = getPlanIds(baseline.getInstId(), schemaName, baseline.getId());

            // insert plans
            for (BaselineInfoRecord plan : plans) {
                if (plan == null) {
                    continue;
                }
                if (!planIdsCurrent.contains(plan.getPlanId()) || persistPlanStats) {
                    Map<Integer, ParameterContext> planParams = plan.buildInsertParamsForPlan();
                    MetaDbUtil.insert(REPLACE_PLAN, planParams, connection);
                } else {
                    Map<Integer, ParameterContext> planParams = plan.buildInsertParamsForPlanStats();
                    MetaDbUtil.update(UPDATE_PLAN_STATS, planParams, connection);
                }
                planIdsCurrent.remove(new Integer(plan.getPlanId()));
            }
            if (!planIdsCurrent.isEmpty()) {
                // remove other plan
                for (Integer planId : planIdsCurrent) {
                    deletePlan(baseline.getInstId(), schemaName, baseline.getId(), planId);
                }
            }
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.PROCESS_END,
                    new String[] {"spm persist", baseline.getSchemaName() + "," + baseline.getId()},
                    LogLevel.NORMAL);
        } catch (SQLException e) {
            MetaDbUtil.rollback(connection, e, LOGGER, schemaName, "spm persist");
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"spm persist", e.getMessage()},
                    LogLevel.CRITICAL);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "persist into",
                BASELINE_INFO_TABLE + "/" + PLAN_INFO_TABLE,
                e.getMessage());
        } finally {
            MetaDbUtil.endTransaction(connection, LOGGER);
        }
    }

    public int updateExtendField(String instId, String schema, int baselineId, int planId, String extend) {
        // Validate required parameters
        if (StringUtils.isEmpty(instId) || StringUtils.isEmpty(schema)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_CHECK_ARGUMENTS,
                "updateExtendField: instId=" + instId + ", schema=" + schema);
        }

        try {
            Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, extend);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, instId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setInt, baselineId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setInt, planId);

            int affectedRows = MetaDbUtil.update(UPDATE_PLAN_EXTEND_FIELD, params, connection);

            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.PROCESS_END,
                    new String[] {
                        "spm updateExtendField",
                        String.format("schema=%s, baselineId=%d, planId=%d, affectedRows=%d",
                            schema, baselineId, planId, affectedRows)},
                    LogLevel.NORMAL);

            return affectedRows;
        } catch (Exception e) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED,
                    new String[] {
                        "spm updateExtendField failed",
                        String.format("instId=%s, schema=%s, baselineId=%d, planId=%d, error=%s",
                            instId, schema, baselineId, planId, e.getMessage())},
                    LogLevel.CRITICAL);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                PLAN_INFO_TABLE,
                e.getMessage());
        }
    }

    private List<Integer> getPlanIds(String instId, String schemaName, int baselineId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, baselineId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, instId);
            List<CommonIntegerRecord> records =
                MetaDbUtil.query(GET_PLAN_IDS_BY_BASELINE_ID, params, CommonIntegerRecord.class, connection);
            return records.stream().map(cir -> cir.value).collect(Collectors.toList());
        } catch (Exception e) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED,
                    new String[] {"spm getPlanIds by baseline id " + baselineId, e.getMessage()},
                    LogLevel.CRITICAL);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                PLAN_INFO_TABLE,
                e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    public void planMigration() {
        if (!InstConfUtil.getBool(ConnectionParams.ENABLE_SPM)) {
            return;
        }
        try {
            String instId = ServerInstIdManager.getInstance().getInstId();
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            // check if spm_baseline were empty
            List<CommonIntegerRecord> count =
                MetaDbUtil.query(BASELINE_COUNT, params, CommonIntegerRecord.class, connection);

            if (count == null || count.isEmpty()) {
                // not should happen
                return;
            }

            if (count.iterator().next().value != 0) {
                // plan migration already happened
                return;
            }

            // check if baseline_info/plan_info exists
            try {
                count = MetaDbUtil.query(CHECK_BASELINE_INFO_NOT_EMPTY, null, CommonIntegerRecord.class, connection);
                if (count == null || count.isEmpty()) {
                    return;
                }

                count = MetaDbUtil.query(CHECK_PLAN_INFO_NOT_EMPTY, null, CommonIntegerRecord.class, connection);
                if (count == null || count.isEmpty()) {
                    return;
                }
            } catch (SQLException sqlException) {
                if (sqlException.getErrorCode() == 1146) {
                    // baseline_info/plan_info table was not exist
                    return;
                }
            }

            // copy baseline/plan info from baseline_info/plan_info to spm_baseline/spm_plan
            MetaDbUtil.beginTransaction(connection);
            int baselineNum = MetaDbUtil.execute(MOVE_BASELINE_INFO_TO_SPM_BASELINE, params, connection);
            int planNum = MetaDbUtil.execute(MOVE_PLAN_INFO_TO_SPM_PLAN, params, connection);

            MetaDbUtil.commit(connection);
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.PROCESS_END,
                    new String[] {"plan migration", "baselineNum:" + baselineNum + ",planNum:" + planNum},
                    LogLevel.NORMAL);
        } catch (Exception e) {
            MetaDbUtil.rollback(connection, e, LOGGER, "plan migration");
            // only record, won't break the processing of server launch
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"plan migration", e.getMessage()},
                    LogLevel.WARNING);
        }
    }

    public void deleteBaselineWithInstNotExist(Set<String> instSet) {
        try {
            String instStr = concat(instSet);
            if (StringUtils.isEmpty(instStr)) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_CHECK_ARGUMENTS, "deleteBaselineWithInstNotExist");
            }
            String sql = String.format(DELETE_BASELINE_BY_NOT_IN_INST, instStr);

            int num = MetaDbUtil.delete(sql, connection);
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.PROCESS_END,
                    new String[] {"spm delete baseline by not in inst", instStr + ", deleted num:" + num},
                    LogLevel.NORMAL);
        } catch (Exception e) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"deleteBaseline", e.getMessage()},
                    LogLevel.CRITICAL);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                BASELINE_INFO_TABLE + "/" + PLAN_INFO_TABLE,
                e.getMessage());
        }
    }

    public void deleteBaselineByInstSchema(String instId, Set<String> schemas) {
        try {
            String schemaStr = concat(schemas);
            String sql = String.format(DELETE_BASELINE_BY_NOT_IN_SCHEMA, instId, schemaStr);

            if (StringUtils.isEmpty(instId) || StringUtils.isEmpty(schemaStr)) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_CHECK_ARGUMENTS,
                    "deleteBaselineWithInstAndSchema:" + instId + "," + schemaStr);
            }

            int num = MetaDbUtil.delete(sql, connection);
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.PROCESS_END,
                    new String[] {
                        "spm delete baseline by not in schema", instId + "," + schemaStr + ", deleted num:" + num},
                    LogLevel.NORMAL);
        } catch (Exception e) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"deleteBaseline", e.getMessage()},
                    LogLevel.CRITICAL);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                BASELINE_INFO_TABLE + "/" + PLAN_INFO_TABLE,
                e.getMessage());
        }
    }

    public void deleteBaselineByInstSchemaBaselineId(String instId, String schema, Collection<Integer> baselineIds) {
        try {
            // Change context:
            // - Before: an empty kept-id list was formatted into "BASELINE_INFO.ID NOT IN(null)"
            //   because concatInt() returns null for an empty collection; by SQL three-valued
            //   logic the predicate is UNKNOWN for every row, so the BASELINE_SYNC clean job
            //   never deleted stale schema-level baselines.
            // - Path impact: only the empty/null list branch changes to a full clean of the
            //   inst+schema; the non-empty NOT IN path used by the same scheduled job is
            //   unchanged, and fixed baselines remain protected by FIXED!=1 in both branches.
            // - Capability regression: None, the empty-list case previously deleted no rows,
            //   which contradicted the intended "keep nothing" semantics of the clean job.
            final String sql;
            if (baselineIds == null || baselineIds.isEmpty()) {
                sql = String.format(DELETE_BASELINE_BY_INST_SCHEMA, instId, schema);
            } else {
                sql = String
                    .format(DELETE_BASELINE_BY_NOT_IN_BASELINE_ID, instId, schema, concatInt(baselineIds));
            }

            int num = MetaDbUtil.delete(sql, connection);
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.PROCESS_END,
                    new String[] {
                        "spm delete baseline by not in schema",
                        instId + "," + schema + "," + (baselineIds == null ? 0 : baselineIds.size())
                            + ", deleted num:" + num},
                    LogLevel.NORMAL);
        } catch (Exception e) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"deleteBaseline", e.getMessage()},
                    LogLevel.CRITICAL);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                BASELINE_INFO_TABLE + "/" + PLAN_INFO_TABLE,
                e.getMessage());
        }
    }

    public void deleteDriftPlan() {
        try {
            int planNum = MetaDbUtil.delete(DELETE_DRIFT_PLAN, connection);
            int baselineNum = MetaDbUtil.delete(DELETE_DRIFT_BASELINE, connection);
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.PROCESS_END,
                    new String[] {
                        "spm delete drift plan in meta", "baseline:" + baselineNum + ",plan:" + planNum},
                    LogLevel.NORMAL);
        } catch (Exception e) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.SPM, LogPattern.UNEXPECTED, new String[] {"deleteDriftPlan", e.getMessage()},
                    LogLevel.CRITICAL);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                BASELINE_INFO_TABLE + "/" + PLAN_INFO_TABLE,
                e.getMessage());
        }
    }
}
