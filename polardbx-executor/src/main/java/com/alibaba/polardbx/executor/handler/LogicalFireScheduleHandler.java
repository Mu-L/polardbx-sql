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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlAddPartsWarningScanner;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.spm.SPMBaseLineSyncScheduledJob;
import com.alibaba.polardbx.executor.scheduler.executor.statistic.StatisticHllScheduledJob;
import com.alibaba.polardbx.executor.scheduler.executor.statistic.StatisticInfoSchemaTablesScheduleJob;
import com.alibaba.polardbx.executor.scheduler.executor.statistic.StatisticSampleCollectionScheduledJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlFireSchedule;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.executor.scheduler.ScheduledJobsTrigger.restoreTrigger;

public class LogicalFireScheduleHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(LogicalFireScheduleHandler.class);

    public LogicalFireScheduleHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlFireSchedule fireSchedule = (SqlFireSchedule) ((LogicalDal) logicalPlan).getNativeSqlNode();
        boolean byScheduleName = fireSchedule.isByScheduleName();
        boolean byTableName = fireSchedule.isByTableName();
        SqlNode targetExpr = fireSchedule.getTargetExpr();

        long scheduleId = 0;
        ScheduledJobsRecord record = null;
        if (byTableName) {
            String tableSchema = executionContext.getSchemaName();
            String tableName = null;
            SqlIdentifier tableNameAst = (SqlIdentifier) targetExpr;
            if (tableNameAst.names.size() == 1) {
                tableName = SQLUtils.normalize(tableNameAst.getLastName());
            } else if (tableNameAst.names.size() == 2) {
                tableSchema = SQLUtils.normalize(tableNameAst.getComponent(0).getLastName());
                tableName = SQLUtils.normalize(tableNameAst.getComponent(1).getLastName());
            }
            record = ScheduledJobsManager.queryScheduledJobByTableName(tableSchema, tableName);
            if (record == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format("No found scheduled job by table name `%s`.`%s`", tableSchema, tableName));
            }
            scheduleId = record.getScheduleId();
        } else if (byScheduleName) {
            //
        } else {
            scheduleId = fireSchedule.getScheduleId();
            record = ScheduledJobsManager.queryScheduledJobById(scheduleId);
        }

        if (record == null) {
            return new AffectRowCursor(0);
        }
        if (record.getExecutorType().equalsIgnoreCase("STATISTIC_SAMPLE_SKETCH")) {
            ExecutableScheduledJob executableScheduledJob = new ExecutableScheduledJob();
            executableScheduledJob.setScheduleId(record.getScheduleId());
            executableScheduledJob.setFireTime(System.currentTimeMillis());
            executableScheduledJob.setTimeZone("SYSTEM");
            StatisticSampleCollectionScheduledJob job =
                new StatisticSampleCollectionScheduledJob(executableScheduledJob);
            job.setFromScheduleJob(false);
            job.execute();

            logger.info(String.format("fire scheduled job:[%s]", scheduleId));
            return new AffectRowCursor(1);
        } else if (record.getExecutorType().equalsIgnoreCase("STATISTIC_HLL_SKETCH")) {
            ExecutableScheduledJob executableScheduledJob = new ExecutableScheduledJob();
            executableScheduledJob.setScheduleId(record.getScheduleId());
            executableScheduledJob.setFireTime(System.currentTimeMillis());
            executableScheduledJob.setTimeZone("SYSTEM");
            StatisticHllScheduledJob job = new StatisticHllScheduledJob(executableScheduledJob);
            job.setFromScheduleJob(false);
            job.execute();

            logger.info(String.format("fire scheduled job:[%s]", scheduleId));
            return new AffectRowCursor(1);
        } else if (record.getExecutorType().equalsIgnoreCase("STATISTIC_INFO_SCHEMA_TABLES")) {
            ExecutableScheduledJob executableScheduledJob = new ExecutableScheduledJob();
            executableScheduledJob.setScheduleId(record.getScheduleId());
            executableScheduledJob.setFireTime(System.currentTimeMillis());
            executableScheduledJob.setTimeZone("SYSTEM");
            StatisticInfoSchemaTablesScheduleJob job =
                new StatisticInfoSchemaTablesScheduleJob(executableScheduledJob);
            job.setFromScheduleJob(false);
            job.execute();

            logger.info(String.format("fire scheduled job:[%s]", scheduleId));
            return new AffectRowCursor(1);
        } else if (record.getExecutorType().equalsIgnoreCase("BASELINE_SYNC")) {
            ExecutableScheduledJob executableScheduledJob = new ExecutableScheduledJob();
            executableScheduledJob.setScheduleId(record.getScheduleId());
            executableScheduledJob.setFireTime(System.currentTimeMillis());
            executableScheduledJob.setTimeZone("SYSTEM");
            SPMBaseLineSyncScheduledJob job = new SPMBaseLineSyncScheduledJob(executableScheduledJob);
            job.setFromScheduleJob(false);
            job.execute();

            logger.info(String.format("fire scheduled job:[%s]", scheduleId));
            return new AffectRowCursor(1);
        } else if (record.getExecutorType().equalsIgnoreCase("TTL_JOB")) {
            PolarPrivilegeUtils.checkPrivilege(record.getTableSchema(), record.getTableName(), PrivilegePoint.ALTER,
                executionContext);
            Map<String, Object> hintCmdParams = new HashMap<>();
            ExecutionContext newEc = executionContext.copy();
            hintCmdParams.putAll(newEc.getHintCmds());

            if (executionContext.getParamManager()
                .getBoolean(ConnectionParams.TTL_ONLY_SCHEDULED_WARNING_SCANNER_TASK)) {
                TtlAddPartsWarningScanner scanner = new TtlAddPartsWarningScanner();
                Map<String, String> warningMsg = scanner.checkTtlTableAutoAddParts(executionContext);
                ArrayResultCursor result = new ArrayResultCursor("TTL_WARNING_SCANNER");
                result.addColumn("WARNING_TABLE", DataTypes.StringType);
                result.addColumn("WARNING_MSG", DataTypes.StringType);
                result.initMeta();
                for (Map.Entry<String, String> entry : warningMsg.entrySet()) {
                    result.addRow(new Object[] {entry.getKey(), entry.getValue()});
                }
                return result;
            }

            Long fireTimeTs = System.currentTimeMillis() / 1000;// using unix_timestamp
            Integer debugFireTime =
                newEc.getParamManager().getInt(ConnectionParams.TTL_SCHEDULED_JOB_USE_DEBUG_FIRE_TIME);
            if (debugFireTime > 0) {
                fireTimeTs = debugFireTime.longValue();
            }
            int row = ScheduledJobsManager.forceFireScheduledJob(scheduleId, fireTimeTs, hintCmdParams, newEc);
            logger.info(String.format("fire scheduled job:[%s,%s]", scheduleId, fireTimeTs));
            return new AffectRowCursor(row);
        }
        PolarPrivilegeUtils.checkPrivilege(record.getTableSchema(), record.getTableName(), PrivilegePoint.ALTER,
            executionContext);

        logger.info(String.format("fire scheduled job:[%s]", scheduleId));
        int row = ScheduledJobsManager.fireScheduledJob(scheduleId, executionContext);
        return new AffectRowCursor(row);
    }

}