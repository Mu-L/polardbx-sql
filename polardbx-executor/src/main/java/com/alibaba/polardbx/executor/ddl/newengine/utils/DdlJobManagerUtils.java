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

package com.alibaba.polardbx.executor.ddl.newengine.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CheckPhyTableTask;
import com.alibaba.polardbx.executor.ddl.job.task.twophase.InitTwoPhaseDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.PhyDdlExecutionRecord;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.In;
import org.apache.calcite.util.Pair;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.SEMICOLON;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.CheckPhyTableTask.MSG_TEXT;
import static com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponse.Response;

public class DdlJobManagerUtils {

    private static final DdlEngineSchedulerManager SCHEDULER_MANAGER = new DdlEngineSchedulerManager();

    /**
     * Update the progress only.
     *
     * @param jobId A job ID
     * @param progress New progress
     * @return Affected rows
     */
    public static int updateProgress(long jobId, int progress) {
        return new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                return engineAccessor.updateProgress(jobId, progress);
            }
        }.execute();
    }

    public static int updateProgress(long jobId,
                                     int progress,
                                     Connection connection) {
        DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
        ddlEngineAccessor.setConnection(connection);
        ddlEngineAccessor.updateProgress(jobId, progress);
        return progress;
    }

    public static int updatePausedPolicy(long jobId,
                                         DdlState pausedPolicy,
                                         DdlState rollbackPausedPolicy,
                                         Connection connection) {
        DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
        ddlEngineAccessor.setConnection(connection);
        return ddlEngineAccessor.updatePausedPolicy(jobId, pausedPolicy, rollbackPausedPolicy);
    }

    /**
     * @param jobId jobId
     * @param supportContinue supportContinue
     * @param supportCancel supportCancel
     * @param connection connection
     * @return supportedCommands bitmap
     */
    public static int updateSupportedCommands(long jobId,
                                              boolean supportContinue,
                                              boolean supportCancel,
                                              boolean skipSubJob,
                                              Connection connection) {
        DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
        ddlEngineAccessor.setConnection(connection);
        DdlEngineRecord record = new DdlEngineRecord();
        if (supportContinue) {
            record.setSupportContinue();
        }
        if (supportCancel) {
            record.setSupportCancel();
        }
        if (skipSubJob) {
            record.setSkipSubjob();
        }
        ddlEngineAccessor.updateSupportedCommands(jobId, record.supportedCommands);
        return record.supportedCommands;
    }

    /**
     * @param jobId jobId
     * @param supportContinue supportContinue
     * @param supportCancel supportCancel
     * @return supportedCommands bitmap
     */
    public static int updateSupportedCommands(long jobId,
                                              boolean supportContinue,
                                              boolean supportCancel,
                                              boolean skipSubJob) {
        return new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                DdlEngineRecord record = new DdlEngineRecord();
                if (supportContinue) {
                    record.setSupportContinue();
                }
                if (supportCancel) {
                    record.setSupportCancel();
                }
                if (skipSubJob) {
                    record.setSkipSubjob();
                }
                return engineAccessor.updateSupportedCommands(jobId, record.supportedCommands);
            }
        }.execute();
    }

    /**
     * Save the result of a DDL execution that may have succeeded or failed.
     *
     * @param jobId A job ID
     * @param response A DDL response that contains type and content
     * @return Affected rows
     */
    public static int saveResult(long jobId, Response response) {
        return SCHEDULER_MANAGER.saveResult(jobId, response);
    }

    /**
     * Save the DDL context.
     *
     * @param ddlContext A DDL context
     * @return Affected rows
     */
    public static int saveContext(DdlContext ddlContext) {
        return SCHEDULER_MANAGER.saveContext(ddlContext);
    }

    /**
     * Reload physical tables done to cache from the system table.
     *
     * @param phyDdlExecutionRecord Current DDL context
     */
    public static void reloadPhyTablesDone(PhyDdlExecutionRecord phyDdlExecutionRecord) {
        DdlEngineTaskRecord record =
            SCHEDULER_MANAGER.fetchTaskRecord(phyDdlExecutionRecord.getJobId(), phyDdlExecutionRecord.getTaskId());

        clearPhyTablesDone(phyDdlExecutionRecord);

        if (record != null && TStringUtil.isNotEmpty(record.extra)) {
            String[] phyTablesDone = record.extra.split(DdlConstants.SEMICOLON);
            int countReallyDone = 0;
            for (String phyTableDone : phyTablesDone) {
                phyDdlExecutionRecord.addPhyObjectDone(phyTableDone);
                String[] phyTableInfo = phyTableDone.split(DdlConstants.COLON);
                if (phyTableInfo.length != 4 || !TStringUtil.equalsIgnoreCase(phyTableInfo[2], "false")) {
                    countReallyDone++;
                }
            }
            phyDdlExecutionRecord.setNumPhyObjectsDone(countReallyDone);
        }
    }

    public static Map<String, String> reloadPhyTablesHashCode(Long jobId) {
        DdlEngineTaskRecord record =
            SCHEDULER_MANAGER.fetchTaskRecord(jobId, TwoPhaseDdlUtils.TWO_PHASE_DDL_INIT_TASK_NAME).get(0);
        if (record != null && TStringUtil.isNotEmpty(record.value)) {
            InitTwoPhaseDdlTask initTwoPhaseDdlTask =
                (InitTwoPhaseDdlTask) TaskHelper.deSerializeTask(TwoPhaseDdlUtils.TWO_PHASE_DDL_INIT_TASK_NAME,
                    record.value);
            return initTwoPhaseDdlTask.getPhysicalTableHashCodeMap();
        } else {
            return new HashMap<>();
        }
    }

    public static Map<Pair<String, String>, String> reloadCheckPhyTask(Long jobId) {
        List<DdlEngineTaskRecord> records =
            SCHEDULER_MANAGER.fetchAllTaskRecord(jobId, CheckPhyTableTask.TASK_NAME);
        Map<Pair<String, String>, String> notOkPhysicalTable = new HashMap<>();
        for (DdlEngineTaskRecord record : records) {
            if (records != null && TStringUtil.isNotEmpty(record.value)) {
                CheckPhyTableTask checkPhyTableTask =
                    (CheckPhyTableTask) TaskHelper.deSerializeTask(CheckPhyTableTask.TASK_NAME,
                        record.value);
                Map<String, String> checkResult = checkPhyTableTask.getCheckResult();
                String result = checkResult.getOrDefault(MSG_TEXT, "unknown");
                if (!result.equalsIgnoreCase("ok")) {
                    notOkPhysicalTable.put(
                        Pair.of(checkPhyTableTask.getGroupKey(), checkPhyTableTask.getPhyTableName()), result);
                }
            }
        }
        return notOkPhysicalTable;
    }

    /**
     * Clear physical tables done from cache.
     */
    public static void clearPhyTablesDone(PhyDdlExecutionRecord phyDdlExecutionRecord) {
        phyDdlExecutionRecord.clearPhyObjectsDone();
    }

    /**
     * Append new physical table done to current list for a DDL job and also update the progress.
     */
    public static int appendPhyTableDone(PhyDdlExecutionRecord phyDdlExecutionRecord, String phyTableInfo,
                                         boolean withProgress) {
        if (TStringUtil.isEmpty(phyTableInfo)) {
            return 0;
        }

        final int numPhyTablesDone = phyDdlExecutionRecord.getNumPhyObjectsDone();
        final int numPhyTablesTotal = phyDdlExecutionRecord.getNumPhyObjectsTotal();

        return new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                int rowsAffected = 0;

                rowsAffected += engineTaskAccessor
                    .updatePhyDone(phyDdlExecutionRecord.getJobId(), phyDdlExecutionRecord.getTaskId(), phyTableInfo,
                        false);

                if (withProgress && numPhyTablesDone >= 0 && numPhyTablesTotal > 0) {
                    int progress = numPhyTablesDone * 100 / numPhyTablesTotal;
                    rowsAffected += engineAccessor.updateProgress(phyDdlExecutionRecord.getJobId(), progress);
                }

                return rowsAffected;
            }
        }.execute();
    }

    /**
     * Reset the whole list of physical objects done for a DDL job and also update the progress.
     */
    public static int resetPhyTablesDone(PhyDdlExecutionRecord phyDdlExecutionRecord) {
        // Build new object done list.
        StringBuilder buf = new StringBuilder();
        phyDdlExecutionRecord.getPhyObjectsDone().stream()
            .forEach(phyObjectDone -> buf.append(SEMICOLON).append(phyObjectDone));

        String phyObjectsDoneStr = buf.length() > 0 ? buf.deleteCharAt(0).toString() : "";

        final int numPhyTablesDone = phyDdlExecutionRecord.getNumPhyObjectsDone();
        final int numPhyTablesTotal = phyDdlExecutionRecord.getNumPhyObjectsTotal();

        return new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                int rowsAffected = 0;

                rowsAffected += engineTaskAccessor
                    .updatePhyDone(phyDdlExecutionRecord.getJobId(), phyDdlExecutionRecord.getTaskId(),
                        phyObjectsDoneStr, true);

                if (numPhyTablesTotal > 0) {
                    int progress = numPhyTablesDone * 100 / numPhyTablesTotal;
                    rowsAffected += engineAccessor.updateProgress(phyDdlExecutionRecord.getJobId(), progress);
                }

                return rowsAffected;
            }
        }.execute();
    }

}
