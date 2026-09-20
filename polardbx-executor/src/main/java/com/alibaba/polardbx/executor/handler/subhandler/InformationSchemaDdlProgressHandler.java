package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.backfill.ThrottleInfo;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlBackFillSpeedSyncAction;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.executor.ddl.workqueue.FastCheckerThreadPool;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.metadb.misc.OmcRecord;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaDdlProgress;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowJobsHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author wumu
 */
public class InformationSchemaDdlProgressHandler extends BaseVirtualViewSubClassHandler {
    static final Logger LOGGER = LoggerFactory.getLogger(InformationSchemaDdlProgressHandler.class);

    public InformationSchemaDdlProgressHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaDdlProgress;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        DdlJobManager ddlJobManager = new DdlJobManager();
        List<DdlEngineRecord> ddlRecordList = ddlJobManager.fetchRecords(DdlState.ALL_STATES);
        ddlRecordList = ddlRecordList.stream().filter(e -> !e.isSubJob()).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(ddlRecordList)) {
            return cursor;
        }

        final int jobIdIndex = InformationSchemaDdlProgress.getJobIdIndex();
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
        Set<String> jobIds = virtualView.getEqualsFilterValues(jobIdIndex, params);

        List<Long> ddlJobIds = ddlRecordList.stream().map(DdlEngineRecord::getJobId).collect(Collectors.toList());
        Map<Long, FastCheckerThreadPool.FastCheckerInfo> mergedResult = collectFastCheckerInfoMap(ddlJobIds);

        for (DdlEngineRecord record : ddlRecordList) {
            final long jobId = record.jobId;

            if (CollectionUtils.isNotEmpty(jobIds) && !jobIds.contains(String.valueOf(jobId))) {
                continue;
            }

            List<DdlEngineTaskRecord> allTasks = ddlJobManager.fetchAllSuccessiveTaskLessPartialInfoByJobId(jobId);
            List<DdlEngineTaskRecord> allBackFillTasks =
                allTasks.stream().filter(e -> StringUtils.containsIgnoreCase(e.getName(), "BackFill"))
                    .collect(Collectors.toList());

            List<DdlEngineTaskRecord> allOmcPhyDdlTasks =
                allTasks.stream().filter(e -> StringUtils.containsIgnoreCase(e.getName(), "OmcPhyDdlTask"))
                    .collect(Collectors.toList());

            if (!allOmcPhyDdlTasks.isEmpty()) {
                buildOmcPhyDdlProgress(cursor, record, allOmcPhyDdlTasks);
            } else if (!allBackFillTasks.isEmpty()) {
                buildBackFillProgress(cursor, record, allBackFillTasks, mergedResult);
            } else {
                buildNormalDdlProgress(cursor, record);
            }
        }

        return cursor;
    }

    private void buildOmcPhyDdlProgress(ArrayResultCursor cursor, DdlEngineRecord record,
                                        List<DdlEngineTaskRecord> allOmcPhyDdlTasks) {
        long jobId = record.jobId;
        GsiBackfillManager backfillManager = new GsiBackfillManager(SystemDbHelper.DEFAULT_DB_NAME);
        Map<Long, ThrottleInfo> throttleInfoMap = collectThrottleInfoMap();

        for (DdlEngineTaskRecord taskRecord : allOmcPhyDdlTasks) {
            long taskId = taskRecord.taskId;
            long backfillTaskId = taskId;
            List<OmcRecord> omcRecords = OmcUtils.getOmcRecords(jobId, taskId);
            if (omcRecords.isEmpty()) {
                continue;
            }

            long approximateTotalRows = omcRecords.stream().map(OmcRecord::getTotalRowCount).reduce(0L, Long::sum);

            List<Long> changesetIds = omcRecords.stream().map(OmcRecord::getChangesetId).collect(Collectors.toList());
            List<GsiBackfillManager.BackFillAggInfo> backFillAggInfoList =
                backfillManager.queryBackFillAggInfoByTaskId(taskId, changesetIds);
            if (backFillAggInfoList.isEmpty()) {
                continue;
            }

            GsiBackfillManager.BackFillAggInfo backFillAggInfo = backFillAggInfoList.get(0);
            long finishedRows = backFillAggInfo.getSuccessRowCount();
            long duration = backFillAggInfo.getDuration() == 0 ? 1L : backFillAggInfo.getDuration();
            String aveSpeed = finishedRows / duration + " rows/s";

            String currentSpeed = "-";
            ThrottleInfo throttleInfo = throttleInfoMap.get(taskId);
            if (throttleInfo != null) {
                currentSpeed = throttleInfo.getSpeed() + " rows/s";
            }

            String progress;
            DecimalFormat df = new DecimalFormat("0.00");
            if (approximateTotalRows > 0) {
                // 有统计信息
                double percent = (double) finishedRows * 100.0 / approximateTotalRows;
                percent = Math.min(percent, 100.0);
                progress = df.format(percent) + "%";
            } else {
                // 无统计信息
                int progressInt = record.progress;
                progressInt = Math.min(progressInt, 100);
                progress = progressInt + "%";
            }

            String checkProgress = "-";
            addRow(cursor, jobId, String.valueOf(backfillTaskId), record.schemaName, record.objectName,
                record.state, progress, String.valueOf(finishedRows), String.valueOf(approximateTotalRows),
                currentSpeed, aveSpeed, checkProgress,
                new Timestamp(record.gmtCreated), new Timestamp(record.gmtModified),
                record.ddlStmt);
        }
    }

    private void buildBackFillProgress(ArrayResultCursor cursor, DdlEngineRecord record,
                                       List<DdlEngineTaskRecord> allBackFillTasks,
                                       Map<Long, FastCheckerThreadPool.FastCheckerInfo> mergedResult) {
        long jobId = record.jobId;
        GsiBackfillManager backfillManager = new GsiBackfillManager(SystemDbHelper.DEFAULT_DB_NAME);
        Map<Long, ThrottleInfo> throttleInfoMap = collectThrottleInfoMap();
        List<GsiBackfillManager.BackFillAggInfo> backFillAggInfoList =
            backfillManager.queryBackFillAggInfoById(
                allBackFillTasks.stream().map(e -> e.taskId).collect(Collectors.toList()));
        for (GsiBackfillManager.BackFillAggInfo backFillAggInfo : backFillAggInfoList) {
            long backfillTaskId = backFillAggInfo.getBackFillId();
            long finishedRows = backFillAggInfo.getSuccessRowCount();
            long approximateTotalRows = backFillAggInfo.getTotalRowCount();
            long duration = backFillAggInfo.getDuration() == 0 ? 1L : backFillAggInfo.getDuration();
            String aveSpeed = finishedRows / duration + " rows/s";

            String currentSpeed = "-";
            ThrottleInfo throttleInfo = throttleInfoMap.get(backfillTaskId);
            if (throttleInfo != null) {
                currentSpeed = throttleInfo.getSpeed() + " rows/s";
            }

            String progress;
            DecimalFormat df = new DecimalFormat("0.00");
            if (approximateTotalRows > 0) {
                // 有统计信息
                double percent = (double) finishedRows * 100.0 / approximateTotalRows;
                percent = Math.min(percent, 100.0);
                progress = df.format(percent) + "%";
            } else {
                // 无统计信息
                int progressInt = record.progress;
                progressInt = Math.min(progressInt, 100);
                progress = progressInt + "%";
            }

            String checkProgress = "-";
            FastCheckerThreadPool.FastCheckerInfo fastCheckerInfo = mergedResult.get(jobId);
            if (fastCheckerInfo != null) {
                double checkPercent =
                    fastCheckerInfo.getPhyTaskFinished().get() * 100.0 / fastCheckerInfo.getPhyTaskSum().get();
                checkProgress = df.format(checkPercent) + "%";
            }

            addRow(cursor, jobId, String.valueOf(backfillTaskId), record.schemaName, record.objectName,
                record.state, progress, String.valueOf(finishedRows), String.valueOf(approximateTotalRows),
                currentSpeed, aveSpeed, checkProgress,
                new Timestamp(record.gmtCreated), new Timestamp(record.gmtModified),
                record.ddlStmt);
        }
    }

    private void buildNormalDdlProgress(ArrayResultCursor cursor, DdlEngineRecord record) {
        addRow(cursor, record.jobId, "-", record.schemaName, record.objectName, record.state,
            record.progress + "%", "-", "-", "-", "-", "-",
            new Timestamp(record.gmtCreated), new Timestamp(record.gmtModified), record.ddlStmt);
    }

    private static Map<Long, ThrottleInfo> collectThrottleInfoMap() {
        Map<Long, ThrottleInfo> throttleInfoMap = new HashMap<>();
        try {
            List<List<Map<String, Object>>> result = SyncManagerHelper.syncIgnoreExceptions(
                new DdlBackFillSpeedSyncAction(), SystemDbHelper.DEFAULT_DB_NAME, SyncScope.MASTER_ONLY);
            for (List<Map<String, Object>> list : GeneralUtil.emptyIfNull(result)) {
                for (Map<String, Object> map : GeneralUtil.emptyIfNull(list)) {
                    throttleInfoMap.put(Long.parseLong(String.valueOf(map.get("BACKFILL_ID"))),
                        new ThrottleInfo(
                            Long.parseLong(String.valueOf(map.get("BACKFILL_ID"))),
                            Double.parseDouble(String.valueOf(map.get("SPEED"))),
                            Long.parseLong(String.valueOf(map.get("TOTAL_ROWS")))
                        ));
                }
            }
        } catch (Exception e) {
            LOGGER.error("collect ThrottleInfo from remote nodes error", e);
        }
        return throttleInfoMap;
    }

    private static Map<Long, FastCheckerThreadPool.FastCheckerInfo> collectFastCheckerInfoMap(List<Long> ddlJobIds) {
        //handle fastchecker info
        Map<Long, FastCheckerThreadPool.FastCheckerInfo> mergedResult = new TreeMap<>();
        DdlEngineShowJobsHandler.FastCheckerInfoSyncAction
            syncAction = new DdlEngineShowJobsHandler.FastCheckerInfoSyncAction(ddlJobIds);
        GmsSyncManagerHelper.sync(syncAction, SystemDbHelper.DEFAULT_DB_NAME, SyncScope.MASTER_ONLY, results -> {
            if (results == null) {
                return;
            }

            for (Pair<GmsNodeManager.GmsNode, List<Map<String, Object>>> result : results) {
                if (CollectionUtils.isEmpty(result.getValue())) {
                    continue;
                }
                for (Map<String, Object> row : result.getValue()) {
                    long jobId = DataTypes.LongType.convertFrom(row.get("DDL_JOB_ID"));
                    long taskSum = DataTypes.LongType.convertFrom(row.get("TASK_SUM"));
                    long taskFinished = DataTypes.LongType.convertFrom(row.get("TASK_FINISHED"));

                    mergedResult.putIfAbsent(jobId, new FastCheckerThreadPool.FastCheckerInfo());
                    mergedResult.get(jobId).getPhyTaskSum().addAndGet((int) taskSum);
                    mergedResult.get(jobId).getPhyTaskFinished().addAndGet((int) taskFinished);
                }
            }
        });

        return mergedResult;
    }

    private static void addRow(ArrayResultCursor cursor,
                               long jobId,
                               String backfillId,
                               String schemaName,
                               String tableName,
                               String state,
                               String progress,
                               String finishedRows,
                               String approximateTotalRows,
                               String currentSpeed,
                               String aveSpeed,
                               String checkProgress,
                               Timestamp startTime,
                               Timestamp endTime,
                               String ddlStmt) {
        cursor.addRow(new Object[] {
            jobId, backfillId, schemaName, tableName, state, progress, finishedRows, approximateTotalRows,
            currentSpeed, aveSpeed, checkProgress, startTime, endTime, ddlStmt});
    }
}
