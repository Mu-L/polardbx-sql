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
import com.alibaba.polardbx.executor.ddl.job.task.omc.OmcPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlBackFillSpeedSyncAction;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.ddl.omc.OmcPhyDdlContext;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.executor.ddl.workqueue.OmcCheckerThreadPool;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillStatus;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.metadb.misc.OmcRecord;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaOmcProgress;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

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
public class InformationSchemaOmcProgressHandler extends BaseVirtualViewSubClassHandler {
    static final Logger LOGGER = LoggerFactory.getLogger(InformationSchemaOmcProgressHandler.class);

    public InformationSchemaOmcProgressHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaOmcProgress;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        DdlJobManager ddlJobManager = new DdlJobManager();
        List<DdlEngineRecord> ddlRecordList = ddlJobManager.fetchRecords(DdlState.ALL_STATES);
//        ddlRecordList = ddlRecordList.stream().filter(e -> !e.isSubJob()).collect(Collectors.toList());
        ddlRecordList = ddlRecordList.stream().filter(e -> DdlType.needShowDdlProgress(DdlType.valueOf(e.ddlType)))
            .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(ddlRecordList)) {
            return cursor;
        }

        final int jobIdIndex = InformationSchemaOmcProgress.getJobIdIndex();
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
        Set<String> jobIds = virtualView.getEqualsFilterValues(jobIdIndex, params);

        Map<Long, ThrottleInfo> throttleInfoMap = collectThrottleInfoMap();

        for (DdlEngineRecord record : ddlRecordList) {
            final long jobId = record.jobId;

            if (CollectionUtils.isNotEmpty(jobIds) && !jobIds.contains(String.valueOf(jobId))) {
                continue;
            }

            List<DdlEngineTaskRecord> allTasks = ddlJobManager.fetchAllSuccessiveTaskLessPartialInfoByJobId(jobId);
            List<DdlEngineTaskRecord> allOmcPhyDdlTasks =
                allTasks.stream().filter(e -> StringUtils.containsIgnoreCase(e.getName(), "OmcPhyDdlTask"))
                    .collect(Collectors.toList());

            for (DdlEngineTaskRecord taskRecord : allOmcPhyDdlTasks) {
                long taskId = taskRecord.taskId;
                boolean rebuildCleanup = isRebuildCleanupTask(taskRecord);
                List<OmcRecord> omcRecords = OmcUtils.getOmcRecords(taskRecord.jobId, taskId);
                if (omcRecords.isEmpty()) {
                    continue;
                }

                GsiBackfillManager backfillManager = new GsiBackfillManager(SystemDbHelper.DEFAULT_DB_NAME);
                List<Long> changesetIds =
                    omcRecords.stream().map(OmcRecord::getChangesetId).collect(Collectors.toList());
                List<GsiBackfillManager.BackFillAggInfo> backFillAggInfoList =
                    backfillManager.queryBackFillAggInfoById(changesetIds);
                Map<Long, GsiBackfillManager.BackFillAggInfo> backFillAggInfoMap =
                    backFillAggInfoList.stream()
                        .collect(Collectors.toMap(GsiBackfillManager.BackFillAggInfo::getBackFillId, e -> e));
                Map<Long, OmcCheckerThreadPool.OmcCheckerInfo> checkerInfoMap = collectOmcCheckerInfoMap(changesetIds);

                for (OmcRecord omcRecord : omcRecords) {
                    long changesetId = omcRecord.getChangesetId();
                    GsiBackfillManager.BackFillAggInfo backFillAggInfo = backFillAggInfoMap.get(changesetId);
                    if (backFillAggInfo == null) {
                        continue;
                    }

                    long finishRowCount = backFillAggInfo.getSuccessRowCount();
                    long totalRowCount = omcRecord.getTotalRowCount();
                    finishRowCount = adjustFinishRowCount(rebuildCleanup, backFillAggInfo.getStatus(),
                        finishRowCount, totalRowCount);
                    String progress = "-";
                    DecimalFormat df = new DecimalFormat("0.00");
                    if (totalRowCount > 0) {
                        // 有统计信息
                        double percent = (double) finishRowCount * 100.0 / totalRowCount;
                        percent = Math.min(percent, 100.0);
                        progress = df.format(percent) + "%";
                    }

                    String checkerProgress = "-";
                    OmcCheckerThreadPool.OmcCheckerInfo checkerInfo = checkerInfoMap.get(changesetId);
                    if (checkerInfo != null) {
                        checkerProgress = String.format("%s/%s", checkerInfo.getPhyTaskFinished().get(),
                            checkerInfo.getPhyTaskSum().get());
                    } else if (omcRecord.getOmcStatus() > OmcPhyDdlContext.OmcState.CHECKER.ordinal()) {
                        checkerProgress = "100%";
                    }

                    String applySpeed = throttleInfoMap.get(changesetId) != null ?
                        throttleInfoMap.get(changesetId).getSpeed() + " rows/s" : "-";

                    cursor.addRow(new Object[] {
                        jobId,
                        taskId,
                        changesetId,
                        omcRecord.getTableSchema(),
                        omcRecord.getTableName(),
                        omcRecord.getStorageInstId(),
                        omcRecord.getPhysicalDb(),
                        omcRecord.getPhysicalTable(),
                        omcRecord.getSpaceId(),
                        OmcPhyDdlContext.RunningState.values()[omcRecord.getStatus()].name(),
                        OmcPhyDdlContext.OmcState.values()[omcRecord.getOmcStatus()].name(),
                        progress,
                        finishRowCount,
                        totalRowCount,
                        applySpeed,
                        checkerProgress,
                        omcRecord.getStartTime(),
                        omcRecord.getEndTime(),
                        omcRecord.getCutOverTime(),
                        omcRecord.isRollback() ? "true" : "false",
                    });
                }
            }
        }
        return cursor;
    }

    static boolean isRebuildCleanupTask(DdlEngineTaskRecord taskRecord) {
        try {
            OmcPhyDdlTask task = (OmcPhyDdlTask) TaskHelper.fromDdlEngineTaskRecord(taskRecord);
            return StringUtils.isNotEmpty(task.getCleanupKeepFilter());
        } catch (Throwable t) {
            LOGGER.warn("Failed to identify REBUILD CLEANUP task " + taskRecord.getTaskId(), t);
            return false;
        }
    }

    static long adjustFinishRowCount(boolean rebuildCleanup, long backfillStatus, long finishRowCount,
                                     long totalRowCount) {
        if (rebuildCleanup && backfillStatus == BackfillStatus.SUCCESS.getValue()) {
            return totalRowCount;
        }
        return finishRowCount;
    }

    @Data
    public static class OmcCheckerInfoSyncAction implements IGmsSyncAction {
        List<Long> changesetIds;

        public OmcCheckerInfoSyncAction(List<Long> changesetIds) {
            this.changesetIds = changesetIds;
        }

        @Override
        public Object sync() {
            ArrayResultCursor resultCursor = new ArrayResultCursor("OMCCHECKER");
            resultCursor.addColumn("CHANGESET_ID", DataTypes.LongType);
            resultCursor.addColumn("TASK_SUM", DataTypes.LongType);
            resultCursor.addColumn("TASK_FINISHED", DataTypes.LongType);

            for (Long changesetId : changesetIds) {
                OmcCheckerThreadPool.OmcCheckerInfo checkerInfo = OmcCheckerThreadPool.getInstance()
                    .queryCheckTaskInfo(changesetId);
                if (checkerInfo != null) {
                    resultCursor.addRow(
                        new Object[] {
                            changesetId, checkerInfo.getPhyTaskSum().get(), checkerInfo.getPhyTaskFinished().get()}
                    );
                }
            }
            return resultCursor;
        }
    }

    private static Map<Long, OmcCheckerThreadPool.OmcCheckerInfo> collectOmcCheckerInfoMap(List<Long> changesetIds) {
        //handle fastchecker info
        Map<Long, OmcCheckerThreadPool.OmcCheckerInfo> mergedResult = new TreeMap<>();
        OmcCheckerInfoSyncAction syncAction = new OmcCheckerInfoSyncAction(changesetIds);
        GmsSyncManagerHelper.sync(syncAction, SystemDbHelper.DEFAULT_DB_NAME, SyncScope.MASTER_ONLY, results -> {
            if (results == null) {
                return;
            }

            for (Pair<GmsNodeManager.GmsNode, List<Map<String, Object>>> result : results) {
                if (CollectionUtils.isEmpty(result.getValue())) {
                    continue;
                }
                for (Map<String, Object> row : result.getValue()) {
                    long changesetId = DataTypes.LongType.convertFrom(row.get("CHANGESET_ID"));
                    long taskSum = DataTypes.LongType.convertFrom(row.get("TASK_SUM"));
                    long taskFinished = DataTypes.LongType.convertFrom(row.get("TASK_FINISHED"));

                    mergedResult.putIfAbsent(changesetId, new OmcCheckerThreadPool.OmcCheckerInfo());
                    mergedResult.get(changesetId).getPhyTaskSum().addAndGet((int) taskSum);
                    mergedResult.get(changesetId).getPhyTaskFinished().addAndGet((int) taskFinished);
                }
            }
        });

        return mergedResult;
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
}
