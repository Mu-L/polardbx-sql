package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.recyclebin.PhyRecycleBinInfoAccessor;
import com.alibaba.polardbx.gms.recyclebin.PhyRecycleBinInfoRecord;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.PURGE_RECYLE_BIN;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * recycle bin 2.0 scheduled job
 *
 * @author luoyanxin.pt
 */
public class PurgeRecycleBinScheduledJob extends SchedulerExecutor {
    private static final String SKIP_CLEAN_PHY_RECYCLE_BIN_TASK = "SKIP_CLEAN_PHY_RECYCLE_BIN_TASK";
    private static final Logger logger = LoggerFactory.getLogger(PurgeRecycleBinScheduledJob.class);
    private final ExecutableScheduledJob executableScheduledJob;

    public PurgeRecycleBinScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    public long getScheduleId() {
        return this.executableScheduledJob.getScheduleId();
    }

    @Override
    public boolean execute() {
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();

        try {
            // Mark as RUNNING.
            boolean casSuccess =
                ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if (!casSuccess) {
                ModuleLogInfo moduleLogInfo = ModuleLogInfo.getInstance();
                if (moduleLogInfo != null) {
                    moduleLogInfo.logRecord(
                        Module.SCHEDULE_JOB,
                        STATE_CHANGE_FAIL,
                        new String[] {PURGE_RECYLE_BIN + "," + fireTime, QUEUED.name(), RUNNING.name()},
                        WARNING);
                }
                return false;
            }

            final Map savedMdcContext = MDC.getCopyOfContextMap();
            String remark;
            try {
                MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
                remark = cleanUpIfNeed();
            } finally {
                if (savedMdcContext != null) {
                    MDC.setContextMap(savedMdcContext);
                } else {
                    MDC.clear();
                }
            }

            long finishTime = System.currentTimeMillis() / 1000;
            return ScheduledJobsManager
                .casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
        } catch (Throwable t) {
            logger.error(t);
            ModuleLogInfo moduleLogInfo = ModuleLogInfo.getInstance();
            if (moduleLogInfo != null) {
                moduleLogInfo.logRecord(
                    Module.TRX,
                    UNEXPECTED,
                    new String[] {
                        PURGE_RECYLE_BIN + "," + fireTime,
                        t.getMessage()
                    },
                    CRITICAL,
                    t
                );
            }
            String remark = "purge physical recycle bin task error: " + t.getMessage();
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, remark, t.getMessage());
            return false;
        }
    }

    private String cleanUpIfNeed() {
        if (!InstConfUtil.isInPurgePhyRecyclebinMaintenanceTimeWindow()) {
            return SKIP_CLEAN_PHY_RECYCLE_BIN_TASK;
        }
        StringBuffer sb = new StringBuffer();
        long purgeRecycleBinBeforeMinutes =
            getInstConfigAsLong(ConnectionProperties.MAX_PHY_RECYCLEBIN_RETENTION_MINUTES,
                Long.valueOf(ConnectionParams.MAX_PHY_RECYCLEBIN_RETENTION_MINUTES.getDefault()));
        List<PhyRecycleBinInfoRecord> recycleBinInfoRecords = new ArrayList<>();
        List<PhyRecycleBinInfoRecord> finishedRecords = new ArrayList<>();
        try (Connection connection = MetaDbUtil.getConnection()) {
            if (connection == null) {
                logger.error("Failed to get MetaDb connection for querying recycle bin records");
                sb.append("clean up finished records: 0");
                return sb.toString();
            }
            PhyRecycleBinInfoAccessor recycleBinInfoAccessor = new PhyRecycleBinInfoAccessor();
            recycleBinInfoAccessor.setConnection(connection);
            recycleBinInfoRecords.addAll(recycleBinInfoAccessor.getUnDropRecordByMinute(purgeRecycleBinBeforeMinutes));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        for (PhyRecycleBinInfoRecord recycleBinInfoRecord : recycleBinInfoRecords) {
            // check storage existence
            HaSwitchParams haSwitchParams = null;
            try {
                StorageHaManager storageHaManager = StorageHaManager.getInstance();
                if (storageHaManager != null) {
                    haSwitchParams =
                        storageHaManager.getStorageHaSwitchParams(recycleBinInfoRecord.getStorageInstId());
                }
            } catch (Exception e) {
                logger.warn("Failed to get HaSwitchParams for storage " + recycleBinInfoRecord.getStorageInstId()
                    + ", skip record: db=" + recycleBinInfoRecord.getCurDbName()
                    + ", tb=" + recycleBinInfoRecord.getCurTbName(), e);
            }
            if (haSwitchParams == null) {
                logger.warn("HaSwitchParams is null for storage " + recycleBinInfoRecord.getStorageInstId()
                    + ", storage may have been removed, skip and mark as finished: db="
                    + recycleBinInfoRecord.getCurDbName() + ", tb=" + recycleBinInfoRecord.getCurTbName());
                finishedRecords.add(recycleBinInfoRecord);
                continue;
            }
            try (Connection conn = DbTopologyManager.getConnectionForStorage(recycleBinInfoRecord.getStorageInstId())) {
                if (conn == null) {
                    logger.warn("Connection is null for storage " + recycleBinInfoRecord.getStorageInstId()
                        + ", skip record: db=" + recycleBinInfoRecord.getCurDbName()
                        + ", tb=" + recycleBinInfoRecord.getCurTbName());
                    continue;
                }
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate("drop table if exists " + recycleBinInfoRecord.getCurDbName() + "."
                        + recycleBinInfoRecord.getCurTbName());
                }
                finishedRecords.add(recycleBinInfoRecord);
                logger.info("Successfully dropped recycle bin table: storage=" + recycleBinInfoRecord.getStorageInstId()
                    + ", db=" + recycleBinInfoRecord.getCurDbName()
                    + ", tb=" + recycleBinInfoRecord.getCurTbName());
            } catch (Exception e) {
                logger.error("Failed to drop recycle bin table: storage=" + recycleBinInfoRecord.getStorageInstId()
                    + ", db=" + recycleBinInfoRecord.getCurDbName()
                    + ", tb=" + recycleBinInfoRecord.getCurTbName(), e);
                // continue to process remaining records
            }
        }
        try (Connection connection = MetaDbUtil.getConnection()) {
            if (connection == null) {
                logger.error("Failed to get MetaDb connection for updating recycle bin records");
                sb.append("clean up finished records: " + finishedRecords.size());
                return sb.toString();
            }
            PhyRecycleBinInfoAccessor recycleBinInfoAccessor = new PhyRecycleBinInfoAccessor();
            recycleBinInfoAccessor.setConnection(connection);
            for (PhyRecycleBinInfoRecord recycleBinInfoRecord : finishedRecords) {
                try {
                    recycleBinInfoAccessor.updateByStorageAndTb(recycleBinInfoRecord.getStorageInstId(),
                        recycleBinInfoRecord.getCurTbName(), PhyRecycleBinInfoRecord.STATUS_DROP);
                    recycleBinInfoAccessor.deleteFinishRecordByStorageAndTb(recycleBinInfoRecord.getStorageInstId(),
                        recycleBinInfoRecord.getCurTbName());
                } catch (Exception e) {
                    logger.error(
                        "Failed to update meta for recycle bin record: storage="
                            + recycleBinInfoRecord.getStorageInstId()
                            + ", tb=" + recycleBinInfoRecord.getCurTbName(), e);
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to get MetaDb connection for updating recycle bin records", e);
        }
        sb.append("clean up finished records: " + finishedRecords.size());
        return sb.toString();
    }

    long getInstConfigAsLong(String key, Long defaultVal) {
        MetaDbInstConfigManager mgr = MetaDbInstConfigManager.getInstance();
        if (mgr == null) {
            return defaultVal;
        }
        String val = mgr.getInstProperty(key);
        if (StringUtils.isEmpty(val)) {
            return defaultVal;
        }
        try {
            return Long.valueOf(val);
        } catch (Exception e) {
            logger.error(String.format("parse param:[%s=%s] error", key, val), e);
            return defaultVal;
        }
    }
}