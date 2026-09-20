package com.alibaba.polardbx.executor.ddl.newengine.meta;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.DdlTaskBarrierAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlTaskBarrierRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author luoyanxin.pt
 */
public class DdlTaskBarrierManager {
    private static final Logger LOGGER = SQLRecorderLogger.ddlLogger;
    @Getter
    protected static DdlTaskBarrierManager instance = new DdlTaskBarrierManager();
    public static long BARRIER_CHECK_INTERVAL_MS = 100L;
    public static long BARRIER_GET_LOCK_TIMEOUT_MS = 500L;
    private final ReentrantLock lock = new ReentrantLock();

    private DdlTaskBarrierManager() {
    }

    public void reachBarrier(String schemaName, String barrierName, long taskId,
                             AtomicBoolean alreadyReach, ExecutionContext ec) {
        boolean jobInterrupted = CrossEngineValidator.isJobInterrupted(ec);
        boolean getBarrierLock = false;
        LOGGER.info(
            String.format(
                "[tableSchema: %s, barrier:%s, taskId:%d] reach barrier point",
                schemaName,
                barrierName, taskId));
        while (!jobInterrupted && !getBarrierLock) {
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                try {
                    MetaDbUtil.beginTransaction(metaDbConn);
                    DdlTaskBarrierAccessor accessor = new DdlTaskBarrierAccessor();
                    accessor.setConnection(metaDbConn);
                    DdlTaskBarrierRecord record = accessor.queryForUpdateBySchBarrier(schemaName, barrierName);
                    if (record == null) {
                        boolean getReentrantLock = false;
                        try {
                            if (lock.tryLock(BARRIER_GET_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                                getReentrantLock = true;
                                record = accessor.queryForUpdateBySchBarrier(schemaName, barrierName);
                                if (record == null) {
                                    accessor.initBarrier(schemaName, barrierName, taskId);
                                    record = accessor.queryForUpdateBySchBarrier(schemaName, barrierName);
                                    getBarrierLock = true;
                                    LOGGER.info(
                                        String.format(
                                            "[tableSchema: %s, barrier:%s, taskId:%d] get the barrier lock",
                                            schemaName,
                                            barrierName, taskId));
                                } else {
                                    if (record.ref_cnt == 0) {
                                        accessor.increaseBarrierRef(schemaName, barrierName, taskId);
                                        getBarrierLock = true;
                                        LOGGER.info(
                                            String.format(
                                                "[tableSchema: %s, barrier:%s, taskId:%d] get the barrier lock",
                                                schemaName,
                                                barrierName, taskId));
                                    } else if (record.lastUpdatedTaskId == taskId) {
                                        // if the last taskId is the same as current taskId, then we can get the barrier lock again
                                        // this is for the case that the barrier lock is got by this taskId,
                                        // but the barrier task is not stored in metaDb( maybe CN leader is crashed),
                                        // so we need to retry to get the barrier lock again
                                        getBarrierLock = true;
                                        LOGGER.info(
                                            String.format(
                                                "[tableSchema: %s, barrier:%s, taskId:%d] get the barrier lock again",
                                                schemaName,
                                                barrierName, taskId));
                                    } else {
                                        Thread.sleep(BARRIER_CHECK_INTERVAL_MS);
                                        jobInterrupted = CrossEngineValidator.isJobInterrupted(ec);
                                    }
                                }
                            }
                        } finally {
                            if(getReentrantLock) {
                                lock.unlock();
                            }
                        }
                    } else {
                        if (record.ref_cnt == 0) {
                            accessor.increaseBarrierRef(schemaName, barrierName, taskId);
                            getBarrierLock = true;
                            LOGGER.info(
                                String.format(
                                    "[tableSchema: %s, barrier:%s, taskId:%d] get the barrier lock",
                                    schemaName,
                                    barrierName, taskId));
                        } else if (record.lastUpdatedTaskId == taskId) {
                            // if the last taskId is the same as current taskId, then we can get the barrier lock again
                            // this is for the case that the barrier lock is got by this taskId,
                            // but the barrier task is not stored in metaDb( maybe CN leader is crashed),
                            // so we need to retry to get the barrier lock again
                            getBarrierLock = true;
                            LOGGER.info(
                                String.format(
                                    "[tableSchema: %s, barrier:%s, taskId:%d] get the barrier lock again",
                                    schemaName,
                                    barrierName, taskId));
                        } else {
                            Thread.sleep(BARRIER_CHECK_INTERVAL_MS);
                            jobInterrupted = CrossEngineValidator.isJobInterrupted(ec);
                        }
                    }
                } finally {
                    MetaDbUtil.endTransaction(metaDbConn, null);
                    if (getBarrierLock) {
                        alreadyReach.set(true);
                    }
                }
            } catch (Exception ex) {
                alreadyReach.set(false);
                throw GeneralUtil.nestedException(ex);
            }
        }
    }

    public void releaseBarrier(String schemaName, String barrierName, long taskId, Connection metaDbConn) {
        DdlTaskBarrierAccessor accessor = new DdlTaskBarrierAccessor();
        accessor.setConnection(metaDbConn);
        accessor.decreaseBarrierRef(schemaName, barrierName);
        LOGGER.info(
            String.format(
                "[tableSchema: %s, barrier:%s, taskId:%d] release barrier",
                schemaName,
                barrierName, taskId));
    }

    public void deleteBarrierBySchema(String tableSchema, Connection metaDbConn) {
        DdlTaskBarrierAccessor accessor = new DdlTaskBarrierAccessor();
        accessor.setConnection(metaDbConn);
        accessor.deleteBarrierBySchema(tableSchema);
    }
}
