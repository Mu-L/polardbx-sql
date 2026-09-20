package com.alibaba.polardbx.executor.ddl.workqueue;

import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.Getter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wumu
 */
public class OmcCheckerThreadPool extends BackFillThreadPool {
    private static final OmcCheckerThreadPool INSTANCE = new OmcCheckerThreadPool();

    public OmcCheckerThreadPool() {
        super(Math.max(ThreadCpuStatUtil.NUM_CORES, 4));
        cache = CacheBuilder.newBuilder()
            .maximumSize(4096)
            .expireAfterWrite(7, TimeUnit.DAYS)
            .expireAfterAccess(7, TimeUnit.DAYS)
            .build();
    }

    public static OmcCheckerThreadPool getInstance() {
        return INSTANCE;
    }

    /**
     * this cache is used to count fastChecker's progress by ddl jobId
     * cache < changesetId, FastCheckerInfo>
     */
    Cache<Long, OmcCheckerInfo> cache;

    @Getter
    public static class OmcCheckerInfo {
        private final AtomicInteger phyTaskSum;
        private final AtomicInteger phyTaskFinished;

        public OmcCheckerInfo() {
            phyTaskSum = new AtomicInteger(0);
            phyTaskFinished = new AtomicInteger(0);
        }
    }

    public void increaseCheckTaskInfo(long changesetId, int newTaskSum, int newTaskFinished) {
        try {
            OmcCheckerInfo info = this.cache.get(changesetId, OmcCheckerInfo::new);
            info.getPhyTaskSum().addAndGet(newTaskSum);
            info.getPhyTaskFinished().addAndGet(newTaskFinished);
        } catch (Exception e) {
            SQLRecorderLogger.ddlLogger.error(
                "failed to increase omcChecker task info", e
            );
        }
    }

    public OmcCheckerInfo queryCheckTaskInfo(long changesetId) {
        try {
            return this.cache.getIfPresent(changesetId);
        } catch (Exception e) {
            SQLRecorderLogger.ddlLogger.error(
                "failed to query omcChecker task info", e
            );
        }
        return null;
    }

    public void invalidateTaskInfo(long changesetId) {
        this.cache.invalidate(changesetId);
    }
}
