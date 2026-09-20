package com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TtlScanWarningManager extends AbstractLifecycle {
    protected static final TtlScanWarningManager instance = new TtlScanWarningManager();

    private TtlAddPartsWarningScanner addPartsWarningScanner = new TtlAddPartsWarningScanner();

    private volatile ScheduledThreadPoolExecutor warningScannerThread =
        ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("Ttl-Table-AddPartsWarning-Scanner-Thread", true),
            new ThreadPoolExecutor.DiscardPolicy());

    public static TtlScanWarningManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    @Override
    protected void doInit() {
        super.doInit();
        initTtlWarningScanner();
    }

    protected void initTtlWarningScanner() {
        resetWarningScannerScheduleInterval();
    }

    public synchronized void resetWarningScannerScheduleInterval() {

        if (warningScannerThread != null) {
            if (!warningScannerThread.isShutdown()) {
                warningScannerThread.shutdownNow();
            }
        }

        warningScannerThread =
            ExecutorUtil.createScheduler(1,
                new NamedThreadFactory("Ttl-Table-AddPartsWarning-Scanner-Thread", true),
                new ThreadPoolExecutor.DiscardPolicy());

        Long newIntervalSeconds = DynamicConfig.getInstance().getTtlAddPartsWarningScanIntervalSeconds();
        warningScannerThread.scheduleWithFixedDelay(
            addPartsWarningScanner,
            300L,
            newIntervalSeconds,
            TimeUnit.SECONDS
        );
    }
}