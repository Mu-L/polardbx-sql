package com.alibaba.polardbx.executor.operator.scan.impl;

import lombok.Data;

@Data
public class AdaptiveColumnarScanStatus {

    enum Status {
        QUEUE,
        RUNNING,
        SUCCESS,
        FAILED
    }

    private final ScanWorkId workId;
    private final boolean splitTask;

    private Status status;
    private String errorMsg;

    private long startTime;
    private long scheduleTime;
    private long endTime;

    // Adaptive control
    private int startRowGroupId;
    private int granularity;
    private int threadLimit;
    private long acquiredFilterIOPermits;
    private long acquiredFilterPermits;
    private long acquiredProjectIOPermits;
    private long acquiredProjectPermits;
    private long remainingPermits;

    private long totalScanRows;
    private long totalScanBytes;
    private long totalFilteredRows;

    public AdaptiveColumnarScanStatus(ScanWorkId workId, boolean splitTask) {
        this.workId = workId;
        this.splitTask = splitTask;
    }
}
