package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.ColumnarScanMonitor;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AdaptiveColumnarScanMonitor implements ColumnarScanMonitor {

    public static final int MAXIMUM_SIZE = 4096;
    public static final int DURATION = 7;

    private final Cache<ScanWorkId, Object> cache;
    private final ConcurrentHashMap<ScanWorkId, AdaptiveColumnarScanStatus> statusMap;

    public AdaptiveColumnarScanMonitor() {
        this.statusMap = new ConcurrentHashMap<>();
        this.cache = Caffeine.newBuilder()
            .maximumSize(MAXIMUM_SIZE)
            .expireAfterWrite(DURATION, TimeUnit.DAYS)
            .evictionListener((key, value, cause) -> statusMap.remove(key))
            .build();
    }

    @Override
    public void resize(int maximumSize) {
        if (maximumSize < 0) {
            // maximumSize must be 0 or positive
            return;
        }
        cache.policy().eviction().ifPresent(e -> e.setMaximum(maximumSize));
    }

    @Override
    public AdaptiveColumnarScanStatus create(ScanWorkId.WorkId workId, AtomicInteger taskSequenceNumber,
                                             boolean splitTask) {
        ScanWorkId scanWorkId = ScanWorkId.of(workId, taskSequenceNumber.getAndIncrement());
        AdaptiveColumnarScanStatus status = new AdaptiveColumnarScanStatus(scanWorkId, splitTask);
        addStatus(status);
        return status;
    }

    @Override
    public void addStatus(AdaptiveColumnarScanStatus status) {
        ScanWorkId workId = status.getWorkId();

        cache.get(workId, k -> {
            statusMap.put(k, status);
            return new Object();
        });
    }

    @Override
    public List<Object[]> generatePackets() {
        List<Object[]> packets = new ArrayList<>();
        Iterator<AdaptiveColumnarScanStatus> statusIterator = getStatusIterator();

        while (statusIterator.hasNext()) {
            AdaptiveColumnarScanStatus status = statusIterator.next();

            Object[] row = new Object[26];
            int index = 0;
            row[index++] = status.getWorkId().getWorkId().getQueryId();
            row[index++] = status.getWorkId().getWorkId().getLogicalSchema();
            row[index++] = status.getWorkId().getWorkId().getLogicalTable();
            row[index++] = status.getWorkId().getWorkId().getFilePath();
            row[index++] = status.getWorkId().getWorkId().getStripeId();
            row[index++] = status.getWorkId().getWorkId().getWorkNumber();
            row[index++] = status.getWorkId().getSequence();
            row[index++] = status.isSplitTask();
            row[index++] = status.getStatus();
            row[index++] = status.getErrorMsg();
            row[index++] = new java.sql.Timestamp(status.getStartTime());
            row[index++] = new java.sql.Timestamp(status.getScheduleTime());
            row[index++] = new java.sql.Timestamp(status.getEndTime());

            // QUEUE_TIME_COST
            row[index++] = status.getScheduleTime() - status.getStartTime();
            // RUNNING_TIME_COST
            row[index++] = status.getEndTime() - status.getStartTime();

            row[index++] = status.getStartRowGroupId();
            row[index++] = status.getGranularity();
            row[index++] = status.getThreadLimit();
            row[index++] = status.getAcquiredFilterIOPermits();
            row[index++] = status.getAcquiredFilterPermits();
            row[index++] = status.getAcquiredProjectIOPermits();
            row[index++] = status.getAcquiredProjectPermits();
            row[index++] = status.getRemainingPermits();
            row[index++] = status.getTotalScanRows();
            row[index++] = status.getTotalScanBytes();
            row[index++] = status.getTotalFilteredRows();
            packets.add(row);
        }

        return packets;
    }

    @Override
    public Iterator<AdaptiveColumnarScanStatus> getStatusIterator() {
        return statusMap.values().iterator();
    }

}
