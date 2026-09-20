package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.operator.scan.impl.AdaptiveColumnarScanMonitor;
import com.alibaba.polardbx.executor.operator.scan.impl.AdaptiveColumnarScanStatus;
import com.alibaba.polardbx.executor.operator.scan.impl.ScanWorkId;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public interface ColumnarScanMonitor {
    AdaptiveColumnarScanMonitor INSTANCE = new AdaptiveColumnarScanMonitor();

    static ColumnarScanMonitor getInstance() {
        return INSTANCE;
    }

    void resize(int maximumSize);

    AdaptiveColumnarScanStatus create(ScanWorkId.WorkId workId, AtomicInteger taskSequenceNumber, boolean splitTask);

    void addStatus(AdaptiveColumnarScanStatus status);

    List<Object[]> generatePackets();

    Iterator<AdaptiveColumnarScanStatus> getStatusIterator();
}
