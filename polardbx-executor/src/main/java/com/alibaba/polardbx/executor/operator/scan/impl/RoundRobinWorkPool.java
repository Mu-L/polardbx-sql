package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.metadata.SplitType;
import com.alibaba.polardbx.executor.mpp.planner.EarlyStopManager;
import com.alibaba.polardbx.executor.mpp.planner.EarlyStopManagerImpl;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.executor.operator.scan.WorkPool;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RoundRobinWorkPool implements WorkPool<ColumnarSplit, Chunk> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EarlyStopManagerImpl.class);
    private Map<Integer, List<ColumnarSplit>> splitMap;
    private boolean noMoreSplits;
    private int currentIndex;
    private int removedSplitCount;
    private EarlyStopManager earlyStopManager;

    public RoundRobinWorkPool(EarlyStopManager earlyStopManager) {
        this.earlyStopManager = earlyStopManager;
        this.splitMap = new LinkedHashMap<>();
        this.noMoreSplits = false;
        this.currentIndex = 0;
        this.removedSplitCount = 0;
    }

    @Override
    public void addSplit(int driverId, ColumnarSplit split) {

        splitMap.compute(driverId, (driverIdKey,allSplits) -> {
            if (allSplits == null) {
                allSplits = new ArrayList<>();
            }

            allSplits.add(split);

            return allSplits;
        });
    }

    @Override
    public void noMoreSplits(int driverId) {
        noMoreSplits = true;
    }

    @Override
    public ScanWork<ColumnarSplit, Chunk> pickUp(int driverId) {
        Preconditions.checkArgument(noMoreSplits);

        List<ColumnarSplit> splitList = splitMap.get(driverId);
        if (splitList == null || splitList.isEmpty()) {
            return null;
        }

        if (removedSplitCount == splitList.size()) {
            return null;
        }

        while (currentIndex < splitList.size()) {
            ColumnarSplit columnarSplit = splitList.get(currentIndex);
            if (columnarSplit == null) {

                if (removedSplitCount == splitList.size()) {
                    // run out.
                    return null;
                }

                // check next split.
                currentIndex++;
                if (currentIndex == splitList.size()) {
                    currentIndex = 0;
                }
                continue;
            }

            ScanWork<ColumnarSplit, Chunk> scanWork;
            if (earlyStopManager != null && earlyStopManager.isEarlyStopRegistered(columnarSplit.getFilePrefix())) {
                scanWork = null;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("kill work id = " + columnarSplit.getFilePrefix());
                }
            } else {
                scanWork = columnarSplit.nextWork();
            }

            if (scanWork == null) {
                // this split has no more scan work.
                splitList.set(currentIndex, null);
                removedSplitCount++;
                if (removedSplitCount == splitList.size()) {
                    // run out.
                    return null;
                }

                // check next split.
                currentIndex++;
                if (currentIndex == splitList.size()) {
                    currentIndex = 0;
                }
            } else {
                if (columnarSplit.getSplitType() == SplitType.ORC) {
                    // for orc, just produce one scan-work at once.
                    // and then, we should use scan-work from the next split.
                    currentIndex++;
                    if (currentIndex == splitList.size()) {
                        currentIndex = 0;
                    }
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("produce scan work = " + scanWork.getWorkId());
                }

                return scanWork;
            }

        }

        return null;
    }
}
