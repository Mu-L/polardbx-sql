package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import org.apache.calcite.sql.SqlKind;
import org.openjdk.jol.info.ClassLayout;

public class TopNThresholdFilter implements RFBloomFilter {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TopNThresholdFilter.class).instanceSize();

    @FieldMemoryCounter(value = false)
    private final GlobalTopNThreshold globalTopNThreshold;

    private final boolean isReversed;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE;
    }

    public TopNThresholdFilter(GlobalTopNThreshold globalTopNThreshold, SqlKind sqlKind) {
        this.globalTopNThreshold = globalTopNThreshold;
        this.isReversed = sqlKind == SqlKind.LESS_THAN;
    }

    @Override
    public boolean mightContainLong(long value) {
        if (!globalTopNThreshold.isInitialized()) {
            return true;
        }

        // asc/desc:
        // cannot handle nullable value.
        // value == 0: maybe the value in this position is null.
        if (value == 0) {
            return true;
        }

        if (globalTopNThreshold.getIndexRow().compare(value, false, isReversed) > 0) {
            // discard.
            return false;
        }

        return true;
    }

    @Override
    public boolean isNull() {
        return globalTopNThreshold.isNull();
    }

    @Override
    public int getIntThreshold() {
        return globalTopNThreshold.getIntThreshold();
    }

    @Override
    public long getLongThreshold() {
        return globalTopNThreshold.getLongThreshold();
    }

    @Override
    public boolean isInitialized() {
        return globalTopNThreshold.isInitialized();
    }

    @Override
    public void putInt(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mightContainInt(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong(long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sizeInBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void merge(RFBloomFilter other) {
        throw new UnsupportedOperationException();
    }
}
