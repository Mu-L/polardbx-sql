package com.alibaba.polardbx.common.columnar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionStorageStatistics {
    // For thread level VersionStorageStatistics object.
    private static final ThreadLocal<VersionStorageStatistics> THREAD_LOCAL = new ThreadLocal<>();

    // must be used in try-finally code block.
    public static void setThreadLocalStatistics(VersionStorageStatistics statistics) {
        THREAD_LOCAL.set(statistics);
    }

    public static VersionStorageStatistics getThreadLocalStatistics() {
        return THREAD_LOCAL.get();
    }

    // must be used in try-finally code block.
    public static void removeThreadLocalStatistics() {
        THREAD_LOCAL.remove();
    }

    private int hash = System.identityHashCode(this);

    // for GMS round-trip time.
    private long maxGMSRt = Long.MIN_VALUE;
    private long minGMSRt = Long.MAX_VALUE;
    private long sumGMSRt = 0;
    private long gmsRtCount = 0;

    // for OSS csv round-trip time.
    private long maxCSVRt = Long.MIN_VALUE;
    private long minCSVRt = Long.MAX_VALUE;
    private long sumCSVRt = 0;
    private long csvRtCount = 0;

    // for OSS orc round-tripe time.
    private long maxORCRt = Long.MIN_VALUE;
    private long minORCRt = Long.MAX_VALUE;
    private long sumORCRt = 0;
    private long orcRtCount = 0;

    // for OSS del round-trip time.
    private long maxDELRt = Long.MIN_VALUE;
    private long minDELRt = Long.MAX_VALUE;
    private long sumDELRt = 0;
    private long delRtCount = 0;

    // for preheat round-trip time.
    private long maxPreheatRt = Long.MIN_VALUE;
    private long minPreheatRt = Long.MAX_VALUE;
    private long sumPreheatRt = 0;
    private long preheatRtCount = 0;

    private transient Lock gmsRtUpdateLock = new ReentrantLock();
    private transient Lock csvRtUpdateLock = new ReentrantLock();
    private transient Lock orcRtUpdateLock = new ReentrantLock();
    private transient Lock delRtUpdateLock = new ReentrantLock();
    private transient Lock preheatRtUpdateLock = new ReentrantLock();

    public VersionStorageStatistics() {
    }

    public static VersionStorageStatistics from(Collection<VersionStorageStatistics> collection) {
        VersionStorageStatistics result = new VersionStorageStatistics();
        for (VersionStorageStatistics stats : collection) {
            result.csvRtCount += stats.csvRtCount;
            result.sumCSVRt += stats.sumCSVRt;
            result.maxCSVRt = Math.max(result.maxCSVRt, stats.maxCSVRt);
            result.minCSVRt = Math.min(result.minCSVRt, stats.minCSVRt);

            result.orcRtCount += stats.orcRtCount;
            result.sumORCRt += stats.sumORCRt;
            result.maxORCRt = Math.max(result.maxORCRt, stats.maxORCRt);
            result.minORCRt = Math.min(result.minORCRt, stats.minORCRt);

            result.delRtCount += stats.delRtCount;
            result.sumDELRt += stats.sumDELRt;
            result.maxDELRt = Math.max(result.maxDELRt, stats.maxDELRt);
            result.minDELRt = Math.min(result.minDELRt, stats.minDELRt);

            result.gmsRtCount += stats.gmsRtCount;
            result.sumGMSRt += stats.sumGMSRt;
            result.maxGMSRt = Math.max(result.maxGMSRt, stats.maxGMSRt);
            result.minGMSRt = Math.min(result.minGMSRt, stats.minGMSRt);

            result.preheatRtCount += stats.preheatRtCount;
            result.sumPreheatRt += stats.sumPreheatRt;
            result.maxPreheatRt = Math.max(result.maxPreheatRt, stats.maxPreheatRt);
            result.minPreheatRt = Math.min(result.minPreheatRt, stats.minPreheatRt);
        }
        return result;
    }

    @JsonCreator
    public VersionStorageStatistics(
        @JsonProperty("hash") int hash,
        @JsonProperty("maxGMSRt") long maxGMSRt,
        @JsonProperty("minGMSRt") long minGMSRt,
        @JsonProperty("sumGMSRt") long sumGMSRt,
        @JsonProperty("gmsRtCount") long gmsRtCount,
        @JsonProperty("maxCSVRt") long maxCSVRt,
        @JsonProperty("minCSVRt") long minCSVRt,
        @JsonProperty("sumCSVRt") long sumCSVRt,
        @JsonProperty("csvRtCount") long csvRtCount,
        @JsonProperty("maxORCRt") long maxORCRt,
        @JsonProperty("minORCRt") long minORCRt,
        @JsonProperty("sumORCRt") long sumORCRt,
        @JsonProperty("orcRtCount") long orcRtCount,
        @JsonProperty("maxDELRt") long maxDELRt,
        @JsonProperty("minDELRt") long minDELRt,
        @JsonProperty("sumDELRt") long sumDELRt,
        @JsonProperty("delRtCount") long delRtCount,
        @JsonProperty("maxPreheatRt") long maxPreheatRt,
        @JsonProperty("minPreheatRt") long minPreheatRt,
        @JsonProperty("sumPreheatRt") long sumPreheatRt,
        @JsonProperty("preheatRtCount") long preheatRtCount) {
        this.maxGMSRt = maxGMSRt;
        this.minGMSRt = minGMSRt;
        this.sumGMSRt = sumGMSRt;
        this.gmsRtCount = gmsRtCount;
        this.maxCSVRt = maxCSVRt;
        this.minCSVRt = minCSVRt;
        this.sumCSVRt = sumCSVRt;
        this.csvRtCount = csvRtCount;
        this.maxORCRt = maxORCRt;
        this.minORCRt = minORCRt;
        this.sumORCRt = sumORCRt;
        this.orcRtCount = orcRtCount;
        this.maxDELRt = maxDELRt;
        this.minDELRt = minDELRt;
        this.sumDELRt = sumDELRt;
        this.delRtCount = delRtCount;
        this.maxPreheatRt = maxPreheatRt;
        this.minPreheatRt = minPreheatRt;
        this.sumPreheatRt = sumPreheatRt;
        this.preheatRtCount = preheatRtCount;
        this.hash = hash;
    }

    @JsonProperty
    public int getHash() {
        return hash;
    }

    @JsonProperty
    public long getMaxGMSRt() {
        return maxGMSRt == Long.MIN_VALUE ? 0 : maxGMSRt;
    }

    @JsonProperty
    public long getMinGMSRt() {
        return minGMSRt == Long.MAX_VALUE ? 0 : minGMSRt;
    }

    @JsonProperty
    public long getSumRtm() {
        return sumGMSRt;
    }

    @JsonProperty
    public long getGmsRtCount() {
        return gmsRtCount;
    }

    @JsonProperty
    public long getMaxCSVRt() {
        return maxCSVRt == Long.MIN_VALUE ? 0 : maxCSVRt;
    }

    @JsonProperty
    public long getMinCSVRt() {
        return minCSVRt == Long.MAX_VALUE ? 0 : minCSVRt;
    }

    @JsonProperty
    public long getSumCSVRt() {
        return sumCSVRt;
    }

    @JsonProperty
    public long getCsvRtCount() {
        return csvRtCount;
    }

    @JsonProperty
    public long getMaxORCRt() {
        return maxORCRt == Long.MIN_VALUE ? 0 : maxORCRt;
    }

    @JsonProperty
    public long getMinORCRt() {
        return minORCRt == Long.MAX_VALUE ? 0 : minORCRt;
    }

    @JsonProperty
    public long getSumORCRt() {
        return sumORCRt;
    }

    @JsonProperty
    public long getOrcRtCount() {
        return orcRtCount;
    }

    @JsonProperty
    public long getMaxDELRt() {
        return maxDELRt == Long.MIN_VALUE ? 0 : maxDELRt;
    }

    @JsonProperty
    public long getMinDELRt() {
        return minDELRt == Long.MAX_VALUE ? 0 : minDELRt;
    }

    @JsonProperty
    public long getSumDELRt() {
        return sumDELRt;
    }

    @JsonProperty
    public long getDelRtCount() {
        return delRtCount;
    }

    @JsonProperty
    public long getMaxPreheatRt() {
        return maxPreheatRt == Long.MIN_VALUE ? 0 : maxPreheatRt;
    }

    @JsonProperty
    public long getMinPreheatRt() {
        return minPreheatRt == Long.MAX_VALUE ? 0 : minPreheatRt;
    }

    @JsonProperty
    public long getSumPreheatRt() {
        return sumPreheatRt;
    }

    @JsonProperty
    public long getPreheatRtCount() {
        return preheatRtCount;
    }

    public void updateGmsStatistics(long rt) {
        gmsRtUpdateLock.lock();
        try {
            maxGMSRt = Math.max(maxGMSRt, rt);
            minGMSRt = Math.min(minGMSRt, rt);
            sumGMSRt += rt;
            gmsRtCount++;
        } finally {
            gmsRtUpdateLock.unlock();
        }

        // Also update global counters using LongAdder for better performance
        ColumnarDataSourceMetrics.updateGmsStatistics(rt);
    }

    public void updateCsvStatistics(long rt) {
        csvRtUpdateLock.lock();
        try {
            maxCSVRt = Math.max(maxCSVRt, rt);
            minCSVRt = Math.min(minCSVRt, rt);
            sumCSVRt += rt;
            csvRtCount++;
        } finally {
            csvRtUpdateLock.unlock();
        }

        // Also update global counters
        ColumnarDataSourceMetrics.updateCsvStatistics(rt);
    }

    public void updateOrcStatistics(long rt) {
        orcRtUpdateLock.lock();
        try {
            maxORCRt = Math.max(maxORCRt, rt);
            minORCRt = Math.min(minORCRt, rt);
            sumORCRt += rt;
            orcRtCount++;
        } finally {
            orcRtUpdateLock.unlock();
        }

        // Also update global counters
        ColumnarDataSourceMetrics.updateOrcStatistics(rt);
    }

    public void updateDelStatistics(long rt) {
        delRtUpdateLock.lock();
        try {
            maxDELRt = Math.max(maxDELRt, rt);
            minDELRt = Math.min(minDELRt, rt);
            sumDELRt += rt;
            delRtCount++;
        } finally {
            delRtUpdateLock.unlock();
        }

        // Also update global counters
        ColumnarDataSourceMetrics.updateDelStatistics(rt);
    }

    public void updatePreheatStatistics(long rt) {
        preheatRtUpdateLock.lock();
        try {
            maxPreheatRt = Math.max(maxPreheatRt, rt);
            minPreheatRt = Math.min(minPreheatRt, rt);
            sumPreheatRt += rt;
            preheatRtCount++;
        } finally {
            preheatRtUpdateLock.unlock();
        }
    }

    public void updatePreheatStatisticsInBatch(long minRt, long maxRt, long sumRt, int count) {
        preheatRtUpdateLock.lock();
        try {
            maxPreheatRt = Math.max(maxPreheatRt, maxRt);
            minPreheatRt = Math.min(minPreheatRt, minRt);
            sumPreheatRt += sumRt;
            preheatRtCount += count;
        } finally {
            preheatRtUpdateLock.unlock();
        }
    }

    @Override
    public String toString() {
        return "VersionStorageStatistics{" +
            "hash=" + hash +
            ", delRtCount=" + delRtCount +
            ", sumDELRt=" + sumDELRt +
            ", minDELRt=" + getMinDELRt() +
            ", maxDELRt=" + getMaxDELRt() +
            ", orcRtCount=" + orcRtCount +
            ", sumORCRt=" + sumORCRt +
            ", minORCRt=" + getMinORCRt() +
            ", maxORCRt=" + getMaxORCRt() +
            ", csvRtCount=" + csvRtCount +
            ", sumCSVRt=" + sumCSVRt +
            ", minCSVRt=" + getMinCSVRt() +
            ", maxCSVRt=" + getMaxCSVRt() +
            ", gmsRtCount=" + gmsRtCount +
            ", sumGMSRt=" + sumGMSRt +
            ", minGMSRt=" + getMinGMSRt() +
            ", maxGMSRt=" + getMaxGMSRt() +
            ", preheatRtCount=" + preheatRtCount +
            ", sumPreheatRt=" + getSumPreheatRt() +
            ", minPreheatRt=" + getMinPreheatRt() +
            ", maxPreheatRt=" + getMaxPreheatRt() +
            '}';
    }

    public String toPrintable() {
        return "orcRtCount-" + orcRtCount +
            "/sumORCRt-" + sumORCRt +
            "/minORCRt-" + getMinORCRt() +
            "/maxORCRt-" + getMaxORCRt() +
            "/avgORCRt-" + (orcRtCount == 0 ? 0 : sumORCRt / orcRtCount) +

            "/gmsRtCount-" + gmsRtCount +
            "/sumGMSRt-" + sumGMSRt +
            "/minGMSRt-" + getMinGMSRt() +
            "/maxGMSRt-" + getMaxGMSRt() +
            "/avgGMSRt-" + (gmsRtCount == 0 ? 0 : sumGMSRt / gmsRtCount) +

            "/csvRtCount-" + csvRtCount +
            "/sumCSVRt-" + sumCSVRt +
            "/minCSVRt-" + getMinCSVRt() +
            "/maxCSVRt-" + getMaxCSVRt() +
            "/avgCSVRt-" + (csvRtCount == 0 ? 0 : sumCSVRt / csvRtCount) +

            "/delRtCount-" + delRtCount +
            "/sumDELRt-" + sumDELRt +
            "/minDELRt-" + getMinDELRt() +
            "/maxDELRt-" + getMaxDELRt() +
            "/avgDELRt-" + (delRtCount == 0 ? 0 : sumDELRt / delRtCount) +

            "/preheatRtCount-" + preheatRtCount +
            "/sumPreheatRt-" + sumPreheatRt +
            "/minPreheatRt-" + minPreheatRt +
            "/maxPreheatRt-" + maxPreheatRt +
            "/avgPreheatRt-" + (preheatRtCount == 0 ? 0 : sumPreheatRt / preheatRtCount);
    }
}