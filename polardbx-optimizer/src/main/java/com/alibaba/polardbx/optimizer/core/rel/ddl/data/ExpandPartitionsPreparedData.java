package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.optimizer.partition.PartitionInfo;

import java.util.List;

/**
 * PreparedData for ALTER TABLE t EXPAND PARTITIONS TO N.
 */
public class ExpandPartitionsPreparedData extends DdlPreparedData {

    /**
     * Physical table id in GMS (used to detect table drop-and-recreate).
     */
    private long tableId;

    /**
     * Number of partitions when Expand was first issued (C0).
     */
    private int initialPartitionCount;

    /**
     * Target total partition count (N). N must be > C0 and N % C0 == 0.
     */
    private int targetPartitionCount;

    /**
     * N / C0 - each original partition will be split into this many new ones.
     */
    private int expandFactor;

    /**
     * Whether expanding subpartitions (v1 always false).
     */
    private boolean subPartitions;

    /**
     * Ordered list of original logical partition names captured at the moment this
     * Expand task was first created. Used to identify which partitions still need
     * splitting regardless of what the current live partition list looks like.
     */
    private List<String> originalPartitionNames;

    /**
     * When resuming an interrupted Expand (plan record exists in MetaDB), this list
     * contains ONLY the partition names whose plan_json status is "PENDING".
     * The JobFactory uses this list directly instead of doing name-based existence
     * matching against the live PartitionInfo, which would be wrong when a SPLIT
     * produces a new partition that happens to share a name with an original one
     * (e.g., original p3 split → p11/p12, then p8 split → p3/p13).
     * <p>
     * Null means this is a fresh run (not a resume); JobFactory falls back to
     * name-based filtering in that case.
     */
    private List<String> resumePendingPartitionNames;

    // ---- getters & setters ----

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public int getInitialPartitionCount() {
        return initialPartitionCount;
    }

    public void setInitialPartitionCount(int initialPartitionCount) {
        this.initialPartitionCount = initialPartitionCount;
    }

    public int getTargetPartitionCount() {
        return targetPartitionCount;
    }

    public void setTargetPartitionCount(int targetPartitionCount) {
        this.targetPartitionCount = targetPartitionCount;
    }

    public int getExpandFactor() {
        return expandFactor;
    }

    public void setExpandFactor(int expandFactor) {
        this.expandFactor = expandFactor;
    }

    public boolean isSubPartitions() {
        return subPartitions;
    }

    public void setSubPartitions(boolean subPartitions) {
        this.subPartitions = subPartitions;
    }

    public List<String> getOriginalPartitionNames() {
        return originalPartitionNames;
    }

    public void setOriginalPartitionNames(List<String> originalPartitionNames) {
        this.originalPartitionNames = originalPartitionNames;
    }

    public List<String> getResumePendingPartitionNames() {
        return resumePendingPartitionNames;
    }

    public void setResumePendingPartitionNames(List<String> resumePendingPartitionNames) {
        this.resumePendingPartitionNames = resumePendingPartitionNames;
    }
}
