package com.alibaba.polardbx.optimizer.core.rel.util;

import com.alibaba.polardbx.common.utils.CaseInsensitive;

import java.util.Set;
import java.util.TreeSet;

/**
 * The common group key info of logicalView
 * <pre>
 *     all the tables pushdown into one logicalView, its tables must satisfy:
 *     1. all the tables are replicas tables/broadcast table has at least one common group key;
 *     2. the common groupKeySet of replicas tables/broadcast table must full cover all the groupKey
 *          of the single/partitioned table in the logical view
 * </pre>
 *
 * @author chenghui.lch
 */
public class LogicalViewCommonGroupInfo {

    /**
     * The common groupKey set of the tables of the logical view
     * <pre>
     *     for single/partitions table, the commonGroupKeySet are the all groupKey of their physical topology
     *     for replicas/broadcast table, the commonGroupKeySet are the all groupKey of their all replicas phy table
     * </pre>
     */
    protected Set<String> commonGroupKeySet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    /**
     * Label if allow random selected groupKey to execute phySql
     * <pre>
     *     if true,  that means all the tables of groupKeys are replicas tables/broadcast table
     *     if false, that means contain at least one table of groupKeys is partitioned table/single table
     * </pre>
     */
    protected boolean allowRandomSelected = true;

    /**
     * The target tgId of partitioned tables or single tables
     * if the partitioned/single tables are push down to the logical view,
     * a logical view only allow at most one tgId
     */
    protected Long forceMatchTgId = 0L;

    public LogicalViewCommonGroupInfo() {
    }

    public Set<String> getCommonGroupKeySet() {
        return commonGroupKeySet;
    }

    public void setCommonGroupKeySet(Set<String> commonGroupKeySet) {
        this.commonGroupKeySet = commonGroupKeySet;
    }

    public boolean isAllowRandomSelected() {
        return allowRandomSelected;
    }

    public void setAllowRandomSelected(boolean allowRandomSelected) {
        this.allowRandomSelected = allowRandomSelected;
    }

    public Long getForceMatchTgId() {
        return forceMatchTgId;
    }

    public void setForceMatchTgId(Long forceMatchTgId) {
        this.forceMatchTgId = forceMatchTgId;
    }
}
