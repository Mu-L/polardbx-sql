package com.alibaba.polardbx.optimizer.core.rel.util;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;

import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author chenghui.lch
 * <pre>
 * The common group key info collected
 * from the plan which is row plan or direct plan without any pushdown tables
 *
 * e.g. ToDrdsRelVisitor will use DirectPlanCommonGroupInfo to collate the common groupKey of the whole
 * logical plan to check if the logical plan is allowed to use DirectPlan to pushdown
 * </pre>
 */
public class DirectPlanCommonGroupInfo {
    /**
     * Label if all table are replicas tables
     */
    protected boolean allTableReplicas = true;
    /**
     * Label if any table is replicas table
     */
    protected boolean containAnyReplicasTable = false;

    /**
     * Label if any table is partitioned table
     */
    protected boolean containAnyPartitionedTable = false;

    /**
     * the logical table count in this plan
     */
    protected int visitedTableCount = 0;

    /**
     * The common group key set on replicas tables or single table
     * (only use for auto-db)
     */
    protected Set<String> commonGroupKeySet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    /**
     * Label if it is allowed to random select a group key for query
     */
    protected boolean allowRandomQueryGroupKey = true;

    /**
     * Label if it has already found the conflict group key set in the plan
     */
    protected boolean alreadyFoundConflictGroupKeySet = false;

    public DirectPlanCommonGroupInfo() {
    }

    public void updateCommonGroupKeyByTablePrunedResult(
        String tableSchema,
        String tableName,
        PartPrunedResult pruneResult) {

        PartitionInfo partitionInfo = pruneResult.getPartInfo();
        visitedTableCount++;

        boolean allowRandomQueryGroupSetOfNewTbl = true;
        boolean isPartitionedTable = false;
        if (partitionInfo.isReplicasTable()) {
            this.containAnyReplicasTable = true;
        } else if (partitionInfo.isGsiBroadcastOrBroadcast()) {
            this.allTableReplicas = false;
        } else if (partitionInfo.isGsiSingleOrSingleTable()) {
            this.allTableReplicas = false;
        } else if (partitionInfo.isGsiOrPartitionedTable()) {
            if (!partitionInfo.isGsiOrPartitionedTableWithOnlyOnePhyPartition()) {
                this.containAnyPartitionedTable = true;
                this.allTableReplicas = false;
                isPartitionedTable = true;
            }
            allowRandomQueryGroupSetOfNewTbl = false;
        }

        Set<String> grpKeySetOfNewTbl = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        if (isPartitionedTable) {
            grpKeySetOfNewTbl.addAll(pruneResult.getGroupKeySetFromPrunedPartitions());
        } else {
            grpKeySetOfNewTbl.addAll(partitionInfo.getPartSpecSearcher().getGroupKeySetOfAllPhyPartSpecs());
        }

        Set<String> commonGrpKetSet = this.commonGroupKeySet;
        updateCommonGroupKetIfNeed(commonGrpKetSet, grpKeySetOfNewTbl, allowRandomQueryGroupSetOfNewTbl);
    }

    public void updateCommonGroupKeyByTableName(ExecutionContext ec, String tableSchema, String tableName) {
        if (!DbInfoManager.getInstance().isNewPartitionDb(tableSchema)) {
            return;
        }
        SchemaManager schemaManager = ec.getSchemaManager(tableSchema);
        TableMeta tm = schemaManager.getTable(tableName);
        PartitionInfo partitionInfo = tm.getPartitionInfo();
        visitedTableCount++;

        boolean allowRandomQueryGroupSetOfNewTbl = true;
        if (partitionInfo.isReplicasTable()) {
            this.containAnyReplicasTable = true;
        } else if (partitionInfo.isGsiBroadcastOrBroadcast()) {
            this.allTableReplicas = false;
        } else if (partitionInfo.isGsiSingleOrSingleTable()) {
            this.allTableReplicas = false;
            allowRandomQueryGroupSetOfNewTbl = false;
        } else if (partitionInfo.isGsiOrPartitionedTable()) {
            if (!partitionInfo.isGsiOrPartitionedTableWithOnlyOnePhyPartition()) {
                this.containAnyPartitionedTable = true;
                this.allTableReplicas = false;
            }
            allowRandomQueryGroupSetOfNewTbl = false;
        }

        Set<String> grpKeySetOfNewTbl = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        grpKeySetOfNewTbl.addAll(partitionInfo.getPartSpecSearcher().getGroupKeySetOfAllPhyPartSpecs());
        Set<String> commonGrpKetSet = this.commonGroupKeySet;
        updateCommonGroupKetIfNeed(commonGrpKetSet, grpKeySetOfNewTbl, allowRandomQueryGroupSetOfNewTbl);
    }

    private void updateCommonGroupKetIfNeed(Set<String> commonGrpKetSet,
                                            Set<String> grpKeySetOfNewTbl,
                                            boolean allowRandomQueryGroupSetOfNewTbl) {

        if (this.alreadyFoundConflictGroupKeySet) {
            return;
        }

        if (visitedTableCount <= 1) {
            commonGrpKetSet.addAll(grpKeySetOfNewTbl);
        } else {
            boolean allowRandomQueryGroupSetOfCurrTableSet = this.allowRandomQueryGroupKey;
            if (allowRandomQueryGroupSetOfCurrTableSet && allowRandomQueryGroupSetOfNewTbl) {
                commonGrpKetSet.retainAll(grpKeySetOfNewTbl);
                if (commonGrpKetSet.isEmpty()) {
                    this.alreadyFoundConflictGroupKeySet = true;
                }
            } else if (allowRandomQueryGroupSetOfCurrTableSet && !allowRandomQueryGroupSetOfNewTbl) {
                if (commonGrpKetSet.containsAll(grpKeySetOfNewTbl)) {
                    commonGrpKetSet.retainAll(grpKeySetOfNewTbl);
                } else {
                    commonGrpKetSet.clear();
                    this.alreadyFoundConflictGroupKeySet = true;
                }
                this.allowRandomQueryGroupKey = false;
            } else if (!allowRandomQueryGroupSetOfCurrTableSet && allowRandomQueryGroupSetOfNewTbl) {
                if (grpKeySetOfNewTbl.containsAll(commonGrpKetSet)) {
                    grpKeySetOfNewTbl.retainAll(commonGrpKetSet);
                    commonGrpKetSet.clear();
                    commonGrpKetSet.addAll(grpKeySetOfNewTbl);
                } else {
                    // ignore grpKeySetOfNewTbl
                    commonGrpKetSet.clear();
                    this.alreadyFoundConflictGroupKeySet = true;
                }
                this.allowRandomQueryGroupKey = false;
            } else {
                if (commonGrpKetSet.equals(grpKeySetOfNewTbl)) {
                    //  ignore grpKeySetOfNewTbl
                    //
                } else {
                    commonGrpKetSet.clear();
                    this.alreadyFoundConflictGroupKeySet = true;
                }
                this.allowRandomQueryGroupKey = false;
            }
        }
    }

    public void updateCommonGroupKeyByGroupInfo(DirectPlanCommonGroupInfo newReplicasTableGroupInfo) {

        if (newReplicasTableGroupInfo.visitedTableCount <= 0) {
            return;
        }
        if (!newReplicasTableGroupInfo.allTableReplicas) {
            this.allTableReplicas = false;
        }
        if (newReplicasTableGroupInfo.containAnyReplicasTable) {
            this.containAnyReplicasTable = true;
        }
        if (newReplicasTableGroupInfo.containAnyPartitionedTable) {
            this.containAnyPartitionedTable = true;
        }
        this.visitedTableCount += newReplicasTableGroupInfo.visitedTableCount;
        updateCommonGroupKetIfNeed(this.commonGroupKeySet, newReplicasTableGroupInfo.commonGroupKeySet,
            newReplicasTableGroupInfo.allowRandomQueryGroupKey);
    }

    public boolean isContainAnyReplicasTables() {
        return containAnyReplicasTable;
    }

    public boolean isContainAnyPartitionedTables() {
        return containAnyPartitionedTable;
    }

    public Set<String> getCommonGroupKeySet() {
        return commonGroupKeySet;
    }

    public String getRandomReadTargetGroupKeyIfAllowed(ExecutionContext ec, boolean usingLockMode) {
        if (commonGroupKeySet.isEmpty()) {
            return null;
        }
        boolean allowReplicasRandomReading =
            ec.getParamManager().getBoolean(ConnectionParams.ENABLE_REPLICAS_RANDOM_READ);
        int randIdx = 0;
        if (allowReplicasRandomReading && !usingLockMode) {
            Random rnd = new Random();
            randIdx = Math.abs(rnd.nextInt(commonGroupKeySet.size()));
        }
        return (String) commonGroupKeySet.toArray()[randIdx];
    }

    public String getFirstGroupKey() {
        if (commonGroupKeySet.isEmpty()) {
            return null;
        }
        return (String) commonGroupKeySet.toArray()[0];
    }

    public DirectPlanCommonGroupInfo copy() {
        DirectPlanCommonGroupInfo newInfo = new DirectPlanCommonGroupInfo();
        newInfo.allTableReplicas = this.allTableReplicas;
        newInfo.containAnyReplicasTable = this.containAnyReplicasTable;
        newInfo.containAnyPartitionedTable = this.containAnyPartitionedTable;
        newInfo.visitedTableCount = this.visitedTableCount;
        newInfo.allowRandomQueryGroupKey = this.allowRandomQueryGroupKey;
        newInfo.commonGroupKeySet.addAll(this.commonGroupKeySet);
        return newInfo;
    }

    public boolean isAllTableReplicas() {
        return allTableReplicas;
    }

    public boolean isAllowRandomQueryGroupKey() {
        return allowRandomQueryGroupKey;
    }

}
