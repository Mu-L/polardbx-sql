/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.utils.InplaceSplitUtils;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class AlterTableGroupSplitPartitionPreparedData extends AlterTableGroupBasePreparedData {

    public AlterTableGroupSplitPartitionPreparedData() {
    }

    private List<SqlPartition> newPartitions;

    private boolean includeFullPartitionDefinition;
    private List<String> targetStorageInstIds;
    private Map<SqlNode, RexNode> partBoundExprInfo;
    private SqlNode atVal;
    protected boolean splitSubPartition;
    protected Map<String, String> newAndOldPhysicalPartitionMap = new TreeMap<>(String::compareToIgnoreCase);
    private Map<String, Set<String>> srcTargetPartitionMap = new TreeMap<>(String::compareToIgnoreCase);
    private boolean firstPartitionLevelActiveForInplaceBackfill = true;

    /**
     * Compute the number of new partitions per source partition, with a divisibility check.
     */
    private int computeNewPerSource(int totalNew, int sourceCount) {
        if (totalNew % sourceCount != 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "New partition count (" + totalNew
                    + ") is not evenly divisible by source partition count ("
                    + sourceCount + ")");
        }
        return totalNew / sourceCount;
    }

    public void preparePartitionRelationship(PartitionInfo partitionInfo, PartitionInfo newPartitionInfo) {
        if (inplaceBackfill && partitionInfo.containSubPartitions()) {
            boolean useSubPartTemplate = partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();
            if (splitSubPartition) {
                int newPerSource = computeNewPerSource(getNewPartitionNames().size(), getOldPartitionNames().size());
                if (useSubPartTemplate) {
                    for (int i = 0; i < getOldPartitionNames().size(); i++) {
                        String oldChildPartitionName = getOldPartitionNames().get(i);
                        List<String> sourceNewNames = getNewPartitionNames().subList(
                            i * newPerSource, (i + 1) * newPerSource);
                        for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                            String oldPartName = null;
                            for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                                if (oldChildPartitionName.equalsIgnoreCase(subPartSpec.getTemplateName())) {
                                    oldPartName = subPartSpec.getName();
                                    break;
                                }
                            }
                            assert oldPartName != null;
                            for (String newTempName : sourceNewNames) {
                                newAndOldPhysicalPartitionMap.put(
                                    partitionSpec.getName() + newTempName, oldPartName);
                            }
                        }
                    }
                } else if (getOldPartitionNames().size() > 1) {
                    // Non-template multi-split: direct physical name mapping for DN allocation
                    for (int i = 0; i < getOldPartitionNames().size(); i++) {
                        String oldSubPartName = getOldPartitionNames().get(i);
                        for (int j = 0; j < newPerSource; j++) {
                            String newSubPartName = getNewPartitionNames().get(i * newPerSource + j);
                            newAndOldPhysicalPartitionMap.put(newSubPartName, oldSubPartName);
                        }
                    }
                }
            } else {
                processSplitLogicalPartition(partitionInfo, newPartitionInfo);
            }
        }
    }

    public void processSplitLogicalPartition(PartitionInfo partitionInfo, PartitionInfo newPartitionInfo) {
        int newPerSource = computeNewPerSource(newPartitions.size(), getOldPartitionNames().size());
        int partIdx = 0;
        for (int s = 0; s < getOldPartitionNames().size(); s++) {
            String oldParentPartitionName = getOldPartitionNames().get(s);
            PartitionSpec partitionSpec =
                partitionInfo.getPartitionBy().getPartitionByPartName(oldParentPartitionName);

            int subPartitionCnt = partitionSpec.getSubPartitions().size();
            for (int i = 0; i < newPerSource; i++) {
                SqlPartition sqlPartition = newPartitions.get(partIdx);
                assert sqlPartition.getSubPartitions().size() == subPartitionCnt;
                for (int j = 0; j < subPartitionCnt; j++) {
                    String oldSubPartitionName = partitionSpec.getSubPartitions().get(j).getName();
                    SqlSubPartition sqlNode = (SqlSubPartition) sqlPartition.getSubPartitions().get(j);
                    String newSubPartitionName =
                        ((SqlIdentifier) sqlNode.getName()).getSimple();
                    newAndOldPhysicalPartitionMap.put(newSubPartitionName, oldSubPartitionName);
                }
                partIdx++;
            }
        }
    }

    public void rebuildPartitionRelationship(PartitionInfo partitionInfo, PartitionInfo newPartitionInfo) {
        // newPartitionNames maybe updated but newPartitions not
        srcTargetPartitionMap.clear();
        if (getOldPartitionNames().size() > 1) {
            // Multi-partition split
            if (splitSubPartition) {
                // Multi-split subpartition
                firstPartitionLevelActiveForInplaceBackfill = false;
                boolean useSubPartTemplate = partitionInfo.getPartitionBy()
                    .getSubPartitionBy().isUseSubPartTemplate();
                int totalNew = getNewPartitionNames().size();
                int sourceCount = getOldPartitionNames().size();
                if (totalNew % sourceCount == 0) {
                    int newPerSource = totalNew / sourceCount;
                    if (useSubPartTemplate) {
                        for (int i = 0; i < sourceCount; i++) {
                            String oldTemplateName = getOldPartitionNames().get(i);
                            List<String> newTemplateNames = getNewPartitionNames().subList(
                                i * newPerSource, (i + 1) * newPerSource);
                            for (PartitionSpec partSpec : partitionInfo.getPartitionBy().getPartitions()) {
                                String oldPhysName = null;
                                for (PartitionSpec sub : partSpec.getSubPartitions()) {
                                    if (oldTemplateName.equalsIgnoreCase(sub.getTemplateName())) {
                                        oldPhysName = sub.getName();
                                        break;
                                    }
                                }
                                for (String newTemp : newTemplateNames) {
                                    srcTargetPartitionMap.computeIfAbsent(oldPhysName,
                                            k -> new TreeSet<>(String::compareToIgnoreCase))
                                        .add(partSpec.getName() + newTemp);
                                }
                            }
                        }
                    } else {
                        // Non-template: oldPartitionNames are physical subpartition names
                        for (int i = 0; i < sourceCount; i++) {
                            String oldPartName = getOldPartitionNames().get(i);
                            for (int j = 0; j < newPerSource; j++) {
                                String newPartName = getNewPartitionNames().get(i * newPerSource + j);
                                srcTargetPartitionMap.computeIfAbsent(oldPartName,
                                    k -> new TreeSet<>(String::compareToIgnoreCase)).add(newPartName);
                            }
                        }
                    }
                } else {
                    // Redistribution case (e.g., hot value re-split): not evenly divisible.
                    // Cannot use inplace backfill because old partitions may reside on different DNs.
                    // Leave srcTargetPartitionMap empty so changeset falls back to full catch-up.
                    this.inplaceBackfill = false;
                }
            } else if (partitionInfo != null && partitionInfo.containSubPartitions()) {
                // Multi-split with subpartitions: map at physical (subpartition) level.
                firstPartitionLevelActiveForInplaceBackfill = true;
                // newPartitions is ordered by source partition: first newPerSource entries from source[0],
                // next newPerSource entries from source[1], etc.
                PartitionByDefinition partBy = partitionInfo.getPartitionBy();
                int totalNewParts = newPartitions.size();
                int sourceCount = getOldPartitionNames().size();
                if (totalNewParts % sourceCount == 0) {
                    int newPerSource = totalNewParts / sourceCount;
                    int partIdx = 0;
                    for (int s = 0; s < sourceCount; s++) {
                        String oldPartName = getOldPartitionNames().get(s);
                        PartitionSpec oldPartSpec = partBy.getPartitionByPartName(oldPartName);
                        int subPartCnt = oldPartSpec.getSubPartitions().size();
                        for (int i = 0; i < newPerSource; i++) {
                            String newPartName =
                                ((SqlIdentifier) newPartitions.get(partIdx).getName()).getSimple();
                            PartitionSpec newPartSpec =
                                newPartitionInfo.getPartitionBy().getPartitionByPartName(newPartName);
                            for (int j = 0; j < subPartCnt; j++) {
                                String oldSubPartName = oldPartSpec.getSubPartitions().get(j).getName();
                                String newSubPartName = newPartSpec.getSubPartitions().get(j).getName();
                                srcTargetPartitionMap.computeIfAbsent(oldSubPartName,
                                    key -> new TreeSet<>(String::compareToIgnoreCase)).add(newSubPartName);
                            }
                            partIdx++;
                        }
                    }
                } else {
                    // Redistribution case (e.g., hot value re-split): not evenly divisible.
                    // Cannot use inplace backfill because old partitions may reside on different DNs.
                    // Leave srcTargetPartitionMap empty so changeset falls back to full catch-up.
                    this.inplaceBackfill = false;
                }
            } else {
                // Multi-split without subpartitions: map at first-level partition level
                firstPartitionLevelActiveForInplaceBackfill = true;
                int totalNew = getNewPartitionNames().size();
                int sourceCount = getOldPartitionNames().size();
                if (totalNew % sourceCount == 0) {
                    // Normal multi-split: each source partition maps to N new partitions
                    int newPerSource = totalNew / sourceCount;
                    for (int i = 0; i < sourceCount; i++) {
                        String oldPartName = getOldPartitionNames().get(i);
                        for (int j = 0; j < newPerSource; j++) {
                            String newPartName = getNewPartitionNames().get(i * newPerSource + j);
                            srcTargetPartitionMap.computeIfAbsent(oldPartName,
                                key -> new TreeSet<>(String::compareToIgnoreCase)).add(newPartName);
                        }
                    }
                } else {
                    // Redistribution case (e.g., hot value re-split): not evenly divisible.
                    // Cannot use inplace backfill because old partitions may reside on different DNs.
                    // Leave srcTargetPartitionMap empty so changeset falls back to full catch-up.
                    this.inplaceBackfill = false;
                }
            }
            return;
        }
        PartitionByDefinition partBy = partitionInfo.getPartitionBy();
        boolean useNotTemplatePart =
            partBy.getSubPartitionBy() != null && !partBy.getSubPartitionBy().isUseSubPartTemplate();
        if (partitionInfo.containSubPartitions()) {
            boolean useSubPartTemplate = partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();
            if (splitSubPartition) {
                if (useSubPartTemplate) {
                    firstPartitionLevelActiveForInplaceBackfill = false;
                    assert getOldPartitionNames().size() == 1;
                    String oldChilePartitionName = getOldPartitionNames().get(0);
                    for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                        String oldPartName = null;
                        for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                            if (oldChilePartitionName.equalsIgnoreCase(subPartSpec.getTemplateName())) {
                                oldPartName = subPartSpec.getName();
                                break;
                            }
                        }
                        assert oldPartName != null;
                        for (String newTempName : getNewPartitionNames()) {
                            srcTargetPartitionMap.computeIfAbsent(oldPartName,
                                    key -> new TreeSet<>(String::compareToIgnoreCase))
                                .add(partitionSpec.getName() + newTempName);
                        }
                    }
                } else {
                    firstPartitionLevelActiveForInplaceBackfill = false;
                    assert getOldPartitionNames().size() == 1;
                    String oldPartName = getOldPartitionNames().get(0);
                    for (String newTempName : getNewPartitionNames()) {
                        srcTargetPartitionMap.computeIfAbsent(oldPartName,
                            key -> new TreeSet<>(String::compareToIgnoreCase)).add(newTempName);
                    }
                }
            } else if (!useNotTemplatePart) {
                firstPartitionLevelActiveForInplaceBackfill = true;
                assert getOldPartitionNames().size() == 1;
                String oldParentPartitionName = getOldPartitionNames().get(0);
                PartitionSpec oldPartitionSpec =
                    partitionInfo.getPartitionBy().getPartitionByPartName(oldParentPartitionName);

                int subPartitionCnt = oldPartitionSpec.getSubPartitions().size();
                for (PartitionSpec partitionSpec : newPartitionInfo.getPartitionBy().getPartitions()) {
                    for (int i = 0; i < subPartitionCnt; i++) {
                        PartitionSpec subPartSpec = partitionSpec.getSubPartitions().get(i);
                        if (subPartSpec.getLocation().isVisiable()) {
                            break;
                        }
                        String oldSubPartitionName = oldPartitionSpec.getSubPartitions().get(i).getName();
                        srcTargetPartitionMap.computeIfAbsent(oldSubPartitionName,
                            key -> new TreeSet<>(String::compareToIgnoreCase)).add(subPartSpec.getName());
                    }
                }
            }
        } else {
            firstPartitionLevelActiveForInplaceBackfill = true;
            assert getOldPartitionNames().size() == 1;
            String oldPartName = getOldPartitionNames().get(0);
            for (String newTempName : getNewPartitionNames()) {
                srcTargetPartitionMap.computeIfAbsent(oldPartName,
                    key -> new TreeSet<>(String::compareToIgnoreCase)).add(newTempName);
            }
        }

    }

    public List<String> getSplitPartitions() {
        return getOldPartitionNames();
    }

    public void setSplitPartitions(List<String> splitPartitions) {
        setOldPartitionNames(splitPartitions);
    }

    public List<SqlPartition> getNewPartitions() {
        return newPartitions;
    }

    public void setNewPartitions(List<SqlPartition> newPartitions) {
        this.newPartitions = newPartitions;
        List<String> newPartitionNames = new ArrayList<>();
        Map<String, String> newPartitionLocalities = new HashMap<>();
        for (SqlPartition sqlPartition : newPartitions) {
            String partitionName = ((SqlIdentifier) (sqlPartition).getName()).getSimple();
            if (GeneralUtil.isNotEmpty(sqlPartition.getSubPartitions())) {
                for (SqlNode sqlNode : sqlPartition.getSubPartitions()) {
                    String subPartitionName = ((SqlIdentifier) ((SqlSubPartition) sqlNode).getName()).getSimple();
                    newPartitionNames.add(subPartitionName);
                    newPartitionLocalities.put(subPartitionName, sqlPartition.getLocality());
                }
            } else {
                newPartitionNames.add(partitionName);
                newPartitionLocalities.put(sqlPartition.getName().toString(), sqlPartition.getLocality());
            }
        }
        setNewPartitionNames(newPartitionNames);
        setNewPartitionLocalities(newPartitionLocalities);
    }

    public boolean isIncludeFullPartitionDefinition() {
        return includeFullPartitionDefinition;
    }

    public void setIncludeFullPartitionDefinition(boolean includeFullPartitionDefinition) {
        this.includeFullPartitionDefinition = includeFullPartitionDefinition;
    }

    public List<String> getTargetStorageInstIds() {
        return targetStorageInstIds;
    }

    public void setTargetStorageInstIds(List<String> targetStorageInstIds) {
        this.targetStorageInstIds = targetStorageInstIds;
    }

    public Map<SqlNode, RexNode> getPartBoundExprInfo() {
        return partBoundExprInfo;
    }

    public void setPartBoundExprInfo(
        Map<SqlNode, RexNode> partBoundExprInfo) {
        this.partBoundExprInfo = partBoundExprInfo;
    }

    public SqlNode getAtVal() {
        return atVal;
    }

    public void setAtVal(SqlNode atVal) {
        this.atVal = atVal;
    }

    public boolean isSplitSubPartition() {
        return splitSubPartition;
    }

    public void setSplitSubPartition(boolean splitSubPartition) {
        this.splitSubPartition = splitSubPartition;
    }

    @Override
    public boolean isInplaceBackfill() {
        return inplaceBackfill;
    }

    public void checkAndResetInplaceBackfill(TableMeta tableMeta, ExecutionContext ec) {
        if (inplaceBackfill) {
            PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
            if (!StringUtils.isEmpty(getTargetImplicitTableGroupName())) {
                Long originTableGroupId = partitionInfo.getTableGroupId();
                TableGroupConfig srcTgInfo = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
                    .getTableGroupConfigById(originTableGroupId);
                if (!srcTgInfo.getTableGroupRecord().tg_name.equalsIgnoreCase(getTargetImplicitTableGroupName())) {
                    setInplaceBackfill(false);
                    return;
                }
            }
            if (partitionInfo.containSubPartitions()) {
                if (splitSubPartition) {
                    firstPartitionLevelActiveForInplaceBackfill = false;
                } else {
                    firstPartitionLevelActiveForInplaceBackfill = true;
                }
            } else {
                firstPartitionLevelActiveForInplaceBackfill = true;
            }
            boolean canUseInplaceSplit =
                InplaceSplitUtils.checkCharSetAndCollationSupport(tableMeta, this.inplaceBackfill,
                    this.firstPartitionLevelActiveForInplaceBackfill, ec);
            this.inplaceBackfill = canUseInplaceSplit;
        }
    }

    //
    @Override
    public boolean isFirstPartitionLevelActiveForInplaceBackfill() {
        return firstPartitionLevelActiveForInplaceBackfill;
    }

    @Override
    public Map<String, Set<String>> getSrcTargetPartitionMap() {
        return srcTargetPartitionMap;
    }

    public void prepareInvisiblePartitionGroupFromBase(Boolean withSubPartition) {
        super.prepareInvisiblePartitionGroup(withSubPartition);
    }

    public List<String> getPhysicalPartitionNames() {
        return getNewPartitionNames();
    }

    @Override
    public void prepareInvisiblePartitionGroup(Boolean withSubPartition) {
        // For split partition, we need special handling to ensure new partitions
        // are allocated to the same physical DN as the original partition
        if (!inplaceBackfill) {
            prepareInvisiblePartitionGroupFromBase(withSubPartition);
        } else {
            /**
             * 新分区的物理位置分配原则
             * 没有子分区的表
             * split partitition p1 into p1_0, p1_1,..,p1_n
             * p1和p1_0, p1_1,..,p1_n在同一个物理DN，所有新老分区在一个DN
             * 带子分区的表
             * 模版化子分区
             * 原来p1的定义：
             * partition by key(a) subpartition by key(b)
             * partition p1 values less than(1000) subpartition 3;
             * p1sp1 a<1000, b<333
             * p1sp2  a<1000,333<=b<667
             * p1sp3 a<1000,667<=b<1000
             * 分裂逻辑分区
             * split partitition p1 into p1_0, p1_1,..,p1_n
             * p1_0sp1 a<333, b<333
             * p1_0sp2  a<333,333<=b<667
             * p1_0sp3 a<333,667<=b<1000
             * p1_1sp1 333<=a<667, b<333
             * p1_1sp2  333<=a<667,333<=b<667
             * p1_1sp3  333<=a<667,667<=b<1000
             * p1_1sp1 667<=a<1000, b<333
             * p1_1sp2  667<=a<1000,333<=b<667
             * p1_1sp3  667<=a<1000,667<=b<1000
             *
             * p1{spn}和{p1_0,p1_1,..,p1_3}.{spn}在同一个物理DN，所有同一模版子分区下的新老分区在一个DN
             *
             * 分裂子分区
             * p1sp1 a<1000, b<333
             * p2sp1 a<2000, b<333
             * p3sp1 a<3000, b<333
             *
             * split partitition sp1 into sp1_1, sp1_2,..,sp1_3
             * p1sp1_1 a<1000, b<111
             * p1sp1_2 a<1000, b<222
             * p1sp1_3 a<1000, b<333
             * p2sp1_1 a<2000, b<111
             * p2sp1_2 a<2000, b<222
             * p2sp1_3 a<2000, b<333
             * p3sp1_1 a<3000, b<111
             * p3sp1_2 a<3000, b<222
             * p3sp1_3 a<3000, b<333
             * pn{spn}和pn.{sp1_1,sp1_2,sp1_3}在同一个物理DN，所有同一逻辑分区下的新老分区在一个DN
             *
             * 非模版化子分区:
             * 原来p1的定义：
             * partition by key(a) subpartition by key(b)
             * partition p1 values less than(1000) subpartition 3;
             * partition p2 values less than(2000) subpartition 2;
             * p1sp1 a<1000, b<333
             * p1sp2  a<1000,333<=b<667
             * p1sp3 a<1000,667<=b<1000
             *
             * 分裂逻辑分区
             * split partitition p1 into p1_1, p1_2,..,p1_n
             * p1_1sp1 a<333, b<333
             * p1_1sp2  a<333,333<=b<667
             * p1_1sp3 a<333,667<=b<1000
             * p1_2sp1 333<=a<667, b<333
             * p1_2sp2  333<=a<667,333<=b<667
             * p1_2sp3  333<=a<667,667<=b<1000
             * p1_3sp1 667<=a<1000, b<333
             * p1_3sp2  667<=a<1000,333<=b<667
             * p1_3sp3  667<=a<1000,667<=b<1000
             * p1{spn}和{p1_0,p1_1,..,p1_3}.{spn}在同一个物理DN，所有同一逻辑分区下的新老分区在一个DN
             *
             * 分裂子分区
             * p1sp1 a<1000, b<333
             * p1sp2 a<1000, 333<=b<667
             * p1sp3 a<1000, 667<=b<1000
             *
             * split partitition p1sp1 into p1sp1_1, p1sp1_2,sp1_3
             * p1sp1_1 a<1000, b<111
             * p1sp1_2 a<1000, b<222
             * p1sp1_3 a<1000, b<333
             * p1sp1 和p1.{sp1_1,sp1_2,sp1_3}在同一个物理DN, 所有新老分区在一个DN
             * */
            List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
            TableGroupConfig tableGroupConfig = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
                .getTableGroupConfigByName(getTableGroupName());
            assert getOldPartitionNames().size() >= 1;
            String oldParentPartitionName = getOldPartitionNames().get(0);
            Long tableGroupId = tableGroupConfig.getTableGroupRecord().id;
            LocalityDesc defaultLocalityDesc = new LocalityDesc();
            PartitionGroupRecord oldPartitionGroupRecord = tableGroupConfig.getPartitionGroupByName(
                oldParentPartitionName);
            if (oldPartitionGroupRecord != null) {
                defaultLocalityDesc =
                    LocalityInfoUtils.parse(oldPartitionGroupRecord.locality);
            }
            if (withSubPartition) {
                List<String> physicalNewPartitionNames;
                if (isUseTemplatePart() && splitSubPartition) {
                    physicalNewPartitionNames = getLogicalParts().stream()
                        .flatMap(a -> getNewPartitionNames().stream().map(b -> a + b))
                        .collect(Collectors.toList());
                } else {
                    physicalNewPartitionNames = getPhysicalPartitionNames();
                }
                for (String newPartitionName : physicalNewPartitionNames) {
                    PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
                    partitionGroupRecord.visible = 0;
                    partitionGroupRecord.partition_name = newPartitionName;

                    if (!splitSubPartition || isUseTemplatePart() || getOldPartitionNames().size() > 1) {
                        String relativePartitionName =
                            newAndOldPhysicalPartitionMap.get(newPartitionName);
                        oldPartitionGroupRecord = tableGroupConfig.getPartitionGroupByName(relativePartitionName);
                    }
                    partitionGroupRecord.setPhy_db(oldPartitionGroupRecord.getPhy_db());
                    partitionGroupRecord.setGroup_Name(oldPartitionGroupRecord.getGroup_Name());
                    partitionGroupRecord.tg_id = tableGroupId;
                    partitionGroupRecord.locality = oldPartitionGroupRecord.locality != null
                        ? oldPartitionGroupRecord.locality : "";
                    partitionGroupRecord.pax_group_id = 0L;
                    inVisiblePartitionGroups.add(partitionGroupRecord);
                }
            } else {
                // For multi-partition split, each source partition's new partitions should be on
                // the same DN as the source partition
                boolean isMultiPartitionSplit = getOldPartitionNames().size() > 1;
                if (isMultiPartitionSplit) {
                    int newPerSource =
                        computeNewPerSource(getNewPartitionNames().size(), getOldPartitionNames().size());
                    for (int i = 0; i < getOldPartitionNames().size(); i++) {
                        String sourcePartName = getOldPartitionNames().get(i);
                        PartitionGroupRecord sourceGroupRecord =
                            tableGroupConfig.getPartitionGroupByName(sourcePartName);
                        if (sourceGroupRecord == null) {
                            ddlLog.warn("Multi-partition split: source partition group not found for '"
                                + sourcePartName + "', using fallback");
                            sourceGroupRecord = oldPartitionGroupRecord;
                        }
                        for (int j = 0; j < newPerSource; j++) {
                            String newPartitionName = getNewPartitionNames().get(i * newPerSource + j);
                            PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
                            partitionGroupRecord.visible = 0;
                            partitionGroupRecord.partition_name = newPartitionName;
                            partitionGroupRecord.tg_id = tableGroupId;
                            partitionGroupRecord.setPhy_db(sourceGroupRecord.getPhy_db());
                            partitionGroupRecord.setGroup_Name(sourceGroupRecord.getGroup_Name());
                            partitionGroupRecord.locality = sourceGroupRecord.locality != null
                                ? sourceGroupRecord.locality : "";
                            partitionGroupRecord.pax_group_id = 0L;
                            inVisiblePartitionGroups.add(partitionGroupRecord);
                        }
                    }
                } else {
                    for (String newPartitionName : getNewPartitionNames()) {
                        PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
                        partitionGroupRecord.visible = 0;
                        LocalityDesc localityDesc =
                            LocalityInfoUtils.parse(getNewPartitionLocalities().get(newPartitionName));
                        partitionGroupRecord.partition_name = newPartitionName;
                        partitionGroupRecord.tg_id = tableGroupId;
                        partitionGroupRecord.setPhy_db(oldPartitionGroupRecord.getPhy_db());
                        partitionGroupRecord.setGroup_Name(oldPartitionGroupRecord.getGroup_Name());

                        if (localityDesc.holdEmptyLocality() && defaultLocalityDesc.holdEmptyLocality()) {
                            partitionGroupRecord.locality = "";
                        } else if (!localityDesc.holdEmptyLocality()) {
                            partitionGroupRecord.locality = localityDesc.toString();
                        } else {
                            partitionGroupRecord.locality = localityDesc.toString();
                        }

                        partitionGroupRecord.pax_group_id = 0L;
                        inVisiblePartitionGroups.add(partitionGroupRecord);
                    }
                }
            }
            setInvisiblePartitionGroups(inVisiblePartitionGroups);
        }
    }
}