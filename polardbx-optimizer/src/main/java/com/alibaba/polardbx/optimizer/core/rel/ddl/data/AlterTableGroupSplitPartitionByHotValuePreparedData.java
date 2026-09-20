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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.utils.KeyWordsUtil;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class AlterTableGroupSplitPartitionByHotValuePreparedData extends AlterTableGroupSplitPartitionPreparedData {

    int[] insertPos;

    Map<String, List<Long[]>> splitPointInfos;

    boolean skipSplit;
    String hotKeyPartitionName;
    boolean hasSubPartition;
    String parentPartitionName;
    int hotKeyNum;
    List<String> newPhysicalPartitionNames;

    public AlterTableGroupSplitPartitionByHotValuePreparedData() {
    }

    public int[] getInsertPos() {
        return insertPos;
    }

    public void setInsertPos(int[] insertPos) {
        this.insertPos = insertPos;
    }

    public boolean isSkipSplit() {
        return skipSplit;
    }

    public void setSkipSplit(boolean skipSplit) {
        this.skipSplit = skipSplit;
    }

    public String getHotKeyPartitionName() {
        return hotKeyPartitionName;
    }

    public void setNewPhysicalPartitionNames(List<String> newPhysicalPartitionNames) {
        this.newPhysicalPartitionNames = newPhysicalPartitionNames;
    }

    public List<String> getPhysicalPartitionNames() {
        return newPhysicalPartitionNames;
    }

    public void setHotKeyPartitionName(String hotKeyPartitionName) {
        this.hotKeyPartitionName = hotKeyPartitionName;
    }

    public boolean hotPartitionNameNeedChange() {
        if (StringUtils.isNotEmpty(hotKeyPartitionName)) {
            List<String> oldPartNames = getOldPartitionNames();
            List<String> newPartNames = getNewPartitionNames();
            if (GeneralUtil.isNotEmpty(oldPartNames) && GeneralUtil.isNotEmpty(newPartNames)
                && oldPartNames.size() == newPartNames.size()) {
                for (int i = 0; i < oldPartNames.size(); i++) {
                    if (!oldPartNames.get(i).equalsIgnoreCase(newPartNames.get(i))) {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

    public List<Pair<String, String>> getChangeHotPartitionNames() {
        List<Pair<String, String>> changePartitionsPair = new ArrayList<>();
        List<String> oldPartNames = getOldPartitionNames();
        List<String> newPartNames = getNewPartitionNames();
        if (GeneralUtil.isNotEmpty(oldPartNames) && GeneralUtil.isNotEmpty(newPartNames)
            && oldPartNames.size() == newPartNames.size()) {
            for (int i = 0; i < oldPartNames.size(); i++) {
                Pair<String, String> pair = new Pair<>(oldPartNames.get(i), newPartNames.get(i));
                changePartitionsPair.add(pair);
                PartitionNameUtil.validatePartName(pair.getValue(), KeyWordsUtil.isKeyWord(pair.getValue()), false);
            }
        }
        return changePartitionsPair;
    }

    @Override
    public void processSplitLogicalPartition(PartitionInfo partitionInfo, PartitionInfo newPartitionInfo) {
        assert getOldPartitionNames().size() == 1;
        String oldParentPartitionName = getOldPartitionNames().get(0);
        PartitionSpec partitionSpec =
            partitionInfo.getPartitionBy().getPartitionByPartName(oldParentPartitionName);
        List<PartitionSpec> parentPartSpecs = partitionInfo.getPartitionBy().getPartitions();
        List<PartitionSpec> newParentPartSpecs = newPartitionInfo.getPartitionBy().getPartitions();
        Set<String> parentPartNames = new TreeSet<>(String::compareToIgnoreCase);
        List<PartitionSpec> newGeneratedPartSpecs = new ArrayList<>();
        for (PartitionSpec parentPartSpec : parentPartSpecs) {
            parentPartNames.add(parentPartSpec.getName());
        }
        for (PartitionSpec parentPartSpec : newParentPartSpecs) {
            if (!parentPartNames.contains(parentPartSpec.getName())) {
                newGeneratedPartSpecs.add(parentPartSpec);
            }
        }
        int subPartitionCnt = partitionSpec.getSubPartitions().size();
        for (int i = 0; i < subPartitionCnt; i++) {
            String oldSubPartitionName = partitionSpec.getSubPartitions().get(i).getName();
            for (PartitionSpec sqlPartition : newGeneratedPartSpecs) {
                assert sqlPartition.getSubPartitions().size() == subPartitionCnt;
                String newSubPartitionName = sqlPartition.getSubPartitions().get(i).getName();
                newAndOldPhysicalPartitionMap.put(newSubPartitionName, oldSubPartitionName);
            }
        }
    }

    public Map<String, List<Long[]>> getSplitPointInfos() {
        return splitPointInfos;
    }

    public void setSplitPointInfos(Map<String, List<Long[]>> splitPointInfos) {
        this.splitPointInfos = splitPointInfos;
    }

    public boolean isHasSubPartition() {
        return hasSubPartition;
    }

    public void setHasSubPartition(boolean hasSubPartition) {
        this.hasSubPartition = hasSubPartition;
    }

    public String getParentPartitionName() {
        return parentPartitionName;
    }

    public void setParentPartitionName(String parentPartitionName) {
        this.parentPartitionName = parentPartitionName;
    }

    @Override
    public boolean isInplaceBackfill() {
        return inplaceBackfill;
    }

    public int getHotKeyNum() {
        return hotKeyNum;
    }

    public void setHotKeyNum(int hotKeyNum) {
        this.hotKeyNum = hotKeyNum;
    }
}
