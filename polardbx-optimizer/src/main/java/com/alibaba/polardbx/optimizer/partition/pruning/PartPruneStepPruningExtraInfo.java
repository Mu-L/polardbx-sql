package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.util.LogicalViewCommonGroupInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * @author chenghui.lch
 */
public class PartPruneStepPruningExtraInfo {

    protected boolean enableReplicasTableRandomReading = false;
    protected String randomReadTargetGroupKey;
    protected boolean onlyScanOnCommonGroupKeySetForReplicasTable = true;
    protected LogicalViewCommonGroupInfo commonGroupKeyInfo = null;

    public PartPruneStepPruningExtraInfo() {
    }

    public void initReplicasTableRandomReading(ExecutionContext ec, boolean usingLockMode) {
        boolean enableReplicasRandomRead =
            ec.getParamManager().getBoolean(ConnectionParams.ENABLE_REPLICAS_RANDOM_READ);
        boolean allowRandomSelected = commonGroupKeyInfo.isAllowRandomSelected();
        boolean allowReplicasRandomReading = enableReplicasRandomRead && allowRandomSelected && (!usingLockMode);
        this.enableReplicasTableRandomReading = allowReplicasRandomReading;
        if (allowReplicasRandomReading) {
            Set<String> commonGrpKeySet = commonGroupKeyInfo.getCommonGroupKeySet();
            List<String> commonGrpKeyList = new ArrayList<>(commonGrpKeySet);
            Random rnd = new Random();
            int randIdx = Math.abs(rnd.nextInt(commonGrpKeyList.size()));
            this.randomReadTargetGroupKey = commonGrpKeyList.get(randIdx);
        }
    }

    public String getRandomReadTargetGroupKey() {
        return randomReadTargetGroupKey;
    }

    public boolean isOnlyScanOnCommonGroupKeySetForReplicasTable() {
        return onlyScanOnCommonGroupKeySetForReplicasTable;
    }

    public LogicalViewCommonGroupInfo getCommonGroupKeyInfo() {
        return commonGroupKeyInfo;
    }

    public void setCommonGroupKeyInfo(LogicalViewCommonGroupInfo commonGroupKeyInfo) {
        this.commonGroupKeyInfo = commonGroupKeyInfo;
    }
}
