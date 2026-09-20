package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.Map;
import java.util.Set;

@TaskName(name = "DropTablePhyDdlWithCheckTask")
public class DropTablePhyDdlWithCheckTask extends DropTablePhyDdlTask {

    Map<String, Set<String>> targetTableTopology;
    boolean ignoreCheck = false;

    @JSONCreator
    public DropTablePhyDdlWithCheckTask(String schemaName, PhysicalPlanData physicalPlanData,
                                        Map<String, Set<String>> targetTableTopology) {
        super(schemaName, physicalPlanData);
        this.targetTableTopology = targetTableTopology;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        checkTargetTableExistence();
        super.executeImpl(executionContext);
    }

    private void checkTargetTableExistence() {
        if (GeneralUtil.isNotEmpty(targetTableTopology) && !ignoreCheck) {
            for (Map.Entry<String, Set<String>> entry : targetTableTopology.entrySet()) {
                for (String phyTb : entry.getValue()) {
                    if (!ScaleOutUtils.checkTableExistence(schemaName, entry.getKey(), phyTb)) {
                        throw GeneralUtil.nestedException(
                            "The target table '" + phyTb + "' does not exist in '" + entry.getKey()
                                + "'; therefore, this DDL operation cannot drop the source table.");
                    }
                }
            }
        }
    }
}
