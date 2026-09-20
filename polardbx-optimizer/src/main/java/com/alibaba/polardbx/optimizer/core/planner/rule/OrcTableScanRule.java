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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.logical.LogicalTableScan;

/**
 * @author chenzilin
 */
public class OrcTableScanRule extends RelOptRule {
    public static final OrcTableScanRule INSTANCE =
        new OrcTableScanRule(operand(LogicalTableScan.class, RelOptRule.none()));

    protected OrcTableScanRule(RelOptRuleOperand operand) {
        super(operand, "OrcTableScanRule:INSTANCE");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalTableScan logicalTableScan = call.rel(0);
        OrcTableScan orcTableScan =
            OrcTableScan.create(logicalTableScan.getCluster(), logicalTableScan.getTable(),
                logicalTableScan.getHints(), logicalTableScan.getIndexNode(), logicalTableScan.getFlashback(),
                logicalTableScan.getFlashbackOperator(),
                logicalTableScan.getPartitions());
        call.transformTo(orcTableScan);
    }
}


