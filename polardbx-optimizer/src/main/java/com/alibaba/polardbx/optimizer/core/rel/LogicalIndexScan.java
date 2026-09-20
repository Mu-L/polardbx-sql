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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect.LockMode;

/**
 * @author chenmo.cm
 */
public class LogicalIndexScan extends LogicalView {

    private String mainTableName;
    private boolean projectSwitched = false;

    public LogicalIndexScan(String mainTableName, RelOptTable indexTable, TableScan primaryScan, LockMode lockMode) {
        super(primaryScan, indexTable, primaryScan.getHints(), lockMode, null);
        this.mainTableName = mainTableName;
        this.flashback = primaryScan.getFlashback();
    }

    public LogicalIndexScan(String mainTableName, RelNode rel, RelOptTable table, SqlNodeList hints, LockMode lockMode,
                            RexNode flashback,
                            boolean aggIsPushed) {
        super(rel, table, hints, lockMode, null);
        this.mainTableName = mainTableName;
        this.flashback = flashback;
        this.pushDownOpt.calculateRowType();
        this.pushDownOpt.setAggIsPushed(aggIsPushed);
    }

    public LogicalIndexScan(LogicalView logicalView) {
        super(logicalView);
    }

    private LogicalIndexScan(LogicalIndexScan logicalIndexScan, LockMode lockMode) {
        super(logicalIndexScan, lockMode);
    }

    public LogicalIndexScan(RelInput relInput) {
        super(new LogicalView(relInput));
    }

    @Override
    public String explainNodeName() {
        return "IndexScan";
    }

    @Override
    public LogicalIndexScan copy(RelTraitSet traitSet) {
        LogicalIndexScan newIndexScan = new LogicalIndexScan(this);
        newIndexScan.mainTableName = mainTableName;
        newIndexScan.projectSwitched = projectSwitched;
        newIndexScan.traitSet = traitSet;
        newIndexScan.pushDownOpt = pushDownOpt.copy(newIndexScan, this.getPushedRelNode());
        return newIndexScan;
    }

    @Override
    public RelNode clone() {
        LogicalIndexScan scan = new LogicalIndexScan(this, lockMode);
        scan.setScalarList(scalarList);
        scan.mainTableName = this.mainTableName;
        scan.projectSwitched = this.projectSwitched;
        return scan;
    }

    /**
     * @return true if this index scan is scanning a UGSI.
     */
    public boolean isUniqueGsi() {
        GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean = RelUtils.getGsiIndexMetaBean(this);
        return null != gsiIndexMetaBean && !gsiIndexMetaBean.nonUnique;
    }

    public String getMainTableName() {
        return mainTableName;
    }

    public boolean isProjectSwitched() {
        return projectSwitched;
    }

    public void setProjectSwitched(boolean projectSwitched) {
        this.projectSwitched = projectSwitched;
    }
}
