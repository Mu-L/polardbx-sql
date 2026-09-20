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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import com.alibaba.polardbx.optimizer.PlannerContext;

/**
 * @author chenmo.cm
 */
public class DrdsInsertConvertRule extends ConverterRule {
    public static final DrdsInsertConvertRule SMP_INSTANCE = new DrdsInsertConvertRule(DrdsConvention.INSTANCE);

    public static final DrdsInsertConvertRule COL_INSTANCE = new DrdsInsertConvertRule(CBOUtil.getColConvention());

    private final Convention outConvention;

    DrdsInsertConvertRule(Convention outConvention) {
        super(LogicalInsert.class, Convention.NONE, outConvention, "DrdsInsertConvertRule");
        this.outConvention = outConvention;
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalInsert logicalInsert = (LogicalInsert) rel;
        TableMeta tableMeta = CBOUtil.getTableMeta(logicalInsert.getTable());
        // support load data in oss
        if (PlannerContext.getPlannerContext(logicalInsert).getExecutionContext().getLoadDataContext() == null)
            if (Engine.isFileStore(tableMeta.getEngine())) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "dml in file store");
            }
        RelTraitSet relTraitSet = logicalInsert.getTraitSet().simplify();
        LogicalInsert newLogicalInsert = (LogicalInsert) logicalInsert.copy(relTraitSet.replace(outConvention),
            convertList(logicalInsert.getInputs(), outConvention));
        if (CBOUtil.isColumnarOptimizer(newLogicalInsert) && !ExternalizedDmlRewriter.needsHandling(tableMeta)) {
            newLogicalInsert.setInsertSelectMode(LogicalInsert.InsertSelectMode.MPP);
        }
        return newLogicalInsert;
    }
}
