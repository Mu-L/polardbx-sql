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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Map;

public class LogicalDynamicValues extends DynamicValues {

    protected LogicalDynamicValues(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType,
                                   ImmutableList<ImmutableList<RexNode>> tuples) {
        super(cluster, traits, rowType, tuples);
    }

    protected LogicalDynamicValues(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType,
                                   ImmutableList<ImmutableList<RexNode>> tuples, RawStringMode rawStringMode) {
        super(cluster, traits, rowType, tuples, rawStringMode);
    }

    public LogicalDynamicValues(RelInput input) {
        super(input.getCluster(), input.getTraitSet(), input.getRowType("type"), input.getDynamicTuples("tuples"),
            input.getEnum("rawStringMode", RawStringMode.class, null));
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalDynamicValues(getCluster(), traitSet, rowType, tuples, getRawStringMode());
    }

    public static LogicalDynamicValues createDrdsValues(
        RelOptCluster cluster, RelTraitSet traits, RelDataType rowType, ImmutableList<ImmutableList<RexNode>> tuples) {
        return new LogicalDynamicValues(cluster, traits.replace(DrdsConvention.INSTANCE), rowType, tuples);
    }

    public static LogicalDynamicValues createDrdsValues(
        RelOptCluster cluster, RelTraitSet traits, RelDataType rowType,
        ImmutableList<ImmutableList<RexNode>> tuples, RawStringMode rawStringMode) {
        return new LogicalDynamicValues(
            cluster, traits.replace(DrdsConvention.INSTANCE), rowType, tuples, rawStringMode);
    }

    /**
     * Detect the raw-string execution mode from the tuple shape and the bound parameter values.
     * A mode is reported only when the shape matches AND the backing parameter value is a
     * RawString: the marker promises raw-string execution, so both conditions are enforced here.
     * This is the single source of truth for the checks previously duplicated between the
     * executor factory and the row-count metadata handler. Returns null otherwise.
     */
    public static RawStringMode detectRawStringMode(Map<Integer, ParameterContext> params, RelDataType rowType,
                                                    ImmutableList<ImmutableList<RexNode>> tuples) {
        if (tuples.size() != 1) {
            return null;
        }
        ImmutableList<RexNode> tuple = tuples.get(0);
        if (tuple.isEmpty()) {
            return null;
        }
        if (tuple.size() == rowType.getFieldCount()) {
            for (RexNode rexNode : tuple) {
                RexDynamicParam dynamicParam = extractLiteralDynamicParam(rexNode);
                if (dynamicParam == null || !isRawStringValue(params, dynamicParam)) {
                    return null;
                }
            }
            return RawStringMode.COLUMN_ARRAY;
        }
        if (tuple.size() == 1) {
            RexDynamicParam dynamicParam = extractLiteralDynamicParam(tuple.get(0));
            if (dynamicParam != null && isRawStringValue(params, dynamicParam)) {
                return RawStringMode.ROW_LIST;
            }
        }
        return null;
    }

    /**
     * Accept a bare literal dynamic param or CAST(bare literal dynamic param) with a plain index
     * (no subIndex / skIndex). Mirrors the parameter shape accepted by the raw-string executors.
     * Shared by the executor factory and the row-count metadata handler as the single definition
     * of the raw-string param shape.
     */
    public static RexDynamicParam extractLiteralDynamicParam(RexNode rexNode) {
        RexDynamicParam dynamicParam = null;
        if (rexNode instanceof RexDynamicParam) {
            dynamicParam = (RexDynamicParam) rexNode;
        } else if (rexNode instanceof RexCall && rexNode.getKind() == SqlKind.CAST) {
            RexCall call = (RexCall) rexNode;
            if (call.getOperands().size() == 1 && call.getOperands().get(0) instanceof RexDynamicParam) {
                dynamicParam = (RexDynamicParam) call.getOperands().get(0);
            }
        }
        if (dynamicParam == null || !dynamicParam.literal()
            || dynamicParam.getIndex() < 0
            || dynamicParam.getSubIndex() >= 0
            || dynamicParam.getSkIndex() >= 0) {
            return null;
        }
        return dynamicParam;
    }

    private static boolean isRawStringValue(Map<Integer, ParameterContext> params, RexDynamicParam dynamicParam) {
        if (params == null) {
            return false;
        }
        ParameterContext parameterContext = params.get(dynamicParam.getIndex() + 1);
        return parameterContext != null && parameterContext.getValue() instanceof RawString;
    }
}
