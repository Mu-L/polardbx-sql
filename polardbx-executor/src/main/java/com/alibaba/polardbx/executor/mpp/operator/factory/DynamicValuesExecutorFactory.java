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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.executor.operator.DynamicValueExec;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.RawStringDynamicValueExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DynamicValuesExecutorFactory extends ExecutorFactory {

    private DynamicValues dynamicValues;

    public DynamicValuesExecutorFactory(DynamicValues dynamicValues) {
        this.dynamicValues = dynamicValues;
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        List<DataType> outputColumns = CalciteUtils.getTypes(dynamicValues.getRowType());

        Executor rawStringExecutor = tryCreateRawStringExecutor(outputColumns, context);
        if (rawStringExecutor != null) {
            return rawStringExecutor;
        }

        List<DynamicParamExpression> dynamicExpressions = new ArrayList<>();
        List<List<IExpression>> expressions = dynamicValues.getTuples()
            .stream().map(expression -> expression.stream().map(
                e -> RexUtils.buildRexNode(e, context, dynamicExpressions)
            ).collect(Collectors.toList())).collect(Collectors.toList());
        Executor exec = new DynamicValueExec(expressions, outputColumns, context);
        registerRuntimeStat(exec, dynamicValues, context);
        return exec;
    }

    /**
     * Try to build a RawString-backed executor for a single-tuple DynamicValues. The shape is
     * decided by the optimizer-maintained marker; plans deserialized from older versions carry
     * no marker, so it is re-detected from the tuple shape and the runtime parameter values
     * (which also enforces the RawString check). See the two methods below for the two modes.
     * Returns null when there is no raw-string mode or the runtime parameter is not a
     * RawString, so the caller falls back to DynamicValueExec.
     */
    private Executor tryCreateRawStringExecutor(List<DataType> outputColumns, ExecutionContext context) {
        Map<Integer, ParameterContext> params = context.getParams() == null ? Collections.emptyMap() :
            context.getParams().getCurrentParameter();

        DynamicValues.RawStringMode mode = dynamicValues.getRawStringMode();
        if (mode == null) {
            mode = LogicalDynamicValues.detectRawStringMode(params, dynamicValues.getRowType(),
                dynamicValues.getTuples());
        }
        if (mode == null) {
            return null;
        }

        List<RexNode> tuple = dynamicValues.getTuples().get(0);

        Executor exec = mode == DynamicValues.RawStringMode.COLUMN_ARRAY
            ? tryCreateColumnArrayExecutor(tuple, outputColumns, context, params)
            : tryCreateRowListExecutor(tuple.get(0), outputColumns, context, params);
        if (exec != null) {
            registerRuntimeStat(exec, dynamicValues, context);
        }
        return exec;
    }

    /**
     * Column-array mode, produced by single-column IN rewrites (the IN list is merged into
     * one array param) and folded VALUES (ENABLE_DYNAMIC_VALUES_OPTIMIZATION):
     * one dynamic param per column, each param is a RawString holding that column's value
     * array, e.g. VALUES ROW(CAST(?0 AS INT), CAST(?1 AS VARCHAR)) with ?0=[1,2], ?1=[a,b].
     * Rows are produced by zipping the arrays at the same position.
     */
    private Executor tryCreateColumnArrayExecutor(List<RexNode> tuple, List<DataType> outputColumns,
                                                  ExecutionContext context,
                                                  Map<Integer, ParameterContext> params) {
        List<IExpression> expressions = new ArrayList<>(tuple.size());
        List<RawString> rawStrings = new ArrayList<>(tuple.size());
        for (int columnIndex = 0; columnIndex < tuple.size(); columnIndex++) {
            RexNode rexNode = tuple.get(columnIndex);
            RexDynamicParam dynamicParam = LogicalDynamicValues.extractLiteralDynamicParam(rexNode);
            if (dynamicParam == null) {
                return null;
            }

            ParameterContext parameterContext = params.get(dynamicParam.getIndex() + 1);
            if (parameterContext == null || !(parameterContext.getValue() instanceof RawString)) {
                return null;
            }

            RexInputRef inputRef = new RexInputRef(columnIndex, dynamicParam.getType());
            RexNode rowExpression =
                rexNode.accept(new RexUtil.ReplaceDynamicParamsShuttle(dynamicParam, inputRef));
            expressions.add(RexUtils.buildRexNode(rowExpression, context));
            rawStrings.add((RawString) parameterContext.getValue());
        }

        return new RawStringDynamicValueExec(expressions, rawStrings, outputColumns, context);
    }

    /**
     * Row-list mode, produced by multi-column IN rewrites (e.g. COLInToSemiJoinRule):
     * the tuple holds a single dynamic param (possibly CAST-wrapped) whose RawString elements
     * are row lists, e.g. (a, b) IN (?) with ?=[[1,a],[2,b]] while the row type has 2 columns.
     */
    private Executor tryCreateRowListExecutor(RexNode rexNode, List<DataType> outputColumns,
                                              ExecutionContext context,
                                              Map<Integer, ParameterContext> params) {
        RexDynamicParam dynamicParam = LogicalDynamicValues.extractLiteralDynamicParam(rexNode);
        if (dynamicParam == null) {
            return null;
        }

        ParameterContext parameterContext = params.get(dynamicParam.getIndex() + 1);
        if (parameterContext == null || !(parameterContext.getValue() instanceof RawString)) {
            return null;
        }

        List<IExpression> expressions = new ArrayList<>(outputColumns.size());
        for (int columnIndex = 0; columnIndex < outputColumns.size(); columnIndex++) {
            RexInputRef inputRef = new RexInputRef(columnIndex, dynamicParam.getType());
            RexNode rowExpression =
                rexNode.accept(new RexUtil.ReplaceDynamicParamsShuttle(dynamicParam, inputRef));
            expressions.add(RexUtils.buildRexNode(rowExpression, context));
        }

        return new RawStringDynamicValueExec(
            expressions, Collections.singletonList((RawString) parameterContext.getValue()), outputColumns, context);
    }

}
