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

package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import static org.apache.calcite.sql.SqlKind.MIN;

/**
 * Created by chuanqin on 17/8/11.
 */
public class MinV2 extends Aggregator {

    private Object outputValue;

    public MinV2() {
    }

    public MinV2(int index, int filterArg) {
        super(new int[] {index}, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return MIN;
    }

    @Override
    protected void conductAgg(Object value) {
        if (outputValue == null) {
            outputValue = value;
        } else {
            outputValue = returnType.compare(value, outputValue) <= 0 ? value : outputValue;
        }

    }

    @Override
    public Aggregator getNew() {
        return new MinV2(aggTargetIndexes[0], filterArg);
    }

    @Override
    public Object eval(Row row) {
        return outputValue;
    }

    @Override
    public void setFunction(IFunction function) {

    }

    @Override
    public IFunction.FunctionType getFunctionType() {
        return IFunction.FunctionType.Aggregate;
    }

    @Override
    public DataType getReturnType() {
        return returnType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"MIN_V2"};
    }

    @Override
    public void clear() {

    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public int getPrecision() {
        return 0;
    }
}
