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

package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.polardbx.executor.ai.ModelManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * AI_DESCRIBE_MODEL(name)
 * <p>
 * Returns detailed information about a specific AI model as JSON.
 */
public class AiDescribeModelFunction extends AbstractScalarFunction {
    public AiDescribeModelFunction() {
    }

    public AiDescribeModelFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 1) {
            throw new RuntimeException("AI_DESCRIBE_MODEL requires 1 argument: name");
        }

        String name = DataTypes.StringType.convertFrom(args[0]);

        return ModelManager.getInstance().describeModel(name);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_DESCRIBE_MODEL"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }
}
