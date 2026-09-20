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

public class AiUpdateFunctionFunction extends AbstractScalarFunction {

    public AiUpdateFunctionFunction() {
    }

    public AiUpdateFunctionFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        AiFunctionPrivilegeUtils.checkPrivilege(ec, "AI_UPDATE_FUNCTION");
        if (args.length < 2) {
            throw new RuntimeException(
                "AI_UPDATE_FUNCTION requires 2 arguments: function_name, model_name");
        }

        String functionName = DataTypes.StringType.convertFrom(args[0]);
        String modelName = DataTypes.StringType.convertFrom(args[1]);

        ModelManager.getInstance().updateDefaultModelForFunction(functionName, modelName);
        return "OK";
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_UPDATE_FUNCTION"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }
}
