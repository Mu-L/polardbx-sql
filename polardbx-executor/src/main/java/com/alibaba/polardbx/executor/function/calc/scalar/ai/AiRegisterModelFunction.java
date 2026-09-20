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
 * AI_REGISTER_MODEL(name, provider, endpoint, model [, options_json])
 * <p>
 * Registers a new AI model configuration.
 */
public class AiRegisterModelFunction extends AbstractScalarFunction {
    public AiRegisterModelFunction() {
    }

    public AiRegisterModelFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        AiFunctionPrivilegeUtils.checkPrivilege(ec, "AI_REGISTER_MODEL");
        if (args.length < 4) {
            throw new RuntimeException(
                "AI_REGISTER_MODEL requires at least 4 arguments: name, provider, endpoint, model");
        }

        String name = DataTypes.StringType.convertFrom(args[0]);
        String provider = DataTypes.StringType.convertFrom(args[1]);
        String endpoint = DataTypes.StringType.convertFrom(args[2]);
        String model = DataTypes.StringType.convertFrom(args[3]);
        String optionsJson = args.length > 4 ? DataTypes.StringType.convertFrom(args[4]) : null;

        return ModelManager.getInstance().registerModel(name, provider, endpoint, model, optionsJson);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_REGISTER_MODEL"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }
}
