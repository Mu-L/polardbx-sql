/*
 * Copyright 2019 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.optimizer.core.function.calc.scalar.vector;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * VEC_DISTANCE_COSINE(v1, v2) - Compute Cosine distance between two vectors.
 * Both arguments can be either float32 binary (byte[]) or text format (String).
 * Returns DOUBLE.
 * <p>
 * Cosine distance = 1 - cosine_similarity
 * cosine_similarity = dot(a, b) / (norm(a) * norm(b))
 */
public class VecDistanceCosine extends AbstractScalarFunction {
    public VecDistanceCosine(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public String[] getFunctionNames() {
        return new String[] {"VEC_DISTANCE_COSINE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args == null || args.length < 2 || args[0] == null || args[1] == null) {
            return null;
        }
        return VectorFunctions.cosine(
            VectorFunctions.decodeArgument(args[0], "VEC_DISTANCE_COSINE"),
            VectorFunctions.decodeArgument(args[1], "VEC_DISTANCE_COSINE"));
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.DoubleType;
    }
}
