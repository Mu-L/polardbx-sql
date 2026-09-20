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
import com.alibaba.polardbx.optimizer.core.datatype.VectorCodec;
import com.alibaba.polardbx.optimizer.core.datatype.VectorErrorMapper;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * VEC_TOTEXT(binary) - Convert float32 little-endian binary to vector text "[0.1, 0.2, 0.3]".
 * Compatible with DN binary storage format where each dimension is a 4-byte IEEE 754 float.
 */
public class VecToText extends AbstractScalarFunction {
    public VecToText(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public String[] getFunctionNames() {
        return new String[] {"VEC_TOTEXT", "FROM_VECTOR", "VECTOR_TO_STRING"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args == null || args.length < 1 || args[0] == null) {
            return null;
        }
        Object arg = args[0];
        if (arg instanceof byte[]) {
            return VectorCodec.formatText(VectorCodec.decodeBinary((byte[]) arg));
        }
        if (arg instanceof String) {
            // If already text, validate and normalize the format
            return VectorCodec.formatText(VectorCodec.parseText((String) arg));
        }
        throw VectorErrorMapper.invalidArgument("VEC_TOTEXT",
            "expected VECTOR binary, got " + arg.getClass().getName());
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }
}
