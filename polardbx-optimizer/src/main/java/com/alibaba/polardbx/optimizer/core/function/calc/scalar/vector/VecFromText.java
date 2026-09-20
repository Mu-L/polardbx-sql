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
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * VEC_FROMTEXT(text) - Convert vector text "[0.1, 0.2, 0.3]" to float32 little-endian binary.
 * Compatible with DN binary storage format where each dimension is a 4-byte IEEE 754 float.
 */
public class VecFromText extends AbstractScalarFunction {
    public VecFromText(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public String[] getFunctionNames() {
        return new String[] {"VEC_FROMTEXT", "TO_VECTOR", "STRING_TO_VECTOR"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args == null || args.length < 1 || args[0] == null) {
            return null;
        }
        Object arg = args[0];
        if (arg instanceof String) {
            return VectorCodec.encodeBinary(VectorCodec.parseText((String) arg));
        }
        if (arg instanceof byte[]) {
            return VectorCodec.encodeBinary(VectorCodec.parseUtf8((byte[]) arg));
        }
        return VectorCodec.encodeBinary(VectorCodec.parseText(DataTypes.StringType.convertFrom(arg)));
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.VectorType;
    }
}
