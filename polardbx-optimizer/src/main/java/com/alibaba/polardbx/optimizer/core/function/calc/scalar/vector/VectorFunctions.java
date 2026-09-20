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

import com.alibaba.polardbx.optimizer.core.datatype.VectorCodec;
import com.alibaba.polardbx.optimizer.core.datatype.VectorErrorMapper;

/**
 * Shared, stateless CN implementation of explicit VECTOR functions.
 */
public final class VectorFunctions {

    private VectorFunctions() {
    }

    public static double euclidean(float[] left, float[] right) {
        validateOperands("VEC_DISTANCE_EUCLIDEAN", left, right);
        double sum = 0.0D;
        for (int i = 0; i < left.length; i++) {
            double difference = (double) left[i] - right[i];
            sum += difference * difference;
        }
        return Math.sqrt(sum);
    }

    public static double cosine(float[] left, float[] right) {
        validateOperands("VEC_DISTANCE_COSINE", left, right);
        double dot = 0.0D;
        double leftNorm = 0.0D;
        double rightNorm = 0.0D;
        for (int i = 0; i < left.length; i++) {
            double leftValue = left[i];
            double rightValue = right[i];
            dot += leftValue * rightValue;
            leftNorm += leftValue * leftValue;
            rightNorm += rightValue * rightValue;
        }
        double denominator = Math.sqrt(leftNorm) * Math.sqrt(rightNorm);
        if (denominator == 0.0D) {
            return 0.0D;
        }
        double similarity = Math.max(-1.0D, Math.min(1.0D, dot / denominator));
        return 1.0D - similarity;
    }

    public static double innerProductDistance(float[] left, float[] right) {
        validateOperands("VEC_DISTANCE_INNER_PRODUCT", left, right);
        double dot = 0.0D;
        for (int i = 0; i < left.length; i++) {
            dot += (double) left[i] * right[i];
        }
        return -dot;
    }

    public static long dimension(float[] vector) {
        if (vector == null) {
            throw VectorErrorMapper.invalidVector("VECTOR_DIM: vector must not be null");
        }
        VectorCodec.validateFinite(vector);
        return vector.length;
    }

    public static float[] decodeArgument(Object value, String functionName) {
        if (value instanceof byte[]) {
            return VectorCodec.decodeBinary((byte[]) value);
        }
        if (value instanceof String) {
            return VectorCodec.parseText((String) value);
        }
        throw VectorErrorMapper.invalidArgument(functionName,
            "expected VECTOR binary or vector text, got " + value.getClass().getName());
    }

    private static void validateOperands(String functionName, float[] left, float[] right) {
        if (left == null || right == null) {
            throw VectorErrorMapper.invalidArgument(functionName, "vectors must not be null");
        }
        VectorCodec.validateFinite(left);
        VectorCodec.validateFinite(right);
        if (left.length != right.length) {
            throw VectorErrorMapper.invalidArgument(functionName,
                "dimension mismatch, " + left.length + " vs " + right.length);
        }
    }
}
