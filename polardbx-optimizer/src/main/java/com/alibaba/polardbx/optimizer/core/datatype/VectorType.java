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
package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * VECTOR data type for distributed vector index support.
 * Stores vector data as byte[] in DN-compatible float32 little-endian binary format.
 * Each dimension is a 4-byte IEEE 754 float in little-endian order.
 * Total length = dimension * 4 bytes.
 */
public class VectorType extends AbstractDataType<byte[]> {

    private final int dimension; // 0 means unlimited dimension

    public VectorType() {
        this(0);
    }

    public VectorType(int dimension) {
        this.dimension = dimension;
    }

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {
            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getBytes(index);
            }

            @Override
            public Object get(Row rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
    }

    @Override
    public byte[] getMaxValue() {
        throw new NotSupportException("VECTOR type does not support MAX operation");
    }

    @Override
    public byte[] getMinValue() {
        throw new NotSupportException("VECTOR type does not support MIN operation");
    }

    @Override
    public Calculator getCalculator() {
        throw new NotSupportException("VECTOR type does not support direct calculation");
    }

    @Override
    public int getSqlType() {
        return Types.VARBINARY;
    }

    @Override
    public String getStringSqlType() {
        return "VECTOR";
    }

    @Override
    public int compare(Object o1, Object o2) {
        throw new NotSupportException("VECTOR type does not support direct comparison");
    }

    @Override
    public Class<byte[]> getDataClass() {
        return byte[].class;
    }

    @Override
    public byte[] convertFrom(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            byte[] binary = (byte[]) value;
            validateDimension(VectorCodec.decodeBinary(binary).length);
            return binary;
        }
        if (value instanceof String) {
            byte[] binary = textToVector((String) value);
            validateDimension(VectorCodec.decodeBinary(binary).length);
            return binary;
        }
        if (value instanceof float[]) {
            float[] vector = (float[]) value;
            validateDimension(vector.length);
            return fromFloatArray(vector);
        }
        throw VectorErrorMapper.invalidVector("Cannot convert " + value.getClass().getName() + " to VECTOR");
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_BLOB;
    }

    public int getDimension() {
        return dimension;
    }

    @Override
    public int getPrecision() {
        return dimension;
    }

    @Override
    public int length() {
        return dimension * Float.SIZE / Byte.SIZE;
    }

    @Override
    public boolean equalDeeply(DataType that) {
        if (that == null || that.getClass() != this.getClass()) {
            return false;
        }
        return dimension == ((VectorType) that).dimension;
    }

    // ========== Static utility methods for vector binary format conversion ==========

    /**
     * Convert float32 little-endian binary to float array.
     * Binary format: each float is 4 bytes in little-endian order.
     */
    public static float[] toFloatArray(byte[] binary) {
        return VectorCodec.decodeBinary(binary);
    }

    /**
     * Convert float array to float32 little-endian binary format.
     */
    public static byte[] fromFloatArray(float[] vector) {
        return VectorCodec.encodeBinary(vector);
    }

    /**
     * Parse vector text format "[0.1, 0.2, 0.3]" to float32 little-endian binary.
     */
    public static byte[] textToVector(String text) {
        return text == null ? null : VectorCodec.encodeBinary(VectorCodec.parseText(text));
    }

    /**
     * Convert float32 little-endian binary to vector text format "[0.1, 0.2, 0.3]".
     */
    public static String vectorToText(byte[] binary) {
        return binary == null ? null : VectorCodec.formatText(VectorCodec.decodeBinary(binary));
    }

    /**
     * Parse vector text "[0.1, 0.2, 0.3]" to float array.
     */
    public static float[] parseVectorText(String text) {
        return VectorCodec.parseText(text);
    }

    /**
     * Convert float array to text format "[0.1, 0.2, 0.3]".
     * Output format matches DN behavior (MySQL my_gcvt for float):
     * - 6 significant digits (FLT_DIG)
     * - integers without ".0" suffix
     * - decimal notation for wide range, scientific only for extreme values
     * - scientific notation without '+' sign (e.g. "1e15" not "1e+15")
     */
    public static String floatArrayToText(float[] vector) {
        return VectorCodec.formatText(vector);
    }

    private void validateDimension(int actualDimension) {
        VectorCodec.validateDimension(actualDimension, dimension);
    }
}
