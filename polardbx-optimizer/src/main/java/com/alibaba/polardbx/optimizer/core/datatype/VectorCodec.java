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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.List;

/**
 * Canonical CN codec for the DN VECTOR float32 little-endian representation.
 */
public final class VectorCodec {

    private static final int FLOAT_BYTES = Float.SIZE / Byte.SIZE;

    private VectorCodec() {
    }

    public static float[] decodeBinary(byte[] binary) {
        if (binary == null) {
            return null;
        }
        if (binary.length % FLOAT_BYTES != 0) {
            throw VectorErrorMapper.invalidVector(
                "Invalid VECTOR binary length " + binary.length + "; expected a multiple of 4");
        }
        float[] vector = new float[binary.length / FLOAT_BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(binary).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < vector.length; i++) {
            vector[i] = buffer.getFloat();
        }
        validateFinite(vector);
        return vector;
    }

    public static byte[] encodeBinary(float[] vector) {
        if (vector == null) {
            return null;
        }
        validateFinite(vector);
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * FLOAT_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (float value : vector) {
            buffer.putFloat(value);
        }
        return buffer.array();
    }

    public static float[] parseText(String text) {
        if (text == null) {
            return null;
        }
        String normalized = text.trim();
        if (normalized.length() < 2 || normalized.charAt(0) != '['
            || normalized.charAt(normalized.length() - 1) != ']') {
            throw VectorErrorMapper.invalidVector("Invalid VECTOR text format");
        }
        String elements = normalized.substring(1, normalized.length() - 1).trim();
        if (elements.isEmpty()) {
            return new float[0];
        }
        String[] parts = elements.split(",", -1);
        float[] vector = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i].trim();
            if (part.isEmpty()) {
                throw VectorErrorMapper.invalidVector(
                    "Invalid VECTOR text format: empty element at index " + i);
            }
            try {
                vector[i] = Float.parseFloat(part);
            } catch (NumberFormatException e) {
                throw VectorErrorMapper.invalidVector(
                    "Invalid VECTOR element at index " + i + ": " + part);
            }
        }
        validateFinite(vector);
        return vector;
    }

    public static float[] parseUtf8(byte[] text) {
        return text == null ? null : parseText(new String(text, StandardCharsets.UTF_8));
    }

    /**
     * Parse a JSON-typed value using the DN contract: only one-dimensional numeric arrays are vectors.
     */
    public static float[] parseJson(Object json) {
        if (json == null) {
            return null;
        }
        Object value = json;
        if (json instanceof String) {
            try {
                value = JSON.parse((String) json);
            } catch (JSONException e) {
                throw VectorErrorMapper.invalidVector("Invalid JSON value for VECTOR");
            }
        }
        if (value == null) {
            return null;
        }
        if (!(value instanceof List)) {
            throw VectorErrorMapper.invalidVector("JSON value for VECTOR must be a one-dimensional numeric array");
        }

        List<?> elements = (List<?>) value;
        float[] vector = new float[elements.size()];
        for (int i = 0; i < elements.size(); i++) {
            Object element = elements.get(i);
            if (!(element instanceof Number)) {
                throw VectorErrorMapper.invalidVector(
                    "JSON VECTOR element at index " + i + " must be numeric");
            }
            try {
                vector[i] = Float.parseFloat(element.toString());
            } catch (NumberFormatException e) {
                throw VectorErrorMapper.invalidVector(
                    "Invalid JSON VECTOR element at index " + i);
            }
        }
        validateFinite(vector);
        return vector;
    }

    public static String formatText(float[] vector) {
        if (vector == null) {
            return null;
        }
        validateFinite(vector);
        StringBuilder result = new StringBuilder("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) {
                result.append(',');
            }
            result.append(formatFloat(vector[i]));
        }
        return result.append(']').toString();
    }

    public static long dimension(byte[] binary) {
        return decodeBinary(binary).length;
    }

    public static void validateDimension(int actual, int expected) {
        if (expected > 0 && actual != expected) {
            throw VectorErrorMapper.invalidVector(
                "VECTOR dimension mismatch; expected " + expected + " but got " + actual);
        }
    }

    public static void validateFinite(float[] vector) {
        for (int i = 0; i < vector.length; i++) {
            if (!Float.isFinite(vector[i])) {
                throw VectorErrorMapper.invalidVector(
                    "VECTOR element at index " + i + " must be finite");
            }
        }
    }

    private static String formatFloat(float value) {
        if (Float.floatToRawIntBits(value) == Float.floatToRawIntBits(-0.0f)) {
            return "-0";
        }
        if (value == 0.0f) {
            return "0";
        }

        String formatted = String.format(Locale.ROOT, "%.6g", (double) value);
        int exponentIndex = formatted.indexOf('e');
        if (exponentIndex < 0) {
            exponentIndex = formatted.indexOf('E');
        }
        if (exponentIndex >= 0) {
            String mantissa = stripDecimalZeros(formatted.substring(0, exponentIndex));
            int exponent = Integer.parseInt(formatted.substring(exponentIndex + 1));
            String plain = toPlainDecimal(mantissa, exponent);
            return plain.length() <= 12 ? plain : mantissa + "e" + exponent;
        }
        return stripDecimalZeros(formatted);
    }

    private static String stripDecimalZeros(String value) {
        if (!value.contains(".")) {
            return value;
        }
        String stripped = value.replaceAll("0+$", "");
        return stripped.endsWith(".") ? stripped.substring(0, stripped.length() - 1) : stripped;
    }

    private static String toPlainDecimal(String mantissa, int exponent) {
        boolean negative = mantissa.startsWith("-");
        if (negative) {
            mantissa = mantissa.substring(1);
        }
        int dot = mantissa.indexOf('.');
        String integer = dot >= 0 ? mantissa.substring(0, dot) : mantissa;
        String fraction = dot >= 0 ? mantissa.substring(dot + 1) : "";
        String digits = integer + fraction;
        int decimalPosition = integer.length() + exponent;

        StringBuilder result = new StringBuilder();
        if (negative) {
            result.append('-');
        }
        if (decimalPosition <= 0) {
            result.append("0.");
            for (int i = 0; i < -decimalPosition; i++) {
                result.append('0');
            }
            result.append(digits);
        } else if (decimalPosition >= digits.length()) {
            result.append(digits);
            for (int i = digits.length(); i < decimalPosition; i++) {
                result.append('0');
            }
        } else {
            result.append(digits, 0, decimalPosition).append('.').append(digits, decimalPosition, digits.length());
        }
        return stripDecimalZeros(result.toString());
    }
}
