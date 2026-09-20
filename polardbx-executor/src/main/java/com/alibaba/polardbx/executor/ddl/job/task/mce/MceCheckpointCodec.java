package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Versioned codec for the ordered, typed primary-key tuple stored in an MCE checkpoint LONGTEXT field.
 */
final class MceCheckpointCodec {

    static final String TYPE = "MCE_PK_TUPLE_V1";

    private static final int VERSION = 1;
    private static final Pattern UPPER_HEX = Pattern.compile("[0-9A-F]*");

    private MceCheckpointCodec() {
    }

    static String encode(List<ParameterContext> tuple) {
        if (tuple == null || tuple.isEmpty()) {
            throw error("tuple must contain at least one element");
        }

        JSONArray elements = new JSONArray(tuple.size());
        for (int i = 0; i < tuple.size(); i++) {
            ParameterContext normalized = normalize(tuple.get(i), i + 1);
            JSONObject element = new JSONObject(true);
            element.put("method", normalized.getParameterMethod().name());
            element.put("value", serialize(normalized, i));
            elements.add(element);
        }

        JSONObject root = new JSONObject(true);
        root.put("version", VERSION);
        root.put("arity", tuple.size());
        root.put("elements", elements);
        return root.toJSONString();
    }

    static List<ParameterContext> decode(String payload, int expectedArity, String checkpointType) {
        if (!TYPE.equals(checkpointType)) {
            throw error("unsupported checkpoint type " + checkpointType);
        }
        if (payload == null || payload.isEmpty()) {
            throw error("payload must not be empty");
        }
        if (expectedArity <= 0) {
            throw error("expected arity must be positive");
        }
        if (payload.charAt(0) != '{') {
            throw error("checkpoint payload must use the versioned tuple format");
        }
        return decodeVersioned(payload, expectedArity);
    }

    private static List<ParameterContext> decodeVersioned(String payload, int expectedArity) {
        final JSONObject root;
        try {
            root = JSON.parseObject(payload);
        } catch (RuntimeException e) {
            throw error("malformed versioned payload");
        }
        if (root == null || root.size() != 3 || !root.containsKey("version") || !root.containsKey("arity")
            || !root.containsKey("elements")) {
            throw error("versioned payload must contain exactly version, arity and elements");
        }

        Object versionValue = root.get("version");
        if (!(versionValue instanceof Integer) || ((Integer) versionValue) != VERSION) {
            throw error("unsupported or invalid version");
        }
        Object arityValue = root.get("arity");
        if (!(arityValue instanceof Integer) || ((Integer) arityValue) <= 0) {
            throw error("invalid arity");
        }
        int arity = (Integer) arityValue;
        if (arity != expectedArity) {
            throw error("checkpoint arity " + arity + " does not match expected arity " + expectedArity);
        }

        Object elementsValue = root.get("elements");
        if (!(elementsValue instanceof JSONArray)) {
            throw error("elements must be an array");
        }
        JSONArray elements = (JSONArray) elementsValue;
        if (elements.size() != arity) {
            throw error("element count " + elements.size() + " does not match arity " + arity);
        }

        List<ParameterContext> tuple = new ArrayList<>(arity);
        for (int i = 0; i < arity; i++) {
            Object elementValue = elements.get(i);
            if (!(elementValue instanceof JSONObject)) {
                throw error("element " + i + " must be an object");
            }
            JSONObject element = (JSONObject) elementValue;
            if (element.size() != 2 || !element.containsKey("method") || !element.containsKey("value")) {
                throw error("element " + i + " must contain exactly method and value");
            }
            Object methodValue = element.get("method");
            Object serializedValue = element.get("value");
            if (!(methodValue instanceof String) || !(serializedValue instanceof String)) {
                throw error("element " + i + " method and value must be strings");
            }
            tuple.add(deserialize(i + 1, (String) methodValue, (String) serializedValue, i));
        }
        return tuple;
    }

    private static ParameterContext normalize(ParameterContext context, int index) {
        if (context == null || context.getParameterMethod() == null || context.getArgs() == null
            || context.getArgs().length < 2 || context.getArgs()[1] == null) {
            throw error("element " + (index - 1) + " has no non-null typed value");
        }
        ParameterMethod method = context.getParameterMethod();
        Object value = context.getArgs()[1];
        if (method == ParameterMethod.setObject1) {
            method = normalizedObjectMethod(value, index - 1);
            if (value instanceof BigInteger) {
                value = new BigDecimal((BigInteger) value);
            } else if (value instanceof Decimal) {
                value = ((Decimal) value).toBigDecimal();
            }
        }
        ParameterContext normalized = new ParameterContext(method, new Object[] {index, value});
        validateRuntimeType(normalized, index - 1);
        return normalized;
    }

    private static ParameterMethod normalizedObjectMethod(Object value, int elementIndex) {
        if (value instanceof Byte) {
            return ParameterMethod.setByte;
        } else if (value instanceof Short) {
            return ParameterMethod.setShort;
        } else if (value instanceof Integer) {
            return ParameterMethod.setInt;
        } else if (value instanceof Long) {
            return ParameterMethod.setLong;
        } else if (value instanceof Float) {
            return ParameterMethod.setFloat;
        } else if (value instanceof Double) {
            return ParameterMethod.setDouble;
        } else if (value instanceof BigDecimal || value instanceof BigInteger || value instanceof Decimal) {
            return ParameterMethod.setBigDecimal;
        } else if (value instanceof String) {
            return ParameterMethod.setString;
        } else if (value instanceof byte[]) {
            return ParameterMethod.setBytes;
        }
        throw error("element " + elementIndex + " setObject1 runtime type is not supported");
    }

    private static String serialize(ParameterContext context, int elementIndex) {
        validateRuntimeType(context, elementIndex);
        switch (context.getParameterMethod()) {
        case setByte:
        case setShort:
        case setFloat:
        case setBit:
            return context.getArgs()[1].toString();
        case setInt:
        case setLong:
        case setDouble:
        case setBytes:
        case setBigDecimal:
        case setString:
            return Transformer.serializeParam(context);
        default:
            throw error("element " + elementIndex + " has unsupported method "
                + context.getParameterMethod().name());
        }
    }

    private static ParameterContext deserialize(int index, String methodName, String value, int elementIndex) {
        final ParameterMethod method;
        try {
            method = ParameterMethod.valueOf(methodName);
        } catch (RuntimeException e) {
            throw error("element " + elementIndex + " has unknown method");
        }

        final Object typedValue;
        try {
            switch (method) {
            case setByte:
                typedValue = Byte.valueOf(value);
                break;
            case setShort:
                typedValue = Short.valueOf(value);
                break;
            case setFloat:
                typedValue = Float.valueOf(value);
                break;
            case setBit:
                typedValue = new BigInteger(value);
                break;
            case setBytes:
                if ((value.length() & 1) != 0 || !UPPER_HEX.matcher(value).matches()) {
                    throw new IllegalArgumentException("non-canonical hex");
                }
                typedValue = Transformer.deserializeParam(method, value);
                break;
            case setInt:
            case setLong:
            case setDouble:
            case setBigDecimal:
            case setString:
                typedValue = Transformer.deserializeParam(method, value);
                break;
            default:
                throw error("element " + elementIndex + " has unsupported method " + methodName);
            }
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (RuntimeException e) {
            throw error("element " + elementIndex + " has invalid value for " + methodName);
        }

        ParameterContext context = new ParameterContext(method, new Object[] {index, typedValue});
        validateRuntimeType(context, elementIndex);
        if (!value.equals(serialize(context, elementIndex))) {
            throw error("element " + elementIndex + " has non-canonical value for " + methodName);
        }
        return context;
    }

    private static void validateRuntimeType(ParameterContext context, int elementIndex) {
        Object value = context.getArgs()[1];
        boolean valid;
        switch (context.getParameterMethod()) {
        case setByte:
            valid = value instanceof Byte;
            break;
        case setShort:
            valid = value instanceof Short;
            break;
        case setInt:
            valid = value instanceof Integer;
            break;
        case setLong:
            valid = value instanceof Long;
            break;
        case setFloat:
            valid = value instanceof Float && Float.isFinite((Float) value);
            break;
        case setDouble:
            valid = value instanceof Double && Double.isFinite((Double) value);
            break;
        case setBigDecimal:
            valid = value instanceof BigDecimal;
            break;
        case setString:
            valid = value instanceof String;
            break;
        case setBytes:
            valid = value instanceof byte[];
            break;
        case setBit:
            valid = value instanceof BigInteger;
            break;
        default:
            valid = false;
        }
        if (!valid) {
            throw error("element " + elementIndex + " method/value type mismatch for "
                + context.getParameterMethod().name());
        }
    }

    private static TddlRuntimeException error(String reason) {
        return new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
            "[MCE] invalid checkpoint codec: " + reason);
    }
}
