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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.string;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BigBitType;
import com.alibaba.polardbx.optimizer.core.datatype.BlobType;
import com.alibaba.polardbx.optimizer.core.datatype.BytesType;
import com.alibaba.polardbx.optimizer.core.datatype.ClobType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.datatype.JsonType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.datatype.StringType;
import com.alibaba.polardbx.optimizer.core.datatype.TimeType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Locale;

/**
 * <pre>
 * HEX(str), HEX(N)
 *
 * For a string argument str, HEX() returns a hexadecimal string representation of str where each byte of each character in str is converted to two hexadecimal digits. (Multi-byte characters therefore become more than two digits.) The inverse of this operation is performed by the UNHEX() function.
 *
 * For a numeric argument N, HEX() returns a hexadecimal string representation of the value of N treated as a longlong (BIGINT) number. This is equivalent to CONV(N,10,16). The inverse of this operation is performed by CONV(HEX(N),16,10).
 *
 * mysql> SELECT 0x616263, HEX('abc'), UNHEX(HEX('abc'));
 *         -> 'abc', 616263, 'abc'
 * mysql> SELECT HEX(255), CONV(HEX(255),16,10);
 *         -> 'FF', 255
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午3:59:21
 * @since 5.1.0
 */
public class Hex extends AbstractScalarFunction {
    public Hex() {
    }

    public Hex(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private final static char[] digVecUpper = "0123456789ABCDEF".toCharArray();
    private static final BigDecimal SIGNED_LONG_MIN = BigDecimal.valueOf(Long.MIN_VALUE);
    private static final BigDecimal SIGNED_LONG_MAX = BigDecimal.valueOf(Long.MAX_VALUE);
    private static final double TWO_TO_63 = 0x1.0p63;
    private static final double TWO_TO_64 = 0x1.0p64;
    private static final BigInteger TWO_TO_63_AS_BIG_INTEGER = BigInteger.ONE.shiftLeft(Long.SIZE - 1);

    @Override
    public String[] getFunctionNames() {
        return new String[] {"HEX"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }
        DataType operandType = operandTypes.get(0);
        /*
         * MySQL decides HEX semantics from Item::result_type(): STRING_RESULT values are encoded byte by byte, while
         * numeric result types are converted through val_int(). PolarDB-X has a richer DataType hierarchy, so exact-class
         * comparison is not a safe substitute for that split. Keep the classification explicit and ordered here. This
         * preserves the old numeric behavior for YEAR/BIT/BOOLEAN and future number types, while every future BytesType
         * subtype automatically inherits raw-octet semantics. A byte-backed type with a different public representation
         * must add its dedicated branch before the BytesType family check.
         */
        if (operandType instanceof BytesType) {
            byte[] bytes = DataTypes.BytesType.convertFrom(args[0]);
            return octetToHex(bytes);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.VectorType, operandType)) {
            byte[] bytes = (byte[]) args[0];
            return octetToHex(bytes);
        } else if (operandType instanceof BlobType) {
            Blob blob = DataTypes.BlobType.convertFrom(args[0]);
            try {
                return octetToHex(blob.getBytes(1, (int) blob.length()));
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        } else if (operandType instanceof ClobType) {
            Clob clob = DataTypes.ClobType.convertFrom(args[0]);
            try {
                return octetToHex(clob.getSubString(1, (int) clob.length()).getBytes(StandardCharsets.UTF_8));
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        } else if (isStringResultType(operandType)) {
            return octetToHex(stringResultToBytes(args[0], operandType));
        } else {
            return numberToHex(args[0], operandType);
        }
    }

    private static boolean isStringResultType(DataType operandType) {
        return DataTypeUtil.isStringType(operandType)
            || operandType instanceof JsonType
            || operandType instanceof EnumType
            || DataTypeUtil.isMysqlTimeType(operandType);
    }

    /**
     * Convert a MySQL STRING_RESULT value to the physical bytes seen by HEX.
     * <p>
     * Character chunks are stored internally as UTF-8. For non-UTF-8 columns they must be encoded back to the declared
     * column charset before the hexadecimal conversion. BINARY-collated slices are already physical octets and must not
     * pass through a character encoder, because doing so would corrupt invalid UTF-8 and zero padding. JSON and ENUM do
     * not retain a SliceType charset in the current DataType model, so their textual value uses the chunk UTF-8 encoding.
     * Temporal values use the same scale-aware presentation string that MySQL exposes through val_str().
     */
    private static byte[] stringResultToBytes(Object value, DataType operandType) {
        if (operandType instanceof DateType) {
            return ((DateType) operandType).toStandardString(value).getBytes(StandardCharsets.UTF_8);
        } else if (operandType instanceof TimeType) {
            return ((TimeType) operandType).toStandardString(value).getBytes(StandardCharsets.UTF_8);
        } else if (operandType instanceof TimestampType) {
            return ((TimestampType) operandType).toStandardString(value).getBytes(StandardCharsets.UTF_8);
        } else if (operandType instanceof EnumType) {
            return ((EnumType) operandType).convertFrom(value).getBytes(StandardCharsets.UTF_8);
        } else if (operandType instanceof JsonType) {
            return DataTypes.StringType.convertFrom(value).getBytes(StandardCharsets.UTF_8);
        }

        Slice utf8Value = DataTypes.VarcharType.convertFrom(value);
        if (operandType instanceof SliceType) {
            SliceType sliceType = (SliceType) operandType;
            if (sliceType.getCharsetName() == CharsetName.BINARY) {
                return utf8Value.getBytes();
            }
            return sliceType.getCharsetHandler().encodeFromUtf8(utf8Value).getBytes();
        } else if (operandType instanceof StringType) {
            return ((StringType) operandType).getCharsetHandler().encodeFromUtf8(utf8Value).getBytes();
        }

        // SensitiveStringType and legacy string implementations do not expose a charset handler. Their contract is
        // UTF-8, and Slice#getBytes is safe for sliced buffers whose base array has a non-zero offset.
        return utf8Value.getBytes();
    }

    /**
     * Convert a numeric result using the algorithm selected by the instance's MySQL compatibility version.
     * <p>
     * MySQL 5.7 and 8.0 intentionally differ for REAL_RESULT and DECIMAL_RESULT. MySQL 5.7 calls val_real(), rounds by
     * adding +0.5 or -0.5, and casts to ulonglong. Consequently it loses DECIMAL precision beyond 2^53, rounds exact
     * halves away from zero, and accepts positive values up to (but excluding) 2^64. MySQL 8.0 instead calls the signed
     * val_int() path, whose DECIMAL and REAL conversions have their own rounding rules and a signed 64-bit range.
     * <p>
     * Keep this split based on the declared operand type rather than the Java value class. A DECIMAL chunk value and a
     * DOUBLE chunk value may both arrive as Number implementations, but MySQL chooses the branch from Item::result_type.
     * Integer types, including BIGINT UNSIGNED, retain the shared behavior below. This arrangement is also forward
     * compatible: new real/decimal DataType implementations that participate in DataTypeUtil's semantic families pick
     * up the correct versioned behavior without exact-class checks.
     */
    private static String numberToHex(Object value, DataType operandType) {
        if (operandType instanceof ULongType) {
            UInt64 unsignedValue = DataTypes.ULongType.convertFrom(value);
            return unsignedValue.toBigInteger().toString(16).toUpperCase(Locale.ROOT);
        }
        if (!InstanceVersion.isMYSQL80()
            && (DataTypeUtil.isRealType(operandType) || DataTypeUtil.isDecimalType(operandType))) {
            return mysql57RealOrDecimalToHex(value);
        }

        long signedValue;
        if (value instanceof Decimal) {
            signedValue = decimalToSignedLong(((Decimal) value).toBigDecimal());
        } else if (value instanceof BigDecimal) {
            signedValue = decimalToSignedLong((BigDecimal) value);
        } else if (value instanceof BigInteger) {
            if (operandType instanceof BigBitType) {
                // BIT(64) is delivered as BigInteger but val_int() exposes its low 64 bits.
                signedValue = ((BigInteger) value).longValue();
            } else {
                signedValue = integerToSignedLong((BigInteger) value);
            }
        } else if (value instanceof Double || value instanceof Float) {
            signedValue = (long) Math.rint(((Number) value).doubleValue());
        } else if (value instanceof Number) {
            signedValue = ((Number) value).longValue();
        } else if (value instanceof Boolean) {
            signedValue = (Boolean) value ? 1L : 0L;
        } else {
            signedValue = DataTypes.LongType.convertFrom(value);
        }
        return Long.toHexString(signedValue).toUpperCase(Locale.ROOT);
    }

    /**
     * Reproduce MySQL 5.7 Item_func_hex's REAL/DECIMAL conversion without first narrowing the positive domain to a
     * Java signed long. Java's cast from double to long saturates at Long.MAX_VALUE, whereas MySQL 5.7 accepts the upper
     * unsigned half [2^63, 2^64). Subtracting 2^63 keeps that half in Java's signed range; adding it back as BigInteger
     * preserves the exact 64-bit bit pattern used by longlong2str().
     * <p>
     * The comparison deliberately happens before rounding, matching MySQL 5.7. Boundary values at or below -2^63 and
     * at or above 2^64 therefore produce ULLONG_MAX (sixteen 'F' digits), including infinities. DECIMAL is converted to
     * double on purpose: avoiding that precision loss would look safer but would be incompatible with MySQL 5.7.
     */
    private static String mysql57RealOrDecimalToHex(Object value) {
        double realValue;
        if (value instanceof Decimal) {
            realValue = ((Decimal) value).toBigDecimal().doubleValue();
        } else if (value instanceof BigDecimal) {
            realValue = ((BigDecimal) value).doubleValue();
        } else {
            realValue = DataTypes.DoubleType.convertFrom(value);
        }

        if (realValue <= -TWO_TO_63 || realValue >= TWO_TO_64) {
            return "FFFFFFFFFFFFFFFF";
        }

        double roundedValue = realValue + (realValue > 0D ? 0.5D : -0.5D);
        if (roundedValue < TWO_TO_63) {
            return Long.toHexString((long) roundedValue).toUpperCase(Locale.ROOT);
        }

        long unsignedLowerHalf = (long) (roundedValue - TWO_TO_63);
        return TWO_TO_63_AS_BIG_INTEGER.add(BigInteger.valueOf(unsignedLowerHalf))
            .toString(16).toUpperCase(Locale.ROOT);
    }

    private static long decimalToSignedLong(BigDecimal decimal) {
        if (decimal.compareTo(SIGNED_LONG_MIN) < 0) {
            return Long.MIN_VALUE;
        } else if (decimal.compareTo(SIGNED_LONG_MAX) > 0) {
            return Long.MAX_VALUE;
        }
        return decimal.setScale(0, RoundingMode.HALF_UP).longValue();
    }

    private static long integerToSignedLong(BigInteger integer) {
        if (integer.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
            return Long.MIN_VALUE;
        } else if (integer.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
            return Long.MAX_VALUE;
        }
        return integer.longValue();
    }

    /**
     * 依照 mysql 5.7 sql/auth/password.cc 函数 char *octet2hex(char *to, const char *str, uint len)
     */
    private static String octetToHex(byte[] input) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < input.length; i++) {
            int unsignedByte = input[i] & 0xFF;
            builder.append(digVecUpper[unsignedByte >> 4]);
            builder.append(digVecUpper[unsignedByte & 0x0F]);
        }
        return builder.toString();
    }
}
