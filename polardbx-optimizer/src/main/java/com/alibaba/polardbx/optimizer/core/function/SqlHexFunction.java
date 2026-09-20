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
package com.alibaba.polardbx.optimizer.core.function;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.EnumSqlType;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.nio.charset.StandardCharsets;

/**
 * MySQL-compatible declaration of {@code HEX()}.
 * <p>
 * The runtime function and this planner declaration use the compatibility instance's native MySQL metadata rules.
 * MySQL 8 numeric-result arguments reserve exactly 16 hexadecimal digits. MySQL 5.7 instead declares every result as
 * twice the argument's maximum display/byte length, including numeric arguments, even though a numeric value still
 * produces no more than 16 digits at runtime. String-result arguments use the same byte-length rule in both versions.
 * The old fixed VARCHAR(2000) return type truncated metadata for wide character, binary, JSON and temporal inputs even
 * when execution returned the complete value.
 */
public class SqlHexFunction extends SqlFunction {
    private static final int NUMERIC_RESULT_LENGTH = 16;
    private static final int LEGACY_FALLBACK_LENGTH = 2000;

    public SqlHexFunction() {
        super("HEX",
            SqlKind.OTHER_FUNCTION,
            SqlHexFunction::inferHexReturnType,
            InferTypes.FIRST_KNOWN,
            OperandTypes.ANY,
            SqlFunctionCategory.NUMERIC);
    }

    private static RelDataType inferHexReturnType(SqlOperatorBinding opBinding) {
        RelDataType operandType = opBinding.getOperandType(0);
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        int resultLength;
        if (InstanceVersion.isMYSQL80() && isNumericResult(operandType)) {
            resultLength = NUMERIC_RESULT_LENGTH;
        } else if (isNumericResult(operandType)) {
            resultLength = inferMysql57NumericResultLength(operandType, typeFactory);
        } else {
            resultLength = inferStringResultLength(operandType, typeFactory);
        }
        RelDataType resultType = typeFactory.createSqlType(SqlTypeName.VARCHAR, resultLength);
        return typeFactory.createTypeWithNullability(resultType, operandType.isNullable());
    }

    private static boolean isNumericResult(RelDataType operandType) {
        SqlTypeName typeName = operandType.getSqlTypeName();
        return SqlTypeUtil.isNumeric(operandType)
            || typeName == SqlTypeName.BOOLEAN
            || typeName == SqlTypeName.BIT
            || typeName == SqlTypeName.BIG_BIT;
    }

    /**
     * Approximate MySQL 5.7 Item::max_length from PolarDB-X's RelDataType and double it as Item_func_hex::fix_length_and_dec
     * does. The two type systems store integer width differently: types built from DDL use their storage bit width,
     * while Calcite-created types use decimal precision. Treat both representations as the native default. An explicit
     * legacy display width is otherwise preserved. DECIMAL adds room for a sign and, when applicable, a decimal point.
     * <p>
     * FLOAT and DOUBLE have MySQL 5.7 default display lengths 12 and 22. Calcite's generic default precision is 15 for
     * both, so it is normalized here. The result remains capped by PolarDB-X's maximum VARCHAR precision, just like the
     * string path, to keep planner metadata representable.
     */
    private static int inferMysql57NumericResultLength(RelDataType operandType, RelDataTypeFactory typeFactory) {
        SqlTypeName typeName = operandType.getSqlTypeName();
        int precision = operandType.getPrecision();
        long inputDisplayLength;
        switch (typeName) {
        case BOOLEAN:
            inputDisplayLength = 1;
            break;
        case TINYINT:
            inputDisplayLength = defaultOrExplicitWidth(precision, 8, 3, 4);
            break;
        case TINYINT_UNSIGNED:
            inputDisplayLength = defaultOrExplicitWidth(precision, 8, 3, 3);
            break;
        case SMALLINT:
            inputDisplayLength = defaultOrExplicitWidth(precision, 16, 5, 6);
            break;
        case SMALLINT_UNSIGNED:
            inputDisplayLength = defaultOrExplicitWidth(precision, 16, 5, 5);
            break;
        case MEDIUMINT:
            inputDisplayLength = defaultOrExplicitWidth(precision, 24, 7, 9);
            break;
        case MEDIUMINT_UNSIGNED:
            inputDisplayLength = defaultOrExplicitWidth(precision, 24, 7, 8);
            break;
        case INTEGER:
            inputDisplayLength = defaultOrExplicitWidth(precision, 32, 10, 11);
            break;
        case INTEGER_UNSIGNED:
            inputDisplayLength = defaultOrExplicitWidth(precision, 32, 10, 10);
            break;
        case BIGINT:
        case BIGINT_UNSIGNED:
        case SIGNED:
        case UNSIGNED:
            inputDisplayLength = defaultOrExplicitWidth(precision, 64, 19, 20);
            break;
        case YEAR:
            inputDisplayLength = 4;
            break;
        case DECIMAL:
            inputDisplayLength = specifiedPrecisionOr(operandType, 10) + 1L;
            if (operandType.getScale() > 0) {
                inputDisplayLength++;
            }
            break;
        case FLOAT:
            inputDisplayLength = precision == 15 || precision == RelDataType.PRECISION_NOT_SPECIFIED ? 12 : precision;
            break;
        case REAL:
            inputDisplayLength = precision == 7 || precision == RelDataType.PRECISION_NOT_SPECIFIED ? 12 : precision;
            break;
        case DOUBLE:
            inputDisplayLength = precision == 15 || precision == RelDataType.PRECISION_NOT_SPECIFIED ? 22 : precision;
            break;
        case BIT:
        case BIG_BIT:
            inputDisplayLength = specifiedPrecisionOr(operandType, 1);
            break;
        default:
            inputDisplayLength = LEGACY_FALLBACK_LENGTH / 2;
            break;
        }
        int maxVarcharLength = typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.VARCHAR);
        return (int) Math.min(inputDisplayLength * 2L, maxVarcharLength);
    }

    private static int defaultOrExplicitWidth(int precision, int ddlDefaultPrecision, int calciteDefaultPrecision,
                                              int mysqlDefaultDisplayWidth) {
        if (precision == RelDataType.PRECISION_NOT_SPECIFIED
            || precision == ddlDefaultPrecision
            || precision == calciteDefaultPrecision) {
            return mysqlDefaultDisplayWidth;
        }
        return precision;
    }

    private static int inferStringResultLength(RelDataType operandType, RelDataTypeFactory typeFactory) {
        SqlTypeName typeName = operandType.getSqlTypeName();
        int maxVarcharLength = typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.VARCHAR);
        long inputByteLength;
        if ("VECTOR".equals(typeName.name())) {
            inputByteLength = specifiedPrecisionOr(operandType, maxVarcharLength);
            return (int) Math.min(inputByteLength * 2L, maxVarcharLength);
        }
        switch (typeName) {
        case CHAR:
        case VARCHAR:
        case BINARY_VARCHAR:
        case USER_SYMBOL:
            inputByteLength = characterByteLength(operandType, maxVarcharLength);
            break;
        case BINARY:
        case VARBINARY:
            inputByteLength = specifiedPrecisionOr(operandType, maxVarcharLength);
            break;
        case ENUM:
            inputByteLength = enumByteLength(operandType);
            break;
        case DATE:
            inputByteLength = MySQLTimeTypeUtil.MAX_DATE_WIDTH;
            break;
        case TIME:
        case TIME_WITH_LOCAL_TIME_ZONE:
            inputByteLength = temporalDisplayLength(operandType, MySQLTimeTypeUtil.MAX_TIME_WIDTH);
            break;
        case DATETIME:
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            inputByteLength = temporalDisplayLength(operandType, MySQLTimeTypeUtil.MAX_DATETIME_WIDTH);
            break;
        case BLOB:
        case JSON:
        case GEOMETRY:
            inputByteLength = maxVarcharLength;
            break;
        case NULL:
            inputByteLength = 0;
            break;
        default:
            /*
             * ANY is common for dynamic parameters and extension types. Retaining the old 2000-character capacity is
             * deliberately conservative: concrete types use the exact rules above, while unknown future types do not
             * suffer a metadata regression until they declare whether they expose numeric or string result semantics.
             */
            inputByteLength = LEGACY_FALLBACK_LENGTH / 2;
            break;
        }
        return (int) Math.min(inputByteLength * 2L, maxVarcharLength);
    }

    private static long characterByteLength(RelDataType operandType, int defaultLength) {
        long characterLength = specifiedPrecisionOr(operandType, defaultLength);
        CharsetName charsetName = CharsetName.defaultCharset();
        if (operandType.getCollation() != null) {
            CollationName collationName = CollationName.of(operandType.getCollation().getCollationName());
            if (collationName != null) {
                charsetName = CollationName.getCharsetOf(collationName.name());
            }
        } else if (operandType.getCharset() != null) {
            charsetName = CharsetName.of(operandType.getCharset());
        }
        return characterLength * charsetName.getMaxLen();
    }

    private static long enumByteLength(RelDataType operandType) {
        if (!(operandType instanceof EnumSqlType)) {
            return LEGACY_FALLBACK_LENGTH / 2;
        }
        int maxBytes = 0;
        for (String value : ((EnumSqlType) operandType).getStringValues()) {
            maxBytes = Math.max(maxBytes, value.getBytes(StandardCharsets.UTF_8).length);
        }
        return maxBytes;
    }

    /**
     * Return MySQL's maximum textual width for a temporal value.
     * <p>
     * Calcite represents the fractional-seconds precision of TIME, DATETIME and TIMESTAMP in {@link
     * RelDataType#getScale()}. BasicSqlType deliberately omits the separate precision field from the canonical type
     * digest for these types. Consequently, canonization may return a type whose precision came from an earlier
     * equivalent TIME(fsp) or DATETIME(fsp) instance. Using getPrecision() here would make HEX metadata depend on type
     * creation order: for example, TIME(3) could incorrectly reserve only 10 input characters instead of 14.
     * <p>
     * MySQL's display width is stable: the base value is HH:MM:SS (including the possible extended-hour sign) or
     * YYYY-MM-DD HH:MM:SS, followed by a decimal point and exactly fsp digits when fsp is positive. Clamp the scale to
     * MySQL's supported range so malformed extension types cannot inflate the VARCHAR result beyond native semantics.
     */
    private static int temporalDisplayLength(RelDataType operandType, int baseDisplayLength) {
        int fractionalSecondScale = Math.max(0,
            Math.min(operandType.getScale(), MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE));
        return fractionalSecondScale == 0 ? baseDisplayLength : baseDisplayLength + 1 + fractionalSecondScale;
    }

    private static int specifiedPrecisionOr(RelDataType operandType, int defaultLength) {
        return operandType.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
            ? defaultLength
            : operandType.getPrecision();
    }
}
