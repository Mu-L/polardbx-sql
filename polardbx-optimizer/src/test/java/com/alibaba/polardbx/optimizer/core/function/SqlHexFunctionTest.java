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
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.ExplicitOperatorBinding;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class SqlHexFunctionTest {
    private static final RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private static final SqlHexFunction OPERATOR = new SqlHexFunction();

    @Test
    public void testNumericAndBitReturnLengthIsAlwaysSixteen() {
        assertReturnLengthForVersion(true, 16, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
        assertReturnLengthForVersion(true, 16, TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 65, 30));
        assertReturnLengthForVersion(true, 16, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT_UNSIGNED));
        assertReturnLengthForVersion(true, 16, TYPE_FACTORY.createSqlType(SqlTypeName.BIG_BIT, 64));
        assertReturnLengthForVersion(true, 16, TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN));
    }

    @Test
    public void testMySql57NumericReturnLengthUsesInputDisplayWidth() {
        assertReturnLengthForVersion(false, 8, TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT));
        assertReturnLengthForVersion(false, 6, TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT_UNSIGNED));
        assertReturnLengthForVersion(false, 12, TYPE_FACTORY.createSqlType(SqlTypeName.SMALLINT));
        assertReturnLengthForVersion(false, 10, TYPE_FACTORY.createSqlType(SqlTypeName.SMALLINT_UNSIGNED));
        assertReturnLengthForVersion(false, 18, TYPE_FACTORY.createSqlType(SqlTypeName.MEDIUMINT));
        assertReturnLengthForVersion(false, 16, TYPE_FACTORY.createSqlType(SqlTypeName.MEDIUMINT_UNSIGNED));
        assertReturnLengthForVersion(false, 22, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
        assertReturnLengthForVersion(false, 20, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER_UNSIGNED));
        assertReturnLengthForVersion(false, 40, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT));
        assertReturnLengthForVersion(false, 40, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT_UNSIGNED));
        assertReturnLengthForVersion(false, 8, TYPE_FACTORY.createSqlType(SqlTypeName.YEAR));
        assertReturnLengthForVersion(false, 24, TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 10, 2));
        assertReturnLengthForVersion(false, 24, TYPE_FACTORY.createSqlType(SqlTypeName.FLOAT));
        assertReturnLengthForVersion(false, 24, TYPE_FACTORY.createSqlType(SqlTypeName.REAL));
        assertReturnLengthForVersion(false, 44, TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE));
        assertReturnLengthForVersion(false, 128, TYPE_FACTORY.createSqlType(SqlTypeName.BIG_BIT, 64));
        assertReturnLengthForVersion(false, 2, TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN));
        assertReturnLengthForVersion(false, 4, TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT, 2));
    }

    @Test
    public void testCharacterReturnLengthUsesMaximumEncodedByteLength() {
        RelDataType utf8mb4 = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 10);
        utf8mb4 = TYPE_FACTORY.createTypeWithCharsetAndCollation(
            utf8mb4,
            CharsetName.UTF8MB4.toJavaCharset(),
            new SqlCollation(
                CharsetName.UTF8MB4.toJavaCharset(),
                CollationName.UTF8MB4_GENERAL_CI.name(),
                SqlCollation.Coercibility.IMPLICIT));
        assertReturnLength(80, utf8mb4);

        assertReturnLength(32, TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY, 16));
    }

    @Test
    public void testEnumAndTemporalReturnLengthsFollowDisplayValues() {
        assertReturnLength(12,
            TYPE_FACTORY.createEnumSqlType(SqlTypeName.ENUM, Arrays.asList("a", "中文")));
        assertReturnLength(20, TYPE_FACTORY.createSqlType(SqlTypeName.DATE));

        assertReturnLength(20, TYPE_FACTORY.createSqlType(SqlTypeName.TIME, 10, 0));
        assertReturnLength(28, TYPE_FACTORY.createSqlType(SqlTypeName.TIME, 10, 3));
        assertReturnLength(34, TYPE_FACTORY.createSqlType(SqlTypeName.TIME, 10, 6));

        assertReturnLength(38, TYPE_FACTORY.createSqlType(SqlTypeName.DATETIME, 19, 0));
        assertReturnLength(46, TYPE_FACTORY.createSqlType(SqlTypeName.DATETIME, 19, 3));
        assertReturnLength(52, TYPE_FACTORY.createSqlType(SqlTypeName.DATETIME, 19, 6));

        // Temporal precision is intentionally absent from BasicSqlType's canonical digest. These equivalent TIME(3)
        // and DATETIME(3) instances may therefore retain the base precision of the object created above. HEX metadata
        // must use scale rather than this order-dependent precision.
        assertReturnLength(28, TYPE_FACTORY.createSqlType(SqlTypeName.TIME, 14, 3));
        assertReturnLength(46, TYPE_FACTORY.createSqlType(SqlTypeName.DATETIME, 23, 3));
    }

    @Test
    public void testWideAndUnknownInputsHaveSafeMetadataCapacity() {
        assertReturnLength(65536, TYPE_FACTORY.createSqlType(SqlTypeName.JSON));
        assertReturnLength(65536, TYPE_FACTORY.createSqlType(SqlTypeName.BLOB));
        assertReturnLength(2000, TYPE_FACTORY.createSqlType(SqlTypeName.ANY));
        assertReturnLength(0, TYPE_FACTORY.createSqlType(SqlTypeName.NULL));
    }

    @Test
    public void testNullabilityFollowsOperand() {
        RelDataType notNull = TYPE_FACTORY.createTypeWithNullability(
            TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), false);
        Assert.assertFalse(inferReturnType(notNull).isNullable());
        Assert.assertTrue(inferReturnType(TYPE_FACTORY.createTypeWithNullability(notNull, true)).isNullable());
    }

    private static void assertReturnLength(int expected, RelDataType operandType) {
        RelDataType resultType = inferReturnType(operandType);
        Assert.assertEquals(SqlTypeName.VARCHAR, resultType.getSqlTypeName());
        Assert.assertEquals(expected, resultType.getPrecision());
    }

    private static void assertReturnLengthForVersion(boolean mysql80, int expected, RelDataType operandType) {
        boolean originalVersion = InstanceVersion.isMYSQL80();
        try {
            InstanceVersion.setMYSQL80(mysql80);
            assertReturnLength(expected, operandType);
        } finally {
            InstanceVersion.setMYSQL80(originalVersion);
        }
    }

    private static RelDataType inferReturnType(RelDataType operandType) {
        return OPERATOR.inferReturnType(new ExplicitOperatorBinding(
            TYPE_FACTORY,
            OPERATOR,
            Collections.singletonList(operandType)));
    }
}
