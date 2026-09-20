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
package com.alibaba.polardbx.optimizer.core.function.calc.scalar.string;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.BigBitType;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.BytesType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.datatype.StringType;
import com.alibaba.polardbx.optimizer.core.datatype.TimeType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;

public class HexTest {

    private static final byte[] UUID_BYTES = new byte[] {
        0x01, (byte) 0x9d, (byte) 0xcd, (byte) 0xb3,
        (byte) 0xfc, (byte) 0x87, 0x79, 0x1d,
        (byte) 0x9c, 0x03, (byte) 0x8a, 0x12,
        (byte) 0x85, (byte) 0xa9, 0x01, 0x26
    };
    private static final String UUID_HEX = "019DCDB3FC87791D9C038A1285A90126";

    @Test
    public void testFunctionName() {
        Assert.assertArrayEquals(new String[] {"HEX"}, new Hex().getFunctionNames());
    }

    @Test
    public void testBytesTypeOperandKeepsOriginalOctets() {
        Assert.assertEquals(UUID_HEX, computeHex(DataTypes.BytesType, UUID_BYTES));
    }

    @Test
    public void testFixedBinaryOperandKeepsOriginalOctets() {
        Assert.assertEquals(UUID_HEX, computeHex(new BinaryType(16), UUID_BYTES));
    }

    @Test
    public void testVarbinaryOperandKeepsOriginalOctets() {
        Assert.assertEquals(UUID_HEX, computeHex(DataTypes.BinaryType, UUID_BYTES));
    }

    @Test
    public void testGeometryBinaryRepresentationKeepsOriginalOctets() {
        byte[] wkb = new byte[] {0x01, 0x01, 0x00, 0x00, 0x00};
        Assert.assertEquals("0101000000", computeHex(DataTypes.BinaryType, wkb));
    }

    @Test
    public void testBinaryBoundaryBytesAndPaddingArePreserved() {
        byte[] bytes = new byte[] {0x00, 0x01, (byte) 0x80, (byte) 0xff, 0x00};
        Assert.assertEquals("000180FF00", computeHex(new BinaryType(bytes.length), bytes));
    }

    @Test
    public void testEmptyVarbinaryProducesEmptyHexString() {
        Assert.assertEquals("", computeHex(DataTypes.BinaryType, new byte[0]));
    }

    @Test
    public void testNullBinaryProducesNull() {
        Assert.assertNull(computeHex(new BinaryType(16), null));
    }

    @Test
    public void testFutureBytesTypeSubclassUsesOctetSemantics() {
        Assert.assertEquals(UUID_HEX, computeHex(new FutureBytesType(), UUID_BYTES));
    }

    @Test
    public void testOtherByteBackedTypesKeepTheirDedicatedBranch() {
        Assert.assertEquals(UUID_HEX, computeHex(DataTypes.VectorType, UUID_BYTES));
    }

    @Test
    public void testBlobAndClobUseTheirPhysicalStringRepresentations() throws Exception {
        Assert.assertEquals("00FF41", computeHex(DataTypes.BlobType,
            new SerialBlob(new byte[] {0x00, (byte) 0xff, 0x41})));
        Assert.assertEquals("41E4B8AD", computeHex(DataTypes.ClobType, new SerialClob("A中".toCharArray())));
    }

    @Test
    public void testCharacterOperandsUseDeclaredCharsetBytes() {
        Assert.assertEquals("E4B8AD", computeHex(DataTypes.VarcharType, "中"));
        Assert.assertEquals("E9", computeHex(
            new VarcharType(CharsetName.LATIN1, CollationName.LATIN1_SWEDISH_CI), "é"));
        Assert.assertEquals("E9", computeHex(
            new StringType(CharsetName.LATIN1, CollationName.LATIN1_SWEDISH_CI), "é"));
        Assert.assertEquals("E4B8AD", computeHex(DataTypes.SensitiveStringType, "中"));
    }

    @Test
    public void testBinaryCollationDoesNotTranscodeInvalidUtf8OrPadding() {
        byte[] value = new byte[] {(byte) 0xff, 0x00, 0x41};
        VarcharType binaryVarchar = new VarcharType(CharsetName.BINARY, CollationName.BINARY);
        Assert.assertEquals("FF0041", computeHex(binaryVarchar, Slices.wrappedBuffer(value)));
    }

    @Test
    public void testJsonAndEnumUseStringResultSemantics() {
        Assert.assertEquals("7B2261223A20317D", computeHex(DataTypes.JsonType, "{\"a\": 1}"));
        EnumType enumType = new EnumType(Arrays.asList("a", "ab"));
        Assert.assertEquals("6162", computeHex(enumType, new EnumValue(enumType, "ab")));
    }

    @Test
    public void testTemporalTypesUseScaleAwareDisplayString() {
        Assert.assertEquals("323032342D30312D3032", computeHex(DataTypes.DateType, "2024-01-02"));
        Assert.assertEquals("31323A33343A35362E373839",
            computeHex(new TimeType(3), "12:34:56.789"));
        Assert.assertEquals("323032342D30312D30322030333A30343A30352E363738",
            computeHex(new DateTimeType(3), "2024-01-02 03:04:05.678"));
        Assert.assertEquals("323032342D30312D30322030333A30343A30352E363738",
            computeHex(new TimestampType(3), "2024-01-02 03:04:05.678"));
    }

    @Test
    public void testSignedIntegralAndCompatibilityNumericTypes() {
        Assert.assertEquals("0", computeHex(DataTypes.IntegerType, 0));
        Assert.assertEquals("FF", computeHex(DataTypes.IntegerType, 255));
        Assert.assertEquals("FFFFFFFFFFFFFFFF", computeHex(DataTypes.LongType, -1L));
        Assert.assertEquals("8000000000000000", computeHex(DataTypes.LongType, Long.MIN_VALUE));
        Assert.assertEquals("7E8", computeHex(DataTypes.YearType, 2024L));
        Assert.assertEquals("1", computeHex(DataTypes.BooleanType, true));
        Assert.assertEquals("FFFFFFFFFFFFFFFF",
            computeHex(new BigBitType(), new BigInteger("18446744073709551615")));
    }

    @Test
    public void testUnsignedBigintKeepsFullMySqlDomain() {
        Assert.assertEquals("FFFFFFFFFFFFFFFF", computeHex(DataTypes.ULongType, UInt64.MAX_UINT64));
        Assert.assertEquals("8000000000000000",
            computeHex(DataTypes.ULongType, UInt64.fromString("9223372036854775808")));
    }

    @Test
    public void testMySql8DecimalUsesHalfUpRoundingAndSignedSaturation() {
        Assert.assertEquals("3", computeHexForVersion(true, DataTypes.DecimalType, Decimal.fromString("2.5")));
        Assert.assertEquals("FFFFFFFFFFFFFFFD",
            computeHexForVersion(true, DataTypes.DecimalType, Decimal.fromString("-2.5")));
        Assert.assertEquals("7FFFFFFFFFFFFFFF",
            computeHexForVersion(true, DataTypes.DecimalType, Decimal.fromString("10000000000000000000.0")));
        Assert.assertEquals("8000000000000000",
            computeHexForVersion(true, DataTypes.DecimalType, new BigDecimal("-10000000000000000000.0")));
    }

    @Test
    public void testRealUsesMySql8RintRoundingAndSignedSaturation() {
        Assert.assertEquals("2", computeHexForVersion(true, DataTypes.DoubleType, 2.5D));
        Assert.assertEquals("4", computeHexForVersion(true, DataTypes.DoubleType, 3.5D));
        Assert.assertEquals("FFFFFFFFFFFFFFFE", computeHexForVersion(true, DataTypes.DoubleType, -2.5D));
        Assert.assertEquals("7FFFFFFFFFFFFFFF",
            computeHexForVersion(true, DataTypes.DoubleType, Double.POSITIVE_INFINITY));
        Assert.assertEquals("8000000000000000",
            computeHexForVersion(true, DataTypes.DoubleType, Double.NEGATIVE_INFINITY));
    }

    @Test
    public void testMySql57RealRoundsHalfAwayFromZeroAndUsesUnsignedRange() {
        Assert.assertEquals("3", computeHexForVersion(false, DataTypes.DoubleType, 2.5D));
        Assert.assertEquals("4", computeHexForVersion(false, DataTypes.DoubleType, 3.5D));
        Assert.assertEquals("FFFFFFFFFFFFFFFD", computeHexForVersion(false, DataTypes.DoubleType, -2.5D));
        Assert.assertEquals("8000000000000000", computeHexForVersion(false, DataTypes.DoubleType, 0x1.0p63));
        Assert.assertEquals("FFFFFFFFFFFFF800",
            computeHexForVersion(false, DataTypes.DoubleType, Math.nextDown(0x1.0p64)));
        Assert.assertEquals("FFFFFFFFFFFFFFFF",
            computeHexForVersion(false, DataTypes.DoubleType, Double.POSITIVE_INFINITY));
        Assert.assertEquals("FFFFFFFFFFFFFFFF",
            computeHexForVersion(false, DataTypes.DoubleType, Double.NEGATIVE_INFINITY));
    }

    @Test
    public void testMySql57DecimalUsesDoublePrecisionAndUnsignedRange() {
        Assert.assertEquals("3",
            computeHexForVersion(false, DataTypes.DecimalType, Decimal.fromString("2.5")));
        Assert.assertEquals("FFFFFFFFFFFFFFFD",
            computeHexForVersion(false, DataTypes.DecimalType, Decimal.fromString("-2.5")));
        Assert.assertEquals("20000000000000",
            computeHexForVersion(false, DataTypes.DecimalType, Decimal.fromString("9007199254740993")));
        Assert.assertEquals("8AC7230489E80000",
            computeHexForVersion(false, DataTypes.DecimalType, new BigDecimal("10000000000000000000")));
        Assert.assertEquals("FFFFFFFFFFFFFFFF",
            computeHexForVersion(false, DataTypes.DecimalType, Decimal.fromString("-9223372036854775808")));
        Assert.assertEquals("FFFFFFFFFFFFFFFF",
            computeHexForVersion(false, DataTypes.DecimalType, Decimal.fromString("18446744073709551616")));
    }

    @Test
    public void testLegacyBigIntegerNumericValueUsesSignedSaturation() {
        Assert.assertEquals("FF", computeHex(DataTypes.LongType, BigInteger.valueOf(255)));
        Assert.assertEquals("7FFFFFFFFFFFFFFF",
            computeHex(DataTypes.LongType, new BigInteger("9223372036854775808")));
        Assert.assertEquals("8000000000000000",
            computeHex(DataTypes.LongType, new BigInteger("-9223372036854775809")));
        Assert.assertEquals("FF", computeHex(DataTypes.IntegerType, "255"));
    }

    private static Object computeHex(DataType dataType, Object value) {
        Hex target = new Hex();
        target.setOperandFields(Collections.singletonList(new OperandField(dataType)));
        return target.compute(new Object[] {value}, null);
    }

    private static Object computeHexForVersion(boolean mysql80, DataType dataType, Object value) {
        boolean originalVersion = InstanceVersion.isMYSQL80();
        try {
            InstanceVersion.setMYSQL80(mysql80);
            return computeHex(dataType, value);
        } finally {
            InstanceVersion.setMYSQL80(originalVersion);
        }
    }

    private static class OperandField extends Field {
        private final DataType dataType;

        private OperandField(DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public DataType getDataType() {
            return dataType;
        }
    }

    /**
     * Models a future byte-oriented data type added under the BytesType family. The test prevents an accidental return
     * to exact-class comparison, which would make such a type silently use HEX's numeric fallback.
     */
    private static class FutureBytesType extends BytesType {
    }
}
