package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.core.datatype.VectorType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Types;
import java.util.Arrays;

/**
 * Unit tests for all methods of {@link VectorType}.
 * Test methods are pure unit tests; the class-level guard only verifies that the configured DN is MySQL 8.0.
 * <p>
 * Expected values for formatFloat / floatArrayToText are derived from DN's
 * vec_totext output (MySQL my_gcvt with MY_GCVT_MAX_FIELD_WIDTH=12 for float).
 */
public class VectorTypeTest {

    private static final float FLOAT_DELTA = 1e-6f;

    @BeforeClass
    public static void requireMysql80Dn() {
        VectorIndexTestBase.assumeMysql80Dn();
    }

    // ========== Constructor & getDimension ==========

    @Test
    public void testDefaultConstructor() {
        VectorType vt = new VectorType();
        Assert.assertEquals(0, vt.getDimension());
    }

    @Test
    public void testConstructorWithDimension() {
        VectorType vt = new VectorType(128);
        Assert.assertEquals(128, vt.getDimension());
    }

    @Test
    public void testConstructorWithZeroDimension() {
        VectorType vt = new VectorType(0);
        Assert.assertEquals(0, vt.getDimension());
    }

    // ========== getSqlType ==========

    @Test
    public void testGetSqlType() {
        Assert.assertEquals(Types.VARBINARY, new VectorType().getSqlType());
    }

    // ========== getStringSqlType ==========

    @Test
    public void testGetStringSqlTypeNoDimension() {
        Assert.assertEquals("VECTOR", new VectorType(0).getStringSqlType());
    }

    @Test
    public void testGetStringSqlTypeWithDimension() {
        Assert.assertEquals("VECTOR", new VectorType(4).getStringSqlType());
    }

    @Test
    public void testGetStringSqlTypeLargeDimension() {
        Assert.assertEquals("VECTOR", new VectorType(2048).getStringSqlType());
    }

    // ========== getDataClass ==========

    @Test
    public void testGetDataClass() {
        Assert.assertEquals(byte[].class, new VectorType().getDataClass());
    }

    // ========== fieldType ==========

    @Test
    public void testFieldType() {
        Assert.assertEquals(MySQLStandardFieldType.MYSQL_TYPE_BLOB, new VectorType().fieldType());
    }

    // ========== NotSupportException methods ==========

    @Test(expected = NotSupportException.class)
    public void testGetMaxValueThrows() {
        new VectorType().getMaxValue();
    }

    @Test(expected = NotSupportException.class)
    public void testGetMinValueThrows() {
        new VectorType().getMinValue();
    }

    @Test(expected = NotSupportException.class)
    public void testGetCalculatorThrows() {
        new VectorType().getCalculator();
    }

    @Test(expected = NotSupportException.class)
    public void testCompareThrows() {
        new VectorType().compare(new byte[4], new byte[4]);
    }

    // ========== convertFrom ==========

    @Test
    public void testConvertFromNull() {
        Assert.assertNull(new VectorType().convertFrom(null));
    }

    @Test
    public void testConvertFromByteArray() {
        byte[] input = new byte[] {1, 2, 3, 4};
        Assert.assertSame(input, new VectorType().convertFrom(input));
    }

    @Test
    public void testConvertFromString() {
        byte[] result = new VectorType().convertFrom("[1.0, 2.0, 3.0]");
        Assert.assertEquals(12, result.length);
        float[] floats = VectorType.toFloatArray(result);
        Assert.assertEquals(1.0f, floats[0], FLOAT_DELTA);
        Assert.assertEquals(2.0f, floats[1], FLOAT_DELTA);
        Assert.assertEquals(3.0f, floats[2], FLOAT_DELTA);
    }

    @Test
    public void testConvertFromFloatArray() {
        float[] input = {1.0f, 2.0f, 3.0f};
        byte[] result = new VectorType().convertFrom(input);
        Assert.assertEquals(12, result.length);
        Assert.assertArrayEquals(input, VectorType.toFloatArray(result), FLOAT_DELTA);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testConvertFromUnsupportedTypeInt() {
        new VectorType().convertFrom(123);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testConvertFromUnsupportedTypeDouble() {
        new VectorType().convertFrom(1.0);
    }

    // ========== getResultGetter ==========

    @Test
    public void testGetResultGetterNotNull() {
        Assert.assertNotNull(new VectorType().getResultGetter());
    }

    // ========== toFloatArray (static) ==========

    @Test
    public void testToFloatArrayNull() {
        Assert.assertNull(VectorType.toFloatArray(null));
    }

    @Test
    public void testToFloatArrayEmpty() {
        Assert.assertEquals(0, VectorType.toFloatArray(new byte[0]).length);
    }

    @Test
    public void testToFloatArrayBasic() {
        ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buf.putFloat(1.0f);
        buf.putFloat(-2.5f);
        float[] result = VectorType.toFloatArray(buf.array());
        Assert.assertEquals(2, result.length);
        Assert.assertEquals(1.0f, result[0], FLOAT_DELTA);
        Assert.assertEquals(-2.5f, result[1], FLOAT_DELTA);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testToFloatArrayInvalidLength3() {
        VectorType.toFloatArray(new byte[3]);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testToFloatArrayInvalidLength5() {
        VectorType.toFloatArray(new byte[5]);
    }

    // ========== fromFloatArray (static) ==========

    @Test
    public void testFromFloatArrayNull() {
        Assert.assertNull(VectorType.fromFloatArray(null));
    }

    @Test
    public void testFromFloatArrayEmpty() {
        Assert.assertEquals(0, VectorType.fromFloatArray(new float[0]).length);
    }

    @Test
    public void testFromFloatArrayBasic() {
        float[] input = {1.0f, 2.0f, 3.0f, 4.0f};
        byte[] result = VectorType.fromFloatArray(input);
        Assert.assertEquals(16, result.length);
        ByteBuffer buf = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        for (float v : input) {
            Assert.assertEquals(v, buf.getFloat(), FLOAT_DELTA);
        }
    }

    @Test
    public void testFromFloatArrayRoundtrip() {
        float[] input = {0.1f, -0.2f, 3.14159f, 100.0f, 0.0f};
        Assert.assertArrayEquals(input, VectorType.toFloatArray(VectorType.fromFloatArray(input)), FLOAT_DELTA);
    }

    // ========== textToVector (static) ==========

    @Test
    public void testTextToVectorNull() {
        Assert.assertNull(VectorType.textToVector(null));
    }

    @Test(expected = TddlRuntimeException.class)
    public void testTextToVectorEmpty() {
        VectorType.textToVector("");
    }

    @Test
    public void testTextToVectorBasic() {
        float[] floats = VectorType.toFloatArray(VectorType.textToVector("[1.0, 2.0, 3.0]"));
        Assert.assertEquals(3, floats.length);
        Assert.assertEquals(1.0f, floats[0], FLOAT_DELTA);
        Assert.assertEquals(2.0f, floats[1], FLOAT_DELTA);
        Assert.assertEquals(3.0f, floats[2], FLOAT_DELTA);
    }

    // ========== vectorToText (static) ==========

    @Test
    public void testVectorToTextNull() {
        Assert.assertNull(VectorType.vectorToText(null));
    }

    @Test
    public void testVectorToTextEmpty() {
        Assert.assertEquals("[]", VectorType.vectorToText(VectorType.fromFloatArray(new float[0])));
    }

    @Test
    public void testVectorToTextRoundtrip() {
        byte[] binary = VectorType.fromFloatArray(new float[] {1.0f, 2.0f, 3.0f});
        String text = VectorType.vectorToText(binary);
        float[] back = VectorType.parseVectorText(text);
        Assert.assertEquals(1.0f, back[0], FLOAT_DELTA);
        Assert.assertEquals(2.0f, back[1], FLOAT_DELTA);
        Assert.assertEquals(3.0f, back[2], FLOAT_DELTA);
    }

    // ========== parseVectorText (static) ==========

    @Test
    public void testParseVectorTextNull() {
        Assert.assertNull(VectorType.parseVectorText(null));
    }

    @Test(expected = TddlRuntimeException.class)
    public void testParseVectorTextEmpty() {
        VectorType.parseVectorText("");
    }

    @Test
    public void testParseVectorTextEmptyBrackets() {
        Assert.assertEquals(0, VectorType.parseVectorText("[]").length);
    }

    @Test
    public void testParseVectorTextBasic() {
        float[] r = VectorType.parseVectorText("[1.0, 2.0, 3.0]");
        Assert.assertEquals(3, r.length);
        Assert.assertEquals(1.0f, r[0], FLOAT_DELTA);
        Assert.assertEquals(2.0f, r[1], FLOAT_DELTA);
        Assert.assertEquals(3.0f, r[2], FLOAT_DELTA);
    }

    @Test
    public void testParseVectorTextNoSpaces() {
        float[] r = VectorType.parseVectorText("[1,2,3]");
        Assert.assertArrayEquals(new float[] {1, 2, 3}, r, FLOAT_DELTA);
    }

    @Test
    public void testParseVectorTextExtraSpaces() {
        float[] r = VectorType.parseVectorText("  [ 1.0 ,  2.0 ,  3.0 ]  ");
        Assert.assertArrayEquals(new float[] {1, 2, 3}, r, FLOAT_DELTA);
    }

    @Test
    public void testParseVectorTextNegativeValues() {
        float[] r = VectorType.parseVectorText("[-1.5, 0, 2.5]");
        Assert.assertEquals(-1.5f, r[0], FLOAT_DELTA);
        Assert.assertEquals(0.0f, r[1], FLOAT_DELTA);
        Assert.assertEquals(2.5f, r[2], FLOAT_DELTA);
    }

    @Test
    public void testParseVectorTextSingleElement() {
        float[] r = VectorType.parseVectorText("[42.0]");
        Assert.assertEquals(1, r.length);
        Assert.assertEquals(42.0f, r[0], FLOAT_DELTA);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testParseVectorTextNoBrackets() {
        VectorType.parseVectorText("1.0, 2.0");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testParseVectorTextMissingOpen() {
        VectorType.parseVectorText("1.0, 2.0]");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testParseVectorTextMissingClose() {
        VectorType.parseVectorText("[1.0, 2.0");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testParseVectorTextEmptyElement() {
        VectorType.parseVectorText("[1.0,,2.0]");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testParseVectorTextInvalidNumber() {
        VectorType.parseVectorText("[1.0, abc, 3.0]");
    }

    // ==========================================================================
    // floatArrayToText — expected values from DN vec_totext output.
    //
    // DN uses MySQL my_gcvt with:
    //   FLT_DIG = 6 (significant digits for float)
    //   MY_GCVT_MAX_FIELD_WIDTH = 12 (max output chars; uses scientific notation if exceeded)
    // ==========================================================================

    @Test
    public void testFloatArrayToTextNull() {
        Assert.assertNull(VectorType.floatArrayToText(null));
    }

    @Test
    public void testFloatArrayToTextEmpty() {
        Assert.assertEquals("[]", VectorType.floatArrayToText(new float[0]));
    }

    // --- DN id=1: [0,0,0,0] ---
    @Test
    public void testFloatArrayToTextZeros() {
        Assert.assertEquals("[0,0,0,0]",
            VectorType.floatArrayToText(new float[] {0, 0, 0, 0}));
    }

    // --- DN id=2: [1,2,3,4] ---
    @Test
    public void testFloatArrayToTextIntegers() {
        Assert.assertEquals("[1,2,3,4]",
            VectorType.floatArrayToText(new float[] {1, 2, 3, 4}));
    }

    // --- DN id=3: [-1,-2,-3,-4] ---
    @Test
    public void testFloatArrayToTextNegativeIntegers() {
        Assert.assertEquals("[-1,-2,-3,-4]",
            VectorType.floatArrayToText(new float[] {-1, -2, -3, -4}));
    }

    // --- DN id=4: [0.5,1.5,-0.25,0.123456] ---
    @Test
    public void testFloatArrayToTextDecimals() {
        Assert.assertEquals("[0.5,1.5,-0.25,0.123456]",
            VectorType.floatArrayToText(new float[] {0.5f, 1.5f, -0.25f, 0.123456f}));
    }

    // --- DN id=5: [10000000000,10000000000,10000000000,110000000000] ---
    @Test
    public void testFloatArrayToText_1e10() {
        Assert.assertEquals("[10000000000,10000000000,10000000000,110000000000]",
            VectorType.floatArrayToText(new float[] {1e10f, 1e10f, 1e10f, 11e10f}));
    }

    // --- DN id=6: [0.0000000001,0.0000000002,0.0000000003,0.0000000004] ---
    @Test
    public void testFloatArrayToText_1eNeg10() {
        Assert.assertEquals("[0.0000000001,0.0000000002,0.0000000003,0.0000000004]",
            VectorType.floatArrayToText(new float[] {1e-10f, 2e-10f, 3e-10f, 4e-10f}));
    }

    // --- DN id=7: [1e15,2e15,3e15,4e15] ---
    @Test
    public void testFloatArrayToText_1e15() {
        Assert.assertEquals("[1e15,2e15,3e15,4e15]",
            VectorType.floatArrayToText(new float[] {1e15f, 2e15f, 3e15f, 4e15f}));
    }

    // --- DN id=8: [1e-15,2e-15,3e-15,4e-15] ---
    @Test
    public void testFloatArrayToText_1eNeg15() {
        Assert.assertEquals("[1e-15,2e-15,3e-15,4e-15]",
            VectorType.floatArrayToText(new float[] {1e-15f, 2e-15f, 3e-15f, 4e-15f}));
    }

    // --- DN id=12: [3.14159,2.71828,1.41421,1.73205] ---
    @Test
    public void testFloatArrayToTextMathConstants() {
        Assert.assertEquals("[3.14159,2.71828,1.41421,1.73205]",
            VectorType.floatArrayToText(new float[] {3.14159f, 2.71828f, 1.41421f, 1.73205f}));
    }

    // --- DN id=13: [0.1,0.01,0.001,0.0001] ---
    @Test
    public void testFloatArrayToTextSmallDecimals() {
        Assert.assertEquals("[0.1,0.01,0.001,0.0001]",
            VectorType.floatArrayToText(new float[] {0.1f, 0.01f, 0.001f, 0.0001f}));
    }

    // --- DN id=14: [100000,1000000,10000000,100000000] ---
    @Test
    public void testFloatArrayToTextMediumIntegers() {
        Assert.assertEquals("[100000,1000000,10000000,100000000]",
            VectorType.floatArrayToText(new float[] {1e5f, 1e6f, 1e7f, 1e8f}));
    }

    // --- DN id=16: [0.123457,-0.123457,0.000123457,-0.000123457] ---
    @Test
    public void testFloatArrayToTextPrecision() {
        Assert.assertEquals("[0.123457,-0.123457,0.000123457,-0.000123457]",
            VectorType.floatArrayToText(new float[] {0.123457f, -0.123457f, 0.000123457f, -0.000123457f}));
    }

    // --- DN id=17: [1.00001,1.0001,1.001,1.01] ---
    @Test
    public void testFloatArrayToTextNearOne() {
        Assert.assertEquals("[1.00001,1.0001,1.001,1.01]",
            VectorType.floatArrayToText(new float[] {1.00001f, 1.0001f, 1.001f, 1.01f}));
    }

    // --- DN id=18: [0.00001,0.000001,0.0000001,0.00000001] ---
    @Test
    public void testFloatArrayToText_1eNeg5to8() {
        Assert.assertEquals("[0.00001,0.000001,0.0000001,0.00000001]",
            VectorType.floatArrayToText(new float[] {1e-5f, 1e-6f, 1e-7f, 1e-8f}));
    }

    // --- DN id=20: [15000000000,0.000025,-35000000000,-0.000045] ---
    @Test
    public void testFloatArrayToTextMixed() {
        Assert.assertEquals("[15000000000,0.000025,-35000000000,-0.000045]",
            VectorType.floatArrayToText(new float[] {1.5e10f, 2.5e-5f, -3.5e10f, -4.5e-5f}));
    }

    // --- DN id=23: [99999900000,1.2345e13,-0.00987654,5.55555e-8] ---
    @Test
    public void testFloatArrayToTextMixedScientific() {
        Assert.assertEquals("[99999900000,1.2345e13,-0.00987654,5.55555e-8]",
            VectorType.floatArrayToText(new float[] {9.99999e10f, 1.2345e13f, -9.87654e-3f, 5.55555e-8f}));
    }

    // --- DN id=24: [1e-14,1e14,-1e-14,-1e14] ---
    @Test
    public void testFloatArrayToText_1e14Boundary() {
        Assert.assertEquals("[1e-14,1e14,-1e-14,-1e14]",
            VectorType.floatArrayToText(new float[] {1e-14f, 1e14f, -1e-14f, -1e14f}));
    }

    // ========== MY_GCVT_MAX_FIELD_WIDTH=12 boundary tests ==========

    // --- DN id=30: [10000000000,100000000000,1e12,1e13] ---
    @Test
    public void testFieldWidthBoundaryPositive() {
        Assert.assertEquals("[10000000000,100000000000,1e12,1e13]",
            VectorType.floatArrayToText(new float[] {1e10f, 1e11f, 1e12f, 1e13f}));
    }

    // --- DN id=50: [100000000000,1e12,0.000000001,0.0000000001] ---
    @Test
    public void testFieldWidthBoundary11vs12() {
        Assert.assertEquals("[100000000000,1e12,0.000000001,0.0000000001]",
            VectorType.floatArrayToText(new float[] {1e11f, 1e12f, 1e-9f, 1e-10f}));
    }

    // --- DN id=52: [-1e11,-1e12,-0.000000001,-1e-10] ---
    @Test
    public void testFieldWidthBoundaryNegative() {
        Assert.assertEquals("[-1e11,-1e12,-0.000000001,-1e-10]",
            VectorType.floatArrayToText(new float[] {-1e11f, -1e12f, -1e-9f, -1e-10f}));
    }

    // --- DN id=40: [150000000000,15000000000,1.5e12,1.5e13] ---
    @Test
    public void testFieldWidthBoundaryWithMantissa() {
        Assert.assertEquals("[150000000000,15000000000,1.5e12,1.5e13]",
            VectorType.floatArrayToText(new float[] {1.5e11f, 1.5e10f, 1.5e12f, 1.5e13f}));
    }

    // --- DN id=42: [0.0000000015,1.5e-10,1.5e-11,1.5e-12] ---
    @Test
    public void testFieldWidthBoundaryNegativeExpWithMantissa() {
        Assert.assertEquals("[0.0000000015,1.5e-10,1.5e-11,1.5e-12]",
            VectorType.floatArrayToText(new float[] {1.5e-9f, 1.5e-10f, 1.5e-11f, 1.5e-12f}));
    }

    // --- DN id=43: [999999000000,100001000000,9.99999e-10,1.00001e-10] ---
    @Test
    public void testFieldWidthBoundaryMaxSigDigits() {
        Assert.assertEquals("[999999000000,100001000000,9.99999e-10,1.00001e-10]",
            VectorType.floatArrayToText(new float[] {9.99999e11f, 1.00001e11f, 9.99999e-10f, 1.00001e-10f}));
    }

    // --- DN id=51: [110000000000,1.1e12,0.0000000011,1.1e-10] ---
    @Test
    public void testFieldWidthBoundary_1_1eX() {
        Assert.assertEquals("[110000000000,1.1e12,0.0000000011,1.1e-10]",
            VectorType.floatArrayToText(new float[] {1.1e11f, 1.1e12f, 1.1e-9f, 1.1e-10f}));
    }

    // --- DN id=53: [-1.1e11,-1.1e12,-1.1e-9,-1.1e-10] ---
    @Test
    public void testFieldWidthBoundaryNegativeWithMantissa() {
        Assert.assertEquals("[-1.1e11,-1.1e12,-1.1e-9,-1.1e-10]",
            VectorType.floatArrayToText(new float[] {-1.1e11f, -1.1e12f, -1.1e-9f, -1.1e-10f}));
    }

    // ========== Special float values in binary roundtrip ==========

    @Test(expected = TddlRuntimeException.class)
    public void testSpecialFloatBinaryRoundtrip() {
        float[] input = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0.0f};
        VectorType.fromFloatArray(input);
    }

    @Test
    public void testSingleDimensionVector() {
        byte[] binary = VectorType.textToVector("[42.5]");
        Assert.assertEquals(4, binary.length);
        Assert.assertEquals(42.5f, VectorType.toFloatArray(binary)[0], FLOAT_DELTA);
    }

    @Test
    public void testLargeDimensionFromFloatArray() {
        int dim = 4096;
        float[] input = new float[dim];
        Arrays.fill(input, 1.0f);
        byte[] binary = VectorType.fromFloatArray(input);
        Assert.assertEquals(dim * 4, binary.length);
        for (float v : VectorType.toFloatArray(binary)) {
            Assert.assertEquals(1.0f, v, FLOAT_DELTA);
        }
    }

    // ========== Full text roundtrip ==========

    @Test
    public void testFullRoundtripIntegers() {
        Assert.assertEquals("[1,2,3,4]",
            VectorType.vectorToText(VectorType.textToVector("[1,2,3,4]")));
    }

    @Test
    public void testFullRoundtripDecimals() {
        float[] original = {0.5f, 1.5f, -2.5f};
        byte[] binary = VectorType.fromFloatArray(original);
        byte[] binary2 = VectorType.textToVector(VectorType.vectorToText(binary));
        Assert.assertArrayEquals(original, VectorType.toFloatArray(binary2), FLOAT_DELTA);
    }

    @Test
    public void testFullRoundtripHighDimension() {
        float[] original = new float[128];
        for (int i = 0; i < 128; i++) {
            original[i] = i * 0.1f;
        }
        Assert.assertArrayEquals(original,
            VectorType.toFloatArray(VectorType.fromFloatArray(original)), FLOAT_DELTA);
    }

    // ========== convertFrom type dispatch roundtrip ==========

    @Test
    public void testConvertFromStringRoundtrip() {
        byte[] binary = new VectorType(3).convertFrom("[10, 20, 30]");
        float[] floats = VectorType.toFloatArray(binary);
        Assert.assertEquals(10.0f, floats[0], FLOAT_DELTA);
        Assert.assertEquals(20.0f, floats[1], FLOAT_DELTA);
        Assert.assertEquals(30.0f, floats[2], FLOAT_DELTA);
    }

    @Test
    public void testConvertFromFloatArrayToText() {
        byte[] binary = new VectorType().convertFrom(new float[] {-1.0f, 0.0f, 1.0f});
        Assert.assertEquals("[-1,0,1]", VectorType.vectorToText(binary));
    }

    // ========== DN id=15: float precision loss edge case ==========
    // DN: vec_fromtext('[999999, 9999999, 99999990, 999999900]')
    // DN output: [999999,10000000,100000000,1000000000]
    // (9999999 -> 10000000 due to float32 precision: only 6-7 significant digits)
    @Test
    public void testFloatArrayToTextPrecisionLoss() {
        Assert.assertEquals("[999999,10000000,100000000,1000000000]",
            VectorType.floatArrayToText(new float[] {999999f, 9999999f, 99999990f, 999999900f}));
    }
}
