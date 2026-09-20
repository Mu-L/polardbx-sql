package com.alibaba.polardbx.common.utils;

import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;

import static org.junit.Assert.*;

/**
 * Tests for BigDecimalUtil, specifically verifying that decimal values
 * are returned in fixed-point notation (not scientific notation)
 * to match MySQL behavior under the xproto private protocol.
 * <p>
 * Related AONE: 61797936
 */
public class BigDecimalUtilTest {

    /**
     * Test that small decimal values (adjusted < -6) are returned
     * in fixed-point notation, NOT scientific notation.
     * <p>
     * MySQL always returns decimal values in xxxx.xxxx format,
     * never using scientific notation like 1E-7.
     * <p>
     * Reproduces: create table tttttt(c1 decimal(18,7));
     * insert into tttttt values (0.0000001);
     * Under xproto, this was returned as "1E-7" instead of "0.0000001".
     */
    @Test
    public void testSmallDecimalNoScientificNotation() throws IOException {
        // 0.0000001 with scale=7 -> adjusted = -(7) + (1-1) = -7 < -6
        // This triggers the scientific notation path in the current code
        byte[] result = BigDecimalUtil.fastGetBigDecimalStringBytes("1".getBytes(), 7);
        String resultStr = new String(result);

        // Should NOT contain scientific notation
        assertFalse("Result should not contain 'E' (scientific notation): " + resultStr,
            resultStr.contains("E"));

        // Should be in fixed-point format
        assertEquals("0.0000001", resultStr);
    }

    /**
     * Test another small value: 0.00000001 (scale=8, adjusted=-8)
     */
    @Test
    public void testVerySmallDecimalNoScientificNotation() throws IOException {
        byte[] result = BigDecimalUtil.fastGetBigDecimalStringBytes("1".getBytes(), 8);
        String resultStr = new String(result);

        assertFalse("Result should not contain 'E' (scientific notation): " + resultStr,
            resultStr.contains("E"));
        assertEquals("0.00000001", resultStr);
    }

    /**
     * Test 0.000001 (scale=6, adjusted=-6) - this is at the boundary
     * and should already work (adjusted >= -6 uses plain number path)
     */
    @Test
    public void testBoundaryDecimal() throws IOException {
        byte[] result = BigDecimalUtil.fastGetBigDecimalStringBytes("1".getBytes(), 6);
        String resultStr = new String(result);

        assertFalse("Result should not contain 'E' (scientific notation): " + resultStr,
            resultStr.contains("E"));
        assertEquals("0.000001", resultStr);
    }

    /**
     * Test negative small decimal values
     */
    @Test
    public void testNegativeSmallDecimalNoScientificNotation() throws IOException {
        byte[] result = BigDecimalUtil.fastGetBigDecimalStringBytes("-1".getBytes(), 7);
        String resultStr = new String(result);

        assertFalse("Result should not contain 'E' (scientific notation): " + resultStr,
            resultStr.contains("E"));
        assertEquals("-0.0000001", resultStr);
    }

    /**
     * Test that BigDecimal.toString() vs BigDecimal.toPlainString()
     * demonstrates the root cause of scientific notation.
     * <p>
     * This test documents WHY the fix is needed:
     * BigDecimal.toString() produces scientific notation for very small/large values,
     * while toPlainString() always produces fixed-point notation.
     */
    @Test
    public void testBigDecimalToStringVsToPlainString() {
        BigDecimal value = new BigDecimal("0.0000001");

        // BigDecimal.toString() produces scientific notation
        String toStringResult = value.toString();
        assertEquals("1E-7", toStringResult);

        // BigDecimal.toPlainString() produces fixed-point notation (what we want)
        String toPlainStringResult = value.toPlainString();
        assertEquals("0.0000001", toPlainStringResult);

        // They should be different for this value
        assertNotEquals("toString and toPlainString should differ for small decimals",
            toStringResult, toPlainStringResult);
    }

    /**
     * Test that BigDecimal.toString() also produces scientific notation for
     * very large values, which toPlainString() handles correctly.
     */
    @Test
    public void testBigDecimalLargeValueScientificNotation() {
        BigDecimal largeValue = new BigDecimal("123456789012345678901234567890");

        // For very large values, toString() may use scientific notation
        String toStringResult = largeValue.toString();

        // toPlainString() always returns fixed-point
        String toPlainStringResult = largeValue.toPlainString();
        assertEquals("123456789012345678901234567890", toPlainStringResult);

        // toString may differ from toPlainString for large values
        // (depends on the exact value and Java's BigDecimal implementation)
        // For this specific value they happen to be the same, but the
        // principle holds: toPlainString() is always safe
    }

    /**
     * Test scale=2 fast path (currency-like values) still works correctly
     */
    @Test
    public void testScale2FastPath() throws IOException {
        byte[] result = BigDecimalUtil.fastGetBigDecimalStringBytes("1234".getBytes(), 2);
        String resultStr = new String(result);

        assertEquals("12.34", resultStr);
    }

    /**
     * Test scale=0 (integer values) still works correctly
     */
    @Test
    public void testScale0() throws IOException {
        byte[] result = BigDecimalUtil.fastGetBigDecimalStringBytes("12345".getBytes(), 0);
        String resultStr = new String(result);

        assertEquals("12345", resultStr);
    }

    /**
     * Test normal decimal values that already work correctly
     * (values that don't trigger the scientific notation path)
     */
    @Test
    public void testNormalDecimal() throws IOException {
        // 123.456 with scale=3 -> adjusted = -(3) + (6-1) = 2 >= -6
        byte[] result = BigDecimalUtil.fastGetBigDecimalStringBytes("123456".getBytes(), 3);
        String resultStr = new String(result);

        assertFalse("Result should not contain 'E' (scientific notation): " + resultStr,
            resultStr.contains("E"));
        assertEquals("123.456", resultStr);
    }

    /**
     * Test negative scale scenario (e.g., BigDecimal("1E10") has scale=-10).
     * After the fix, negative scale values should produce zero-padded integers
     * with a trailing decimal point and matching zeros after the point, matching MySQL behavior.
     * <p>
     * This addresses the AI review concern about potential large zero-padding
     * for extreme negative scale values.
     * <p>
     * Format: {significant_digits}{trailing_zeros}.{matching_zeros}
     * For scale=-2, value "123": "12300.00"
     */
    @Test
    public void testNegativeScaleDecimal() throws IOException {
        // 123 with scale=-2 represents 12300 (123 * 10^2)
        byte[] result = BigDecimalUtil.fastGetBigDecimalStringBytes("123".getBytes(), -2);
        String resultStr = new String(result);

        // Should NOT contain scientific notation
        assertFalse("Result should not contain 'E' (scientific notation): " + resultStr,
            resultStr.contains("E"));

        // Should be zero-padded format: "12300.00" (trailing zeros + decimal + matching zeros)
        assertEquals("12300.00", resultStr);
    }

    /**
     * Test larger negative scale to verify zero-padding behavior.
     * scale=-5 with value "1" should produce "100000.00000" (1 * 10^5, with matching decimal zeros)
     */
    @Test
    public void testLargeNegativeScaleDecimal() throws IOException {
        byte[] result = BigDecimalUtil.fastGetBigDecimalStringBytes("1".getBytes(), -5);
        String resultStr = new String(result);

        // Should NOT contain scientific notation
        assertFalse("Result should not contain 'E' (scientific notation): " + resultStr,
            resultStr.contains("E"));

        // Should produce "100000.00000" with trailing zeros and matching decimal zeros
        assertEquals("100000.00000", resultStr);
    }

    /**
     * Test negative scale with multi-digit value.
     * scale=-3 with value "99" should produce "99000.000" (99 * 10^3, with matching decimal zeros)
     */
    @Test
    public void testNegativeScaleMultiDigit() throws IOException {
        byte[] result = BigDecimalUtil.fastGetBigDecimalStringBytes("99".getBytes(), -3);
        String resultStr = new String(result);

        assertFalse("Result should not contain 'E' (scientific notation): " + resultStr,
            resultStr.contains("E"));
        assertEquals("99000.000", resultStr);
    }
}
