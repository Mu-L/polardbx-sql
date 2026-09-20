package com.alibaba.polardbx.qatest.dql.sharding.functions;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Test case for AONE-80667120:
 * MD5 hash strings starting with '0e' (e.g., '0e45bc9382e3705be7814b0d985e9cb9')
 * should not be incorrectly parsed as scientific notation when used in modulo operations.
 * <p>
 * The bug was in NumberType.truncateIncorrectDoubleValue() where NUMBER_PATTERN
 * matched '0e45' from the MD5 string, producing BigDecimal("0E+45") whose
 * equals(BigDecimal.ZERO) returned false due to scale mismatch (-45 vs 0).
 * <p>
 * Fix: Use compareTo(BigDecimal.ZERO) == 0 instead of equals(BigDecimal.ZERO).
 */
public class Md5ModuloTest extends ReadBaseTestCase {

    /**
     * Test that MD5 hash strings starting with '0e' can be used in modulo operations
     * without throwing "not in column's value range" error.
     * <p>
     * The string '0e45bc9382e3705be7814b0d985e9cb9' looks like scientific notation '0e45'
     * but should be treated as a string that converts to numeric value 0.
     */
    @Test
    public void testMd5StringModulo() throws SQLException {
        String sql = "SELECT '0e45bc9382e3705be7814b0d985e9cb9' % 1";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            rs.next();
            BigDecimal result = rs.getBigDecimal(1);
            // 0 % 1 should return 0
            assert result != null && result.compareTo(BigDecimal.ZERO) == 0 :
                "Expected 0, but got: " + result;
        }
    }

    /**
     * Test another MD5 hash starting with '0e' to ensure the fix is general.
     */
    @Test
    public void testAnotherMd5StringModulo() throws SQLException {
        String sql = "SELECT '0e1234567890abcdef1234567890abcdef' % 1";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            rs.next();
            BigDecimal result = rs.getBigDecimal(1);
            assert result != null && result.compareTo(BigDecimal.ZERO) == 0 :
                "Expected 0, but got: " + result;
        }
    }

    /**
     * Test that normal numeric strings still work correctly.
     */
    @Test
    public void testNormalNumberModulo() throws SQLException {
        String sql = "SELECT '12345' % 10";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            rs.next();
            BigDecimal result = rs.getBigDecimal(1);
            assert result != null && result.compareTo(new BigDecimal("5")) == 0 :
                "Expected 5, but got: " + result;
        }
    }
}
