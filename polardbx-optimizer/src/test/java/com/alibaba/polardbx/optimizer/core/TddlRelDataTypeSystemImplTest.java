package com.alibaba.polardbx.optimizer.core;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for TddlRelDataTypeSystemImpl, focusing on deriveAvgAggType()
 * which determines the DECIMAL precision and scale for AVG aggregate results.
 * <p>
 * Bug reference: AONE-82183457 - env80 hash_window_simple avg precision baseline mismatch.
 * The key insight: deriveAvgAggType() computes scale = max(6, arg_scale + arg_precision + 1),
 * which for integer columns (precision from default, scale=0) produces a DECIMAL type.
 * But the actual displayed decimal places are governed by the execution engine's
 * div_precision_increment (default 4), not by this planner-level type.
 * <p>
 * This test validates that the type derivation produces correct planner-level types
 * that are consistent with the execution engine's 4-decimal-place AVG output.
 */
public class TddlRelDataTypeSystemImplTest {

    private final RelDataTypeSystem typeSystem = TddlRelDataTypeSystemImpl.getInstance();
    private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem);

    @Test
    public void testGetInstance() {
        RelDataTypeSystem instance = TddlRelDataTypeSystemImpl.getInstance();
        assertNotNull(instance);
        // Same singleton instance
        assertEquals(instance, TddlRelDataTypeSystemImpl.getInstance());
    }

    @Test
    public void testMaxNumericScaleIsThirty() {
        assertEquals(30, typeSystem.getMaxNumericScale());
    }

    @Test
    public void testMaxNumericPrecisionIsSixtyFive() {
        assertEquals(65, typeSystem.getMaxNumericPrecision());
    }

    @Test
    public void testDeriveAvgAggTypeForIntegerColumn() {
        // Simulate avg on INTEGER column (no precision specified, uses Calcite defaults)
        // In Calcite, INTEGER default precision is 10 (RelDataType.PRECISION_NOT_SPECIFIED maps to 10)
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType avgType = typeSystem.deriveAvgAggType(typeFactory, intType);

        // For INTEGER (precision=10, scale=0): result should be DECIMAL
        assertEquals(SqlTypeName.DECIMAL, avgType.getSqlTypeName());
        // The scale should be >= 6 (minimum guaranteed by deriveAvgAggType)
        assertTrue("AVG scale should be at least 6", avgType.getScale() >= 6);
        assertTrue(avgType.isNullable());
    }

    @Test
    public void testDeriveAvgAggTypeForDecimalColumn() {
        // Simulate avg on decimal(10,2) column
        RelDataType decimalType = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2);
        RelDataType avgType = typeSystem.deriveAvgAggType(typeFactory, decimalType);

        // For decimal(10,2): scale = max(6, 2 + 10 + 1) = 13, capped by min(13, 65-10, 30) = 13
        // precision = 10 + 13 = 23
        assertEquals(SqlTypeName.DECIMAL, avgType.getSqlTypeName());
        assertEquals(13, avgType.getScale());
        assertEquals(23, avgType.getPrecision());
        assertTrue(avgType.isNullable());
    }

    @Test
    public void testDeriveAvgAggTypeForDecimalIntEquivalent() {
        // Simulate avg on decimal(11,0) column, which represents int(11) in MySQL
        // This is the exact case from hash_window_simple bug: avg on int(11) column
        RelDataType intLikeType = typeFactory.createSqlType(SqlTypeName.DECIMAL, 11, 0);
        RelDataType avgType = typeSystem.deriveAvgAggType(typeFactory, intLikeType);

        // For decimal(11,0): scale = max(6, 0 + 11 + 1) = 12, capped by min(12, 65-11, 30) = 12
        // precision = 11 + 12 = 23
        assertEquals(SqlTypeName.DECIMAL, avgType.getSqlTypeName());
        assertEquals(12, avgType.getScale());
        assertEquals(23, avgType.getPrecision());
        assertTrue(avgType.isNullable());
    }

    @Test
    public void testDeriveAvgAggTypeForDoubleColumn() {
        // For double/float input, AVG should return DOUBLE type
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        RelDataType avgType = typeSystem.deriveAvgAggType(typeFactory, doubleType);

        assertEquals(SqlTypeName.DOUBLE, avgType.getSqlTypeName());
        assertTrue(avgType.isNullable());
    }

    @Test
    public void testDeriveAvgAggTypeForFloatColumn() {
        // For float input, AVG should also return DOUBLE type
        RelDataType floatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
        RelDataType avgType = typeSystem.deriveAvgAggType(typeFactory, floatType);

        assertEquals(SqlTypeName.DOUBLE, avgType.getSqlTypeName());
        assertTrue(avgType.isNullable());
    }

    @Test
    public void testDeriveAvgAggTypeForBigintColumn() {
        // Simulate avg on BIGINT column
        RelDataType bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RelDataType avgType = typeSystem.deriveAvgAggType(typeFactory, bigintType);

        // Result should be DECIMAL with scale >= 6
        assertEquals(SqlTypeName.DECIMAL, avgType.getSqlTypeName());
        assertTrue("AVG scale should be at least 6", avgType.getScale() >= 6);
        assertTrue(avgType.isNullable());
    }

    @Test
    public void testDeriveAvgAggTypeScaleMinimumIsSix() {
        // For a column with very low precision, scale should still be at least 6
        // Using decimal(4,0) to simulate tinyint
        RelDataType lowPrecType = typeFactory.createSqlType(SqlTypeName.DECIMAL, 4, 0);
        RelDataType avgType = typeSystem.deriveAvgAggType(typeFactory, lowPrecType);

        // For decimal(4,0): scale = max(6, 0 + 4 + 1) = 6 (minimum wins)
        assertEquals(SqlTypeName.DECIMAL, avgType.getSqlTypeName());
        assertTrue("Scale should be at least 6", avgType.getScale() >= 6);
    }

    @Test
    public void testDeriveAvgAggTypeForVarcharColumn() {
        // For varchar input, AVG should return DOUBLE type
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType avgType = typeSystem.deriveAvgAggType(typeFactory, varcharType);

        assertEquals(SqlTypeName.DOUBLE, avgType.getSqlTypeName());
        assertTrue(avgType.isNullable());
    }

    @Test
    public void testIsSchemaCaseInsensitive() {
        // TDDL field names are case-insensitive
        assertTrue(!typeSystem.isSchemaCaseSensitive());
    }
}