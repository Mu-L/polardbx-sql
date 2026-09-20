package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.executor.operator.util.BatchBlockWriter.BatchDecimalBlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for BatchBlockWriter
 */
public class BatchBlockWriterTest {

    /**
     * Test BatchDecimalBlockBuilder.isDecimal128() method
     * This test covers the specific line: return state.isDecimal128();
     */
    @Test
    public void testBatchDecimalBlockBuilderIsDecimal128() {
        // Test case 1: Initially unset state should return false
        DecimalType decimalType = new DecimalType(38, 4);
        BatchDecimalBlockBuilder builder = new BatchDecimalBlockBuilder(10, decimalType);
        Assert.assertFalse("Initial state should not be decimal128", builder.isDecimal128());

        // Test case 2: After writing decimal64 values, should return false
        builder.writeLong(12345L);
        Assert.assertFalse("After writing decimal64, should not be decimal128", builder.isDecimal128());

        // Test case 3: Create a new builder and write decimal128 values, should return true
        BatchDecimalBlockBuilder decimal128Builder = new BatchDecimalBlockBuilder(10, decimalType);
        decimal128Builder.writeDecimal128(123456789L, 987654321L);
        Assert.assertTrue("After writing decimal128, should be decimal128", decimal128Builder.isDecimal128());

        // Test case 4: Create builder with normal decimal type and write decimal, should return false
        BatchDecimalBlockBuilder normalBuilder = new BatchDecimalBlockBuilder(10, DataTypes.DecimalType);
        normalBuilder.writeDecimal(Decimal.fromString("123.456"));
        Assert.assertFalse("Normal decimal builder should not be decimal128", normalBuilder.isDecimal128());

        // Test case 5: Test with null values
        BatchDecimalBlockBuilder builderWithNulls = new BatchDecimalBlockBuilder(10, decimalType);
        builderWithNulls.appendNull();
        builderWithNulls.writeDecimal128(100L, 0L);
        Assert.assertTrue("Builder with decimal128 values should return true even with nulls", builderWithNulls.isDecimal128());
    }

    /**
     * Additional test to verify the state transitions
     */
    @Test
    public void testDecimal128StateTransitions() {
        DecimalType decimalType = new DecimalType(38, 4);
        BatchDecimalBlockBuilder builder = new BatchDecimalBlockBuilder(10, decimalType);

        // Initially unset
        Assert.assertFalse("Initial state should not be decimal128", builder.isDecimal128());
        Assert.assertTrue("Initial state should be unset", builder.isUnset());

        // Write decimal64 first
        builder.writeLong(12345L);
        Assert.assertFalse("After decimal64, should not be decimal128", builder.isDecimal128());
        Assert.assertTrue("After decimal64, should be decimal64", builder.isDecimal64());

        // Create new builder and write decimal128 directly
        BatchDecimalBlockBuilder decimal128Builder = new BatchDecimalBlockBuilder(10, decimalType);
        decimal128Builder.writeDecimal128(Long.MAX_VALUE, Long.MIN_VALUE);
        Assert.assertTrue("Direct decimal128 write should result in decimal128 state", decimal128Builder.isDecimal128());
        Assert.assertFalse("Decimal128 builder should not be decimal64", decimal128Builder.isDecimal64());
        Assert.assertFalse("Decimal128 builder should not be unset", decimal128Builder.isUnset());
    }
}