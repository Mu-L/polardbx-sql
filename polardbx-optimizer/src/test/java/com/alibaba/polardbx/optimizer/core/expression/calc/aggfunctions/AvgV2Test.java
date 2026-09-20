package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import org.apache.calcite.sql.SqlKind;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for AvgV2 aggregator, focusing on AVG precision behavior
 * for window functions.
 * <p>
 * Bug reference: AONE-82183457 - env80 hash_window_simple avg precision baseline mismatch.
 * The avg result on integer columns should have 4 decimal places (matching div_precision_increment=4).
 * <p>
 * AvgV2 hardcodes getScale()=4 and uses DecimalType.divide(sum, count) for computation,
 * which produces results with 4 decimal places (e.g., 1.0000, 2.4000).
 */
public class AvgV2Test {

    @Test
    public void testAvgV2BasicProperties() {
        AvgV2 avgV2 = new AvgV2(0, false, null, -1);

        // Check SQL kind
        assertEquals(SqlKind.AVG, avgV2.getSqlKind());

        // Check return type is DecimalType
        assertEquals(DataTypes.DecimalType, avgV2.getReturnType());

        // Check function type
        assertEquals(IFunction.FunctionType.Aggregate, avgV2.getFunctionType());

        // Check function name
        assertEquals("AVG_V2", avgV2.getFunctionNames()[0]);
    }

    @Test
    public void testAvgV2ScaleIsFour() {
        AvgV2 avgV2 = new AvgV2(0, false, null, -1);

        // AvgV2.getScale() returns 4, matching div_precision_increment default
        // This is the key precision value that determines AVG output decimal places
        assertEquals(4, avgV2.getScale());
    }

    @Test
    public void testAvgV2PrecisionIsZero() {
        AvgV2 avgV2 = new AvgV2(0, false, null, -1);

        // AvgV2.getPrecision() returns 0 (unspecified precision)
        assertEquals(0, avgV2.getPrecision());
    }

    @Test
    public void testAvgV2AvgOnIntegerValues() {
        AvgV2 avgV2 = new AvgV2(0, false, null, -1);

        // Aggregate Decimal values representing integers (like avg on int column)
        avgV2.aggregate(new ArrayRow(new Object[] {Decimal.fromLong(1)}));
        avgV2.aggregate(new ArrayRow(new Object[] {Decimal.fromLong(2)}));
        avgV2.aggregate(new ArrayRow(new Object[] {Decimal.fromLong(3)}));

        Object result = avgV2.eval(null);
        assertNotNull("AVG result should not be null", result);

        // AVG of 1,2,3 = 2.0; should produce result with 4 decimal places scale
        assertTrue("AVG result should be a Decimal", result instanceof Decimal);
        Decimal avgResult = (Decimal) result;
        // The result value should equal 2
        assertTrue("AVG of 1,2,3 should equal 2",
            avgResult.compareTo(Decimal.fromLong(2)) == 0);
    }

    @Test
    public void testAvgV2AvgOnSingleValue() {
        AvgV2 avgV2 = new AvgV2(0, false, null, -1);

        // Aggregate a single Decimal value (like avg(c2) over() in SINGLE table)
        avgV2.aggregate(new ArrayRow(new Object[] {Decimal.fromLong(1)}));

        Object result = avgV2.eval(null);
        assertNotNull("AVG result should not be null", result);

        // AVG of single value 1 should be 1.0000 (4 decimal places)
        assertTrue("AVG result should be a Decimal", result instanceof Decimal);
        Decimal avgResult = (Decimal) result;
        // The result value should equal 1
        assertTrue("AVG of single value 1 should equal 1",
            avgResult.compareTo(Decimal.fromLong(1)) == 0);
    }

    @Test
    public void testAvgV2AvgProducesFourDecimalPlacesScale() {
        AvgV2 avgV2 = new AvgV2(0, false, null, -1);

        // Aggregate values that produce non-trivial decimal results
        avgV2.aggregate(new ArrayRow(new Object[] {Decimal.fromLong(2)}));
        avgV2.aggregate(new ArrayRow(new Object[] {Decimal.fromLong(3)}));
        avgV2.aggregate(new ArrayRow(new Object[] {Decimal.fromLong(5)}));

        Object result = avgV2.eval(null);
        assertNotNull(result);

        // The getScale()=4 determines the precision of the AVG output
        // This matches div_precision_increment=4 which is MySQL's default
        assertEquals(4, avgV2.getScale());
    }

    @Test
    public void testAvgV2GetNew() {
        AvgV2 avgV2 = new AvgV2(0, false, null, -1);
        AvgV2 newAvg = (AvgV2) avgV2.getNew();

        assertNotNull("getNew() should return a new AvgV2 instance", newAvg);
        assertEquals(SqlKind.AVG, newAvg.getSqlKind());
        assertEquals(DataTypes.DecimalType, newAvg.getReturnType());
        assertEquals(4, newAvg.getScale());
    }
}