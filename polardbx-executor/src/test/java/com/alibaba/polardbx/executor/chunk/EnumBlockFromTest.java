package com.alibaba.polardbx.executor.chunk;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link EnumBlock#from(EnumBlock, int, int[])} integer overflow fix.
 *
 * <p>The {@code from} method derives the expected per-row string length from
 * {@code other.data.length / (other.positionCount + 1)} and feeds it into the
 * {@link EnumBlockBuilder} constructor, which multiplies it by the capacity to size
 * the underlying char buffer. With a large position count and long enum values the
 * multiplication must not overflow into a negative initial capacity.
 */
public class EnumBlockFromTest {

    private static final int POSITION_COUNT = 1000;

    private static Map<String, Integer> buildEnumValues() {
        Map<String, Integer> enumValues = new HashMap<>();
        enumValues.put("processing_status_value", 1);
        enumValues.put("completed_status_value_long", 2);
        enumValues.put("cancelled_by_administrator_action", 3);
        return enumValues;
    }

    private static EnumBlock buildSource() {
        Map<String, Integer> enumValues = buildEnumValues();
        EnumBlockBuilder builder = new EnumBlockBuilder(POSITION_COUNT, 50, enumValues);
        String[] values = enumValues.keySet().toArray(new String[0]);
        for (int i = 0; i < POSITION_COUNT; i++) {
            builder.writeString(values[i % values.length]);
        }
        return (EnumBlock) builder.build();
    }

    /**
     * Overflow scenario: full selection of a large block with long enum values
     * must not throw {@link IllegalArgumentException} and must keep the row count.
     */
    @Test
    public void testFromNoOverflow() {
        EnumBlock source = buildSource();

        int selSize = POSITION_COUNT;
        int[] selection = new int[selSize];
        for (int i = 0; i < selSize; i++) {
            selection[i] = i;
        }

        EnumBlock result;
        try {
            result = EnumBlock.from(source, selSize, selection);
        } catch (IllegalArgumentException e) {
            fail("EnumBlock.from should not throw IllegalArgumentException: " + e.getMessage());
            return;
        }

        assertEquals(selSize, result.getPositionCount());
    }

    /**
     * Data correctness: a reversed selection must map each row to its source value.
     */
    @Test
    public void testFromDataCorrectness() {
        EnumBlock source = buildSource();

        int selSize = POSITION_COUNT;
        int[] selection = new int[selSize];
        for (int i = 0; i < selSize; i++) {
            selection[i] = POSITION_COUNT - 1 - i;
        }

        EnumBlock result = EnumBlock.from(source, selSize, selection);

        assertEquals(selSize, result.getPositionCount());
        for (int i = 0; i < selSize; i++) {
            assertEquals("Mismatch at pos: " + i,
                source.getString(selection[i]), result.getString(i));
        }
    }

    /**
     * Boundary scenario: a single-element selection must produce a one-row block.
     */
    @Test
    public void testFromSelSizeOne() {
        EnumBlock source = buildSource();

        int[] selection = new int[] {500};
        EnumBlock result = EnumBlock.from(source, 1, selection);

        assertEquals(1, result.getPositionCount());
        assertTrue(source.getString(500).equals(result.getString(0)));
    }
}
