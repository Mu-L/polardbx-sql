package com.alibaba.polardbx.executor.chunk;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link StringBlock#from(StringBlock, int, int[])} integer overflow fix.
 *
 * <p>The {@code from} method derives the expected per-row string length from
 * {@code other.data.length / (other.positionCount + 1)} and feeds it into the
 * {@link StringBlockBuilder} constructor, which multiplies it by the capacity to size
 * the underlying char buffer. With a large position count and long string values the
 * multiplication must not overflow into a negative initial capacity.
 */
public class StringBlockFromTest {

    private static final int POSITION_COUNT = 1000;

    private static StringBlock buildSource() {
        StringBlockBuilder builder = new StringBlockBuilder(POSITION_COUNT, 60);
        for (int i = 0; i < POSITION_COUNT; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append("row_").append(i).append("_padding_data_to_make_string_longer_than_fifty_chars_");
            builder.writeString(sb.toString());
        }
        return (StringBlock) builder.build();
    }

    /**
     * Overflow scenario: full selection of a large block with long string values
     * must not throw {@link IllegalArgumentException} and must keep the row count.
     */
    @Test
    public void testFromNoOverflow() {
        StringBlock source = buildSource();

        int selSize = POSITION_COUNT;
        int[] selection = new int[selSize];
        for (int i = 0; i < selSize; i++) {
            selection[i] = i;
        }

        StringBlock result;
        try {
            result = StringBlock.from(source, selSize, selection);
        } catch (IllegalArgumentException e) {
            fail("StringBlock.from should not throw IllegalArgumentException: " + e.getMessage());
            return;
        }

        assertEquals(selSize, result.getPositionCount());
    }

    /**
     * Data correctness: a reversed selection must map each row to its source value.
     */
    @Test
    public void testFromDataCorrectness() {
        StringBlock source = buildSource();

        int selSize = POSITION_COUNT;
        int[] selection = new int[selSize];
        for (int i = 0; i < selSize; i++) {
            selection[i] = POSITION_COUNT - 1 - i;
        }

        StringBlock result = StringBlock.from(source, selSize, selection);

        assertEquals(selSize, result.getPositionCount());
        for (int i = 0; i < selSize; i++) {
            assertTrue("Mismatch at pos: " + i,
                result.getString(i).equals(source.getString(selection[i])));
        }
    }

    /**
     * Boundary scenario: a single-element selection must produce a one-row block.
     */
    @Test
    public void testFromSelSizeOne() {
        StringBlock source = buildSource();

        int[] selection = new int[] {500};
        StringBlock result = StringBlock.from(source, 1, selection);

        assertEquals(1, result.getPositionCount());
        assertTrue(source.getString(500).equals(result.getString(0)));
    }
}
