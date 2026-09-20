package com.alibaba.polardbx.executor.balancer;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for BalanceOptions.
 * <p>
 * Covers:
 * - withSolveLevel() builder method
 * - solveLevel field setting and chaining
 */
public class BalanceOptionsTest {

    /**
     * withSolveLevel correctly sets the field
     */
    @Test
    public void testWithSolveLevel() {
        BalanceOptions options = BalanceOptions.withDefault()
            .withDrainNode("dn1,dn2")
            .withSolveLevel("DRAIN_ONLY");
        Assert.assertEquals("DRAIN_ONLY", options.solveLevel);
        Assert.assertEquals("dn1,dn2", options.drainNode);
    }

    /**
     * withSolveLevel returns this for chaining
     */
    @Test
    public void testWithSolveLevelChaining() {
        BalanceOptions options = BalanceOptions.withDefault()
            .withDrainNode("dn3")
            .withSolveLevel("DRAIN_ONLY");
        Assert.assertSame(options, options.withSolveLevel("MIN_COST"));
        Assert.assertEquals("MIN_COST", options.solveLevel);
    }

    /**
     * Default solveLevel should be empty string
     */
    @Test
    public void testDefaultSolveLevelIsEmpty() {
        BalanceOptions options = BalanceOptions.withDefault();
        Assert.assertEquals("", options.solveLevel);
    }
}
