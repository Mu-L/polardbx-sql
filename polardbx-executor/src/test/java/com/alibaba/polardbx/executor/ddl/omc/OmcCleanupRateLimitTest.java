package com.alibaba.polardbx.executor.ddl.omc;

import org.junit.Assert;
import org.junit.Test;

public class OmcCleanupRateLimitTest {

    @Test
    public void testBackfillUsesScannedRowsOnlyForCleanup() {
        Assert.assertEquals(17, OmcBackfillMigrator.calculateProcessedRowCount(null, 100, 17));
        Assert.assertEquals(17, OmcBackfillMigrator.calculateProcessedRowCount("", 100, 17));
        Assert.assertEquals(100,
            OmcBackfillMigrator.calculateProcessedRowCount("((status = 'deleted') IS NOT TRUE)", 100, 17));
    }

    @Test
    public void testChangeSetUsesCurrentBatchOnlyForCleanupRateLimit() {
        Assert.assertEquals(100, OmcChangeSetApplier.calculateRateLimitPermits(null, 100, 17));
        Assert.assertEquals(100, OmcChangeSetApplier.calculateRateLimitPermits("", 100, 17));
        Assert.assertEquals(17,
            OmcChangeSetApplier.calculateRateLimitPermits("((status = 'deleted') IS NOT TRUE)", 100, 17));
    }

    @Test
    public void testChangeSetUsesScannedRowsOnlyForCleanupFeedback() {
        Assert.assertEquals(3, OmcChangeSetApplier.calculateFeedbackRowCount(null, 17, 3));
        Assert.assertEquals(3, OmcChangeSetApplier.calculateFeedbackRowCount("", 17, 3));
        Assert.assertEquals(17,
            OmcChangeSetApplier.calculateFeedbackRowCount("((status = 'deleted') IS NOT TRUE)", 17, 3));
    }
}
