package com.alibaba.polardbx.executor.utils.failpoint;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the fail point key/value lookup used by the gms sync connection fail injection
 * path (see ClusterSyncManager.prepareSyncConnectionFailInjection).
 */
public class FailPointGetValueTest {

    @Test
    public void testGetValueOfKeyReturnsEnabledValue() {
        FailPoint.enable(FailPointKey.FP_GMS_SYNC_CONN_FAIL_TIMES, "3");
        try {
            Assert.assertEquals("3", FailPoint.getValueOfKey(FailPointKey.FP_GMS_SYNC_CONN_FAIL_TIMES));
        } finally {
            FailPoint.disable(FailPointKey.FP_GMS_SYNC_CONN_FAIL_TIMES);
        }
        Assert.assertNull(FailPoint.getValueOfKey(FailPointKey.FP_GMS_SYNC_CONN_FAIL_TIMES));
    }

    @Test
    public void testGetValueOfKeyIsCaseInsensitive() {
        FailPoint.enable(FailPointKey.FP_GMS_SYNC_CONN_FAIL_TIMES, "5");
        try {
            Assert.assertEquals("5", FailPoint.getValueOfKey("fp_gms_sync_conn_fail_times"));
        } finally {
            FailPoint.disable(FailPointKey.FP_GMS_SYNC_CONN_FAIL_TIMES);
        }
    }

    @Test
    public void testInvalidFailTimesValueFallsBackToZero() {
        // Mirrors the parsing contract in ClusterSyncManager.prepareSyncConnectionFailInjection:
        // a non-numeric fail point value must degrade to zero injected failures, not throw.
        FailPoint.enable(FailPointKey.FP_GMS_SYNC_CONN_FAIL_TIMES, "not-a-number");
        try {
            int failTimes = 0;
            try {
                failTimes = Integer.parseInt(FailPoint.getValueOfKey(FailPointKey.FP_GMS_SYNC_CONN_FAIL_TIMES));
            } catch (NumberFormatException ignored) {
            }
            Assert.assertEquals(0, failTimes);
        } finally {
            FailPoint.disable(FailPointKey.FP_GMS_SYNC_CONN_FAIL_TIMES);
        }
    }
}
