package com.alibaba.polardbx.gms.sync;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

/**
 * Unit tests for {@link GmsSyncConnectionFailInjector}, the process-level fault injector used
 * to simulate transient gms sync connection failures in tests.
 */
public class GmsSyncConnectionFailInjectorTest {

    @After
    public void tearDown() {
        GmsSyncConnectionFailInjector.disarm();
    }

    @Test
    public void testNotArmedDoesNotThrow() throws SQLException {
        GmsSyncConnectionFailInjector.disarm();
        GmsSyncConnectionFailInjector.injectIfNeeded();
        GmsSyncConnectionFailInjector.injectIfNeeded();
    }

    @Test
    public void testArmWithNonPositiveTimesIsIgnored() throws SQLException {
        GmsSyncConnectionFailInjector.armOnce(0);
        GmsSyncConnectionFailInjector.injectIfNeeded();
        GmsSyncConnectionFailInjector.armOnce(-1);
        GmsSyncConnectionFailInjector.injectIfNeeded();
    }

    @Test
    public void testInjectFailsExactlyArmedTimes() {
        GmsSyncConnectionFailInjector.armOnce(2);
        try {
            GmsSyncConnectionFailInjector.injectIfNeeded();
            Assert.fail("first injected failure expected");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Injected transient failure"));
        }
        try {
            GmsSyncConnectionFailInjector.injectIfNeeded();
            Assert.fail("second injected failure expected");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Injected transient failure"));
        }
        try {
            GmsSyncConnectionFailInjector.injectIfNeeded();
        } catch (SQLException e) {
            Assert.fail("no more failures expected after armed times are exhausted: " + e.getMessage());
        }
    }

    @Test
    public void testArmTwiceKeepsFirstArming() {
        GmsSyncConnectionFailInjector.armOnce(1);
        GmsSyncConnectionFailInjector.armOnce(5);
        try {
            GmsSyncConnectionFailInjector.injectIfNeeded();
            Assert.fail("injected failure expected");
        } catch (SQLException ignored) {
        }
        try {
            GmsSyncConnectionFailInjector.injectIfNeeded();
        } catch (SQLException e) {
            Assert.fail("second armOnce must not re-arm while already armed: " + e.getMessage());
        }
    }

    @Test
    public void testDisarmStopsInjection() throws SQLException {
        GmsSyncConnectionFailInjector.armOnce(3);
        GmsSyncConnectionFailInjector.disarm();
        GmsSyncConnectionFailInjector.injectIfNeeded();
        GmsSyncConnectionFailInjector.injectIfNeeded();
    }

    @Test
    public void testReArmAfterExhaustion() {
        GmsSyncConnectionFailInjector.armOnce(1);
        try {
            GmsSyncConnectionFailInjector.injectIfNeeded();
            Assert.fail("injected failure expected");
        } catch (SQLException ignored) {
        }
        GmsSyncConnectionFailInjector.disarm();
        GmsSyncConnectionFailInjector.armOnce(1);
        try {
            GmsSyncConnectionFailInjector.injectIfNeeded();
            Assert.fail("injected failure expected after re-arm");
        } catch (SQLException ignored) {
        }
    }
}
