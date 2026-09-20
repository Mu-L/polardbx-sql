package com.alibaba.polardbx.gms.sync;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Process-level fault injector for GMS sync connection establishment.
 * When armed, the first N physical connection attempts of {@link GmsSyncDataSource}
 * fail with a transient {@link SQLException}, simulating network jitter between CN nodes.
 * Kept in polardbx-gms so that it can be invoked from the sync data source without
 * depending on the executor fail point framework; the arming side reads fail point state.
 */
public class GmsSyncConnectionFailInjector {

    private static final AtomicBoolean ARMED = new AtomicBoolean(false);
    private static final AtomicInteger REMAINING_FAIL_TIMES = new AtomicInteger(0);

    public static void armOnce(int failTimes) {
        if (failTimes <= 0) {
            return;
        }
        if (ARMED.compareAndSet(false, true)) {
            REMAINING_FAIL_TIMES.set(failTimes);
        }
    }

    public static void disarm() {
        ARMED.set(false);
        REMAINING_FAIL_TIMES.set(0);
    }

    public static void injectIfNeeded() throws SQLException {
        if (!ARMED.get()) {
            return;
        }
        if (REMAINING_FAIL_TIMES.getAndUpdate(v -> v > 0 ? v - 1 : 0) > 0) {
            throw new SQLException(
                "Injected transient failure when establishing GMS sync connection (simulated network jitter)");
        }
    }
}
