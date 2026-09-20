package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.properties.DynamicConfig;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Global heap admission for the non-streaming Page builder.
 *
 * <p>A build retains the caller's raw values and the final Page array while two deterministic
 * compression passes use only one chunk-sized scratch buffer. The permit remains held until the
 * uploader has accepted the final array, where its own in-flight accounting takes over.
 */
final class BlobPageBuildLimiter {

    private static final long MAX_BUILD_BYTES = 384L * 1024 * 1024;
    private static final long FIXED_BUILD_OVERHEAD = 8L * 1024 * 1024;
    private static final Object LOCK = new Object();
    private static long admittedBytes;

    private BlobPageBuildLimiter() {
    }

    static Permit acquire(long rawBytes) {
        long estimatedBytes = estimatePeakBytes(rawBytes);
        long timeoutMs = DynamicConfig.getInstance().getExtBlobIoTimeoutMs();
        long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        synchronized (LOCK) {
            while (admittedBytes > 0 && admittedBytes + estimatedBytes > MAX_BUILD_BYTES) {
                long remainingNs = deadlineNs - System.nanoTime();
                if (remainingNs <= 0) {
                    throw new IllegalStateException("Blob Page build admission timed out: admittedBytes="
                        + admittedBytes + ", requestedBytes=" + estimatedBytes);
                }
                long waitMs = Math.max(1L,
                    Math.min(1000L, TimeUnit.NANOSECONDS.toMillis(remainingNs)));
                try {
                    LOCK.wait(waitMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Blob Page build admission interrupted", e);
                }
            }
            admittedBytes += estimatedBytes;
        }
        return new Permit(estimatedBytes);
    }

    private static long estimatePeakBytes(long rawBytes) {
        if (rawBytes < 0) {
            throw new IllegalArgumentException("negative Blob Page raw bytes: " + rawBytes);
        }
        if (rawBytes > (Long.MAX_VALUE - FIXED_BUILD_OVERHEAD) / 2L) {
            return Long.MAX_VALUE;
        }
        return Math.max(1L, rawBytes * 2L + FIXED_BUILD_OVERHEAD);
    }

    static final class Permit implements AutoCloseable {
        private final long bytes;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private Permit(long bytes) {
            this.bytes = bytes;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            synchronized (LOCK) {
                admittedBytes = Math.max(0L, admittedBytes - bytes);
                LOCK.notifyAll();
            }
        }
    }
}
