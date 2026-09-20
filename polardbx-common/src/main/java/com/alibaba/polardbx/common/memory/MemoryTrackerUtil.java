package com.alibaba.polardbx.common.memory;

import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryTrackerUtil {
    public static long allocateFromQuota(long memoryUsage, AtomicLong freeSize, final long pageSize,
                                         MemoryTracker parentTracker) {
        long retryTime = 0L;
        while (true) {
            long currentQuota = freeSize.get();
            if (currentQuota >= memoryUsage) {

                // quota is sufficient, allocate from local quota.
                if (freeSize.compareAndSet(currentQuota, currentQuota - memoryUsage)) {

                    // successfully allocated.
                    break;
                }

                // CAS failed, retry.
                retryTime++;
            } else {
                // get paged memory size.
                long needed = memoryUsage - currentQuota;
                long pages = (needed + pageSize - 1) / pageSize;
                long requested = pages * pageSize;

                // allocate from parent.
                // NOTE: maybe throw OOM exception.
                parentTracker.tryAllocateMemory(requested);

                // success to allocate from parent, add into local quota.
                freeSize.addAndGet(requested);

                // retry.
                retryTime++;
            }
        }
        return retryTime;
    }

    public static long releaseToQuota(long memoryUsage, AtomicLong freeSize, final long pageSize,
                                      MemoryTracker parentTracker) {
        long retryTime = 0L;
        while (true) {
            long currentQuota = freeSize.get();
            long newQuota = currentQuota + memoryUsage;
            if (newQuota < 0) {
                throw GeneralUtil.nestedException(MessageFormat.format(
                    "Released memory {0} bytes is larger than quota {1} bytes",
                    memoryUsage, currentQuota
                ));
            }

            // Calculate the final quota and memory to release to parent in one step
            long releaseToParent = 0;
            long finalQuota = newQuota;
            if (newQuota >= pageSize) {
                long excessPages = newQuota / pageSize;
                releaseToParent = excessPages * pageSize;
                finalQuota = newQuota - releaseToParent;
            }

            // Single CAS operation to update to final state
            if (freeSize.compareAndSet(currentQuota, finalQuota)) {
                // CAS succeeded, now release to parent tracker if needed
                if (releaseToParent > 0) {
                    parentTracker.releaseReference(releaseToParent);
                }
                break;
            }

            // CAS failed, try release again.
            retryTime++;
        }

        return retryTime;
    }
}
