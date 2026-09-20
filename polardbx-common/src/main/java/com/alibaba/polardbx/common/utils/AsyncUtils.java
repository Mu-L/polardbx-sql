/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class AsyncUtils {

    private static final Logger logger = LoggerFactory.getLogger(AsyncUtils.class);

    public static void waitAll(Collection<Future> futures) {
        List<Throwable> exceptions = new ArrayList<>();

        for (Future future : futures) {
            try {
                future.get();
            } catch (Throwable ex) {
                exceptions.add(ex.getCause());
            }
        }

        if (!exceptions.isEmpty()) {

            final Throwable ex = exceptions.get(0);
            if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else {
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * return when caller thread is interrupted, but notice that the task is still running
     */
    public static void waitAllInterruptibly(Collection<Future> futures) {
        List<Throwable> exceptions = new ArrayList<>();

        // Wait for all the tasks finish their work
        for (Future future : futures) {
            try {
                future.get();
            } catch (Throwable ex) {
                if (ex instanceof InterruptedException) {
                    // Preserve interrupt status
                    Thread.currentThread().interrupt();
                }
                exceptions.add(ex.getCause());
            }
        }

        if (!exceptions.isEmpty()) {
            // Re-throw the first exception
            final Throwable ex = exceptions.get(0);
            if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else {
                throw new RuntimeException(ex);
            }
        }
    }

    public static boolean shutdownNowAndAwaitTermination(
        ExecutorService service, long timeout, TimeUnit unit) {
        long timeoutNanos = unit.toNanos(timeout);
        // Cancel currently executing tasks
        List<Runnable> remainingTasks = service.shutdownNow();

        // Cancel remaining tasks
        for (Runnable remainingTask : remainingTasks) {
            if (remainingTask instanceof Future) {
                ((Future) remainingTask).cancel(true);
            }
        }

        try {
            // Wait for the duration of the timeout for existing tasks to terminate
            service.awaitTermination(timeoutNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ie) {
            // Preserve interrupt status
            Thread.currentThread().interrupt();
            // (Re-)Cancel if current thread also interrupted
            service.shutdownNow();
        }
        return service.isTerminated();
    }
}
