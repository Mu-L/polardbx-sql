package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.workqueue.BackFillThreadPool;
import com.alibaba.polardbx.executor.ddl.workqueue.PriorityFIFOTask;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntSupplier;

/**
 * Dynamically sizes one MCE task's workers without parking shared BackFillThreadPool threads.
 *
 * <p>A worker that owns a physical partition may retire only when its processor returns YIELDED;
 * processors must first publish all side effects, advance their checkpoint, and release bounded
 * resources. The unfinished target is then atomically returned to the ready queue.</p>
 */
final class McePartitionWorkerCoordinator<T> {

    private static final long RECONCILE_INTERVAL_MS = 200L;

    interface PartitionProcessor<T> {
        PartitionResult process(T target, BooleanSupplier shouldYield);
    }

    static final class PartitionResult {
        private final boolean done;
        private final long processedTotal;

        private PartitionResult(boolean done, long processedTotal) {
            this.done = done;
            this.processedTotal = processedTotal;
        }

        static PartitionResult done(long processedTotal) {
            return new PartitionResult(true, processedTotal);
        }

        static PartitionResult yielded(long processedTotal) {
            return new PartitionResult(false, processedTotal);
        }
    }

    private final Object monitor = new Object();
    private final ReadyTargets<T> readyTargets;
    private final Map<T, Long> processedTotals = new IdentityHashMap<>();
    private final IntSupplier desiredParallelism;
    private final PartitionProcessor<T> processor;
    private final Runnable interruptionCheck;
    private final AtomicBoolean interrupted;
    private final Logger logger;
    private final String taskLabel;
    private final String failurePrefix;
    private final int targetCount;
    private final boolean failOnParentInterruption;

    private int runningWorkers;
    private int inFlightTargets;
    private int retirementClaims;
    private int lastDesired = -1;
    private long totalProcessed;
    private Throwable firstFailure;

    McePartitionWorkerCoordinator(List<T> targets,
                                  IntSupplier desiredParallelism,
                                  PartitionProcessor<T> processor,
                                  Runnable interruptionCheck,
                                  AtomicBoolean interrupted,
                                  Logger logger,
                                  String taskLabel,
                                  String failurePrefix) {
        this(targets, desiredParallelism, null, processor, interruptionCheck, interrupted, logger, taskLabel,
            failurePrefix, true);
    }

    McePartitionWorkerCoordinator(List<T> targets,
                                  IntSupplier desiredParallelism,
                                  Function<T, String> schedulingGroup,
                                  PartitionProcessor<T> processor,
                                  Runnable interruptionCheck,
                                  AtomicBoolean interrupted,
                                  Logger logger,
                                  String taskLabel,
                                  String failurePrefix) {
        this(targets, desiredParallelism, schedulingGroup, processor, interruptionCheck, interrupted, logger,
            taskLabel, failurePrefix, true);
    }

    McePartitionWorkerCoordinator(List<T> targets,
                                  IntSupplier desiredParallelism,
                                  Function<T, String> schedulingGroup,
                                  PartitionProcessor<T> processor,
                                  Runnable interruptionCheck,
                                  AtomicBoolean interrupted,
                                  Logger logger,
                                  String taskLabel,
                                  String failurePrefix,
                                  boolean failOnParentInterruption) {
        List<T> shuffled = new ArrayList<>(targets);
        Collections.shuffle(shuffled);
        this.readyTargets = schedulingGroup == null
            ? new DequeReadyTargets<>(shuffled)
            : new GroupedReadyTargets<>(shuffled, schedulingGroup);
        this.targetCount = shuffled.size();
        this.desiredParallelism = desiredParallelism;
        this.processor = processor;
        this.interruptionCheck = interruptionCheck;
        this.interrupted = interrupted;
        this.logger = logger;
        this.taskLabel = taskLabel;
        this.failurePrefix = failurePrefix;
        this.failOnParentInterruption = failOnParentInterruption;
    }

    long execute() {
        boolean parentInterrupted = false;
        while (true) {
            while (reserveWorkerSafely()) {
                try {
                    BackFillThreadPool.getInstance().executeWithContext(
                        this::runWorker, PriorityFIFOTask.TaskPriority.OMC_BACKFILL_TASK);
                } catch (RuntimeException | Error t) {
                    synchronized (monitor) {
                        runningWorkers--;
                        recordFailureLocked(t);
                        monitor.notifyAll();
                    }
                    break;
                }
            }

            synchronized (monitor) {
                if (isTerminalLocked()) {
                    break;
                }
                try {
                    monitor.wait(RECONCILE_INTERVAL_MS);
                } catch (InterruptedException e) {
                    parentInterrupted = true;
                    if (failOnParentInterruption) {
                        recordFailureLocked(e);
                    }
                }
            }
        }

        if (parentInterrupted) {
            Thread.currentThread().interrupt();
        }
        rethrowFailure();
        return totalProcessed;
    }

    private boolean reserveWorkerSafely() {
        try {
            return reserveWorker();
        } catch (RuntimeException | Error t) {
            synchronized (monitor) {
                recordFailureLocked(t);
                monitor.notifyAll();
            }
            return false;
        }
    }

    private boolean reserveWorker() {
        synchronized (monitor) {
            if (firstFailure != null || interrupted.get() || readyTargets.isEmpty()) {
                return false;
            }
            int desired = effectiveDesiredParallelism();
            if (lastDesired != desired) {
                logger.info(String.format(
                    "[MCE] worker concurrency changed: %s, desired=%d, running=%d, inFlight=%d, ready=%d",
                    taskLabel, desired, runningWorkers, inFlightTargets, readyTargets.size()));
                lastDesired = desired;
            }
            if (runningWorkers >= desired) {
                return false;
            }
            runningWorkers++;
            return true;
        }
    }

    private void runWorker() {
        WorkerControl control = new WorkerControl();
        try {
            while (!interrupted.get()) {
                interruptionCheck.run();
                if (control.getAsBoolean()) {
                    return;
                }

                T target;
                synchronized (monitor) {
                    target = readyTargets.poll();
                    if (target == null) {
                        return;
                    }
                    readyTargets.targetStarted(target);
                    inFlightTargets++;
                }

                PartitionResult result;
                try {
                    result = processor.process(target, control);
                } catch (RuntimeException | Error t) {
                    synchronized (monitor) {
                        inFlightTargets--;
                        readyTargets.targetFinished(target);
                        recordFailureLocked(t);
                        monitor.notifyAll();
                    }
                    return;
                }

                synchronized (monitor) {
                    inFlightTargets--;
                    readyTargets.targetFinished(target);
                    recordProcessedTotalLocked(target, result.processedTotal);
                    if (!result.done) {
                        readyTargets.offer(target);
                    }
                    monitor.notifyAll();
                }
                if (!result.done) {
                    return;
                }
            }
        } catch (RuntimeException | Error t) {
            synchronized (monitor) {
                recordFailureLocked(t);
                monitor.notifyAll();
            }
        } finally {
            synchronized (monitor) {
                if (control.retirementClaimed) {
                    retirementClaims--;
                }
                runningWorkers--;
                monitor.notifyAll();
            }
        }
    }

    private void recordProcessedTotalLocked(T target, long processedTotal) {
        Long previous = processedTotals.put(target, processedTotal);
        long previousTotal = previous == null ? 0L : previous;
        if (processedTotal > previousTotal) {
            totalProcessed += processedTotal - previousTotal;
        }
    }

    private int effectiveDesiredParallelism() {
        return Math.max(1, Math.min(targetCount, desiredParallelism.getAsInt()));
    }

    private boolean isTerminalLocked() {
        if (firstFailure != null || interrupted.get()) {
            return runningWorkers == 0;
        }
        return readyTargets.isEmpty() && inFlightTargets == 0 && runningWorkers == 0;
    }

    private void recordFailureLocked(Throwable t) {
        if (firstFailure == null) {
            firstFailure = t;
        }
        interrupted.set(true);
    }

    private void rethrowFailure() {
        Throwable failure;
        synchronized (monitor) {
            failure = firstFailure;
        }
        if (failure == null) {
            return;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure instanceof TddlRuntimeException) {
            throw (TddlRuntimeException) failure;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
            failurePrefix + failure.getMessage(), failure);
    }

    private final class WorkerControl implements BooleanSupplier {
        private boolean retirementClaimed;

        @Override
        public boolean getAsBoolean() {
            synchronized (monitor) {
                if (retirementClaimed) {
                    return true;
                }
                int desired = effectiveDesiredParallelism();
                if (runningWorkers - retirementClaims > desired) {
                    retirementClaims++;
                    retirementClaimed = true;
                    return true;
                }
                return false;
            }
        }
    }

    private interface ReadyTargets<E> {
        E poll();

        void offer(E target);

        boolean isEmpty();

        int size();

        void targetStarted(E target);

        void targetFinished(E target);
    }

    private static final class DequeReadyTargets<E> implements ReadyTargets<E> {
        private final Deque<E> targets;

        private DequeReadyTargets(List<E> targets) {
            this.targets = new ArrayDeque<>(targets);
        }

        @Override
        public E poll() {
            return targets.pollFirst();
        }

        @Override
        public void offer(E target) {
            targets.addLast(target);
        }

        @Override
        public boolean isEmpty() {
            return targets.isEmpty();
        }

        @Override
        public int size() {
            return targets.size();
        }

        @Override
        public void targetStarted(E target) {
        }

        @Override
        public void targetFinished(E target) {
        }
    }

    /**
     * Picks work from the storage instance with the fewest in-flight targets. This fills one slot
     * per DN before assigning a second slot to any DN, while still allowing the configured total
     * concurrency to exceed the DN count.
     */
    private static final class GroupedReadyTargets<E> implements ReadyTargets<E> {
        private final Map<String, Deque<E>> targetsByGroup = new LinkedHashMap<>();
        private final Map<E, String> targetGroups = new IdentityHashMap<>();
        private final Map<String, Integer> inFlightByGroup = new HashMap<>();
        private final List<String> groups = new ArrayList<>();
        private int readyCount;
        private int nextGroupIndex;

        private GroupedReadyTargets(List<E> targets, Function<E, String> schedulingGroup) {
            for (E target : targets) {
                String group = schedulingGroup.apply(target);
                Deque<E> groupTargets = targetsByGroup.computeIfAbsent(group, ignored -> {
                    groups.add(group);
                    return new ArrayDeque<>();
                });
                groupTargets.addLast(target);
                targetGroups.put(target, group);
                readyCount++;
            }
        }

        @Override
        public E poll() {
            if (readyCount == 0) {
                return null;
            }
            int selectedIndex = -1;
            int selectedInFlight = Integer.MAX_VALUE;
            for (int offset = 0; offset < groups.size(); offset++) {
                int index = (nextGroupIndex + offset) % groups.size();
                String group = groups.get(index);
                if (targetsByGroup.get(group).isEmpty()) {
                    continue;
                }
                int inFlight = inFlightByGroup.getOrDefault(group, 0);
                if (inFlight < selectedInFlight) {
                    selectedIndex = index;
                    selectedInFlight = inFlight;
                }
            }
            if (selectedIndex < 0) {
                return null;
            }
            nextGroupIndex = (selectedIndex + 1) % groups.size();
            readyCount--;
            return targetsByGroup.get(groups.get(selectedIndex)).pollFirst();
        }

        @Override
        public void offer(E target) {
            String group = targetGroups.get(target);
            targetsByGroup.get(group).addLast(target);
            readyCount++;
        }

        @Override
        public boolean isEmpty() {
            return readyCount == 0;
        }

        @Override
        public int size() {
            return readyCount;
        }

        @Override
        public void targetStarted(E target) {
            String group = targetGroups.get(target);
            inFlightByGroup.merge(group, 1, Integer::sum);
        }

        @Override
        public void targetFinished(E target) {
            String group = targetGroups.get(target);
            int remaining = inFlightByGroup.getOrDefault(group, 0) - 1;
            if (remaining <= 0) {
                inFlightByGroup.remove(group);
            } else {
                inFlightByGroup.put(group, remaining);
            }
        }
    }
}
