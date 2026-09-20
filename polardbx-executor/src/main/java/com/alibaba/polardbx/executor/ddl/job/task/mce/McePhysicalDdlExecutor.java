package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs MCE physical column DDL with dynamically adjustable, DN-balanced concurrency.
 */
final class McePhysicalDdlExecutor {

    interface PhysicalDdlProcessor {
        void execute(McePhysicalTableResolver.PhysicalTableTarget target);
    }

    private McePhysicalDdlExecutor() {
    }

    static long execute(List<McePhysicalTableResolver.PhysicalTableTarget> targets,
                        MceTaskRuntimeConfig runtimeConfig,
                        PhysicalDdlProcessor processor,
                        ExecutionContext executionContext,
                        Logger logger,
                        String taskLabel,
                        String failurePrefix) {
        return execute(targets, runtimeConfig, processor, executionContext, logger, taskLabel, failurePrefix, true);
    }

    static long executeRollback(List<McePhysicalTableResolver.PhysicalTableTarget> targets,
                                MceTaskRuntimeConfig runtimeConfig,
                                PhysicalDdlProcessor processor,
                                ExecutionContext executionContext,
                                Logger logger,
                                String taskLabel,
                                String failurePrefix) {
        return execute(targets, runtimeConfig, processor, executionContext, logger, taskLabel, failurePrefix, false);
    }

    private static long execute(List<McePhysicalTableResolver.PhysicalTableTarget> targets,
                                MceTaskRuntimeConfig runtimeConfig,
                                PhysicalDdlProcessor processor,
                                ExecutionContext executionContext,
                                Logger logger,
                                String taskLabel,
                                String failurePrefix,
                                boolean honorJobInterruption) {
        AtomicBoolean interrupted = new AtomicBoolean(false);
        McePartitionWorkerCoordinator<McePhysicalTableResolver.PhysicalTableTarget> coordinator =
            new McePartitionWorkerCoordinator<>(
                targets,
                () -> runtimeConfig.physicalDdlSettings().parallelism,
                target -> target.storageInstId == null ? target.groupName : target.storageInstId,
                (target, ignored) -> {
                    checkInterrupted(executionContext, interrupted, honorJobInterruption);
                    processor.execute(target);
                    MceTaskFailPoint.pauseWhileEnabled(
                        MceTaskFailPoint.FP_MCE_AFTER_PHYSICAL_DDL, executionContext);
                    return McePartitionWorkerCoordinator.PartitionResult.done(1L);
                },
                () -> checkInterrupted(executionContext, interrupted, honorJobInterruption),
                interrupted,
                logger,
                taskLabel,
                failurePrefix,
                honorJobInterruption);
        return coordinator.execute();
    }

    private static void checkInterrupted(ExecutionContext executionContext, AtomicBoolean interrupted,
                                         boolean honorJobInterruption) {
        if (interrupted.get() || (honorJobInterruption
            && (CrossEngineValidator.isJobInterrupted(executionContext) || Thread.currentThread().isInterrupted()))) {
            interrupted.set(true);
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "MCE physical DDL was interrupted");
        }
    }
}
