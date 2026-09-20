package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * @author wumu
 */
public class OmcConcurrentExecuteUtils {
    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;

    public static QueryConcurrencyPolicy getConcurrencyPolicy(ExecutionContext executionContext) {
        boolean fullConcurrent =
            executionContext.getParamManager().getBoolean(ConnectionParams.OMC_FULL_CONCURRENT_POLICY);

        boolean sequential =
            executionContext.getParamManager().getBoolean(ConnectionParams.OMC_SEQUENTIAL_POLICY);

        if (fullConcurrent) {
            return QueryConcurrencyPolicy.DDL_CONCURRENT;
        } else if (sequential) {
            return QueryConcurrencyPolicy.SEQUENTIAL;
        }

        return QueryConcurrencyPolicy.INSTANCE_CONCURRENT;
    }

    public static void executeConcurrently(ExecutionContext originEc, String schemaName,
                                           List<OmcPhyDdlContext> phyDdlContexts,
                                           List<Throwable> phyDdlExceptions,
                                           Consumer<OmcPhyDdlContext> call) {
        QueryConcurrencyPolicy concurrencyPolicy = getConcurrencyPolicy(originEc);

        switch (concurrencyPolicy) {
        case DDL_CONCURRENT:
            executeDdlConcurrent(originEc, schemaName, phyDdlContexts, phyDdlExceptions, call);
            break;
        case SEQUENTIAL:
            executeSequential(originEc, phyDdlContexts, phyDdlExceptions, call);
            break;
        case INSTANCE_CONCURRENT:
            executeInstanceConcurrent(originEc, schemaName, phyDdlContexts, phyDdlExceptions, call);
            break;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, "unsupported concurrency policy");
        }
    }

    public static void executeDdlConcurrent(ExecutionContext executionContext, String schemaName,
                                            List<OmcPhyDdlContext> phyDdlContexts,
                                            List<Throwable> phyDdlExceptions,
                                            Consumer<OmcPhyDdlContext> call) {
        String traceId = executionContext.getTraceId();
        int prefetchShards = executionContext.getParamManager().getInt(ConnectionParams.OMC_PREFETCH_SHARDS);
        int finalPrefetch = Math.max(prefetchShards, 1);

        Map<String, Queue<OmcPhyDdlContext>> phyDdlContextsMap = new HashMap<>();
        phyDdlContexts.forEach(
            o -> phyDdlContextsMap.computeIfAbsent(o.getOmcStorageInfo().getStorageId(),
                k -> new LinkedBlockingQueue<>()).add(o));

        Set<String> storageInstIds = phyDdlContextsMap.keySet();

        Map<String, Integer> storageAndResidues = new HashMap<>();
        storageInstIds.forEach(o -> storageAndResidues.put(o, finalPrefetch));

        Map<String, List<Future<Void>>> storageAndFutures = new HashMap<>();
        storageInstIds.forEach(o -> storageAndFutures.put(o, new ArrayList<>()));

        FailPoint.injectFromHint(FailPointKey.FP_OMC_PHYSICAL_DDL_INTERRUPTED, executionContext, () -> {
            DdlContext ddlContext = executionContext.getDdlContext();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INTERRUPTED, String.valueOf(ddlContext.getJobId()),
                ddlContext.getSchemaName(), ddlContext.getObjectName());
        });

        Map mdcContext = MDC.getCopyOfContextMap();
        for (int execute = 0; execute < phyDdlContexts.size(); ) {
            for (String storageInstId : storageInstIds) {
                int residue = storageAndResidues.get(storageInstId);
                if (residue > 0) {
                    final OmcPhyDdlContext omcPhyDdlContext = phyDdlContextsMap.get(storageInstId).poll();
                    if (omcPhyDdlContext == null) {
                        continue;
                    }

                    FutureTask<Void> task = new FutureTask<>(
                        () -> {
                            MDC.setContextMap(mdcContext);
                            call.accept(omcPhyDdlContext);
                        }, null);
                    executionContext.getExecutorService().submit(schemaName, traceId, AsyncTask.build(task));
                    storageAndFutures.get(storageInstId).add(task);
                    // 减少计数
                    storageAndResidues.put(storageInstId, residue - 1);
                }
            }

            for (String storageInstId : storageInstIds) {
                final List<Future<Void>> futures = storageAndFutures.get(storageInstId);
                List<Future<Void>> doneFutures = new ArrayList<>();
                for (Future<Void> future : futures) {
                    if (future.isDone()) {
                        try {
                            future.get();
                        } catch (Exception e) {
                            phyDdlExceptions.add(e);
                        } finally {
                            doneFutures.add(future);
                        }
                    }
                }
                futures.removeAll(doneFutures);
                // 增加计数
                storageAndResidues.put(storageInstId, storageAndResidues.get(storageInstId) + doneFutures.size());
                execute += doneFutures.size();
            }
            if (executionContext.getDdlContext().isInterrupted() || Thread.currentThread().isInterrupted()) {
                long jobId = executionContext.getDdlJobId();
                phyDdlExceptions.add(new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' has been cancelled"));
                break;
            }
        }
    }

    public static void executeSequential(ExecutionContext executionContext,
                                         List<OmcPhyDdlContext> phyDdlContexts,
                                         List<Throwable> phyDdlExceptions,
                                         Consumer<OmcPhyDdlContext> call) {
        for (OmcPhyDdlContext phyDdlContext : phyDdlContexts) {
            if (CrossEngineValidator.isJobInterrupted(executionContext) || Thread.currentThread().isInterrupted()) {
                long jobId = executionContext.getDdlJobId();
                phyDdlExceptions.add(new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' has been cancelled"));
                break;
            }

            FailPoint.injectFromHint(FailPointKey.FP_OMC_PHYSICAL_DDL_INTERRUPTED, executionContext, () -> {
                DdlContext ddlContext = executionContext.getDdlContext();
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INTERRUPTED, String.valueOf(ddlContext.getJobId()),
                    ddlContext.getSchemaName(), ddlContext.getObjectName());
            });

            try {
                call.accept(phyDdlContext);
            } catch (Throwable e) {
                phyDdlExceptions.add(e);
            }
        }
    }

    public static void executeInstanceConcurrent(ExecutionContext executionContext, String schemaName,
                                                 List<OmcPhyDdlContext> phyDdlContexts,
                                                 List<Throwable> phyDdlExceptions,
                                                 Consumer<OmcPhyDdlContext> call) {
        String traceId = executionContext.getTraceId();
        List<FutureTask<Void>> phyDdlTasks = new ArrayList<>();

        Map<String, List<OmcPhyDdlContext>> phyDdlContextsMap = new HashMap<>();
        phyDdlContexts.forEach(
            o -> phyDdlContextsMap.computeIfAbsent(o.getOmcStorageInfo().getStorageId(), k -> new ArrayList<>())
                .add(o));

        Map mdcContext = MDC.getCopyOfContextMap();
        for (List<OmcPhyDdlContext> phyDdlContextList : phyDdlContextsMap.values()) {
            // Inter-instance in parallel and intra-instance sequentially
            FutureTask<Void> task = new FutureTask<>(
                () -> {
                    MDC.setContextMap(mdcContext);
                    for (OmcPhyDdlContext phyDdlContext : phyDdlContextList) {
                        if (CrossEngineValidator.isJobInterrupted(executionContext)) {
                            long jobId = executionContext.getDdlJobId();
                            phyDdlExceptions.add(new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                                "The job '" + jobId + "' has been cancelled"));
                            break;
                        }

                        FailPoint.injectFromHint(FailPointKey.FP_OMC_PHYSICAL_DDL_INTERRUPTED, executionContext, () -> {
                            DdlContext ddlContext = executionContext.getDdlContext();
                            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INTERRUPTED,
                                String.valueOf(ddlContext.getJobId()),
                                ddlContext.getSchemaName(), ddlContext.getObjectName());
                        });

                        try {
                            call.accept(phyDdlContext);
                        } catch (Throwable e) {
                            phyDdlExceptions.add(e);
                        }
                    }
                }, null);
            phyDdlTasks.add(task);

            executionContext.getExecutorService().submit(schemaName, traceId, AsyncTask.build(task));
        }

        for (FutureTask<Void> task : phyDdlTasks) {
            try {
                task.get();
            } catch (Throwable e) {
                LOG.error("execute phy omc ddl failed", e);
                phyDdlExceptions.add(e);
            }
        }
    }
}
