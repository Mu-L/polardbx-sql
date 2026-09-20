package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.columnar.ExternalizedInsertWritePlanHook;
import com.alibaba.polardbx.executor.columnar.ExternalizedModifyWritePlanHook;
import com.alibaba.polardbx.executor.columnar.TransactionalStagingWriteBatch;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.DmlWriteContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.LogicalModifyViewInputHook;
import com.alibaba.polardbx.optimizer.core.rel.dml.PhysicalRoute;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedInsertInput;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedModifyInput;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedWriteInput;
import com.alibaba.polardbx.optimizer.core.rel.dml.RowWriteBinding;
import com.alibaba.polardbx.optimizer.core.rel.dml.WritePlanHook;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Externalized-value implementation of the statement-scoped Writer instrumentation carried by ExecutionContext.
 */
public final class ExternalizedDmlWriteContext implements DmlWriteContext {
    private final LogicalInsert logicalInsert;
    private final TableMeta primaryTableMeta;
    private final ExternalizedExactRowTransformer exactRowTransformer;

    private final IdentityHashMap<ExecutionContext,
        IdentityHashMap<Writer, WritePlanHook<? extends RoutedWriteInput>>> activeHooks = new IdentityHashMap<>();
    private final IdentityHashMap<RelNode, StagingCompletion> completionByStagingPlan = new IdentityHashMap<>();
    private final IdentityHashMap<DistinctWriter,
        BiFunction<RoutedModifyInput, ExecutionContext, TransactionalStagingWriteBatch>> modifyMaterializers =
        new IdentityHashMap<>();
    private LogicalModifyViewInputHook modifyViewInputHook;

    public ExternalizedDmlWriteContext(LogicalInsert logicalInsert, TableMeta primaryTableMeta,
                                       Map<? extends Writer, List<RowWriteBinding>> exactRowTransforms,
                                       ExecutionContext executionContext) {
        this(logicalInsert, primaryTableMeta, exactRowTransforms, new IdentityHashMap<>(), executionContext);
    }

    public ExternalizedDmlWriteContext(LogicalInsert logicalInsert, TableMeta primaryTableMeta,
                                       Map<? extends Writer, List<RowWriteBinding>> exactRowTransforms,
                                       IdentityHashMap<List<Object>, Object> canonicalRowKeys,
                                       ExecutionContext executionContext) {
        this.logicalInsert = logicalInsert;
        this.primaryTableMeta = primaryTableMeta;
        this.exactRowTransformer =
            new ExternalizedExactRowTransformer(exactRowTransforms, canonicalRowKeys, executionContext);
    }

    @Override
    public List<List<Object>> prepareModifyRows(DistinctWriter writer, List<List<Object>> logicalRows,
                                                ExecutionContext executionContext) {
        return exactRowTransformer.prepareModifyRows(writer, logicalRows, executionContext);
    }

    @Override
    public List<List<Object>> getPreparedModifyRows(DistinctWriter writer, ExecutionContext executionContext) {
        return exactRowTransformer.getPreparedModifyRows(writer, executionContext);
    }

    @Override
    public void prepareInsertRows(InsertWriter writer, List<List<Object>> logicalRows,
                                  ExecutionContext executionContext) {
        exactRowTransformer.prepareInsertRows(writer, logicalRows, executionContext);
    }

    @Override
    public void beforeInsertPlans(InsertWriter writer, RoutedInsertInput routedInput,
                                  ExecutionContext executionContext) {
        WritePlanHook<RoutedInsertInput> hook = exactRowTransformer.takeInsertHook(writer, executionContext);
        if (hook == null && isPrimaryTableWriter(writer) && needsParameterMaterialization()) {
            hook = new ExternalizedInsertWritePlanHook(logicalInsert, primaryTableMeta);
        }
        if (hook == null || hook.isNoop()) {
            return;
        }
        hook.materialize(routedInput, executionContext);
        rememberActiveHook(executionContext, writer, hook);
    }

    @Override
    public void beforeModifyPlans(DistinctWriter writer, RoutedModifyInput routedInput,
                                  ExecutionContext executionContext) {
        WritePlanHook<RoutedModifyInput> hook = exactRowTransformer.takeModifyHook(writer, executionContext);
        if (hook == null) {
            BiFunction<RoutedModifyInput, ExecutionContext, TransactionalStagingWriteBatch> materializer;
            synchronized (this) {
                materializer = modifyMaterializers.get(writer);
            }
            if (materializer != null) {
                hook = new ExternalizedModifyWritePlanHook(materializer);
            }
        }
        if (hook == null || hook.isNoop()) {
            return;
        }
        hook.materialize(routedInput, executionContext);
        rememberActiveHook(executionContext, writer, hook);
    }

    public synchronized void registerModifyMaterializer(
        DistinctWriter writer,
        BiFunction<RoutedModifyInput, ExecutionContext, TransactionalStagingWriteBatch> materializer) {
        modifyMaterializers.put(writer, materializer);
    }

    public synchronized void registerModifyViewInputHook(LogicalModifyViewInputHook inputHook) {
        if (modifyViewInputHook != null) {
            throw new IllegalStateException("LogicalModifyView materializer was registered more than once");
        }
        modifyViewInputHook = inputHook;
    }

    @Override
    public List<RelNode> afterInsertPlans(InsertWriter writer, List<RelNode> primaryPlans,
                                          ExecutionContext executionContext) {
        return attachStagingPlans(executionContext, writer, primaryPlans);
    }

    @Override
    public List<RelNode> afterModifyPlans(DistinctWriter writer, List<RelNode> primaryPlans,
                                          ExecutionContext executionContext) {
        return attachStagingPlans(executionContext, writer, primaryPlans);
    }

    @Override
    public synchronized Map<Integer, ParameterContext> materializeModifyViewParameters(
        PhysicalRoute route, Map<Integer, ParameterContext> parameters, ExecutionContext executionContext) {
        return modifyViewInputHook == null
            ? parameters : modifyViewInputHook.materializeParameters(route, parameters, executionContext);
    }

    @Override
    public synchronized List<RelNode> buildModifyViewStagingPlans(List<RelNode> unmergedPrimaryPlans,
                                                                  ExecutionContext executionContext) {
        if (modifyViewInputHook == null) {
            return Collections.emptyList();
        }
        List<RelNode> stagingPlans = modifyViewInputHook.buildStagingPlans(unmergedPrimaryPlans, executionContext);
        registerStagingCompletion(stagingPlans, modifyViewInputHook::afterExecutionSuccess);
        return stagingPlans;
    }

    @Override
    public synchronized void afterExecutionSuccess(List<RelNode> physicalPlans) {
        IdentityHashMap<RelNode, Boolean> executedPlans = new IdentityHashMap<>();
        for (RelNode plan : physicalPlans) {
            executedPlans.put(plan, Boolean.TRUE);
        }

        Set<StagingCompletion> completions =
            Collections.newSetFromMap(new IdentityHashMap<>());
        for (RelNode plan : physicalPlans) {
            if (!(plan instanceof BaseQueryOperation) || !((BaseQueryOperation) plan).isStagingRelNode()) {
                continue;
            }
            StagingCompletion completion = completionByStagingPlan.get(plan);
            if (completion == null) {
                throw new IllegalStateException("Externalized staging plan has no statement write context");
            }
            completions.add(completion);
        }

        for (StagingCompletion completion : completions) {
            if (completion.plans.stream().anyMatch(plan -> !executedPlans.containsKey(plan))) {
                throw new IllegalStateException("Externalized staging batch was split across physical executions");
            }
            for (RelNode plan : completion.plans) {
                completionByStagingPlan.remove(plan);
            }
            completion.callback.run();
        }
    }

    private synchronized void rememberActiveHook(ExecutionContext executionContext, Writer writer,
                                                 WritePlanHook<? extends RoutedWriteInput> hook) {
        IdentityHashMap<Writer, WritePlanHook<? extends RoutedWriteInput>> hooks =
            activeHooks.computeIfAbsent(executionContext, ignored -> new IdentityHashMap<>());
        if (hooks.put(writer, hook) != null) {
            throw new IllegalStateException("Writer materialization hook was not consumed: "
                + writer.getClass().getName());
        }
    }

    private synchronized List<RelNode> attachStagingPlans(ExecutionContext executionContext, Writer writer,
                                                          List<RelNode> primaryPlans) {
        IdentityHashMap<Writer, WritePlanHook<? extends RoutedWriteInput>> hooks = activeHooks.get(executionContext);
        WritePlanHook<? extends RoutedWriteInput> hook = hooks == null ? null : hooks.remove(writer);
        if (hooks != null && hooks.isEmpty()) {
            activeHooks.remove(executionContext);
        }
        if (hook == null) {
            return primaryPlans;
        }

        List<RelNode> stagingPlans = hook.buildStagingPlans(primaryPlans, executionContext);
        if (stagingPlans.isEmpty()) {
            return primaryPlans;
        }
        registerStagingCompletion(stagingPlans, hook::afterExecutionSuccess);

        List<RelNode> result = new ArrayList<>(stagingPlans.size() + primaryPlans.size());
        result.addAll(stagingPlans);
        result.addAll(primaryPlans);
        return result;
    }

    private void registerStagingCompletion(List<RelNode> stagingPlans, Runnable callback) {
        if (stagingPlans.isEmpty()) {
            return;
        }
        StagingCompletion completion = new StagingCompletion(callback);
        for (RelNode stagingPlan : stagingPlans) {
            BaseQueryOperation operation = (BaseQueryOperation) stagingPlan;
            operation.setStagingRelNode(true);
            if (completionByStagingPlan.put(stagingPlan, completion) != null) {
                throw new IllegalStateException("Staging physical plan was registered more than once");
            }
            completion.plans.add(stagingPlan);
        }
    }

    private boolean needsParameterMaterialization() {
        return logicalInsert != null && primaryTableMeta != null
            && (ExternalizedDmlRewriter.needsHandling(primaryTableMeta)
            || GeneralUtil.isNotEmpty(logicalInsert.getExternalizedUpsertPushdownBindings()));
    }

    private boolean isPrimaryTableWriter(InsertWriter writer) {
        if (primaryTableMeta == null) {
            return false;
        }
        Pair<String, String> target = RelUtils.getQualifiedTableName(writer.getTargetTable());
        return primaryTableMeta.getSchemaName().equalsIgnoreCase(target.left)
            && primaryTableMeta.getTableName().equalsIgnoreCase(target.right);
    }

    private static final class StagingCompletion {
        private final Set<RelNode> plans = Collections.newSetFromMap(new IdentityHashMap<>());
        private final Runnable callback;

        private StagingCompletion(Runnable callback) {
            this.callback = callback;
        }
    }
}
