package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.executor.columnar.ExternalizedInsertRowsWritePlanHook;
import com.alibaba.polardbx.executor.columnar.ExternalizedModifyWritePlanHook;
import com.alibaba.polardbx.executor.columnar.ExternalizedWriteManager;
import com.alibaba.polardbx.executor.columnar.TransactionalStagingWriteBatch;
import com.alibaba.polardbx.executor.function.calc.FetchBlob;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BlobType;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedWriteBinding;
import com.alibaba.polardbx.optimizer.core.rel.dml.PhysicalRoute;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedInsertInput;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedModifyInput;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedWriteInput;
import com.alibaba.polardbx.optimizer.core.rel.dml.RowWriteBinding;
import com.alibaba.polardbx.optimizer.core.rel.dml.WritePlanHook;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Produces externalized physical values on branch-local LogicalRelocate row copies.
 *
 * <p>Relocate classification, unchanged-row comparison, and shard routing consume one logical after-row, but its
 * UPDATE and DELETE+INSERT leaves can require different physical content/addr layouts. Planner-compiled bindings are
 * keyed by the exact leaf writer; transform() copies only rows for a leaf that needs a physical representation and
 * leaves the shared logical rows intact. With an empty transform map the caller keeps the original writer path
 * unchanged.
 *
 * <p>There are five intentionally different write intents:
 * <ul>
 *     <li>MATERIALIZE_NEW runs only on the primary owner, writes staging, and records a canonical BlobRef.</li>
 *     <li>REUSE_EXISTING_ADDR copies a raw old-row address, for example a partition-key relocate whose external
 *     column is absent from SET.</li>
 *     <li>REMATERIALIZE_FOR_PRIMARY_REINSERT fetches an unchanged old value and allocates a new transactional
 *     staging address for the primary-table INSERT branch.</li>
 *     <li>CONSUME_CANONICAL_ADDR is used by GSI leaves after the primary owner. It reads the recorded BlobRef and is
 *     forbidden from writing staging.</li>
 *     <li>CONSUME_CANONICAL_OR_REUSE_EXISTING_ADDR is used when a GSI relocate row may or may not follow a
 *     primary relocate row. It consumes the primary's new address when present, otherwise preserves the old raw
 *     address without FETCH_BLOB or staging.</li>
 * </ul>
 */
public class ExternalizedExactRowTransformer {

    private final IdentityHashMap<Writer, List<RowWriteBinding>> transformsByWriter;
    private final ExternalizedWriteManager writeManager;
    private final ExecutionContext executionContext;
    // One transformer is scoped to one RowSet batch. Identity, rather than row equality, is intentional: two rows
    // may contain identical values but route to different primary branches and therefore own different BlobRefs.
    // LogicalRelocate callbacks share one source List instance. UPSERT can expose the same logical row through a
    // merged UPDATE row and a full after-image, so canonicalRowKeys may join those two representations explicitly.
    private final IdentityHashMap<Object, Map<String, Object>> canonicalAddrBySourceRow =
        new IdentityHashMap<>();
    private final IdentityHashMap<List<Object>, Object> canonicalRowKeys;
    private final IdentityHashMap<ExecutionContext, IdentityHashMap<Writer, WritePlanHook<? extends RoutedWriteInput>>>
        pendingHooks = new IdentityHashMap<>();
    private final IdentityHashMap<ExecutionContext, IdentityHashMap<Writer, List<List<Object>>>> preparedModifyRows =
        new IdentityHashMap<>();

    /**
     * Return whether the planner produced at least one leaf-specific externalized-row transform.
     *
     * @param transformsByWriter compiled transforms keyed by exact Writer identity
     * @return {@code true} when at least one Writer needs exact-row preparation
     */
    public static boolean isRequired(Map<? extends Writer, List<RowWriteBinding>> transformsByWriter) {
        return transformsByWriter != null && !transformsByWriter.isEmpty();
    }

    /**
     * Create a statement-scoped transformer when every logical row representation can serve as its own canonical
     * identity.
     *
     * @param transformsByWriter compiled transforms keyed by exact Writer identity
     * @param executionContext statement execution context used for metadata lookup and materialization
     */
    public ExternalizedExactRowTransformer(Map<? extends Writer, List<RowWriteBinding>> transformsByWriter,
                                           ExecutionContext executionContext) {
        this(transformsByWriter, new IdentityHashMap<>(), executionContext);
    }

    /**
     * Create a statement-scoped transformer with explicit aliases between different row-list representations of the
     * same logical row.
     *
     * <p>UPSERT, for example, can expose one row as both a merged UPDATE row and a full after-image. The alias map
     * lets both representations share the primary owner's canonical external address.</p>
     *
     * @param transformsByWriter compiled transforms keyed by exact Writer identity
     * @param canonicalRowKeys optional row-identity aliases shared by primary, GSI, and replica leaves
     * @param executionContext statement execution context used for metadata lookup and materialization
     */
    public ExternalizedExactRowTransformer(Map<? extends Writer, List<RowWriteBinding>> transformsByWriter,
                                           IdentityHashMap<List<Object>, Object> canonicalRowKeys,
                                           ExecutionContext executionContext) {
        this.transformsByWriter = new IdentityHashMap<>();
        if (transformsByWriter != null) {
            this.transformsByWriter.putAll(transformsByWriter);
        }
        this.canonicalRowKeys = canonicalRowKeys == null ? new IdentityHashMap<>() : canonicalRowKeys;
        this.executionContext = executionContext;
        this.writeManager = new ExternalizedWriteManager(executionContext);
    }

    /**
     * Prepare the exact rows consumed by one distinct modify leaf before its route is calculated.
     *
     * <p>The input rows have already passed SQL expression evaluation, conflict handling, Writer classification, and
     * deduplication. This method copies rows when their logical and physical layouts differ, immediately applies
     * reusable or already-canonical addresses, and registers a one-shot hook for work that requires the owner route.
     * The returned rows preserve input order and cardinality and are used for both routing and plan construction.</p>
     *
     * @param writer exact leaf Writer whose bindings must be applied
     * @param logicalRows Writer-selected rows in their current logical or mixed representation
     * @param executionContext execution context used by this Writer invocation
     * @return exact rows to route and place in physical plans
     */
    public List<List<Object>> prepareModifyRows(DistinctWriter writer, List<List<Object>> logicalRows,
                                                ExecutionContext executionContext) {
        List<RowWriteBinding> transforms = transformsByWriter.get(writer);
        if (transforms == null || transforms.isEmpty()) {
            rememberPreparedModifyRows(executionContext, writer, logicalRows);
            return logicalRows;
        }
        if (logicalRows == null) {
            throw executorMappingError(writer, "logical row batch is null");
        }
        if (logicalRows.isEmpty()) {
            rememberPreparedModifyRows(executionContext, writer, Collections.emptyList());
            return Collections.emptyList();
        }

        ExternalizedWriteManager.RowWriteRequest request =
            ExternalizedWriteManager.RowWriteRequest.copying(logicalRows);
        final List<RowWriteBinding> reuseBindings = bindingsWithIntent(transforms,
            RowWriteBinding.WriteIntent.REUSE_EXISTING_ADDR);
        final List<RowWriteBinding> materializeBindings = bindingsWithIntent(transforms,
            RowWriteBinding.WriteIntent.MATERIALIZE_NEW);
        final List<RowWriteBinding> reinsertBindings = bindingsWithIntent(transforms,
            RowWriteBinding.WriteIntent.REMATERIALIZE_FOR_PRIMARY_REINSERT);
        final List<RowWriteBinding> consumeBindings = bindingsWithIntent(transforms,
            RowWriteBinding.WriteIntent.CONSUME_CANONICAL_ADDR);
        final List<RowWriteBinding> consumeOrReuseBindings = bindingsWithIntent(transforms,
            RowWriteBinding.WriteIntent.CONSUME_CANONICAL_OR_REUSE_EXISTING_ADDR);
        final boolean binlogCompatibilityEnabled =
            ExternalizedWriteManager.isBinlogCompatibilityEnabled(executionContext);
        if (binlogCompatibilityEnabled) {
            materializeBindings.addAll(reinsertBindings);
        } else {
            reuseBindings.addAll(reinsertBindings);
        }
        applyReuseExistingAddr(request, reuseBindings);
        rememberCanonicalAddresses(request, reuseBindings);

        // REUSE_EXISTING_ADDR and CONSUME_CANONICAL_ADDR never own an object. In particular, a GSI-only relocate
        // reuses its old raw address, while a GSI that follows a primary relocate consumes the primary canonical
        // address. Neither case may derive a staging route from the GSI target shard.
        if (materializeBindings.isEmpty()) {
            applyCanonicalAddresses(request, consumeBindings, writer);
            applyCanonicalOrExistingAddresses(request, consumeOrReuseBindings, writer);
            rememberPreparedModifyRows(executionContext, writer, request.getTargetRows());
            return request.getTargetRows();
        }

        final List<RowWriteBinding> activeReinsertBindings =
            binlogCompatibilityEnabled ? reinsertBindings : Collections.emptyList();
        final ExternalizedWriteManager.RowWriteRequest materializationRequest = activeReinsertBindings.isEmpty()
            ? request : buildMaterializationRequest(request, activeReinsertBindings);

        ExternalizedModifyWritePlanHook hook = new ExternalizedModifyWritePlanHook((routedInput, hookEc) -> {
            Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> ownerRoutes = new HashMap<>();
            for (Map.Entry<Integer, PhysicalRoute> entry : routedInput.getRouteByRowIndex().entrySet()) {
                PhysicalRoute route = entry.getValue();
                ownerRoutes.put(entry.getKey(), new TransactionalStagingWriteBatch.OwnerRoute(
                    route.getSchemaName(), route.getGroupName(), route.getPhysicalTableName()));
            }
            TransactionalStagingWriteBatch stagingBatch =
                writeManager.materializeRouted(materializationRequest, materializeBindings, ownerRoutes);
            if (materializationRequest != request) {
                copyMaterializedAddresses(materializationRequest, request, materializeBindings);
            }
            rememberCanonicalAddresses(request, materializeBindings);
            applyCanonicalAddresses(request, consumeBindings, writer);
            applyCanonicalOrExistingAddresses(request, consumeOrReuseBindings, writer);
            return stagingBatch;
        });
        rememberPendingHook(executionContext, writer, hook);
        rememberPreparedModifyRows(executionContext, writer, request.getTargetRows());
        return request.getTargetRows();
    }

    /**
     * Look up the exact rows recorded by {@link #prepareModifyRows} for a replication wrapper.
     *
     * <p>The lookup is non-consuming: primary and replica expansion may both need the same prepared row objects.
     * Writer and ExecutionContext identity, rather than equality, delimit each entry.</p>
     *
     * @param writer exact Writer whose prepared rows are requested
     * @param executionContext execution context used to prepare the rows
     * @return the prepared rows, or {@code null} when this Writer has not prepared a batch in this context
     */
    public synchronized List<List<Object>> getPreparedModifyRows(DistinctWriter writer,
                                                                 ExecutionContext executionContext) {
        IdentityHashMap<Writer, List<List<Object>>> rows = preparedModifyRows.get(executionContext);
        return rows == null ? null : rows.get(writer);
    }

    /**
     * Retain the exact primary rows so a replication Writer does not rerun row transformation or allocate another
     * external address.
     */
    private synchronized void rememberPreparedModifyRows(ExecutionContext executionContext, Writer writer,
                                                         List<List<Object>> rows) {
        preparedModifyRows.computeIfAbsent(executionContext, ignored -> new IdentityHashMap<>()).put(writer, rows);
    }

    /**
     * Prepare rows selected by a parent Writer for a child {@link InsertWriter} that consumes batch parameters.
     *
     * <p>This method mirrors {@link #prepareModifyRows} but writes the prepared rows back into the child
     * ExecutionContext as JDBC batch parameters. Route-independent address reuse happens immediately; new object
     * materialization is registered as a one-shot hook and waits until the InsertWriter has calculated owner routes.
     * SQL evaluation and Writer action classification must already be complete.</p>
     *
     * @param writer exact INSERT leaf whose bindings must be applied
     * @param logicalRows rows selected for that INSERT leaf
     * @param executionContext child execution context whose batch parameters feed the InsertWriter
     */
    public void prepareInsertRows(InsertWriter writer, List<List<Object>> logicalRows,
                                  ExecutionContext executionContext) {
        List<RowWriteBinding> transforms = transformsByWriter.get(writer);
        if (logicalRows == null) {
            throw executorMappingError(writer, "logical row batch is null");
        }
        if (logicalRows.isEmpty()) {
            replaceInsertParameters(executionContext, Collections.emptyList());
            rememberPendingHook(executionContext, writer, WritePlanHook.noop());
            return;
        }

        ExternalizedWriteManager.RowWriteRequest request =
            ExternalizedWriteManager.RowWriteRequest.copying(logicalRows);
        if (transforms == null || transforms.isEmpty()) {
            replaceInsertParameters(executionContext, request.getTargetRows());
            rememberPendingHook(executionContext, writer, WritePlanHook.noop());
            return;
        }

        final List<RowWriteBinding> reuseBindings = bindingsWithIntent(transforms,
            RowWriteBinding.WriteIntent.REUSE_EXISTING_ADDR);
        final List<RowWriteBinding> materializeBindings = bindingsWithIntent(transforms,
            RowWriteBinding.WriteIntent.MATERIALIZE_NEW);
        final List<RowWriteBinding> reinsertBindings = bindingsWithIntent(transforms,
            RowWriteBinding.WriteIntent.REMATERIALIZE_FOR_PRIMARY_REINSERT);
        final List<RowWriteBinding> consumeBindings = bindingsWithIntent(transforms,
            RowWriteBinding.WriteIntent.CONSUME_CANONICAL_ADDR);
        final List<RowWriteBinding> consumeOrReuseBindings = bindingsWithIntent(transforms,
            RowWriteBinding.WriteIntent.CONSUME_CANONICAL_OR_REUSE_EXISTING_ADDR);
        final boolean binlogCompatibilityEnabled =
            ExternalizedWriteManager.isBinlogCompatibilityEnabled(executionContext);
        if (binlogCompatibilityEnabled) {
            materializeBindings.addAll(reinsertBindings);
        } else {
            reuseBindings.addAll(reinsertBindings);
        }
        applyReuseExistingAddr(request, reuseBindings);
        rememberCanonicalAddresses(request, reuseBindings);

        if (materializeBindings.isEmpty()) {
            applyCanonicalAddresses(request, consumeBindings, writer);
            applyCanonicalOrExistingAddresses(request, consumeOrReuseBindings, writer);
            replaceInsertParameters(executionContext, request.getTargetRows());
            rememberPendingHook(executionContext, writer, WritePlanHook.noop());
            return;
        }

        final List<RowWriteBinding> activeReinsertBindings =
            binlogCompatibilityEnabled ? reinsertBindings : Collections.emptyList();
        final ExternalizedWriteManager.RowWriteRequest materializationRequest = activeReinsertBindings.isEmpty()
            ? request : buildMaterializationRequest(request, activeReinsertBindings);
        replaceInsertParameters(executionContext, request.getTargetRows());

        ExternalizedInsertRowsWritePlanHook hook = new ExternalizedInsertRowsWritePlanHook((routedInput, hookEc) -> {
            Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> ownerRoutes = ownerRoutes(routedInput);
            TransactionalStagingWriteBatch stagingBatch =
                writeManager.materializeRouted(materializationRequest, materializeBindings, ownerRoutes);
            if (materializationRequest != request) {
                copyMaterializedAddresses(materializationRequest, request, materializeBindings);
            }
            rememberCanonicalAddresses(request, materializeBindings);
            applyCanonicalAddresses(request, consumeBindings, writer);
            applyCanonicalOrExistingAddresses(request, consumeOrReuseBindings, writer);
            replaceInsertParameters(executionContext, request.getTargetRows());
            return stagingBatch;
        });
        rememberPendingHook(executionContext, writer, hook);
    }

    /**
     * Remove and return the one-shot route-dependent hook registered for a distinct modify leaf.
     *
     * @param writer exact modify leaf that prepared the hook
     * @param executionContext execution context that owns the hook
     * @return pending hook, including an explicit no-op hook, or {@code null} if the leaf prepared no hook
     */
    @SuppressWarnings("unchecked")
    public WritePlanHook<RoutedModifyInput> takeModifyHook(DistinctWriter writer,
                                                           ExecutionContext executionContext) {
        return (WritePlanHook<RoutedModifyInput>) takePendingHook(executionContext, writer);
    }

    /**
     * Remove and return the one-shot route-dependent hook registered for an INSERT leaf.
     *
     * @param writer exact INSERT leaf that prepared the hook
     * @param executionContext execution context that owns the hook
     * @return pending hook, including an explicit no-op hook, or {@code null} if the leaf prepared no hook
     */
    @SuppressWarnings("unchecked")
    public WritePlanHook<RoutedInsertInput> takeInsertHook(InsertWriter writer,
                                                           ExecutionContext executionContext) {
        return (WritePlanHook<RoutedInsertInput>) takePendingHook(executionContext, writer);
    }

    /**
     * Register exactly one hook between row preparation and the corresponding post-route callback.
     */
    private synchronized void rememberPendingHook(ExecutionContext executionContext, Writer writer,
                                                  WritePlanHook<? extends RoutedWriteInput> hook) {
        IdentityHashMap<Writer, WritePlanHook<? extends RoutedWriteInput>> hooks =
            pendingHooks.computeIfAbsent(executionContext, ignored -> new IdentityHashMap<>());
        if (hooks.put(writer, hook) != null) {
            throw executorMappingError(writer, "previous row materialization was not consumed");
        }
    }

    /**
     * Consume the hook for one Writer invocation so stale materialization state cannot leak into a later plan build.
     */
    private synchronized WritePlanHook<? extends RoutedWriteInput> takePendingHook(
        ExecutionContext executionContext, Writer writer) {
        IdentityHashMap<Writer, WritePlanHook<? extends RoutedWriteInput>> hooks = pendingHooks.get(executionContext);
        if (hooks == null) {
            return null;
        }
        WritePlanHook<? extends RoutedWriteInput> hook = hooks.remove(writer);
        if (hooks.isEmpty()) {
            pendingHooks.remove(executionContext);
        }
        return hook;
    }

    private static void replaceInsertParameters(ExecutionContext executionContext, List<List<Object>> rows) {
        List<Map<Integer, ParameterContext>> parameters = BuildPlanUtils.buildInsertBatchParam(rows);
        executionContext.setParams(new Parameters(parameters));
    }

    private static Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> ownerRoutes(
        RoutedInsertInput routedInput) {
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> ownerRoutes = new HashMap<>();
        for (Map.Entry<Integer, PhysicalRoute> entry : routedInput.getRouteByRowIndex().entrySet()) {
            PhysicalRoute route = entry.getValue();
            ownerRoutes.put(entry.getKey(), new TransactionalStagingWriteBatch.OwnerRoute(
                route.getSchemaName(), route.getGroupName(), route.getPhysicalTableName()));
        }
        return ownerRoutes;
    }

    private static List<RowWriteBinding> bindingsWithIntent(List<RowWriteBinding> bindings,
                                                            RowWriteBinding.WriteIntent intent) {
        final List<RowWriteBinding> result = new ArrayList<>();
        for (RowWriteBinding binding : bindings) {
            if (binding.getWriteIntent() == intent) {
                result.add(binding);
            }
        }
        return result;
    }

    private static void applyReuseExistingAddr(ExternalizedWriteManager.RowWriteRequest request,
                                               List<RowWriteBinding> bindings) {
        if (bindings.isEmpty()) {
            return;
        }
        for (int rowIndex = 0; rowIndex < request.getSourceRows().size(); rowIndex++) {
            final List<Object> sourceRow = request.getSourceRows().get(rowIndex);
            final List<Object> targetRow = request.getTargetRows().get(rowIndex);
            for (RowWriteBinding binding : bindings) {
                if (binding.getExistingAddrRowIndex() >= sourceRow.size()
                    || binding.getTargetRowIndex() >= targetRow.size()) {
                    throw executorMappingError("reuse binding row is too short for "
                        + binding.getContentColumnName());
                }
                // Example: a DELETE+INSERT primary relocate changes id but not content. The logical source slot has
                // already been rewritten from FETCH_BLOB(content_addr_) to content_addr_, so copying it preserves the
                // old object identity without a fetch or staging write.
                targetRow.set(binding.getTargetRowIndex(), sourceRow.get(binding.getExistingAddrRowIndex()));
            }
        }
    }

    /**
     * Rematerialization has two representations in one executor row: routing needs the old physical BlobRef, while
     * ExternalizedWriteManager needs logical content. Build a private source copy and restore terminal values with
     * FETCH_BLOB; migration rows already carry their logical content beside the old addr slot.
     */
    private ExternalizedWriteManager.RowWriteRequest buildMaterializationRequest(
        ExternalizedWriteManager.RowWriteRequest physicalRequest,
        List<RowWriteBinding> reinsertBindings) {
        final List<List<Object>> logicalSources = new ArrayList<>(physicalRequest.getSourceRows().size());
        for (List<Object> sourceRow : physicalRequest.getSourceRows()) {
            logicalSources.add(new ArrayList<>(sourceRow));
        }
        if (!reinsertBindings.isEmpty()) {
            final FetchBlob fetchBlob = new FetchBlob();
            for (List<Object> logicalSource : logicalSources) {
                for (RowWriteBinding binding : reinsertBindings) {
                    if (binding.getSourceRowIndex() >= logicalSource.size()) {
                        throw executorMappingError("reinsert binding row is too short for "
                            + binding.getContentColumnName());
                    }
                    if (binding.getAction() != ExternalizedWriteBinding.Action.TERMINAL_REPLACE) {
                        continue;
                    }
                    if (binding.getExistingAddrRowIndex() >= logicalSource.size()) {
                        throw executorMappingError("reinsert address row is too short for "
                            + binding.getContentColumnName());
                    }
                    final Object blobRef = logicalSource.get(binding.getExistingAddrRowIndex());
                    if (blobRef == null) {
                        logicalSource.set(binding.getSourceRowIndex(), null);
                        continue;
                    }
                    final ColumnMeta columnMeta = executionContext
                        .getSchemaManager(binding.getStorageSchemaName())
                        .getTable(binding.getStorageTableName())
                        .getColumnIgnoreCase(binding.getContentColumnName());
                    if (columnMeta == null) {
                        throw executorMappingError("logical metadata is missing for "
                            + binding.getContentColumnName());
                    }
                    final String typeFamily = columnMeta.getDataType() instanceof BlobType ? "BLOB" : "TEXT";
                    logicalSource.set(binding.getSourceRowIndex(), fetchBlob.compute(new Object[] {
                        blobRef, binding.getStorageSchemaName(), binding.getStorageTableName(),
                        binding.getContentColumnName(), typeFamily
                    }, executionContext));
                }
            }
        }
        return ExternalizedWriteManager.RowWriteRequest.copying(logicalSources);
    }

    private static void copyMaterializedAddresses(ExternalizedWriteManager.RowWriteRequest source,
                                                  ExternalizedWriteManager.RowWriteRequest target,
                                                  List<RowWriteBinding> bindings) {
        for (int rowIndex = 0; rowIndex < source.getTargetRows().size(); rowIndex++) {
            final List<Object> sourceRow = source.getTargetRows().get(rowIndex);
            final List<Object> targetRow = target.getTargetRows().get(rowIndex);
            for (RowWriteBinding binding : bindings) {
                if (binding.getTargetRowIndex() >= targetRow.size()) {
                    if (!target.isTargetGrowthAllowed()) {
                        throw executorMappingError("materialized target row is too short for "
                            + binding.getContentColumnName());
                    }
                    while (targetRow.size() < binding.getRequiredRowWidth()) {
                        targetRow.add(null);
                    }
                }
                targetRow.set(binding.getTargetRowIndex(), sourceRow.get(binding.getTargetRowIndex()));
            }
        }
    }

    /**
     * Record the primary owner's result by logical source-row identity. GSI writers receive the same List instance
     * from RowSet, so they can consume this address without re-reading the plaintext or choosing their own route.
     */
    private void rememberCanonicalAddresses(ExternalizedWriteManager.RowWriteRequest request,
                                            List<RowWriteBinding> bindings) {
        for (int rowIndex = 0; rowIndex < request.getSourceRows().size(); rowIndex++) {
            final List<Object> sourceRow = request.getSourceRows().get(rowIndex);
            final List<Object> targetRow = request.getTargetRows().get(rowIndex);
            final Map<String, Object> canonicalByColumn = canonicalAddrBySourceRow.computeIfAbsent(
                canonicalKey(sourceRow), ignored -> new HashMap<>());
            for (RowWriteBinding binding : bindings) {
                final String key = storageKey(binding);
                final Object canonicalAddr = targetRow.get(binding.getTargetRowIndex());
                if (canonicalByColumn.containsKey(key)
                    && !java.util.Objects.equals(canonicalByColumn.get(key), canonicalAddr)) {
                    throw executorMappingError("primary owner produced different canonical addresses for "
                        + binding.getContentColumnName());
                }
                canonicalByColumn.put(key, canonicalAddr);
            }
        }
    }

    private void applyCanonicalAddresses(ExternalizedWriteManager.RowWriteRequest request,
                                         List<RowWriteBinding> bindings, Writer writer) {
        if (bindings.isEmpty()) {
            return;
        }
        for (int rowIndex = 0; rowIndex < request.getSourceRows().size(); rowIndex++) {
            final List<Object> sourceRow = request.getSourceRows().get(rowIndex);
            final List<Object> targetRow = request.getTargetRows().get(rowIndex);
            final Map<String, Object> canonicalByColumn = canonicalAddrBySourceRow.get(canonicalKey(sourceRow));
            for (RowWriteBinding binding : bindings) {
                final String key = storageKey(binding);
                if (canonicalByColumn == null || !canonicalByColumn.containsKey(key)) {
                    throw executorMappingError(writer, "canonical address is missing for "
                        + binding.getContentColumnName() + "; primary owner must execute before GSI/replica leaves");
                }
                targetRow.set(binding.getTargetRowIndex(), canonicalByColumn.get(key));
            }
        }
    }

    private void applyCanonicalOrExistingAddresses(ExternalizedWriteManager.RowWriteRequest request,
                                                   List<RowWriteBinding> bindings, Writer writer) {
        if (bindings.isEmpty()) {
            return;
        }
        for (int rowIndex = 0; rowIndex < request.getSourceRows().size(); rowIndex++) {
            final List<Object> sourceRow = request.getSourceRows().get(rowIndex);
            final List<Object> targetRow = request.getTargetRows().get(rowIndex);
            final Map<String, Object> canonicalByColumn = canonicalAddrBySourceRow.get(canonicalKey(sourceRow));
            for (RowWriteBinding binding : bindings) {
                final String key = storageKey(binding);
                if (canonicalByColumn != null && canonicalByColumn.containsKey(key)) {
                    targetRow.set(binding.getTargetRowIndex(), canonicalByColumn.get(key));
                    continue;
                }
                if (binding.getExistingAddrRowIndex() >= sourceRow.size()) {
                    throw executorMappingError(writer, "existing address row is too short for "
                        + binding.getContentColumnName());
                }
                final Object existingAddr = sourceRow.get(binding.getExistingAddrRowIndex());
                if (existingAddr != null && FetchBlob.decodeBlobRef(existingAddr) == null) {
                    throw executorMappingError(writer, "canonical address is missing and existing value is not a "
                        + "BlobRef for " + binding.getContentColumnName());
                }
                targetRow.set(binding.getTargetRowIndex(), existingAddr);
            }
        }
    }

    private static String storageKey(RowWriteBinding binding) {
        return binding.getStorageSchemaName().toLowerCase(Locale.ROOT) + "\u0000"
            + binding.getStorageTableName().toLowerCase(Locale.ROOT) + "\u0000"
            + binding.getContentColumnName().toLowerCase(Locale.ROOT);
    }

    private Object canonicalKey(List<Object> sourceRow) {
        Object key = canonicalRowKeys.get(sourceRow);
        return key == null ? sourceRow : key;
    }

    private static TddlRuntimeException executorMappingError(DistinctWriter writer, String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Invalid MCE exact-row transform for " + writer.getClass().getSimpleName() + ": " + detail);
    }

    private static TddlRuntimeException executorMappingError(Writer writer, String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Invalid MCE exact-row transform for " + writer.getClass().getSimpleName() + ": " + detail);
    }

    private static TddlRuntimeException executorMappingError(String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Invalid MCE exact-row transform: " + detail);
    }

}
