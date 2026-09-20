package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedWriteBinding;
import com.alibaba.polardbx.optimizer.core.rel.dml.ParameterWriteBinding;
import com.alibaba.polardbx.optimizer.core.rel.dml.RowWriteBinding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Single execution entry for externalized value materialization.
 *
 * <p>The overloaded methods expose the same operation to parameter-backed DML (INSERT-family and
 * statement-constant LogicalModifyView UPDATE) and UPDATE-family executor rows while preserving their incompatible
 * address spaces at compile time.
 */
public final class ExternalizedWriteManager {

    private final ExecutionContext executionContext;
    private final TableIdResolver tableIdResolver;
    private final BlobBatchWriter blobBatchWriter;
    private final IdentityHashMap<List<Object>, Map<String, CachedBlob>> blobRefBySourceRow =
        new IdentityHashMap<>();
    private final Map<String, CachedBlob> statementConstantBlobRefs = new HashMap<>();
    private final Map<TransactionalStagingWriteBatch.OwnerRoute, Map<String, CachedBlob>>
        routedStatementConstantBlobRefs =
        new HashMap<>();
    private final Map<String, Long> tableIds = new HashMap<>();

    public ExternalizedWriteManager(ExecutionContext executionContext) {
        this(executionContext,
            (schema, table, column) ->
                ExternalColumnTableIdResolver.getInstance().resolve(schema, table, column),
            BlobWriter::writeBatch);
    }

    ExternalizedWriteManager(ExecutionContext executionContext, TableIdResolver tableIdResolver,
                             BlobBatchWriter blobBatchWriter) {
        if (executionContext == null || tableIdResolver == null || blobBatchWriter == null) {
            throw invalid("manager dependency is null");
        }
        this.executionContext = executionContext;
        this.tableIdResolver = tableIdResolver;
        this.blobBatchWriter = blobBatchWriter;
    }

    /**
     * Release source-row materialization results after one select-modify batch has completed.
     * Statement constants and resolved table IDs deliberately survive across batches.
     */
    public void clearRowCache() {
        blobRefBySourceRow.clear();
    }

    /**
     * Materialize logical values carried by one-based JDBC parameter maps after primary owner routes are known and
     * before the corresponding business writes are dispatched. This overload serves both INSERT-family writes and
     * statement-constant LogicalModifyView UPDATEs.
     *
     * <p>For example, an MCE UPDATE reaches this method with routed parameters {@code {?1="hello", ?3=NULL}}, a
     * migration binding {@code body(sourceKey=1, targetKey=3)}, and {@code row 0 -> group_0/phy_t_0}. The method reads
     * {@code "hello"} from {@code ?1}, materializes it, and backfills {@code ?3} with the resulting BlobRef. When
     * staging is required, it prepares the companion staging row on the owner route's DN and returns it in the
     * {@link TransactionalStagingWriteBatch}; the caller later places that batch before the owner INSERT/UPDATE.
     * Direct writes perform the same parameter backfill and return an empty batch.
     *
     * <p>This method neither computes routes nor builds or executes business/staging plans.
     */
    public TransactionalStagingWriteBatch materializeRouted(
        ParameterWriteRequest request,
        List<ParameterWriteBinding> bindings,
        int[] logicalRowByBinding,
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> routeByLogicalRow) {
        long writeStart = System.nanoTime();
        try {
            return materializeRoutedParameters(request, bindings, logicalRowByBinding, routeByLogicalRow);
        } finally {
            executionContext.getOrCreateExtColStats().addStagingWrite(System.nanoTime() - writeStart);
        }
    }

    private TransactionalStagingWriteBatch materializeRoutedParameters(
        ParameterWriteRequest request,
        List<ParameterWriteBinding> bindings,
        int[] logicalRowByBinding,
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> routeByLogicalRow) {
        PreparedParameterWrite prepared = preflightParameters(request, bindings);
        TransactionalStagingWriteBatch batch = new TransactionalStagingWriteBatch();
        if (prepared.isEmpty()) {
            return batch;
        }
        if (!request.isBatchWrite()
            && (logicalRowByBinding == null || logicalRowByBinding.length != bindings.size())) {
            throw invalid("logical row mapping does not match binding count");
        }
        if (routeByLogicalRow == null) {
            throw invalid("primary route mapping is null");
        }

        long[] resolvedTableIds = resolveParameterTableIds(prepared, bindings);
        String[][] blobRefs = new String[prepared.values.length][bindings.size()];
        Map<TransactionalStagingWriteBatch.OwnerRoute, String> dnByRoute = new HashMap<>();
        List<BlobWriter.BlobItem> directItems = new ArrayList<>();
        List<int[]> directPositions = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < prepared.values.length; rowIndex++) {
            for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
                byte[] data = prepared.values[rowIndex][bindingIndex];
                if (data == null) {
                    continue;
                }
                if (!shouldUseStaging(data.length)) {
                    directItems.add(new BlobWriter.BlobItem(resolvedTableIds[bindingIndex], data, false));
                    directPositions.add(new int[] {rowIndex, bindingIndex});
                    continue;
                }
                int logicalRow = request.isBatchWrite() ? rowIndex : logicalRowByBinding[bindingIndex];
                TransactionalStagingWriteBatch.OwnerRoute route = routeByLogicalRow.get(logicalRow);
                if (route == null) {
                    throw invalid("missing primary route for logical row " + logicalRow);
                }
                String dnId = dnByRoute.computeIfAbsent(route,
                    ignored -> TransactionalStagingWriteBatch.resolveDnId(
                        route.getSchemaName(), route.getGroupName()));
                int seqId = ExternalStagingTransactionLease.acquire(executionContext.getTransaction(), dnId);
                BlobWriter.PreparedStagingValue staged = BlobWriter.prepareTransactionalStaging(
                    seqId, resolvedTableIds[bindingIndex], data);
                BlobWriter.WriteResult result = BlobWriteReceiptValidator.validate(
                    resolvedTableIds[bindingIndex], staged.getResult(), context(bindings.get(bindingIndex)));
                blobRefs[rowIndex][bindingIndex] = result.getBlobRefHex();
                batch.add(route, seqId, dnId, staged.getRow());
            }
        }
        materializeRoutedParameterDirectWrites(directItems, directPositions, bindings, blobRefs);
        backfillParameters(request, bindings, blobRefs);
        return batch;
    }

    /**
     * Materialize logical values carried by executor rows after UPDATE-family writers have calculated one primary
     * owner route per row and before their business plans are dispatched.
     *
     * <p>For example, given an UPDATE after-image {@code [id, "hello", NULL]}, a binding with
     * {@code sourceRowIndex=1} and {@code targetRowIndex=2}, and {@code row 0 -> group_0/phy_t_0}, the method reads
     * {@code "hello"} from {@code row[1]}, materializes it, and writes the resulting BlobRef into {@code row[2]}.
     * When staging is required, the returned {@link TransactionalStagingWriteBatch} contains the companion row for
     * the same owner DN; the caller later places it before the owner UPDATE/INSERT branch. Direct writes update the
     * target row in the same way and return an empty batch.
     *
     * <p>This method neither computes routes nor builds or executes business/staging plans.
     */
    public TransactionalStagingWriteBatch materializeRouted(
        RowWriteRequest request,
        List<RowWriteBinding> bindings,
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> routeByRowIndex) {
        long writeStart = System.nanoTime();
        try {
            return materializeRoutedRows(request, bindings, routeByRowIndex);
        } finally {
            executionContext.getOrCreateExtColStats().addStagingWrite(System.nanoTime() - writeStart);
        }
    }

    private TransactionalStagingWriteBatch materializeRoutedRows(
        RowWriteRequest request,
        List<RowWriteBinding> bindings,
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> routeByRowIndex) {
        PreparedRowWrite prepared = preflightRows(request, bindings);
        TransactionalStagingWriteBatch batch = new TransactionalStagingWriteBatch();
        if (prepared.isEmpty()) {
            return batch;
        }
        if (routeByRowIndex == null || routeByRowIndex.size() != prepared.values.length) {
            throw invalid("primary UPDATE route mapping does not match row count");
        }
        resolveRowTableIds(prepared, bindings);
        String[][] blobRefs = new String[prepared.values.length][bindings.size()];
        Map<TransactionalStagingWriteBatch.OwnerRoute, String> dnByRoute = new HashMap<>();
        List<BlobWriter.BlobItem> directItems = new ArrayList<>();
        List<PendingRowWrite> pendingDirectWrites = new ArrayList<>();
        IdentityHashMap<Map<String, CachedBlob>, Map<String, Integer>> pendingDirectByCache =
            new IdentityHashMap<>();
        for (int rowIndex = 0; rowIndex < prepared.values.length; rowIndex++) {
            List<Object> sourceRow = request.getSourceRows().get(rowIndex);
            TransactionalStagingWriteBatch.OwnerRoute route = routeByRowIndex.get(rowIndex);
            if (route == null) {
                throw invalid("missing primary UPDATE route for row " + rowIndex);
            }
            for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
                RowWriteBinding binding = bindings.get(bindingIndex);
                byte[] data = prepared.values[rowIndex][bindingIndex];
                String key = storageKey(binding);
                Map<String, CachedBlob> cache = request.isStatementConstant(bindingIndex)
                    ? routedStatementConstantBlobRefs.computeIfAbsent(route, ignored -> new HashMap<>())
                    : blobRefBySourceRow.computeIfAbsent(sourceRow, ignored -> new HashMap<>());
                CachedBlob cached = cache.get(key);
                if (cached != null && !Arrays.equals(cached.content, data)) {
                    throw invalid("cached UPDATE source value changed for " + binding.getContentColumnName());
                }
                if (cached != null) {
                    blobRefs[rowIndex][bindingIndex] = cached.blobRef;
                    continue;
                }
                if (data == null) {
                    cache.put(key, new CachedBlob(null, null));
                    continue;
                }
                Map<String, Integer> cachePending =
                    pendingDirectByCache.computeIfAbsent(cache, ignored -> new HashMap<>());
                Integer pendingIndex = cachePending.get(key);
                if (pendingIndex != null) {
                    PendingRowWrite pending = pendingDirectWrites.get(pendingIndex);
                    if (!Arrays.equals(pending.data, data)) {
                        throw invalid("pending UPDATE source value changed for " + binding.getContentColumnName());
                    }
                    pending.positions.add(new int[] {rowIndex, bindingIndex});
                    continue;
                }
                long tableId = tableIds.get(key);
                if (!shouldUseStaging(data.length)) {
                    cachePending.put(key, directItems.size());
                    directItems.add(new BlobWriter.BlobItem(tableId, data, false));
                    PendingRowWrite pending = new PendingRowWrite(binding, cache, key, data);
                    pending.positions.add(new int[] {rowIndex, bindingIndex});
                    pendingDirectWrites.add(pending);
                    continue;
                }
                String dnId = dnByRoute.computeIfAbsent(route,
                    ignored -> TransactionalStagingWriteBatch.resolveDnId(
                        route.getSchemaName(), route.getGroupName()));
                int seqId = ExternalStagingTransactionLease.acquire(executionContext.getTransaction(), dnId);
                BlobWriter.PreparedStagingValue staged =
                    BlobWriter.prepareTransactionalStaging(seqId, tableId, data);
                BlobWriter.WriteResult result = BlobWriteReceiptValidator.validate(
                    tableId, staged.getResult(), context(binding));
                cache.put(key, new CachedBlob(data, result.getBlobRefHex()));
                blobRefs[rowIndex][bindingIndex] = result.getBlobRefHex();
                batch.add(route, seqId, dnId, staged.getRow());
            }
        }
        materializeRoutedRowDirectWrites(directItems, pendingDirectWrites, blobRefs);
        for (int rowIndex = 0; rowIndex < request.getTargetRows().size(); rowIndex++) {
            List<Object> targetRow = request.getTargetRows().get(rowIndex);
            for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
                targetRow.set(bindings.get(bindingIndex).getTargetRowIndex(), blobRefs[rowIndex][bindingIndex]);
            }
        }
        return batch;
    }

    private void materializeRoutedParameterDirectWrites(
        List<BlobWriter.BlobItem> items,
        List<int[]> positions,
        List<ParameterWriteBinding> bindings,
        String[][] blobRefs) {
        if (items.isEmpty()) {
            return;
        }
        List<BlobWriter.WriteResult> results = blobBatchWriter.write(items);
        if (results == null || results.size() != items.size()) {
            throw invalid("routed parameter batch result count does not match write count");
        }
        for (int i = 0; i < results.size(); i++) {
            int[] position = positions.get(i);
            ParameterWriteBinding binding = bindings.get(position[1]);
            BlobWriter.WriteResult result = BlobWriteReceiptValidator.validate(
                items.get(i).tableId, results.get(i), context(binding));
            blobRefs[position[0]][position[1]] = result.getBlobRefHex();
            track(result);
        }
    }

    private void materializeRoutedRowDirectWrites(
        List<BlobWriter.BlobItem> items,
        List<PendingRowWrite> pendingWrites,
        String[][] blobRefs) {
        if (items.isEmpty()) {
            return;
        }
        List<BlobWriter.WriteResult> results = blobBatchWriter.write(items);
        if (results == null || results.size() != items.size()) {
            throw invalid("routed row batch result count does not match write count");
        }
        for (int i = 0; i < results.size(); i++) {
            PendingRowWrite pending = pendingWrites.get(i);
            BlobWriter.WriteResult result = BlobWriteReceiptValidator.validate(
                items.get(i).tableId, results.get(i), context(pending.binding));
            pending.cache.put(pending.storageKey, new CachedBlob(pending.data, result.getBlobRefHex()));
            for (int[] position : pending.positions) {
                blobRefs[position[0]][position[1]] = result.getBlobRefHex();
            }
            track(result);
        }
    }

    private PreparedParameterWrite preflightParameters(ParameterWriteRequest request,
                                                       List<ParameterWriteBinding> bindings) {
        if (request == null || request.getParameterRows() == null) {
            throw invalid("parameter request is null");
        }
        validateBindings(bindings);
        validateParameterTargets(bindings);
        byte[][][] values = new byte[request.getParameterRows().size()][bindings.size()][];
        for (int rowIndex = 0; rowIndex < request.getParameterRows().size(); rowIndex++) {
            Map<Integer, ParameterContext> parameters = request.getParameterRows().get(rowIndex);
            if (parameters == null) {
                throw invalid("parameter row " + rowIndex + " is null");
            }
            for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
                ParameterWriteBinding binding = bindings.get(bindingIndex);
                ParameterContext source = requireParameter(parameters, binding.getSourceParamKey(),
                    binding.getContentColumnName(), "source");
                requireParameter(parameters, binding.getTargetParamKey(), binding.getContentColumnName(), "target");
                values[rowIndex][bindingIndex] = isNull(source) ? null : parameterBytes(source);
            }
        }
        return new PreparedParameterWrite(values);
    }

    private PreparedRowWrite preflightRows(RowWriteRequest request, List<RowWriteBinding> bindings) {
        if (request == null || request.getSourceRows() == null || request.getTargetRows() == null) {
            throw invalid("row request is null");
        }
        if (request.getSourceRows().size() != request.getTargetRows().size()) {
            throw invalid("source and target row counts differ");
        }
        validateBindings(bindings);
        validateRowTargets(bindings);
        byte[][][] values = new byte[request.getSourceRows().size()][bindings.size()][];
        for (int rowIndex = 0; rowIndex < request.getSourceRows().size(); rowIndex++) {
            List<Object> sourceRow = request.getSourceRows().get(rowIndex);
            List<Object> targetRow = request.getTargetRows().get(rowIndex);
            if (sourceRow == null || targetRow == null) {
                throw invalid("source or target row " + rowIndex + " is null");
            }
            int requiredWidth = targetRow.size();
            Map<String, byte[]> rowValuesByStorage = new HashMap<>();
            for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
                RowWriteBinding binding = bindings.get(bindingIndex);
                if (binding.getSourceRowIndex() >= sourceRow.size()) {
                    throw invalid("source row " + rowIndex + " is too short for "
                        + binding.getContentColumnName());
                }
                if (!request.isTargetGrowthAllowed() && binding.getTargetRowIndex() >= targetRow.size()) {
                    throw invalid("target row " + rowIndex + " is too short for "
                        + binding.getContentColumnName());
                }
                requiredWidth = Math.max(requiredWidth, binding.getRequiredRowWidth());
                Object value = sourceRow.get(binding.getSourceRowIndex());
                byte[] data = value == null ? null : BlobWriter.toBytes(value);
                String key = storageKey(binding);
                if (rowValuesByStorage.containsKey(key)
                    && !Arrays.equals(rowValuesByStorage.get(key), data)) {
                    throw invalid("conflicting source values for " + binding.getContentColumnName());
                }
                Map<String, CachedBlob> cachedValues = blobRefBySourceRow.get(sourceRow);
                CachedBlob cached = cachedValues == null ? null : cachedValues.get(key);
                if (cached != null && !Arrays.equals(cached.content, data)) {
                    throw invalid("source row changed for " + binding.getContentColumnName());
                }
                rowValuesByStorage.put(key, data);
                values[rowIndex][bindingIndex] = data;
            }
            if (request.isTargetGrowthAllowed()) {
                while (targetRow.size() < requiredWidth) {
                    targetRow.add(null);
                }
            }
        }
        validateStatementConstantRows(request, bindings, values);
        return new PreparedRowWrite(values);
    }

    private void validateStatementConstantRows(RowWriteRequest request, List<RowWriteBinding> bindings,
                                               byte[][][] values) {
        for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
            if (!request.isStatementConstant(bindingIndex)) {
                continue;
            }
            byte[] expected = null;
            boolean initialized = false;
            for (byte[][] row : values) {
                if (!initialized) {
                    expected = row[bindingIndex];
                    initialized = true;
                } else if (!Arrays.equals(expected, row[bindingIndex])) {
                    throw invalid("statement-constant value changed for "
                        + bindings.get(bindingIndex).getContentColumnName());
                }
            }
            CachedBlob cached = statementConstantBlobRefs.get(storageKey(bindings.get(bindingIndex)));
            if (cached != null && initialized && !Arrays.equals(cached.content, expected)) {
                throw invalid("statement-constant value changed for "
                    + bindings.get(bindingIndex).getContentColumnName());
            }
        }
    }

    private long[] resolveParameterTableIds(PreparedParameterWrite prepared,
                                            List<ParameterWriteBinding> bindings) {
        long[] resolved = new long[bindings.size()];
        Map<String, Long> pending = new HashMap<>();
        for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
            if (!hasNonNullValue(prepared.values, bindingIndex)) {
                continue;
            }
            ParameterWriteBinding binding = bindings.get(bindingIndex);
            String key = storageKey(binding);
            Long tableId = tableIds.get(key);
            if (tableId == null) {
                tableId = pending.computeIfAbsent(key,
                    ignored -> tableIdResolver.resolve(binding.getStorageSchemaName(), binding.getStorageTableName(),
                        binding.getContentColumnName()));
            }
            resolved[bindingIndex] = tableId;
        }
        tableIds.putAll(pending);
        return resolved;
    }

    private void resolveRowTableIds(PreparedRowWrite prepared, List<RowWriteBinding> bindings) {
        Map<String, Long> pending = new HashMap<>();
        for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
            RowWriteBinding binding = bindings.get(bindingIndex);
            String key = storageKey(binding);
            if (!hasNonNullValue(prepared.values, bindingIndex)
                || tableIds.containsKey(key)
                || pending.containsKey(key)) {
                continue;
            }
            pending.put(key, tableIdResolver.resolve(binding.getStorageSchemaName(),
                binding.getStorageTableName(), binding.getContentColumnName()));
        }
        tableIds.putAll(pending);
    }

    private void backfillParameters(ParameterWriteRequest request, List<ParameterWriteBinding> bindings,
                                    String[][] blobRefs) {
        for (int rowIndex = 0; rowIndex < request.getParameterRows().size(); rowIndex++) {
            Map<Integer, ParameterContext> parameters = request.getParameterRows().get(rowIndex);
            for (int bindingIndex = 0; bindingIndex < bindings.size(); bindingIndex++) {
                int targetKey = bindings.get(bindingIndex).getTargetParamKey();
                String blobRef = blobRefs[rowIndex][bindingIndex];
                parameters.put(targetKey, blobRef == null
                    ? new ParameterContext(ParameterMethod.setNull1, new Object[] {targetKey, null})
                    : new ParameterContext(ParameterMethod.setString, new Object[] {targetKey, blobRef}));
            }
        }
    }

    private void track(BlobWriter.WriteResult result) {
        if (executionContext.getTransaction() != null) {
            executionContext.getTransaction().getBlobWriteTracker().trackFuture(result.getFuture());
        }
    }

    public static boolean isBinlogCompatibilityEnabled(ExecutionContext executionContext) {
        final boolean configured = DynamicConfig.getInstance().isEnableExternalizedBinlogCompatibility();
        if (executionContext.getTransaction() == null) {
            return configured;
        }
        return executionContext.getTransaction().pinExternalizedBinlogCompatibility(configured);
    }

    private boolean shouldUseStaging(int dataLength) {
        final boolean binlogCompatibility = isBinlogCompatibilityEnabled(executionContext);
        final boolean configured = binlogCompatibility || BlobWriter.shouldUseStaging(dataLength);
        if (executionContext.getTransaction() == null) {
            if (configured) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "External-column staging requires a transaction");
            }
            return false;
        }
        if (configured && !executionContext.getTransaction().isDistributedWriteTrx()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "External-column staging requires an explicit XA/TSO transaction");
        }
        return configured;
    }

    private static void validateBindings(List<? extends ExternalizedWriteBinding> bindings) {
        if (bindings == null) {
            throw invalid("binding list is null");
        }
        for (int i = 0; i < bindings.size(); i++) {
            if (bindings.get(i) == null) {
                throw invalid("binding " + i + " is null");
            }
        }
    }

    private static void validateParameterTargets(List<ParameterWriteBinding> bindings) {
        Set<Integer> targets = new HashSet<>();
        for (ParameterWriteBinding binding : bindings) {
            if (!targets.add(binding.getTargetParamKey())) {
                throw invalid("duplicate parameter target " + binding.getTargetParamKey());
            }
        }
        for (int targetIndex = 0; targetIndex < bindings.size(); targetIndex++) {
            for (int sourceIndex = 0; sourceIndex < bindings.size(); sourceIndex++) {
                if (targetIndex != sourceIndex
                    && bindings.get(targetIndex).getTargetParamKey() == bindings.get(sourceIndex).getSourceParamKey()) {
                    throw invalid("parameter target aliases another source");
                }
            }
        }
    }

    private static void validateRowTargets(List<RowWriteBinding> bindings) {
        Set<Integer> targets = new HashSet<>();
        for (RowWriteBinding binding : bindings) {
            if (!targets.add(binding.getTargetRowIndex())) {
                throw invalid("duplicate row target " + binding.getTargetRowIndex());
            }
        }
        for (int targetIndex = 0; targetIndex < bindings.size(); targetIndex++) {
            for (int sourceIndex = 0; sourceIndex < bindings.size(); sourceIndex++) {
                if (targetIndex != sourceIndex
                    && bindings.get(targetIndex).getTargetRowIndex() == bindings.get(sourceIndex).getSourceRowIndex()) {
                    throw invalid("row target aliases another source");
                }
            }
        }
    }

    private static ParameterContext requireParameter(Map<Integer, ParameterContext> parameters, int key,
                                                     String columnName, String role) {
        if (!parameters.containsKey(key)) {
            throw invalid(role + " parameter " + key + " is missing for " + columnName);
        }
        ParameterContext parameter = parameters.get(key);
        if (parameter == null || parameter.getArgs() == null || parameter.getArgs().length < 2) {
            throw invalid(role + " parameter " + key + " is malformed for " + columnName);
        }
        return parameter;
    }

    private static boolean isNull(ParameterContext parameter) {
        return parameter.getParameterMethod() == ParameterMethod.setNull1
            || parameter.getParameterMethod() == ParameterMethod.setNull2
            || parameter.getArgs()[1] == null;
    }

    private static byte[] parameterBytes(ParameterContext parameter) {
        Object value = parameter.getArgs()[1];
        return value instanceof byte[] ? (byte[]) value : BlobWriter.toBytes(value);
    }

    private static boolean hasNonNullValue(byte[][][] values, int bindingIndex) {
        for (byte[][] row : values) {
            if (row[bindingIndex] != null) {
                return true;
            }
        }
        return false;
    }

    private static String context(ExternalizedWriteBinding binding) {
        return binding.getAction().name().toLowerCase(Locale.ROOT) + " column "
            + binding.getContentColumnName();
    }

    private static String storageKey(ExternalizedWriteBinding binding) {
        return (binding.getStorageSchemaName() + "\u0000" + binding.getStorageTableName() + "\u0000"
            + binding.getContentColumnName()).toLowerCase(Locale.ROOT);
    }

    private static TddlRuntimeException invalid(String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Invalid externalized write request: " + detail);
    }

    @FunctionalInterface
    public interface TableIdResolver {
        long resolve(String schemaName, String tableName, String columnName);
    }

    @FunctionalInterface
    interface BlobBatchWriter {
        List<BlobWriter.WriteResult> write(List<BlobWriter.BlobItem> items);
    }

    public static final class ParameterWriteRequest {
        private final List<Map<Integer, ParameterContext>> parameterRows;
        private final boolean batchWrite;

        private ParameterWriteRequest(List<Map<Integer, ParameterContext>> parameterRows, boolean batchWrite) {
            this.parameterRows = parameterRows;
            this.batchWrite = batchWrite;
        }

        public static ParameterWriteRequest single(Map<Integer, ParameterContext> parameters) {
            List<Map<Integer, ParameterContext>> rows = new ArrayList<>(1);
            rows.add(parameters);
            return new ParameterWriteRequest(rows, false);
        }

        public static ParameterWriteRequest from(Parameters parameters) {
            if (parameters == null) {
                throw invalid("parameters are null");
            }
            List<Map<Integer, ParameterContext>> rows = parameters.getBatchParameters();
            return new ParameterWriteRequest(rows, parameters.isBatch() && rows.size() >= 2);
        }

        public static ParameterWriteRequest rows(List<Map<Integer, ParameterContext>> parameterRows,
                                                 boolean batchWrite) {
            return new ParameterWriteRequest(parameterRows, batchWrite);
        }

        public List<Map<Integer, ParameterContext>> getParameterRows() {
            return parameterRows;
        }

        public boolean isBatchWrite() {
            return batchWrite;
        }
    }

    public static final class RowWriteRequest {
        private final List<List<Object>> sourceRows;
        private final List<List<Object>> targetRows;
        private final boolean targetGrowthAllowed;
        private final Set<Integer> statementConstantBindings;

        private RowWriteRequest(List<List<Object>> sourceRows, List<List<Object>> targetRows,
                                boolean targetGrowthAllowed, Set<Integer> statementConstantBindings) {
            this.sourceRows = sourceRows;
            this.targetRows = targetRows;
            this.targetGrowthAllowed = targetGrowthAllowed;
            this.statementConstantBindings = statementConstantBindings == null
                ? java.util.Collections.emptySet()
                : java.util.Collections.unmodifiableSet(new HashSet<>(statementConstantBindings));
        }

        public static RowWriteRequest inPlace(List<List<Object>> rows) {
            return new RowWriteRequest(rows, rows, false, java.util.Collections.emptySet());
        }

        public static RowWriteRequest inPlace(List<List<Object>> rows, Set<Integer> statementConstantBindings) {
            return new RowWriteRequest(rows, rows, false, statementConstantBindings);
        }

        public static RowWriteRequest copying(List<List<Object>> sourceRows) {
            List<List<Object>> targetRows = new ArrayList<>(sourceRows == null ? 0 : sourceRows.size());
            if (sourceRows != null) {
                for (List<Object> sourceRow : sourceRows) {
                    targetRows.add(sourceRow == null ? null : new ArrayList<>(sourceRow));
                }
            }
            return new RowWriteRequest(sourceRows, targetRows, true, java.util.Collections.emptySet());
        }

        public List<List<Object>> getSourceRows() {
            return sourceRows;
        }

        public List<List<Object>> getTargetRows() {
            return targetRows;
        }

        public boolean isTargetGrowthAllowed() {
            return targetGrowthAllowed;
        }

        public boolean isStatementConstant(int bindingIndex) {
            return statementConstantBindings.contains(bindingIndex);
        }
    }

    private static final class PreparedParameterWrite {
        private final byte[][][] values;

        private PreparedParameterWrite(byte[][][] values) {
            this.values = values;
        }

        private boolean isEmpty() {
            return values.length == 0 || values[0].length == 0;
        }
    }

    private static final class PreparedRowWrite {
        private final byte[][][] values;

        private PreparedRowWrite(byte[][][] values) {
            this.values = values;
        }

        private boolean isEmpty() {
            return values.length == 0 || values[0].length == 0;
        }
    }

    private static final class CachedBlob {
        private final byte[] content;
        private final String blobRef;

        private CachedBlob(byte[] content, String blobRef) {
            this.content = content;
            this.blobRef = blobRef;
        }
    }

    private static final class PendingRowWrite {
        private final RowWriteBinding binding;
        private final Map<String, CachedBlob> cache;
        private final String storageKey;
        private final byte[] data;
        private final List<int[]> positions = new ArrayList<>();

        private PendingRowWrite(RowWriteBinding binding, Map<String, CachedBlob> cache,
                                String storageKey, byte[] data) {
            this.binding = binding;
            this.cache = cache;
            this.storageKey = storageKey;
            this.data = data;
        }
    }
}
