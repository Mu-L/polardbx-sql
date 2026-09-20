package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ColumnMceState;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedWriteBinding;
import com.alibaba.polardbx.optimizer.core.rel.dml.ParameterWriteBinding;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Execution-time adapter for externalized values in INSERT-family DML.
 *
 * <p>{@link ExternalizedDmlRewriter}, SQL2Rel, and WriterFactory decide the final column/value layout. After
 * RexCallParam evaluation and batch expansion have produced concrete JDBC parameter rows, INSERT-family handlers
 * invoke this helper in two independent stages:
 * <ul>
 *   <li>terminal {@code EXTERNALIZED}: replace the content parameter with its BlobRef;</li>
 *   <li>MCE migration: keep the content parameter and fill the separately appended addr parameter.</li>
 * </ul>
 * The helper reads the final Rex/JDBC layout and compiles {@link ParameterWriteBinding}s; it never mutates the
 * cached plan or independently interprets MCE metadata.
 *
 * <p>Actual materialization preflight, table-id resolution, Blob upload, parameter backfill, and transaction
 * tracking belong to {@link ExternalizedWriteManager}. Ordinary tables and INSERTs without a relevant column return
 * before that boundary.
 */
public class ExternalizedColumnDmlHelper {

    /**
     * Materialize externalized values produced by an {@code ON DUPLICATE KEY UPDATE} conflict branch.
     *
     * <p>The bindings come from {@link LogicalInsert#getExternalizedUpsertPushdownBindings()}. They were compiled by
     * the planner for concrete JDBC parameter slots that hold evaluated UPSERT {@code SET} expressions, rather than
     * values in the candidate INSERT tuple. A terminal binding replaces its parameter with a BlobRef for an
     * {@link ColumnMceState#EXTERNALIZED} column; a migration binding keeps the content parameter and fills the
     * appended addr parameter for a {@code DUAL_WRITE} or {@code READ_ADDR} column.
     *
     * <p>Candidate INSERT values are intentionally outside this method. They are handled by
     * {@link #transformInsertForExternalizedColumns} and {@link #transformInsertForMceAddrColumns}.
     */
    public static TransactionalStagingWriteBatch materializeUpsertPushdownParameters(
        LogicalInsert logicalInsert, TableMeta tableMeta, ExecutionContext executionContext,
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> routeByLogicalRow) {
        List<ParameterWriteBinding> bindings = logicalInsert.getExternalizedUpsertPushdownBindings();
        if (bindings == null || bindings.isEmpty()) {
            return new TransactionalStagingWriteBatch();
        }
        validateUpsertPushdownBindings(logicalInsert, tableMeta, bindings);

        ExternalizedWriteManager.ParameterWriteRequest request =
            ExternalizedWriteManager.ParameterWriteRequest.from(executionContext.getParams());
        int logicalRow = request.isBatchWrite() ? 0 : requireSingleOwnerLogicalRow(routeByLogicalRow);
        int[] logicalRows = new int[bindings.size()];
        Arrays.fill(logicalRows, logicalRow);
        return new ExternalizedWriteManager(executionContext).materializeRouted(
            request, bindings, logicalRows, routeByLogicalRow);
    }

    private static int requireSingleOwnerLogicalRow(
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> routeByLogicalRow) {
        Integer ownerRow = null;
        TransactionalStagingWriteBatch.OwnerRoute ownerRoute = null;
        for (Map.Entry<Integer, TransactionalStagingWriteBatch.OwnerRoute> entry : routeByLogicalRow.entrySet()) {
            if (ownerRoute == null) {
                ownerRow = entry.getKey();
                ownerRoute = entry.getValue();
            } else if (!ownerRoute.equals(entry.getValue())) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Externalized pushed UPSERT SET value requires a single primary owner route");
            }
        }
        if (ownerRow == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Externalized pushed UPSERT has no primary owner route");
        }
        return ownerRow;
    }

    private static void validateUpsertPushdownBindings(
        LogicalInsert logicalInsert, TableMeta tableMeta, List<ParameterWriteBinding> bindings) {
        if (tableMeta == null) {
            throw invalidUpsertPushdownParamLayout("current table metadata is missing");
        }
        String schemaName = logicalInsert.getSchemaName();
        String tableName = logicalInsert.getLogicalTableName();
        for (ParameterWriteBinding binding : bindings) {
            if (!binding.getStorageSchemaName().equalsIgnoreCase(schemaName)
                || !binding.getStorageTableName().equalsIgnoreCase(tableName)) {
                throw invalidUpsertPushdownParamLayout("binding target does not match current logical table");
            }
            ColumnMceState state = tableMeta.getColumnMceState(binding.getContentColumnName());
            if (binding.getAction() == ExternalizedWriteBinding.Action.TERMINAL_REPLACE) {
                if (state != ColumnMceState.EXTERNALIZED) {
                    throw invalidUpsertPushdownParamLayout("terminal binding state changed for "
                        + binding.getContentColumnName());
                }
            } else if (!state.isDualWrite()
                || tableMeta.getMceAddrColumnName(binding.getContentColumnName()) == null) {
                throw invalidUpsertPushdownParamLayout("migration binding state changed for "
                    + binding.getContentColumnName());
            }
        }
    }

    private static TddlRuntimeException invalidUpsertPushdownParamLayout(String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Invalid externalized UPSERT pushdown parameter layout: " + detail);
    }

    /**
     * Materialize candidate INSERT tuple values for columns in terminal {@link ColumnMceState#EXTERNALIZED} state.
     *
     * <p>These bindings are compiled here from the final INSERT tuple Rex/JDBC layout; they do not come from
     * {@link LogicalInsert#getExternalizedUpsertPushdownBindings()}. The planner has already renamed each logical
     * content write column to its physical addr column. After primary plaintext routing has selected the owner route,
     * this method reads the tuple's plaintext parameter and replaces that same parameter slot with a BlobRef before
     * the physical writer plans execute.
     *
     * <p>UPSERT conflict-branch {@code SET} values are handled separately by
     * {@link #materializeUpsertPushdownParameters}.
     */
    public static TransactionalStagingWriteBatch transformInsertForExternalizedColumns(
        LogicalInsert logicalInsert,
        TableMeta tableMeta,
        ExecutionContext executionContext,
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> routeByLogicalRow) {

        Map<String, String> colMapping = ExternalizedDmlRewriter.buildRenameMapping(tableMeta);
        if (colMapping.isEmpty()) {
            return new TransactionalStagingWriteBatch();
        }

        // Step 1: Find externalized column positions in the INSERT column list
        List<String> fieldNames = logicalInsert.getInsertRowType().getFieldNames();
        List<Integer> extColIndices = new ArrayList<>();
        List<String> extColNames = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (colMapping.containsKey(fieldNames.get(i))) {
                extColIndices.add(i);
                extColNames.add(fieldNames.get(i));
            }
        }
        if (extColIndices.isEmpty()) {
            return new TransactionalStagingWriteBatch();
        }

        // Step 2: Transform parameter values: blob data -> blob_addr via async OSS PutObject
        String schemaName = logicalInsert.getSchemaName();
        if (schemaName == null || schemaName.isEmpty()) {
            schemaName = executionContext.getSchemaName();
        }
        String logicalTableName = logicalInsert.getLogicalTableName();

        return transformParameterValues(logicalInsert, executionContext, schemaName, logicalTableName,
            extColIndices, extColNames, routeByLogicalRow);
    }

    /**
     * Compile terminal externalized slots from the final Rex/JDBC parameter layout and delegate materialization.
     *
     * <p>PreparedStatement batch rows share the keys from one tuple template and are carried as separate parameter
     * maps. A non-batch multi-row INSERT has one flat parameter map, with tuple-specific keys allocated after literal
     * and RexCallParam expansion. In both cases the complete layout is validated before
     * {@link ExternalizedWriteManager} can start the first Blob write.
     */
    private static TransactionalStagingWriteBatch transformParameterValues(
        LogicalInsert logicalInsert,
        ExecutionContext ec,
        String schemaName,
        String tableName,
        List<Integer> extColIndices,
        List<String> extColNames,
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> routeByLogicalRow) {

        Parameters params = ec.getParams();

        // Resolve column index -> parameter key via RexDynamicParam
        final LogicalDynamicValues input = RelUtils.getRelInput(logicalInsert);
        final ImmutableList<ImmutableList<RexNode>> tuples = input.getTuples();
        if (tuples.isEmpty()) {
            throw invalidTerminalParamLayout("tuple template is missing");
        }

        // Validate the complete parameter layout before resolving metadata or starting any blob write.
        // A missing parameter is not SQL NULL: accepting it would either skip an externalized value or
        // allow an earlier row to be uploaded before a later malformed row is discovered.
        List<TerminalParamBinding> paramBindings =
            validateTerminalParamLayout(params, tuples, extColIndices, extColNames);

        List<ParameterWriteBinding> writeBindings = new ArrayList<>();
        List<Integer> logicalRows = new ArrayList<>();
        ExternalizedWriteManager.ParameterWriteRequest request;
        if (params.isBatch()) {
            int[] paramKeys = requireTerminalParamKeys(tuples.get(0), extColIndices, extColNames);
            for (int i = 0; i < paramKeys.length; i++) {
                writeBindings.add(ParameterWriteBinding.terminal(schemaName, tableName, extColNames.get(i),
                    paramKeys[i]));
                logicalRows.add(0);
            }
            List<Map<Integer, ParameterContext>> parameterRows = params.getBatchParameters();
            request = ExternalizedWriteManager.ParameterWriteRequest.rows(parameterRows, parameterRows.size() >= 2);
        } else {
            for (TerminalParamBinding binding : paramBindings) {
                writeBindings.add(ParameterWriteBinding.terminal(schemaName, tableName,
                    extColNames.get(binding.extColOrdinal), binding.paramKey));
                logicalRows.add(binding.logicalRowIndex);
            }
            request = ExternalizedWriteManager.ParameterWriteRequest.single(params.getCurrentParameter());
        }
        int[] logicalRowByBinding = logicalRows.stream().mapToInt(Integer::intValue).toArray();
        return new ExternalizedWriteManager(ec).materializeRouted(
            request, writeBindings, logicalRowByBinding, routeByLogicalRow);
    }

    /**
     * Validate the complete terminal INSERT layout and associate parameter keys with externalized columns.
     *
     * <p>{@code extColIndices} addresses positions in each Rex tuple, while {@code extColNames} defines the separate
     * zero-based externalized-column order. RexDynamicParam indices are converted to one-based JDBC parameter keys.
     * Every referenced {@link ParameterContext} must already exist, but a present SQL NULL is valid.
     *
     * <p>For a non-batch INSERT the returned list contains every tuple-specific parameter binding consumed by the
     * caller. For a JDBC batch, validation scans every parameter row; the caller then compiles the shared tuple
     * template once and passes all rows to {@link ExternalizedWriteManager}.
     */
    private static List<TerminalParamBinding> validateTerminalParamLayout(
        Parameters params,
        ImmutableList<ImmutableList<RexNode>> tuples,
        List<Integer> extColIndices,
        List<String> extColNames) {
        if (params == null) {
            throw invalidTerminalParamLayout("parameters are missing");
        }

        List<TerminalParamBinding> bindings = new ArrayList<>();
        if (params.isBatch()) {
            ImmutableList<RexNode> tuple = tuples.get(0);
            int[] paramKeys = requireTerminalParamKeys(tuple, extColIndices, extColNames);
            requireUniqueTerminalParamKeys(paramKeys, extColNames,
                countDynamicParamKeys(ImmutableList.of(tuple)));
            List<Map<Integer, ParameterContext>> batchParams = params.getBatchParameters();
            for (int rowIdx = 0; rowIdx < batchParams.size(); rowIdx++) {
                Map<Integer, ParameterContext> rowParams = batchParams.get(rowIdx);
                for (int i = 0; i < paramKeys.length; i++) {
                    requireTerminalRowParam(rowParams, paramKeys[i], extColNames.get(i), "batch row " + rowIdx);
                    bindings.add(new TerminalParamBinding(paramKeys[i], i, rowIdx));
                }
            }
        } else {
            Map<Integer, ParameterContext> rowParams = params.getCurrentParameter();
            Map<Integer, Integer> paramKeyCounts = countDynamicParamKeys(tuples);
            for (int rowIdx = 0; rowIdx < tuples.size(); rowIdx++) {
                int[] paramKeys = requireTerminalParamKeys(tuples.get(rowIdx), extColIndices, extColNames);
                requireUniqueTerminalParamKeys(paramKeys, extColNames, paramKeyCounts);
                for (int i = 0; i < paramKeys.length; i++) {
                    requireTerminalRowParam(rowParams, paramKeys[i], extColNames.get(i), "tuple " + rowIdx);
                    bindings.add(new TerminalParamBinding(paramKeys[i], i, rowIdx));
                }
            }
        }
        return bindings;
    }

    private static Map<Integer, Integer> countDynamicParamKeys(
        List<? extends List<RexNode>> tuples) {
        Map<Integer, Integer> counts = new HashMap<>();
        for (List<RexNode> tuple : tuples) {
            for (RexNode node : tuple) {
                if (node instanceof RexDynamicParam) {
                    int paramKey = ((RexDynamicParam) node).getIndex() + 1;
                    counts.put(paramKey, counts.getOrDefault(paramKey, 0) + 1);
                }
            }
        }
        return counts;
    }

    private static void requireUniqueTerminalParamKeys(
        int[] paramKeys, List<String> extColNames, Map<Integer, Integer> paramKeyCounts) {
        for (int i = 0; i < paramKeys.length; i++) {
            int useCount = paramKeyCounts.getOrDefault(paramKeys[i], 0);
            if (useCount != 1) {
                throw invalidTerminalParamLayout("externalized column " + extColNames.get(i)
                    + " parameter key " + paramKeys[i] + " is referenced " + useCount + " times");
            }
        }
    }

    private static int[] requireTerminalParamKeys(
        ImmutableList<RexNode> tuple, List<Integer> extColIndices, List<String> extColNames) {
        int[] paramKeys = new int[extColIndices.size()];
        for (int i = 0; i < extColIndices.size(); i++) {
            int colIdx = extColIndices.get(i);
            if (colIdx < 0 || colIdx >= tuple.size()) {
                throw invalidTerminalParamLayout("externalized column " + extColNames.get(i)
                    + " index " + colIdx + " is outside tuple size " + tuple.size());
            }
            RexNode node = tuple.get(colIdx);
            if (!(node instanceof RexDynamicParam)) {
                throw invalidTerminalParamLayout("externalized column " + extColNames.get(i)
                    + " expected RexDynamicParam at index " + colIdx + " but found "
                    + (node == null ? "null" : node.getClass().getSimpleName()));
            }
            paramKeys[i] = ((RexDynamicParam) node).getIndex() + 1;
            if (paramKeys[i] <= 0) {
                throw invalidTerminalParamLayout("externalized column " + extColNames.get(i)
                    + " has invalid parameter key " + paramKeys[i]);
            }
        }
        return paramKeys;
    }

    private static void requireTerminalRowParam(
        Map<Integer, ParameterContext> rowParams, int paramKey, String columnName, String rowDescription) {
        if (rowParams == null || !rowParams.containsKey(paramKey)) {
            throw invalidTerminalParamLayout("externalized column " + columnName + " parameter key "
                + paramKey + " is missing from " + rowDescription);
        }
        ParameterContext pc = rowParams.get(paramKey);
        if (pc == null || pc.getArgs() == null || pc.getArgs().length < 2) {
            throw invalidTerminalParamLayout("externalized column " + columnName + " parameter key "
                + paramKey + " is malformed in " + rowDescription);
        }
    }

    private static TddlRuntimeException invalidTerminalParamLayout(String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Invalid externalized INSERT parameter layout: " + detail);
    }

    /**
     * Short-lived validated coordinate for one terminal externalized value.
     *
     * <p>This is only a bridge between the Rex tuple layout and {@link ParameterWriteBinding}; it owns neither the
     * {@link ParameterContext} nor the materialized BlobRef.
     */
    private static class TerminalParamBinding {
        /**
         * One-based key in a JDBC parameter map, converted from {@link RexDynamicParam#getIndex()}.
         */
        final int paramKey;

        /**
         * Zero-based index into the paired {@code extColNames} list, not a tuple or JDBC parameter index.
         */
        final int extColOrdinal;

        final int logicalRowIndex;

        TerminalParamBinding(int paramKey, int extColOrdinal, int logicalRowIndex) {
            this.paramKey = paramKey;
            this.extColOrdinal = extColOrdinal;
            this.logicalRowIndex = logicalRowIndex;
        }
    }

    /**
     * Check whether a parameter represents SQL NULL.
     *
     * <p>Absent or malformed contexts are treated as null by this low-level helper. Execution-time INSERT
     * materialization validates the context first; MCE backfill also uses this method before extracting bytes.
     */
    public static boolean isNullParam(ParameterContext pc) {
        if (pc == null) {
            return true;
        }
        ParameterMethod method = pc.getParameterMethod();
        if (method == ParameterMethod.setNull1 || method == ParameterMethod.setNull2) {
            return true;
        }
        if (pc.getArgs() == null || pc.getArgs().length < 2) {
            return true;
        }
        return pc.getArgs()[1] == null;
    }

    /**
     * Convert a non-null parameter value to Blob bytes.
     *
     * <p>Callers must use {@link #isNullParam(ParameterContext)} when SQL NULL must remain distinct from an empty
     * value. This conversion is shared by INSERT materialization and MCE backfill, and delegates to
     * {@link BlobWriter#toBytes} so DML and backfill encode the exact same bytes for the same runtime value
     * (Slice/Blob/Clob included; ambiguous types fail close there).
     */
    public static byte[] extractBlobBytes(ParameterContext pc) {
        if (pc == null || pc.getArgs() == null || pc.getArgs().length < 2) {
            return new byte[0];
        }
        return BlobWriter.toBytes(pc.getArgs()[1]);
    }

    // ==================== MCE Dual-Write Support ====================

    /**
     * Materialize candidate INSERT tuple values for columns in MCE {@code DUAL_WRITE} or {@code READ_ADDR} state.
     *
     * <p>These migration bindings are compiled here from the final INSERT tuple Rex/JDBC layout; they do not come from
     * {@link LogicalInsert#getExternalizedUpsertPushdownBindings()}. For every addr column selected by
     * {@link ExternalizedDmlRewriter#getAppendAddrColumns(TableMeta)}, the planner has retained the plaintext content
     * slot and appended a distinct addr placeholder. This method binds the source content parameter to that target
     * addr parameter, keeps the plaintext unchanged, and fills the addr slot with the materialized BlobRef.
     *
     * <p>UPSERT conflict-branch {@code SET} values are handled separately by
     * {@link #materializeUpsertPushdownParameters}.
     */
    public static TransactionalStagingWriteBatch transformInsertForMceAddrColumns(
        LogicalInsert logicalInsert,
        TableMeta tableMeta,
        ExecutionContext executionContext,
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> routeByLogicalRow) {

        // Find MCE addr columns and their corresponding content column indices
        List<String> fieldNames = logicalInsert.getInsertRowType().getFieldNames();

        List<MceAddrMapping> addrMappings = new ArrayList<>();

        // Column-level: only process addr columns whose paired content column is mid-migration
        // (DUAL_WRITE / READ_ADDR). Delegated to ExternalizedDmlRewriter as the single source of truth,
        // consistent with the APPEND decision in convertColumnList.
        for (ColumnMeta cm :
            ExternalizedDmlRewriter.getAppendAddrColumns(tableMeta)) {
            String addrColName = cm.getName();
            // mappingName holds the source content column name (e.g. "content")
            String contentColName = cm.getMappingName();
            if (contentColName == null || contentColName.isEmpty()) {
                throw invalidMceInsertColumnLayout(addrColName,
                    "content column mapping is missing");
            }

            int addrIdx = -1;
            int contentIdx = -1;
            int addrCount = 0;
            int contentCount = 0;
            for (int i = 0; i < fieldNames.size(); i++) {
                if (fieldNames.get(i).equalsIgnoreCase(addrColName)) {
                    addrIdx = i;
                    addrCount++;
                } else if (fieldNames.get(i).equalsIgnoreCase(contentColName)) {
                    contentIdx = i;
                    contentCount++;
                }
            }
            if (addrCount != 1 || contentCount != 1) {
                throw invalidMceInsertColumnLayout(contentColName,
                    "expected content column " + contentColName + " and addr column " + addrColName
                        + " exactly once but found " + contentCount + " and " + addrCount);
            }
            addrMappings.add(new MceAddrMapping(addrIdx, contentIdx, contentColName));
        }

        if (addrMappings.isEmpty()) {
            return new TransactionalStagingWriteBatch();
        }

        // Transform parameter values: read content column value → upload → write BlobRef to addr column
        return transformMceAddrParams(logicalInsert, executionContext, tableMeta.getSchemaName(),
            tableMeta.getTableName(), addrMappings, routeByLogicalRow);
    }

    private static TddlRuntimeException invalidMceInsertColumnLayout(String columnName, String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Invalid MCE INSERT column layout for " + columnName + ": " + detail);
    }

    /**
     * Mapping between an MCE address column and its source content column in the INSERT list.
     */
    private static class MceAddrMapping {
        final int addrColIdx;     // index in INSERT column list for the addr column
        final int contentColIdx;  // index in INSERT column list for the content column
        final String contentColName;

        MceAddrMapping(int addrColIdx, int contentColIdx, String contentColName) {
            this.addrColIdx = addrColIdx;
            this.contentColIdx = contentColIdx;
            this.contentColName = contentColName;
        }
    }

    /**
     * Compile MCE source-content/target-addr bindings from the final parameter layout.
     *
     * <p>PreparedStatement batch rows reuse one tuple template and therefore one pair of parameter keys. A non-batch
     * multi-row INSERT keeps all tuple-specific keys in the current parameter map, so one binding is compiled per
     * tuple and externalized column. {@link ExternalizedWriteManager} preflights the complete request before it
     * mutates any addr placeholder.
     */
    private static TransactionalStagingWriteBatch transformMceAddrParams(
        LogicalInsert logicalInsert,
        ExecutionContext ec,
        String schemaName,
        String tableName,
        List<MceAddrMapping> addrMappings,
        Map<Integer, TransactionalStagingWriteBatch.OwnerRoute> routeByLogicalRow) {

        Parameters params = ec.getParams();
        final LogicalDynamicValues input = RelUtils.getRelInput(logicalInsert);
        final ImmutableList<ImmutableList<RexNode>> tuples = input.getTuples();
        if (tuples.isEmpty()) {
            return new TransactionalStagingWriteBatch();
        }

        List<ParameterWriteBinding> bindings = new ArrayList<>();
        List<Integer> logicalRows = new ArrayList<>();
        ExternalizedWriteManager.ParameterWriteRequest request;
        if (params.isBatch()) {
            ImmutableList<RexNode> firstTuple = tuples.get(0);
            for (MceAddrMapping mapping : addrMappings) {
                int contentParamKey = requireParamKey(firstTuple, mapping.contentColIdx, "content", mapping);
                int addrParamKey = requireParamKey(firstTuple, mapping.addrColIdx, "addr", mapping);
                bindings.add(ParameterWriteBinding.migration(schemaName, tableName, mapping.contentColName,
                    contentParamKey, addrParamKey));
                logicalRows.add(0);
            }
            request = ExternalizedWriteManager.ParameterWriteRequest.rows(params.getBatchParameters(),
                params.getBatchParameters().size() >= 2);
        } else {
            for (int logicalRow = 0; logicalRow < tuples.size(); logicalRow++) {
                ImmutableList<RexNode> tuple = tuples.get(logicalRow);
                for (MceAddrMapping mapping : addrMappings) {
                    int contentParamKey = requireParamKey(tuple, mapping.contentColIdx, "content", mapping);
                    int addrParamKey = requireParamKey(tuple, mapping.addrColIdx, "addr", mapping);
                    bindings.add(ParameterWriteBinding.migration(schemaName, tableName, mapping.contentColName,
                        contentParamKey, addrParamKey));
                    logicalRows.add(logicalRow);
                }
            }
            request = ExternalizedWriteManager.ParameterWriteRequest.single(params.getCurrentParameter());
        }
        return new ExternalizedWriteManager(ec).materializeRouted(request, bindings,
            logicalRows.stream().mapToInt(Integer::intValue).toArray(), routeByLogicalRow);
    }

    private static int requireParamKey(
        ImmutableList<RexNode> tuple, int colIdx, String role, MceAddrMapping mapping) {
        if (colIdx < 0 || colIdx >= tuple.size()) {
            throw invalidParamLayout(role, mapping, "column index " + colIdx + " is outside tuple size "
                + tuple.size());
        }
        RexNode node = tuple.get(colIdx);
        if (node instanceof RexDynamicParam) {
            return ((RexDynamicParam) node).getIndex() + 1;
        }
        throw invalidParamLayout(role, mapping, "expected RexDynamicParam at column index " + colIdx
            + " but found " + node.getClass().getSimpleName());
    }

    private static TddlRuntimeException invalidParamLayout(
        String role, MceAddrMapping mapping, String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Invalid MCE INSERT parameter layout for " + role + " column of " + mapping.contentColName + ": "
                + detail);
    }

}
