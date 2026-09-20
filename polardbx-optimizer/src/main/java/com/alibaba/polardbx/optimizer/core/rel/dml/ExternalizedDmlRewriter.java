package com.alibaba.polardbx.optimizer.core.rel.dml;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.config.table.ColumnMceState;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.function.SqlValuesFunction;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalUpsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ShardingModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.UpsertRelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.UpsertWriter;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Centralized metadata-driven DML rewrite policy and write-binding compiler for externalized columns.
 * It covers both terminal EXTERNALIZED columns and columns in intermediate MCE lifecycle states.
 *
 * <p>Planner, SQL-to-Rel, Writer, and Handler sites consult this class for mapping selection,
 * APPEND/RENAME decisions, execution gates, metadata validation, and binding compilation instead of
 * independently interpreting column flags. Callers remain responsible for applying the selected
 * rewrite, materializing logical values into BlobRefs, physical routing, and transaction handling.
 * Decisions are driven by each column's {@link ColumnMceState}, which makes APPEND and RENAME
 * mutually exclusive per column.
 *
 * <p>The normal call flow is:
 * <ol>
 *     <li>Interpret and validate the current {@link TableMeta} lifecycle.</li>
 *     <li>Decide whether the original execution path is safe and which physical columns it must write.</li>
 *     <li>Compile that decision into typed {@link ParameterWriteBinding} or {@link RowWriteBinding} objects.</li>
 *     <li>Let the caller apply the plan rewrite; the executor later consumes the bindings to materialize BlobRefs.</li>
 * </ol>
 * Keeping these phases separate lets the existing routing and Writer implementations remain the owners of physical
 * targets while this class remains the owner of externalized-column semantics.
 *
 * <p>Be careful about the three coordinate systems used below:
 * <ul>
 *     <li>{@link ParameterWriteBinding} uses one-based JDBC parameter keys.</li>
 *     <li>{@link RowWriteBinding} uses zero-based executor-row positions.</li>
 *     <li>Writer-local SET/field ordinals must be translated through the Writer's {@link Mapping} before they become
 *     executor-row positions.</li>
 * </ul>
 *
 * <p>Read and write mappings are intentionally separate. In READ_ADDR, SELECT must read the
 * physical addr column so FETCH_BLOB receives a BlobRef, but DML must still write both the plaintext
 * content column and the appended addr column. Reusing the read mapping for writes would reintroduce
 * duplicate addr columns; reusing the write mapping for reads makes FETCH_BLOB decode plaintext.
 *
 * <pre>
 *   ColumnMceState  | column-list action | value transform
 *   NONE            | none               | none
 *   DUAL_WRITE      | APPEND addr slot   | upload content -> BlobRef into addr slot
 *   READ_ADDR       | APPEND addr slot   | (same as DUAL_WRITE)
 *   EXTERNALIZED    | RENAME content->addr | upload content -> BlobRef into (renamed) addr slot
 * </pre>
 */
public class ExternalizedDmlRewriter {

    // ========== Lifecycle interpretation: logical content columns versus physical addr columns ==========

    /**
     * Write-path rename mapping (logical content name -> physical addr name) for columns in the
     * terminal EXTERNALIZED state ONLY. Columns mid-migration (DUAL_WRITE / READ_ADDR) are
     * deliberately excluded — their addr column is APPENDED, not renamed, so including them here
     * would produce a duplicate addr column in the physical INSERT/UPDATE column list.
     */
    public static Map<String, String> buildRenameMapping(TableMeta tableMeta) {
        if (!hasExternalizedWriteLifecycle(tableMeta)) {
            return Collections.emptyMap();
        }
        Map<String, String> mapping = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        validateMetadataPairs(tableMeta);
        for (ColumnMeta cm : tableMeta.getAllColumns()) {
            if (cm.getMappingName() == null || !cm.isExternalizedColumn()) {
                continue;
            }
            // Only rename columns that have reached the terminal EXTERNALIZED state.
            // A content column mid-migration (READ_ADDR) also carries the externalized flag but
            // its addr column is appended separately; renaming it would collide.
            if (tableMeta.getColumnMceState(cm.getName()) == ColumnMceState.EXTERNALIZED) {
                mapping.put(cm.getName(), cm.getMappingName());
            }
        }
        return mapping;
    }

    /**
     * Read-path mapping (logical content name -> physical addr name) for columns whose reads must
     * fetch from the addr column. Includes READ_ADDR and EXTERNALIZED, unlike the write-path rename
     * mapping which includes EXTERNALIZED only.
     */
    public static Map<String, String> buildReadMapping(TableMeta tableMeta) {
        if (!hasExternalizedWriteLifecycle(tableMeta)) {
            return Collections.emptyMap();
        }
        Map<String, String> mapping = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        validateMetadataPairs(tableMeta);
        for (ColumnMeta cm : tableMeta.getAllColumns()) {
            if (cm.getMappingName() == null || !cm.isExternalizedColumn()) {
                continue;
            }
            if (tableMeta.getColumnMceState(cm.getName()).isReadAddr()) {
                mapping.put(cm.getName(), cm.getMappingName());
            }
        }
        return mapping;
    }

    /**
     * MCE addr columns that must be APPENDED to the write column list, i.e. addr columns whose
     * paired content column is mid-migration (DUAL_WRITE / READ_ADDR). Returned as ColumnMeta of
     * the addr columns recorded in {@code mce_column_state.addr_column_name}.
     */
    public static List<ColumnMeta> getAppendAddrColumns(TableMeta tableMeta) {
        if (!hasExternalizedWriteLifecycle(tableMeta)) {
            return Collections.emptyList();
        }
        List<ColumnMeta> result = new ArrayList<>();
        validateMetadataPairs(tableMeta);
        for (ColumnMeta cm : tableMeta.getWriteColumns()) {
            String contentCol = tableMeta.getMceContentColumnByAddr(cm.getName());
            if (contentCol == null) {
                continue;
            }
            ColumnMceState state = tableMeta.getColumnMceState(contentCol);
            if (state.isDualWrite()) {
                result.add(cm);
            }
        }
        return result;
    }

    /**
     * Same as {@link #getAppendAddrColumns(TableMeta)}, further filtered to addr columns whose
     * paired content column is explicitly present in {@code updateColumnList} (the columns named
     * in a SET / ON DUPLICATE KEY UPDATE clause). Used by the UPSERT conflict branch (physical
     * UPDATE built by WriterFactory.createUpdateWriter), which only renames terminal EXTERNALIZED
     * columns and otherwise never appends addr columns for mid-migration content columns being SET.
     */
    public static List<ColumnMeta> getAppendAddrColumnsForUpdateSet(TableMeta tableMeta,
                                                                    List<String> updateColumnList) {
        if (!hasExternalizedWriteLifecycle(tableMeta)
            || updateColumnList == null || updateColumnList.isEmpty()) {
            return Collections.emptyList();
        }
        List<ColumnMeta> result = new ArrayList<>();
        java.util.Set<String> updateColumnSet = new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        updateColumnSet.addAll(updateColumnList);
        for (ColumnMeta cm : getAppendAddrColumns(tableMeta)) {
            if (updateColumnSet.contains(cm.getMappingName())) {
                result.add(cm);
            }
        }
        return result;
    }

    // ========== UPSERT: choose DN/CN execution and compile the selected parameter/row contract ==========

    /**
     * Compile all externalized writes for the UPSERT conflict branch in the final after-row index space.
     * Terminal columns replace their logical content slot in the writer-bound row, while migration columns
     * preserve content and fill the separately appended addr slot.
     */
    public static List<RowWriteBinding> compileUpsertConflictWriteBindings(String storageSchemaName,
                                                                           String storageTableName,
                                                                           List<String> targetColumns,
                                                                           TableMeta tableMeta,
                                                                           List<String> updateColumnList) {
        if (targetColumns == null) {
            throw invalidUpsertPosition("target column list is null");
        }
        if (tableMeta == null || updateColumnList == null || updateColumnList.isEmpty()) {
            return Collections.emptyList();
        }
        // Ordinary tables bail out on the cached O(1) flags before any per-column allocation.
        if (!needsHandling(tableMeta)) {
            return Collections.emptyList();
        }

        final Set<String> updateColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        updateColumns.addAll(updateColumnList);
        final Map<String, String> terminalMapping = buildRenameMapping(tableMeta);
        final List<RowWriteBinding> bindings = new ArrayList<>();
        for (String targetColumn : targetColumns) {
            if (!updateColumns.contains(targetColumn) || !terminalMapping.containsKey(targetColumn)) {
                continue;
            }
            final int rowIndex = requireUniqueUpsertColumnPosition(targetColumns, targetColumn, "terminal content");
            bindings.add(RowWriteBinding.terminal(storageSchemaName, storageTableName, targetColumn,
                rowIndex, targetColumns.size()));
        }

        final List<ColumnMeta> appendAddrColumns =
            getAppendAddrColumnsForUpdateSet(tableMeta, updateColumnList);
        bindings.addAll(compileUpsertMigrationWriteBindings(storageSchemaName, storageTableName,
            targetColumns, appendAddrColumns));
        validateUpsertConflictBindings(bindings);
        return bindings;
    }

    /**
     * Whether an externalized/MCE UPSERT can keep the original DN duplicate-check path.
     *
     * <p>The DN can consume two narrow logical shapes without reading the conflicting row in CN:
     * <ul>
     *     <li>{@code content = VALUES(content)}: the normal incoming-row materialization already provides the
     *     physical BlobRef (and the migration addr slot).</li>
     *     <li>{@code content = <statement constant>}: a literal, NULL, or plain JDBC parameter can be compiled into
     *     stable parameter slots and materialized before the existing InsertWriter builds physical plans.</li>
     * </ul>
     * Current-row expressions and cross-column references still require the logical UPSERT path because their final
     * logical content is unavailable before duplicate checking.
     */
    public static boolean canUseExternalizedUpsertPushdown(LogicalInsert insert, TableMeta tableMeta) {
        if (insert == null || tableMeta == null || !insert.isUpsert() || insert.isSourceSelect()) {
            return false;
        }
        if (!needsHandling(tableMeta)) {
            return true;
        }

        final List<RexNode> assignments = insert.getDuplicateKeyUpdateList();
        final List<String> tableColumns = insert.getTable().getRowType().getFieldNames();
        if (assignments == null || assignments.isEmpty() || tableColumns == null || tableColumns.isEmpty()) {
            return false;
        }

        final Map<String, String> terminalAddrByContent = buildRenameMapping(tableMeta);
        final Map<String, String> migrationAddrByContent = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ColumnMeta addrColumn : getAppendAddrColumns(tableMeta)) {
            migrationAddrByContent.put(addrColumn.getMappingName(), addrColumn.getName());
        }
        final Set<String> externalizedContentColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        externalizedContentColumns.addAll(terminalAddrByContent.keySet());
        externalizedContentColumns.addAll(migrationAddrByContent.keySet());

        final Set<String> seenAssignmentTargets = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final Set<String> updatedMigrationContentColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final Map<String, Integer> migrationAddrAssignmentCount = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (RexNode assignment : assignments) {
            if (!(assignment instanceof RexCall) || ((RexCall) assignment).getOperands().size() != 2
                || !(((RexCall) assignment).getOperands().get(0) instanceof RexInputRef)) {
                return false;
            }
            RexCall assignmentCall = (RexCall) assignment;
            String targetColumn = resolveInputRefColumn(
                (RexInputRef) assignmentCall.getOperands().get(0), tableColumns);
            if (targetColumn == null) {
                return false;
            }
            // Keep malformed/repeated SET targets out of the narrow pushdown path. The parameter compiler needs a
            // one-to-one target layout; returning false here lets the existing CN UPSERT path preserve SQL semantics.
            if (!seenAssignmentTargets.add(targetColumn)) {
                return false;
            }
            RexNode value = assignmentCall.getOperands().get(1);

            String migrationContent = tableMeta.getMceContentColumnByAddr(targetColumn);
            if (migrationContent != null && migrationAddrByContent.containsKey(migrationContent)) {
                // SQL2Rel appends exactly one typed-NULL addr sink for every migrating content SET item. It is
                // rewritten only after the strategy has accepted the paired logical content expression.
                if (!RexLiteral.isNullLiteral(value)) {
                    return false;
                }
                migrationAddrAssignmentCount.merge(migrationContent, 1, Integer::sum);
                continue;
            }

            if (externalizedContentColumns.contains(targetColumn)) {
                if (!isIdentityValuesCall(value, targetColumn, tableColumns)
                    && !isStatementConstantRex(value)) {
                    return false;
                }
                if (migrationAddrByContent.containsKey(targetColumn)) {
                    updatedMigrationContentColumns.add(targetColumn);
                }
                continue;
            }

            // VALUES(externalized_col), a current-row content reference, or a nested use of either would feed a
            // BlobRef into an ordinary target after physical name rewriting. Keep those expressions in CN.
            if (containsExternalizedColumnReference(value, tableColumns, externalizedContentColumns)) {
                return false;
            }
        }

        for (String contentColumn : updatedMigrationContentColumns) {
            if (migrationAddrAssignmentCount.getOrDefault(contentColumn, 0) != 1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compile the accepted UPSERT SET list into the physical parameter layout consumed by the original InsertWriter.
     *
     * <p>This is a planning-time rewrite. The executor only fills the recorded parameter slots; it never changes the
     * cached expression tree. For migration identity VALUES, the appended addr assignment becomes
     * {@code addr = VALUES(addr)}. For constants, content and addr receive separate parameters so plaintext remains
     * in content while only addr receives the BlobRef.
     */
    public static List<ParameterWriteBinding> prepareExternalizedUpsertPushdown(
        LogicalInsert insert, TableMeta tableMeta, AtomicInteger maxParamIndex) {
        if (!canUseExternalizedUpsertPushdown(insert, tableMeta)) {
            throw invalidUpsertPushdown("SET expression is not safe for DN execution");
        }
        if (!needsHandling(tableMeta)) {
            insert.setExternalizedUpsertPushdownBindings(Collections.emptyList());
            return Collections.emptyList();
        }
        if (maxParamIndex == null) {
            throw invalidUpsertPushdown("maximum parameter index is missing");
        }

        // Rewrite a detached assignment list first. The cached LogicalInsert is updated only after every accepted
        // externalized SET item has a complete parameter contract, so a validation failure cannot expose a partially
        // rewritten expression tree to another planning branch.
        final List<String> tableColumns = insert.getTable().getRowType().getFieldNames();
        final List<RexNode> rewrittenAssignments = new ArrayList<>(insert.getDuplicateKeyUpdateList());
        final Map<String, Integer> assignmentOrdinalByTarget =
            buildUniqueAssignmentOrdinalMap(rewrittenAssignments, tableColumns);

        // Terminal columns overwrite their renamed physical addr slot. Migration columns keep two independent SET
        // targets: plaintext content and the appended addr sink injected by SQL-to-Rel.
        final Map<String, String> terminalAddrByContent = buildRenameMapping(tableMeta);
        final Map<String, String> migrationAddrByContent = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ColumnMeta addrColumn : getAppendAddrColumns(tableMeta)) {
            migrationAddrByContent.put(addrColumn.getMappingName(), addrColumn.getName());
        }

        final RexBuilder rexBuilder = insert.getCluster().getRexBuilder();
        final List<ParameterWriteBinding> bindings = new ArrayList<>();
        // Iterate by SET target rather than expression position so content/addr pairs can be resolved by name and
        // duplicate targets have already been rejected by buildUniqueAssignmentOrdinalMap().
        for (Map.Entry<String, Integer> targetEntry : assignmentOrdinalByTarget.entrySet()) {
            final String contentColumn = targetEntry.getKey();
            final String terminalAddr = terminalAddrByContent.get(contentColumn);
            final String migrationAddr = migrationAddrByContent.get(contentColumn);
            if (terminalAddr == null && migrationAddr == null) {
                continue;
            }

            final int contentAssignmentOrdinal = targetEntry.getValue();
            final RexCall contentAssignment = (RexCall) rewrittenAssignments.get(contentAssignmentOrdinal);
            final RexNode logicalValue = contentAssignment.getOperands().get(1);
            if (isIdentityValuesCall(logicalValue, contentColumn, tableColumns)) {
                // VALUES(content) already refers to the incoming row that the normal INSERT materializer rewrites.
                // A migration addr sink must mirror VALUES(addr); terminal rename needs no additional SET rewrite.
                if (migrationAddr != null) {
                    int addrAssignmentOrdinal =
                        requireAssignmentOrdinal(assignmentOrdinalByTarget, migrationAddr, "migration addr");
                    int addrColumnIndex = requireUniqueUpsertColumnPosition(tableColumns, migrationAddr, "addr");
                    RexNode addrValues = rexBuilder.makeCall(((RexCall) logicalValue).getOperator(),
                        RexInputRef.of(addrColumnIndex, insert.getTable().getRowType()));
                    rewrittenAssignments.set(addrAssignmentOrdinal,
                        replaceAssignmentValue(rexBuilder,
                            (RexCall) rewrittenAssignments.get(addrAssignmentOrdinal), addrValues));
                }
                continue;
            }

            // Literal/parameter constants are forced into a stable content parameter. The executor binding reads
            // that logical value and writes its BlobRef either back to the terminal slot or to a second migration
            // addr parameter, leaving migration plaintext untouched.
            RexNode contentParam = ensureStatementConstantParameter(logicalValue, maxParamIndex);
            rewrittenAssignments.set(contentAssignmentOrdinal,
                replaceAssignmentValue(rexBuilder, contentAssignment, contentParam));
            int contentParamKey = ((RexDynamicParam) contentParam).getIndex() + 1;
            if (terminalAddr != null) {
                bindings.add(ParameterWriteBinding.terminal(insert.getSchemaName(), insert.getLogicalTableName(),
                    contentColumn, contentParamKey));
                continue;
            }

            int addrAssignmentOrdinal =
                requireAssignmentOrdinal(assignmentOrdinalByTarget, migrationAddr, "migration addr");
            RexNode evaluationSource = logicalValue instanceof RexCallParam
                ? ((RexCallParam) logicalValue).getRexCall() : logicalValue;
            RexCallParam addrParam = RexUtils.wrapWithRexCallParam(evaluationSource, maxParamIndex,
                evaluationSource instanceof RexDynamicParam);
            rewrittenAssignments.set(addrAssignmentOrdinal,
                replaceAssignmentValue(rexBuilder,
                    (RexCall) rewrittenAssignments.get(addrAssignmentOrdinal), addrParam));
            bindings.add(ParameterWriteBinding.migration(insert.getSchemaName(), insert.getLogicalTableName(),
                contentColumn, contentParamKey, addrParam.getIndex() + 1));
        }

        // Publish the expression rewrite and its executor contract together. Later executor code fills only the
        // recorded parameter keys and never mutates this cached Rex tree.
        insert.setDuplicateKeyUpdateList(rewrittenAssignments);
        insert.setExternalizedUpsertPushdownBindings(bindings);
        return bindings;
    }

    private static RexNode ensureStatementConstantParameter(RexNode value, AtomicInteger maxParamIndex) {
        if (value instanceof RexDynamicParam) {
            return value;
        }
        if (!isStatementConstantRex(value)) {
            throw invalidUpsertPushdown("non-constant SET value reached parameter compilation");
        }
        return RexUtils.wrapWithRexCallParam(value, maxParamIndex, false);
    }

    private static RexNode replaceAssignmentValue(RexBuilder rexBuilder, RexCall assignment, RexNode value) {
        return rexBuilder.makeCall(assignment.getOperator(), assignment.getOperands().get(0), value);
    }

    private static Map<String, Integer> buildUniqueAssignmentOrdinalMap(
        List<RexNode> assignments, List<String> tableColumns) {
        Map<String, Integer> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int ordinal = 0; ordinal < assignments.size(); ordinal++) {
            RexNode assignment = assignments.get(ordinal);
            if (!(assignment instanceof RexCall)
                || !(((RexCall) assignment).getOperands().get(0) instanceof RexInputRef)) {
                throw invalidUpsertPushdown("malformed duplicate-key assignment at ordinal " + ordinal);
            }
            String targetColumn = resolveInputRefColumn(
                (RexInputRef) ((RexCall) assignment).getOperands().get(0), tableColumns);
            if (targetColumn == null || result.put(targetColumn, ordinal) != null) {
                throw invalidUpsertPushdown("duplicate or unknown SET target " + targetColumn);
            }
        }
        return result;
    }

    private static int requireAssignmentOrdinal(
        Map<String, Integer> assignmentOrdinalByTarget, String targetColumn, String role) {
        Integer ordinal = assignmentOrdinalByTarget.get(targetColumn);
        if (ordinal == null) {
            throw invalidUpsertPushdown(role + " SET slot " + targetColumn + " is missing");
        }
        return ordinal;
    }

    private static boolean isStatementConstantRex(RexNode rex) {
        if (rex instanceof RexCallParam) {
            return isStatementConstantRex(((RexCallParam) rex).getRexCall());
        }
        return rex instanceof RexLiteral
            || rex instanceof RexDynamicParam && ((RexDynamicParam) rex).getIndex() >= 0;
    }

    private static boolean isIdentityValuesCall(
        RexNode rex, String updateColumn, List<String> tableColumns) {
        if (!(rex instanceof RexCall) || !(((RexCall) rex).getOperator() instanceof SqlValuesFunction)) {
            return false;
        }
        RexCall call = (RexCall) rex;
        if (call.getOperands().size() != 1 || !(call.getOperands().get(0) instanceof RexInputRef)) {
            return false;
        }
        String valuesColumn = resolveInputRefColumn((RexInputRef) call.getOperands().get(0), tableColumns);
        return valuesColumn != null && valuesColumn.equalsIgnoreCase(updateColumn);
    }

    private static boolean containsExternalizedColumnReference(
        RexNode rex, List<String> tableColumns, Set<String> externalizedContentColumns) {
        if (rex instanceof RexCallParam) {
            return containsExternalizedColumnReference(
                ((RexCallParam) rex).getRexCall(), tableColumns, externalizedContentColumns);
        }
        if (rex instanceof RexInputRef) {
            String column = resolveInputRefColumn((RexInputRef) rex, tableColumns);
            return column == null || externalizedContentColumns.contains(column);
        }
        if (!(rex instanceof RexCall)) {
            return false;
        }
        return ((RexCall) rex).getOperands().stream()
            .anyMatch(operand -> containsExternalizedColumnReference(
                operand, tableColumns, externalizedContentColumns));
    }

    private static String resolveInputRefColumn(RexInputRef inputRef, List<String> tableColumns) {
        int index = inputRef.getIndex();
        return index < 0 || index >= tableColumns.size() ? null : tableColumns.get(index);
    }

    private static TddlRuntimeException invalidUpsertPushdown(String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
            "Invalid externalized UPSERT pushdown layout: " + detail);
    }

    /**
     * Compile migration content/addr pairs in the final UPSERT after-row coordinate space.
     *
     * <p>Every pair must occupy two unique zero-based row positions. Cross-pair aliases are rejected because the
     * executor writes all target addr slots in place after reading their logical content sources.</p>
     */
    private static List<RowWriteBinding> compileUpsertMigrationWriteBindings(String storageSchemaName,
                                                                             String storageTableName,
                                                                             List<String> targetColumns,
                                                                             List<ColumnMeta> appendAddrColumns) {
        final List<RowWriteBinding> bindings = new ArrayList<>();
        if (appendAddrColumns == null || appendAddrColumns.isEmpty()) {
            return bindings;
        }
        if (targetColumns == null) {
            throw invalidUpsertPosition("target column list is null");
        }
        final Set<Integer> contentPositions = new HashSet<>();
        final Set<Integer> addrPositions = new HashSet<>();
        for (ColumnMeta addrColumn : appendAddrColumns) {
            if (addrColumn == null) {
                throw invalidUpsertPosition("addr column metadata is null");
            }
            final String contentColumnName = addrColumn.getMappingName();
            final String addrColumnName = addrColumn.getName();
            final int contentIdx = requireUniqueUpsertColumnPosition(targetColumns, contentColumnName, "content");
            final int addrIdx = requireUniqueUpsertColumnPosition(targetColumns, addrColumnName, "addr");
            if (contentIdx == addrIdx) {
                throw invalidUpsertPosition("content column " + contentColumnName
                    + " aliases addr column " + addrColumnName + " at position " + contentIdx);
            }
            if (!contentPositions.add(contentIdx)) {
                throw invalidUpsertPosition("duplicate content position " + contentIdx
                    + " for column " + contentColumnName);
            }
            if (!addrPositions.add(addrIdx)) {
                throw invalidUpsertPosition("duplicate addr position " + addrIdx
                    + " for column " + addrColumnName);
            }
            bindings.add(RowWriteBinding.migration(storageSchemaName, storageTableName, contentColumnName,
                contentIdx, addrIdx, targetColumns.size()));
        }
        if (bindings.size() != appendAddrColumns.size()) {
            throw invalidUpsertPosition("expected " + appendAddrColumns.size()
                + " bindings but produced " + bindings.size());
        }
        final Set<Integer> crossPairAliases = new HashSet<>(contentPositions);
        crossPairAliases.retainAll(addrPositions);
        if (!crossPairAliases.isEmpty()) {
            throw invalidUpsertPosition("content and addr positions alias across pairs: " + crossPairAliases);
        }
        return bindings;
    }

    private static void validateUpsertConflictBindings(List<RowWriteBinding> bindings) {
        final Set<Integer> targetPositions = new HashSet<>();
        for (RowWriteBinding binding : bindings) {
            if (!targetPositions.add(binding.getTargetRowIndex())) {
                throw invalidUpsertPosition("duplicate target position " + binding.getTargetRowIndex());
            }
        }
        for (RowWriteBinding binding : bindings) {
            if (binding.getAction() == ExternalizedWriteBinding.Action.MIGRATION_APPEND
                && targetPositions.contains(binding.getSourceRowIndex())) {
                throw invalidUpsertPosition("migration source position " + binding.getSourceRowIndex()
                    + " aliases another binding target");
            }
        }
    }

    private static int requireUniqueUpsertColumnPosition(List<String> targetColumns, String columnName, String role) {
        if (columnName == null || columnName.isEmpty()) {
            throw invalidUpsertPosition(role + " column name is empty");
        }
        int position = -1;
        int count = 0;
        for (int i = 0; i < targetColumns.size(); i++) {
            if (columnName.equalsIgnoreCase(targetColumns.get(i))) {
                position = i;
                count++;
            }
        }
        if (count != 1) {
            throw invalidUpsertPosition(role + " column " + columnName
                + " must appear exactly once in target columns, found " + count);
        }
        return position;
    }

    private static TddlRuntimeException invalidUpsertPosition(String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
            "Invalid externalized UPSERT conflict layout: " + detail);
    }

    // ========== Set-based UPDATE: admit only shapes whose final value is known before physical dispatch ==========

    /**
     * Whether the narrow externalized UPDATE pushdown capability can handle every externalized SET assignment.
     * The executor resolves concrete parameter bindings directly from the accepted LogicalModify, so this
     * planner-side predicate and LogicalModifyViewHandler's execution-time validation describe the same plan shape:
     * a single target, statement-constant externalized assignments, and planner-added NULL addr placeholders.
     * A false result only keeps the UPDATE on the existing select-then-modify path; ordinary tables never reach
     * this externalized-only gate.
     */
    public static boolean canUseExternalizedUpdatePushdown(LogicalModify modify, TableMeta tableMeta) {
        if (modify == null || tableMeta == null || !modify.isUpdate()
            || modify.getTableInfo() == null || !modify.getTableInfo().isSingleSource()
            || !modify.getTableInfo().isSingleTarget()
            || !(modify.getOriginalSqlNode() instanceof SqlUpdate)) {
            return false;
        }
        List<String> updateColumns = modify.getUpdateColumnList();
        List<RexNode> sourceExpressions = modify.getSourceExpressionList();
        if (updateColumns == null || sourceExpressions == null
            || updateColumns.size() != sourceExpressions.size()) {
            return false;
        }

        // Validate both representations of SET. The original SqlUpdate proves that the user RHS is a statement
        // constant; the LogicalModify lists prove that SQL-to-Rel produced the exact content/addr slots consumed by
        // the physical parameter builder. Accepting only one representation could admit a stale or reshaped plan.
        SqlUpdate sqlUpdate = (SqlUpdate) modify.getOriginalSqlNode();
        SqlNodeList targetColumns = sqlUpdate.getTargetColumnList();
        SqlNodeList rhsExpressions = sqlUpdate.getSourceExpressionList();
        if (targetColumns == null || rhsExpressions == null || targetColumns.size() != rhsExpressions.size()) {
            return false;
        }

        Map<String, String> terminalMapping = buildRenameMapping(tableMeta);
        Set<String> seenContentColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        boolean foundExternalizedAssignment = false;
        for (int sqlOrdinal = 0; sqlOrdinal < targetColumns.size(); sqlOrdinal++) {
            SqlNode target = targetColumns.get(sqlOrdinal);
            if (!(target instanceof SqlIdentifier)) {
                return false;
            }
            String contentColumn = ((SqlIdentifier) target).getSimple();
            String terminalAddr = terminalMapping.get(contentColumn);
            ColumnMceState state = tableMeta.getColumnMceState(contentColumn);
            if (terminalAddr == null && !state.isDualWrite()) {
                continue;
            }
            if (!isStatementConstantRhs(rhsExpressions.get(sqlOrdinal))
                || !seenContentColumns.add(contentColumn)) {
                return false;
            }
            int sourceOrdinal = findUniqueUpdateColumnOrdinal(updateColumns, contentColumn);
            if (sourceOrdinal < 0) {
                return false;
            }
            foundExternalizedAssignment = true;
            if (terminalAddr != null) {
                continue;
            }
            // A migrating content SET must have exactly one planner-added NULL addr sink. The executor replaces that
            // sink after the fixed physical route is known; a non-NULL or missing sink is not the supported shape.
            String migrationAddr = tableMeta.getMceAddrColumnName(contentColumn);
            int targetOrdinal = findUniqueUpdateColumnOrdinal(updateColumns, migrationAddr);
            if (targetOrdinal < 0 || !(sourceExpressions.get(
                targetOrdinal) instanceof org.apache.calcite.rex.RexLiteral)
                || !org.apache.calcite.rex.RexLiteral.isNullLiteral(sourceExpressions.get(targetOrdinal))) {
                return false;
            }
        }
        return foundExternalizedAssignment;
    }

    private static boolean isStatementConstantRhs(SqlNode rhs) {
        return rhs instanceof SqlLiteral
            || rhs instanceof SqlDynamicParam && ((SqlDynamicParam) rhs).getIndex() >= 0;
    }

    private static int findUniqueUpdateColumnOrdinal(List<String> updateColumns, String expectedColumn) {
        if (expectedColumn == null || expectedColumn.isEmpty()) {
            return -1;
        }
        int result = -1;
        for (int ordinal = 0; ordinal < updateColumns.size(); ordinal++) {
            if (!expectedColumn.equalsIgnoreCase(updateColumns.get(ordinal))) {
                continue;
            }
            if (result >= 0) {
                return -1;
            }
            result = ordinal;
        }
        return result;
    }

    // ========== Shared execution gates and metadata invariants ==========

    /**
     * Whether this table has any column requiring the write path to go through the logical handler.
     * Used by direct-plan/pushdown interception to replace scattered externalized-column checks. Returning false
     * preserves the original direct-plan, pushdown, MPP, and parallel-modify choices for an ordinary table.
     */
    public static boolean needsHandling(TableMeta tableMeta) {
        return tableMeta != null
            && (tableMeta.hasExternalizedColumn() || tableMeta.hasColumnInMceMigration());
    }

    /**
     * Whether RETURNING-based DML must be disabled for this table.
     *
     * <p>The current RETURNING paths bypass the select/Writer boundaries where logical content is converted to a
     * route-owned BlobRef and staging completion is tracked. Disable the optimization for every externalized/MCE
     * lifecycle state until those paths carry the same write contract.</p>
     */
    public static boolean isReturningForbidden(TableMeta tableMeta) {
        return needsHandling(tableMeta);
    }

    /**
     * Fast pre-check used before traversing concrete primary/GSI Writer trees.
     * Ordinary tables return false without allocating the writer-identity transform map.
     */
    public static boolean needsExactRowTransforms(Collection<TableMeta> primaryTargets,
                                                  Collection<? extends Collection<TableMeta>> indexTargets) {
        if (primaryTargets != null) {
            for (TableMeta tableMeta : primaryTargets) {
                if (needsHandling(tableMeta)) {
                    return true;
                }
            }
        }
        if (indexTargets != null) {
            for (Collection<TableMeta> tableMetas : indexTargets) {
                if (tableMetas == null) {
                    continue;
                }
                for (TableMeta tableMeta : tableMetas) {
                    if (needsHandling(tableMeta)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Reject metadata that would make one physical addr column both a terminal rename target and a migration
     * append target. Those actions require different row layouts and cannot be represented by one deterministic
     * write binding.
     */
    public static void validateNoAppendRenameConflict(TableMeta tableMeta) {
        if (!hasExternalizedWriteLifecycle(tableMeta)) {
            return;
        }
        Set<String> renameTargets = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        renameTargets.addAll(buildRenameMapping(tableMeta).values());
        if (renameTargets.isEmpty()) {
            return;
        }

        Set<String> appendAddrNames = getAppendAddrColumns(tableMeta).stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
        if (appendAddrNames.isEmpty()) {
            return;
        }

        Set<String> conflict = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        conflict.addAll(renameTargets);
        conflict.retainAll(appendAddrNames);
        if (!conflict.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "MCE metadata inconsistency: addr column(s) " + conflict
                    + " appear in both RENAME (terminal EXTERNALIZED) and APPEND (mid-migration)"
                    + " sets for table " + tableMeta.getSchemaName() + "." + tableMeta.getTableName()
                    + ". This indicates corrupted MCE column state.");
        }
    }

    /**
     * Validate the complete, reversible content/addr relationship before deriving any rewrite from it.
     * This intentionally fails closed: a partial lifecycle record must not silently produce a physical write
     * against the wrong column.
     */
    static void validateMetadataPairs(TableMeta tableMeta) {
        if (!hasExternalizedWriteLifecycle(tableMeta)) {
            return;
        }

        // Addr columns must be present in the writable physical layout. getAllColumns() also contains logical views,
        // so it cannot by itself prove that a derived APPEND or RENAME target can be written.
        final Map<String, ColumnMeta> writableColumns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ColumnMeta column : tableMeta.getWriteColumns()) {
            writableColumns.put(column.getName(), column);
        }
        final Set<String> readOrRenameTargets = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final Set<String> appendTargets = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        for (ColumnMeta content : tableMeta.getAllColumns()) {
            final String contentName = content.getName();
            final ColumnMceState state = tableMeta.getColumnMceState(contentName);
            if (state == ColumnMceState.NONE) {
                continue;
            }

            // Migration metadata owns the content -> addr relationship. Require its reverse lookup and the addr
            // ColumnMeta.mappingName to point back to the same content column before compiling an APPEND binding.
            final String lifecycleAddr = tableMeta.getMceAddrColumnName(contentName);
            if (state.isDualWrite() && isBlank(lifecycleAddr)) {
                throw metadataPairError(tableMeta, contentName, "missing addr mapping for migration state " + state);
            }

            if (!isBlank(lifecycleAddr)) {
                final String reverseContent = tableMeta.getMceContentColumnByAddr(lifecycleAddr);
                if (!contentName.equalsIgnoreCase(reverseContent)) {
                    throw metadataPairError(tableMeta, contentName,
                        "addr mapping is not reversible: " + lifecycleAddr + " -> " + reverseContent);
                }
                final ColumnMeta addrColumn = writableColumns.get(lifecycleAddr);
                if (addrColumn == null) {
                    throw metadataPairError(tableMeta, contentName,
                        "addr column " + lifecycleAddr + " is missing or not writable");
                }
                if (!contentName.equalsIgnoreCase(addrColumn.getMappingName())) {
                    throw metadataPairError(tableMeta, contentName,
                        "addr column " + lifecycleAddr + " does not map back to content");
                }
                if (state.isDualWrite() && !appendTargets.add(lifecycleAddr)) {
                    throw metadataPairError(tableMeta, contentName,
                        "duplicate append target " + lifecycleAddr);
                }
            }

            if (state.isReadAddr()) {
                // READ_ADDR/EXTERNALIZED also expose the physical read-or-rename target through content.mappingName.
                // It must agree with lifecycle metadata and be unique across all logical content columns.
                final String physicalAddr = content.getMappingName();
                if (!content.isExternalizedColumn() || isBlank(physicalAddr)) {
                    throw metadataPairError(tableMeta, contentName,
                        "missing physical addr mapping for read/rename state " + state);
                }
                if (!isBlank(lifecycleAddr) && !physicalAddr.equalsIgnoreCase(lifecycleAddr)) {
                    throw metadataPairError(tableMeta, contentName,
                        "read/rename target " + physicalAddr + " differs from lifecycle addr " + lifecycleAddr);
                }
                if (!readOrRenameTargets.add(physicalAddr)) {
                    throw metadataPairError(tableMeta, contentName,
                        "duplicate read/rename target " + physicalAddr);
                }
            }
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static boolean hasExternalizedWriteLifecycle(TableMeta tableMeta) {
        return tableMeta != null
            && (tableMeta.hasExternalizedColumn() || tableMeta.hasColumnInMceMigration());
    }

    private static TddlRuntimeException metadataPairError(TableMeta tableMeta, String contentColumn, String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
            "MCE metadata inconsistency for " + tableMeta.getSchemaName() + "." + tableMeta.getTableName() + "."
                + contentColumn + ": " + detail);
    }

    // ========== Logical UPDATE/relocate: compile exact bindings for the concrete leaf Writer ==========

    /**
     * Input rewrite for a terminal externalized value assigned to itself by an in-place LogicalModify.
     *
     * <p>The SELECT projection normally contains two logical copies for {@code SET content = content}: the old-row
     * slot and the writer SET slot, both expressed as {@code FETCH_BLOB(content_addr_)}. Treating the SET slot as a
     * normal externalized value makes the executor write staging and allocate a different BlobRef even though the
     * logical value did not change. Once direct identity is proven, both slots are changed to the raw first operand
     * of FETCH_BLOB and the handler receives {@link RowWriteBinding.WriteIntent#REUSE_EXISTING_ADDR}.
     *
     * <p>This is intentionally strict. Repeated assignments and expressions such as CAST, COALESCE, or CONCAT keep
     * MATERIALIZE_NEW. DDL rejects a logical generated column that references an externalized column, so no
     * executor-side generated expression is allowed to consume either rewritten physical slot.
     */
    public static final class InPlaceUpdateInputRewrite {
        private final LogicalModify modify;
        private final Map<Integer, Set<String>> reuseExistingAddrColumns;

        private InPlaceUpdateInputRewrite(LogicalModify modify,
                                          Map<Integer, Set<String>> reuseExistingAddrColumns) {
            this.modify = modify;
            this.reuseExistingAddrColumns = immutableColumnIntentMap(reuseExistingAddrColumns);
        }

        public LogicalModify getModify() {
            return modify;
        }

        public Map<Integer, Set<String>> getReuseExistingAddrColumns() {
            return reuseExistingAddrColumns;
        }
    }

    /**
     * Return whether an UPDATE target can possibly use the externalized in-place address-reuse rewrite.
     */
    public static boolean needsInPlaceUpdateInputRewrite(LogicalModify modify, ExecutionContext ec) {
        final List<String> updateColumns = modify.getUpdateColumnList();
        final List<Integer> targetTableIndexes = modify.getTableInfo().getTargetTableIndexes();
        if (updateColumns == null || targetTableIndexes == null
            || updateColumns.size() != targetTableIndexes.size()) {
            return false;
        }
        for (int ordinal = 0; ordinal < updateColumns.size(); ordinal++) {
            final TableMeta owner = resolveRelocateOwner(modify, targetTableIndexes.get(ordinal), ec);
            final ColumnMeta content = owner.getColumnIgnoreCase(updateColumns.get(ordinal));
            if (content != null && owner.getColumnMceState(content.getName()) == ColumnMceState.EXTERNALIZED) {
                return true;
            }
        }
        return false;
    }

    /**
     * Rewrite only a provable, sole direct assignment such as {@code SET content = content} to raw-address reuse.
     * WHERE and other SET expressions are independent Project/Filter expressions, so their FETCH_BLOB calls remain
     * untouched. For example, {@code SET content=content, note=LEFT(content, 8)} reuses the address while the note
     * expression still reads the logical content once.
     */
    public static InPlaceUpdateInputRewrite rewriteInPlaceUpdateInput(LogicalModify modify, ExecutionContext ec) {
        final List<String> updateColumns = modify.getUpdateColumnList();
        final List<RexNode> sourceExpressions = modify.getSourceExpressionList();
        final List<Integer> targetTableIndexes = modify.getTableInfo().getTargetTableIndexes();
        if (updateColumns == null || sourceExpressions == null
            || updateColumns.size() != sourceExpressions.size()
            || updateColumns.size() != targetTableIndexes.size()) {
            return new InPlaceUpdateInputRewrite(modify, Collections.emptyMap());
        }

        // The final Project is the row layout consumed by modify Writers: old-row fields first, followed by SET
        // values. Other nodes or malformed widths cannot safely identify both slots of SET content = content.
        RelNode input = modify.getInput();
        while (input instanceof HepRelVertex) {
            input = ((HepRelVertex) input).getCurrentRel();
        }
        if (!(input instanceof Project)) {
            return new InPlaceUpdateInputRewrite(modify, Collections.emptyMap());
        }
        final Project project = (Project) input;
        final List<RexNode> rewrittenProjects = new ArrayList<>(project.getProjects());
        final int setOffset = rewrittenProjects.size() - updateColumns.size();
        if (setOffset < 0) {
            return new InPlaceUpdateInputRewrite(modify, Collections.emptyMap());
        }

        // Group assignments by logical target table and content column. Keeping every ordinal lets the next phase
        // reject repeated SET targets instead of incorrectly treating the last expression as a direct identity.
        final Map<Integer, Map<String, List<Integer>>> updateOrdinals = new HashMap<>();
        for (int ordinal = 0; ordinal < updateColumns.size(); ordinal++) {
            final int tableIndex = targetTableIndexes.get(ordinal);
            final TableMeta owner = resolveRelocateOwner(modify, tableIndex, ec);
            final ColumnMeta content = owner.getColumnIgnoreCase(updateColumns.get(ordinal));
            if (content == null || owner.getColumnMceState(content.getName()) != ColumnMceState.EXTERNALIZED) {
                continue;
            }
            updateOrdinals.computeIfAbsent(tableIndex,
                    ignored -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
                .computeIfAbsent(content.getName(), ignored -> new ArrayList<>())
                .add(ordinal);
        }

        final Map<Integer, Set<String>> reuseExistingAddrColumns = new HashMap<>();
        boolean changed = false;
        // Rewrite only after proving three facts together: one assignment, the original RHS directly references the
        // old-row slot, and both Project positions contain the same FETCH_BLOB expression with a recoverable operand.
        for (Map.Entry<Integer, Map<String, List<Integer>>> tableEntry : updateOrdinals.entrySet()) {
            final int tableIndex = tableEntry.getKey();
            final TableMeta owner = resolveRelocateOwner(modify, tableIndex, ec);
            final Map<String, Integer> sourceColumnIndexes = modify.getSourceColumnIndexMap().get(tableIndex);
            if (sourceColumnIndexes == null) {
                continue;
            }
            for (Map.Entry<String, List<Integer>> columnEntry : tableEntry.getValue().entrySet()) {
                final String contentColumn = columnEntry.getKey();
                final List<Integer> ordinals = columnEntry.getValue();
                final Integer contentRowIndex = sourceColumnIndexes.get(contentColumn);
                if (contentRowIndex == null || contentRowIndex < 0 || contentRowIndex >= setOffset
                    || ordinals.size() != 1) {
                    continue;
                }
                final int setOrdinal = ordinals.get(0);
                final RexNode rhs = sourceExpressions.get(setOrdinal);
                // RexUtil.eq alone is not sufficient here: a planner simplification around a CAST must not silently
                // turn a value-producing assignment into address reuse. Require the original TableModify RHS to be
                // the exact old-row input slot as well as requiring the two Project expressions to match.
                if (!(rhs instanceof RexInputRef) || ((RexInputRef) rhs).getIndex() != contentRowIndex
                    || !RexUtil.eq(rewrittenProjects.get(contentRowIndex),
                    rewrittenProjects.get(setOffset + setOrdinal))) {
                    continue;
                }
                final RexNode rawAddr = fetchBlobRawOperand(rewrittenProjects.get(contentRowIndex));
                if (rawAddr == null) {
                    continue;
                }

                // Both row positions are physical from this point on. Keeping one FETCH_BLOB here would be wasted
                // work, and—more importantly—the handler could mistake its plaintext result for a terminal BlobRef.
                // A logical generated column cannot reference an externalized column (rejected by DDL), so there is
                // no legal executor-side generated expression that still needs the old logical value in this slot.
                rewrittenProjects.set(contentRowIndex, rawAddr);
                rewrittenProjects.set(setOffset + setOrdinal, rawAddr);
                reuseExistingAddrColumns.computeIfAbsent(tableIndex,
                        ignored -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
                    .add(contentColumn);
                changed = true;
            }
        }

        if (!changed) {
            return new InPlaceUpdateInputRewrite(modify, reuseExistingAddrColumns);
        }
        // Copy the Project and LogicalModify rather than mutating cached planner nodes. The returned intent map is
        // consumed later when exact RowWriteBindings are compiled for concrete leaf Writers.
        final RelNode rewrittenInput = LogicalProject.create(project.getInput(), rewrittenProjects,
            project.getRowType().getFieldNames());
        final LogicalModify rewrittenModify = (LogicalModify) modify.copy(modify.getTraitSet(),
            Collections.singletonList(rewrittenInput));
        return new InPlaceUpdateInputRewrite(rewrittenModify, reuseExistingAddrColumns);
    }

    /**
     * DML-specific representation plan for terminal externalized values entering LogicalRelocate.
     *
     * <p>The Project above the locked primary scan normally exposes
     * {@code content = FETCH_BLOB(content_addr_)}. Logical SQL expressions need that value, but a relocate writer that
     * merely copies an unchanged row needs the original address instead. For example:
     *
     * <pre>
     * UPDATE t SET id = id + 1 WHERE content LIKE 'a%'
     *
     * Filter:  FETCH_BLOB(content_addr_) LIKE 'a%'  -- logical read, still required
     * Writer:  content slot = content_addr_         -- physical reuse, no staging
     * </pre>
     *
     * <p>The rewrite changes only the old-row content slot consumed by LogicalRelocate; FETCH_BLOB calls embedded in
     * WHERE or SET expressions remain untouched. If the top Project does not expose a provable FETCH_BLOB operand, the
     * column is intentionally left in the legacy materialize path instead of guessing that an arbitrary expression is
     * a raw address.
     */
    public static final class RelocateInputRewrite {
        private final LogicalModify modify;
        private final Map<Integer, Set<String>> materializeNewColumns;
        private final Map<Integer, Set<String>> reuseExistingAddrColumns;

        private RelocateInputRewrite(LogicalModify modify,
                                     Map<Integer, Set<String>> materializeNewColumns,
                                     Map<Integer, Set<String>> reuseExistingAddrColumns) {
            this.modify = modify;
            this.materializeNewColumns = immutableColumnIntentMap(materializeNewColumns);
            this.reuseExistingAddrColumns = immutableColumnIntentMap(reuseExistingAddrColumns);
        }

        public LogicalModify getModify() {
            return modify;
        }

        public Map<Integer, Set<String>> getMaterializeNewColumns() {
            return materializeNewColumns;
        }

        public Map<Integer, Set<String>> getReuseExistingAddrColumns() {
            return reuseExistingAddrColumns;
        }
    }

    /**
     * Separate logical-value demand from physical-address demand before WriterFactory rows are consumed.
     *
     * <p>A single direct identity assignment such as {@code SET content = content} is treated as address reuse. A
     * repeated assignment is conservatively treated as a new logical value because MySQL evaluates single-table SET
     * items from left to right; for example {@code SET content='x', content=content} must keep {@code 'x'}.</p>
     */
    public static RelocateInputRewrite rewriteRelocateInput(LogicalModify modify, ExecutionContext ec) {
        final Map<Integer, Map<String, List<Integer>>> updateOrdinals = new HashMap<>();
        final List<String> updateColumns = modify.getUpdateColumnList();
        final List<Integer> targetTableIndexes = modify.getTableInfo().getTargetTableIndexes();
        final List<RexNode> sourceExpressions = modify.getSourceExpressionList();
        if (sourceExpressions == null || updateColumns.size() != sourceExpressions.size()
            || updateColumns.size() != targetTableIndexes.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Invalid MCE relocate input: UPDATE columns, expressions and target tables have different sizes");
        }

        // Record every externalized/MCE SET ordinal by owning logical table. Repeated assignments remain visible so
        // MySQL's left-to-right SET semantics can force MATERIALIZE_NEW below.
        for (int ordinal = 0; ordinal < updateColumns.size(); ordinal++) {
            final int tableIndex = targetTableIndexes.get(ordinal);
            final TableMeta owner = resolveRelocateOwner(modify, tableIndex, ec);
            final ColumnMeta column = owner.getColumnIgnoreCase(updateColumns.get(ordinal));
            if (column == null || owner.getColumnMceState(column.getName()) == ColumnMceState.NONE) {
                continue;
            }
            updateOrdinals.computeIfAbsent(tableIndex,
                    ignored -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
                .computeIfAbsent(column.getName(), ignored -> new ArrayList<>())
                .add(ordinal);
        }

        // These sets describe the logical value, not a particular primary/GSI writer:
        //
        //   UPDATE t SET content = CONCAT(content, 'x')
        //     -> materializeNewColumns: primary writes staging, covering GSI consumes its canonical address.
        //
        //   UPDATE t SET id = id + 1 WHERE content LIKE 'a%'
        //     -> reuseExistingAddrColumns: WHERE still evaluates FETCH_BLOB, but every writer copies content_addr_.
        //
        // A column is deliberately absent from both sets when the Project shape cannot prove either case. The
        // binding compiler then keeps the legacy MATERIALIZE_NEW behavior instead of mistaking plaintext for an
        // address.
        final Map<Integer, Set<String>> materializeNewColumns = new HashMap<>();
        final Map<Integer, Set<String>> reuseExistingAddrColumns = new HashMap<>();
        RelNode input = modify.getInput();
        while (input instanceof HepRelVertex) {
            input = ((HepRelVertex) input).getCurrentRel();
        }
        final Project project = input instanceof Project ? (Project) input : null;
        final List<RexNode> rewrittenProjects = project == null ? null : new ArrayList<>(project.getProjects());
        final int setOffset = project == null ? -1 : rewrittenProjects.size() - updateColumns.size();
        boolean changed = false;

        // Classify logical column intent once per source table, before walking the concrete primary/GSI Writer tree.
        // This keeps SQL semantics independent from whichever leaf eventually owns or consumes the BlobRef.
        for (Integer tableIndex : modify.getTableInfo().getTargetTableIndexSet()) {
            final TableMeta owner = resolveRelocateOwner(modify, tableIndex, ec);
            final Map<String, Integer> sourceColumnIndexes = modify.getSourceColumnIndexMap().get(tableIndex);
            if (sourceColumnIndexes == null) {
                continue;
            }
            final Map<String, List<Integer>> tableUpdateOrdinals = updateOrdinals.getOrDefault(tableIndex,
                Collections.emptyMap());
            for (ColumnMeta content : owner.getAllColumns()) {
                final ColumnMceState state = owner.getColumnMceState(content.getName());
                if (state == ColumnMceState.NONE) {
                    continue;
                }
                final List<Integer> ordinals = tableUpdateOrdinals.getOrDefault(content.getName(),
                    Collections.emptyList());
                final Integer contentRowIndex = sourceColumnIndexes.get(content.getName());
                // SET content = content is representation-preserving only when it is the column's sole assignment.
                // SET content = 'x', content = content is not: MySQL's left-to-right assignment makes the second
                // expression observe 'x', so that statement must MATERIALIZE_NEW. Likewise, equivalent-looking
                // planner output from SET content=CAST(content AS CHAR) is still value-producing: require the
                // original TableModify RHS to be the exact old-row input slot before reusing its address.
                final int setOrdinal = ordinals.size() == 1 ? ordinals.get(0) : -1;
                final RexNode rhs = setOrdinal >= 0 ? sourceExpressions.get(setOrdinal) : null;
                final boolean directIdentity = project != null && contentRowIndex != null && ordinals.size() == 1
                    && contentRowIndex >= 0 && contentRowIndex < setOffset
                    && rhs instanceof RexInputRef && ((RexInputRef) rhs).getIndex() == contentRowIndex
                    && RexUtil.eq(rewrittenProjects.get(contentRowIndex),
                    rewrittenProjects.get(setOffset + setOrdinal));
                if (!ordinals.isEmpty() && !directIdentity) {
                    materializeNewColumns.computeIfAbsent(tableIndex,
                            ignored -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
                        .add(content.getName());
                    continue;
                }

                if (state.isDualWrite()) {
                    // Migration rows keep plaintext and the old BlobRef in independent slots. The exact-row binding
                    // compiler addresses that appended addr slot directly, so preserving the logical content Project
                    // is both necessary for READ_ADDR rematerialization and sufficient for legacy address reuse.
                    reuseExistingAddrColumns.computeIfAbsent(tableIndex,
                            ignored -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
                        .add(content.getName());
                    continue;
                }

                if (project == null || contentRowIndex == null || contentRowIndex < 0
                    || contentRowIndex >= setOffset) {
                    // Keep the old materialize behavior when the raw address cannot be proven from the Project.
                    continue;
                }
                final RexNode rawAddr = fetchBlobRawOperand(rewrittenProjects.get(contentRowIndex));
                if (rawAddr == null) {
                    continue;
                }
                rewrittenProjects.set(contentRowIndex, rawAddr);
                if (directIdentity) {
                    rewrittenProjects.set(setOffset + ordinals.get(0), rawAddr);
                }
                reuseExistingAddrColumns.computeIfAbsent(tableIndex,
                        ignored -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
                    .add(content.getName());
                changed = true;
            }
        }

        if (!changed) {
            return new RelocateInputRewrite(modify, materializeNewColumns, reuseExistingAddrColumns);
        }
        // Only old-row/identity slots proven to contain a FETCH_BLOB are replaced. Filter and value-producing SET
        // expressions keep their logical FETCH_BLOB calls in the original input subtree.
        final RelNode rewrittenInput = LogicalProject.create(project.getInput(), rewrittenProjects,
            project.getRowType().getFieldNames());
        final LogicalModify rewrittenModify = (LogicalModify) modify.copy(modify.getTraitSet(),
            Collections.singletonList(rewrittenInput));
        return new RelocateInputRewrite(rewrittenModify, materializeNewColumns, reuseExistingAddrColumns);
    }

    private static RexNode fetchBlobRawOperand(RexNode expression) {
        if (!(expression instanceof RexCall)) {
            return null;
        }
        final RexCall call = (RexCall) expression;
        // FETCH_BLOB(blob_addr, schema, table, column, type_family) is injected by ToDrdsRelVisitor. Only the first
        // operand is the physical value that a REUSE_EXISTING_ADDR writer may copy; the remaining four literals are
        // read-time metadata and must not be mistaken for alternative address operands.
        if (call.getOperator() != TddlOperatorTable.FETCH_BLOB || call.getOperands().size() != 5) {
            return null;
        }
        return call.getOperands().get(0);
    }

    private static Map<Integer, Set<String>> immutableColumnIntentMap(Map<Integer, Set<String>> source) {
        final Map<Integer, Set<String>> result = new LinkedHashMap<>();
        for (Map.Entry<Integer, Set<String>> entry : source.entrySet()) {
            final Set<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            columns.addAll(entry.getValue());
            result.put(entry.getKey(), Collections.unmodifiableSet(columns));
        }
        return Collections.unmodifiableMap(result);
    }

    private static TableMeta resolveRelocateOwner(LogicalModify modify, int tableIndex, ExecutionContext ec) {
        final Pair<String, String> qualifiedName = RelUtils.getQualifiedTableName(
            modify.getTableInfo().getSrcInfos().get(tableIndex).getRefTable());
        return ec.getSchemaManager(qualifiedName.left).getTable(qualifiedName.right);
    }

    /**
     * Compile exact-row bindings for the concrete UPDATE/INSERT leaves reachable from a logical UPSERT.
     *
     * <p>The UPSERT Handler still evaluates VALUES()/SET expressions and its composite Writers still decide whether
     * each conflict is an in-place UPDATE, DELETE+INSERT, or INSERT-then-UPDATE. These bindings describe only the
     * physical representation required after a concrete leaf has been selected. For example:
     *
     * <ul>
     *     <li>{@code SET body=CONCAT(body,'x')} gives the primary UPDATE leaf MATERIALIZE_NEW and a covering GSI
     *     UPDATE leaf CONSUME_CANONICAL_ADDR.</li>
     *     <li>{@code SET id=id+1} gives the primary relocate INSERT leaf
     *     REMATERIALIZE_FOR_PRIMARY_REINSERT for an untouched {@code body}; a following GSI insert consumes that
     *     canonical address.</li>
     *     <li>An INSERT-then-UPDATE row has never been written physically, so its primary insert leaf materializes
     *     every externalized value in the final after-image.</li>
     * </ul>
     *
     * <p>The map is execution-scoped and keyed by Writer identity. Cached Writers remain immutable, while current
     * TableMeta is consulted on every statement execution.
     */
    public static Map<Writer, List<RowWriteBinding>> buildUpsertExactRowTransforms(
        LogicalUpsert upsert, ExecutionContext ec) {
        final IdentityHashMap<Writer, List<RowWriteBinding>> result = new IdentityHashMap<>();
        if (upsert == null || ec == null) {
            return result;
        }

        final Pair<String, String> storageOwner = Pair.of(upsert.getSchemaName(), upsert.getLogicalTableName());
        final TableMeta primaryMeta = ec.getSchemaManager(storageOwner.left).getTable(storageOwner.right);
        if (!needsHandling(primaryMeta)) {
            return result;
        }

        // beforeUpdateMapping identifies the final after-row slots assigned by ON DUPLICATE KEY UPDATE. Normalize an
        // appended migration addr name back to its logical content name so content/addr are classified as one value.
        final Set<String> updatedContentColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final List<String> afterColumns = upsert.getInsertRowType().getFieldNames();
        for (Integer rowIndex : upsert.getBeforeUpdateMapping()) {
            if (rowIndex == null || rowIndex < 0 || rowIndex >= afterColumns.size()) {
                continue;
            }
            final String columnName = afterColumns.get(rowIndex);
            final String contentName = primaryMeta.getMceContentColumnByAddr(columnName);
            updatedContentColumns.add(contentName == null ? columnName : contentName);
        }

        // Untouched values need a distinct relocate policy: an in-place UPDATE leaves them alone, while a primary
        // DELETE+INSERT must either preserve or rematerialize their existing terminal address.
        final Set<String> allExternalizedContentColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (ColumnMeta column : primaryMeta.getAllColumns()) {
            if (primaryMeta.getColumnMceState(column.getName()) != ColumnMceState.NONE) {
                allExternalizedContentColumns.add(column.getName());
            }
        }
        final Set<String> untouchedContentColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        untouchedContentColumns.addAll(allExternalizedContentColumns);
        untouchedContentColumns.removeAll(updatedContentColumns);
        final int afterRowWidth = upsert.getInsertRowType().getFieldCount();
        final boolean primaryMayRelocate = upsert.getPrimaryRelocateWriter() != null;

        // Register primary leaves first. Their MATERIALIZE_NEW/REMATERIALIZE bindings establish the canonical address
        // that later GSI leaves consume for the same logical row.
        if (upsert.getPrimaryUpsertWriter() != null) {
            collectUpsertUpdateTransforms(upsert.getPrimaryUpsertWriter().getUpdaterWriter(), storageOwner,
                updatedContentColumns, ec, result);
            collectUpsertInsertTransforms(upsert.getPrimaryUpsertWriter().getInsertThenUpdateWriter(), storageOwner,
                afterRowWidth, allExternalizedContentColumns, Collections.emptySet(), false, false, ec, result);
        }
        if (upsert.getPrimaryRelocateWriter() != null) {
            final UpsertRelocateWriter writer = upsert.getPrimaryRelocateWriter().unwrap(UpsertRelocateWriter.class);
            collectUpsertUpdateTransforms(writer.getModifyWriter(), storageOwner, updatedContentColumns, ec, result);
            collectUpsertInsertTransforms(writer.getInsertWriter().unwrap(InsertWriter.class), storageOwner,
                afterRowWidth, updatedContentColumns, untouchedContentColumns, true, true, ec, result);
            collectUpsertInsertTransforms(writer.getInsertThenUpdateWriter(), storageOwner, afterRowWidth,
                allExternalizedContentColumns, Collections.emptySet(), false, false, ec, result);
        }

        // GSI leaves never become the storage owner of a primary-table value. Updated columns consume the primary
        // canonical address; an untouched GSI-only relocate may instead reuse its existing address.
        for (UpsertWriter writer : upsert.getGsiUpsertWriters()) {
            collectUpsertUpdateTransforms(writer.getUpdaterWriter(), storageOwner, updatedContentColumns, ec, result);
            collectUpsertInsertTransforms(writer.getInsertThenUpdateWriter(), storageOwner, afterRowWidth,
                allExternalizedContentColumns, Collections.emptySet(), false, false, ec, result);
        }
        for (RelocateWriter relocateWriter : upsert.getGsiRelocateWriters()) {
            final UpsertRelocateWriter writer = relocateWriter.unwrap(UpsertRelocateWriter.class);
            collectUpsertUpdateTransforms(writer.getModifyWriter(), storageOwner, updatedContentColumns, ec, result);
            collectUpsertInsertTransforms(writer.getInsertWriter().unwrap(InsertWriter.class), storageOwner,
                afterRowWidth, updatedContentColumns, untouchedContentColumns, false, primaryMayRelocate, ec, result);
            collectUpsertInsertTransforms(writer.getInsertThenUpdateWriter(), storageOwner, afterRowWidth,
                allExternalizedContentColumns, Collections.emptySet(), false, false, ec, result);
        }

        return Collections.unmodifiableMap(result);
    }

    /**
     * Walk an UPSERT update subtree until reaching the concrete distinct modify leaf that consumes executor rows.
     * Wrapper Writers do not receive DmlWriteContext callbacks and therefore must not be used as transform-map keys.
     */
    private static void collectUpsertUpdateTransforms(Writer writer, Pair<String, String> storageOwner,
                                                      Set<String> updatedContentColumns, ExecutionContext ec,
                                                      IdentityHashMap<Writer, List<RowWriteBinding>> result) {
        if (writer == null) {
            return;
        }
        final Pair<LogicalModify, Mapping> modifyPlan = getModifyPlan(writer);
        if (modifyPlan != null && writer instanceof DistinctWriter) {
            final int rowWidth = modifyPlan.getValue() == null ? 0 : modifyPlan.getValue().getTargetCount();
            final Pair<String, String> target =
                RelUtils.getQualifiedTableName(((DistinctWriter) writer).getTargetTable());
            final TableMeta targetMeta = ec.getSchemaManager(target.left).getTable(target.right);
            final List<RowWriteBinding> transforms = compileModifyTransforms(
                modifyPlan.getKey().getUpdateColumnList(), modifyPlan.getValue(), targetMeta, storageOwner, target,
                rowWidth, updatedContentColumns, Collections.emptySet());
            registerWriterTransforms(writer, transforms, rowWidth, result);
            return;
        }
        for (Writer child : writer.getInputs()) {
            collectUpsertUpdateTransforms(child, storageOwner, updatedContentColumns, ec, result);
        }
    }

    private static void collectUpsertInsertTransforms(InsertWriter writer, Pair<String, String> storageOwner,
                                                      int logicalRowWidth, Set<String> materializeNewColumns,
                                                      Set<String> reuseExistingAddrColumns,
                                                      boolean primaryReinsertLeaf, boolean primaryRowRelocates,
                                                      ExecutionContext ec,
                                                      IdentityHashMap<Writer, List<RowWriteBinding>> result) {
        if (writer == null) {
            return;
        }
        final List<RowWriteBinding> transforms = buildInsertTransforms(writer, storageOwner, logicalRowWidth,
            materializeNewColumns, reuseExistingAddrColumns, primaryReinsertLeaf, primaryRowRelocates, ec);
        registerWriterTransforms(writer, transforms, logicalRowWidth, result);
    }

    private static void registerWriterTransforms(Writer writer, List<RowWriteBinding> transforms,
                                                 int logicalRowWidth,
                                                 IdentityHashMap<Writer, List<RowWriteBinding>> result) {
        if (transforms == null || transforms.isEmpty()) {
            return;
        }
        final Set<Integer> targetIndices = new TreeSet<>();
        for (RowWriteBinding transform : transforms) {
            if (transform.getSourceRowIndex() < 0 || transform.getSourceRowIndex() >= logicalRowWidth
                || transform.getTargetRowIndex() < 0
                || transform.getTargetRowIndex() >= transform.getRequiredRowWidth()
                || !targetIndices.add(transform.getTargetRowIndex())) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Invalid or duplicate UPSERT exact-row mapping for writer "
                        + writer.getClass().getSimpleName());
            }
        }
        result.put(writer, Collections.unmodifiableList(new ArrayList<>(transforms)));
    }

    /**
     * Compile exact-row bindings for all concrete primary/GSI leaves reachable from a LogicalRelocate.
     *
     * <p>The input intent maps are keyed by logical source-table index and were produced before Writer construction.
     * This method translates them into an execution-scoped identity map keyed by the leaf Writer object that will
     * receive {@code prepareModifyRows}. Primary reinsert leaves own new addresses; covering GSI leaves consume those
     * canonical addresses or reuse an old address when no primary relocation occurs.</p>
     */
    public static Map<DistinctWriter, List<RowWriteBinding>> buildExactRowTransforms(
        LogicalRelocate relocate,
        Map<Integer, Set<String>> materializeNewColumns,
        Map<Integer, Set<String>> reuseExistingAddrColumns,
        ExecutionContext ec) {
        final IdentityHashMap<DistinctWriter, List<RowWriteBinding>> result = new IdentityHashMap<>();
        final int logicalRowWidth = relocate.getInput().getRowType().getFieldCount();

        // Relocate Writer trees contain UPDATE and DELETE+INSERT alternatives. Only the primary-table insert leaf is
        // marked as a primary reinsert owner; sibling GSI insert leaves must consume or reuse its address decision.
        for (Map.Entry<Integer, List<RelocateWriter>> entry : relocate.getRelocateWriterMap().entrySet()) {
            final Pair<String, String> storageOwner = RelUtils.getQualifiedTableName(
                relocate.getTableInfo().getSrcInfos().get(entry.getKey()).getRefTable());
            final RelocateWriter primaryRelocate = relocate.getPrimaryRelocateWriter().get(entry.getKey());
            for (RelocateWriter writer : entry.getValue()) {
                collectLeafTransforms(writer.getModifyWriter(), storageOwner, logicalRowWidth,
                    materializeNewColumns.getOrDefault(entry.getKey(), Collections.emptySet()),
                    reuseExistingAddrColumns.getOrDefault(entry.getKey(), Collections.emptySet()), false, false,
                    ec, result);
                final boolean primaryReinsertLeaf = writer == primaryRelocate;
                collectLeafTransforms(writer.getInsertWriter(), storageOwner, logicalRowWidth,
                    materializeNewColumns.getOrDefault(entry.getKey(), Collections.emptySet()),
                    reuseExistingAddrColumns.getOrDefault(entry.getKey(), Collections.emptySet()),
                    primaryReinsertLeaf, primaryRelocate != null, ec, result);
            }
        }

        // Non-relocating UPDATE/DELETE Writer trees still need exact bindings for externalized SET slots, but have no
        // primary reinsert ownership transition.
        for (Map.Entry<Integer, List<DistinctWriter>> entry : relocate.getModifyWriterMap().entrySet()) {
            final Pair<String, String> storageOwner = RelUtils.getQualifiedTableName(
                relocate.getTableInfo().getSrcInfos().get(entry.getKey()).getRefTable());
            for (DistinctWriter writer : entry.getValue()) {
                collectLeafTransforms(writer, storageOwner, logicalRowWidth,
                    materializeNewColumns.getOrDefault(entry.getKey(), Collections.emptySet()),
                    reuseExistingAddrColumns.getOrDefault(entry.getKey(), Collections.emptySet()), false, false,
                    ec, result);
            }
        }

        return Collections.unmodifiableMap(result);
    }

    private static void collectLeafTransforms(Writer writer, Pair<String, String> storageOwner,
                                              int logicalRowWidth,
                                              Set<String> materializeNewColumns,
                                              Set<String> reuseExistingAddrColumns,
                                              boolean primaryReinsertLeaf,
                                              boolean primaryRowRelocates,
                                              ExecutionContext ec,
                                              IdentityHashMap<DistinctWriter, List<RowWriteBinding>> result) {
        if (writer == null) {
            return;
        }

        // Executor row callbacks receive the concrete leaf writer, not a GSI/relocate wrapper. Register bindings
        // against that same object identity; if the current node is only a wrapper, recurse until the leaf that
        // actually consumes the row is found.
        List<RowWriteBinding> transforms = null;
        if (writer instanceof InsertWriter && writer instanceof DistinctWriter) {
            transforms = buildInsertTransforms((InsertWriter) writer, storageOwner, logicalRowWidth,
                materializeNewColumns, reuseExistingAddrColumns, primaryReinsertLeaf, primaryRowRelocates, ec);
        } else {
            Pair<LogicalModify, Mapping> modifyPlan = getModifyPlan(writer);
            if (modifyPlan != null && writer instanceof DistinctWriter) {
                transforms = buildModifyTransforms((DistinctWriter) writer, modifyPlan.getKey(), modifyPlan.getValue(),
                    storageOwner, logicalRowWidth, materializeNewColumns, reuseExistingAddrColumns, ec);
            }
        }

        if (transforms != null) {
            registerTransforms((DistinctWriter) writer, transforms, logicalRowWidth, result);
            return;
        }

        for (Writer child : writer.getInputs()) {
            collectLeafTransforms(child, storageOwner, logicalRowWidth, materializeNewColumns,
                reuseExistingAddrColumns, primaryReinsertLeaf, primaryRowRelocates, ec, result);
        }
    }

    /**
     * Extract the LogicalModify and its SET-ordinal-to-row-slot mapping from a concrete modify leaf.
     * Wrapper Writers return {@code null} and are traversed by the caller until a supported leaf is reached.
     */
    private static Pair<LogicalModify, Mapping> getModifyPlan(Writer writer) {
        if (writer instanceof ShardingModifyWriter) {
            ShardingModifyWriter modifyWriter = (ShardingModifyWriter) writer;
            return Pair.of(modifyWriter.getModify(), modifyWriter.getUpdateSetMapping());
        }
        if (writer instanceof SingleModifyWriter) {
            SingleModifyWriter modifyWriter = (SingleModifyWriter) writer;
            return Pair.of(modifyWriter.getModify(), modifyWriter.getUpdateSetMapping());
        }
        if (writer instanceof BroadcastModifyWriter) {
            BroadcastModifyWriter modifyWriter = (BroadcastModifyWriter) writer;
            return Pair.of(modifyWriter.getModify(), modifyWriter.getUpdateSetMapping());
        }
        return null;
    }

    private static List<RowWriteBinding> buildModifyTransforms(DistinctWriter writer, LogicalModify modify,
                                                               Mapping updateSetMapping,
                                                               Pair<String, String> storageOwner,
                                                               int logicalRowWidth,
                                                               Set<String> materializeNewColumns,
                                                               Set<String> reuseExistingAddrColumns,
                                                               ExecutionContext ec) {
        if (modify.getOperation() != TableModify.Operation.UPDATE) {
            return Collections.emptyList();
        }

        final Pair<String, String> target = RelUtils.getQualifiedTableName(writer.getTargetTable());
        final TableMeta targetMeta = ec.getSchemaManager(target.left).getTable(target.right);
        return compileModifyTransforms(modify.getUpdateColumnList(), updateSetMapping, targetMeta, storageOwner,
            target, logicalRowWidth, materializeNewColumns, reuseExistingAddrColumns);
    }

    /**
     * Translate writer-local UPDATE SET ordinals into positions in the executor's materialized row.
     * Terminal writes replace a renamed addr slot; migration writes preserve the content slot and fill the
     * separately appended addr slot. {@code updateSetMapping} is the only valid bridge between those spaces.
     */
    static List<RowWriteBinding> compileModifyTransforms(List<String> updateColumns, Mapping updateSetMapping,
                                                         TableMeta targetMeta,
                                                         Pair<String, String> storageOwner,
                                                         Pair<String, String> target, int logicalRowWidth) {
        return compileModifyTransforms(updateColumns, updateSetMapping, targetMeta, storageOwner, target,
            logicalRowWidth, Collections.emptySet(), Collections.emptySet());
    }

    /**
     * Compile the intent-aware physical representation contract for one concrete modify leaf.
     *
     * <p>{@code updateColumns} and {@code updateSetMapping}'s source side are Writer-local SET ordinals. The mapping's
     * target side is the zero-based executor row consumed by DmlWriteContext. Lifecycle metadata then determines
     * whether one ordinal is a terminal renamed addr slot or whether two ordinals form a migration content/addr pair.
     * Any missing, repeated, or aliased slot fails closed before a physical plan is built.</p>
     */
    static List<RowWriteBinding> compileModifyTransforms(List<String> updateColumns, Mapping updateSetMapping,
                                                         TableMeta targetMeta,
                                                         Pair<String, String> storageOwner,
                                                         Pair<String, String> target, int logicalRowWidth,
                                                         Set<String> materializeNewColumns,
                                                         Set<String> reuseExistingAddrColumns) {
        if (updateColumns == null || updateColumns.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Integer> terminalOrdinals = new ArrayList<>();
        final Map<String, List<Integer>> migrationContentOrdinals =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Map<String, List<Integer>> migrationAddrOrdinals = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Map<String, String> canonicalContentNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        // Phase 1: classify the Writer-local physical SET list. WriterFactory has already renamed a terminal content
        // target to its addr name, while a migration target has one content slot plus one appended addr slot. The
        // later phases translate these physical SET ordinals through updateSetMapping into executor-row positions.
        for (int i = 0; i < updateColumns.size(); i++) {
            final String updateColumn = updateColumns.get(i);

            final ColumnMeta terminalContent = targetMeta.getColumnByMappingName(updateColumn);
            if (terminalContent != null
                && targetMeta.getColumnMceState(terminalContent.getName()) == ColumnMceState.EXTERNALIZED) {
                terminalOrdinals.add(i);
                continue;
            }

            final ColumnMeta content = targetMeta.getColumnIgnoreCase(updateColumn);
            if (content != null) {
                final ColumnMceState state = targetMeta.getColumnMceState(content.getName());
                if (state == ColumnMceState.EXTERNALIZED) {
                    throw optimizerMappingError(target, content.getName(),
                        "terminal content SET slot was not renamed to " + content.getMappingName());
                }
                if (state.isDualWrite()) {
                    canonicalContentNames.put(content.getName(), content.getName());
                    migrationContentOrdinals.computeIfAbsent(content.getName(), key -> new ArrayList<>()).add(i);
                }
            }

            final String contentColumn = targetMeta.getMceContentColumnByAddr(updateColumn);
            if (contentColumn != null && targetMeta.getColumnMceState(contentColumn).isDualWrite()) {
                canonicalContentNames.put(contentColumn, contentColumn);
                migrationAddrOrdinals.computeIfAbsent(contentColumn, key -> new ArrayList<>()).add(i);
            }
        }

        final Set<String> migrationColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        migrationColumns.addAll(migrationContentOrdinals.keySet());
        migrationColumns.addAll(migrationAddrOrdinals.keySet());
        // Phase 2: prove every migration value has exactly one readable content source and one writable addr target.
        // A partial pair would either lose plaintext dual-write or leave an unmaterialized addr placeholder.
        for (String contentColumn : migrationColumns) {
            final List<Integer> contentOrdinals =
                migrationContentOrdinals.getOrDefault(contentColumn, Collections.emptyList());
            final List<Integer> addrOrdinals =
                migrationAddrOrdinals.getOrDefault(contentColumn, Collections.emptyList());
            final String addrColumn = targetMeta.getMceAddrColumnName(contentColumn);
            if (contentOrdinals.size() == 1 && addrOrdinals.isEmpty()) {
                throw optimizerMappingError(target, contentColumn, "missing addr SET slot " + addrColumn);
            }
            if (contentOrdinals.isEmpty() && addrOrdinals.size() == 1) {
                throw optimizerMappingError(target, contentColumn, "missing content SET slot " + contentColumn);
            }
            if (contentOrdinals.size() != 1 || addrOrdinals.size() != 1) {
                throw optimizerMappingError(target, contentColumn,
                    "migration rewrite requires exactly one content and addr SET slot, found "
                        + contentOrdinals.size() + "/" + addrOrdinals.size());
            }
        }

        final int expectedActionCount = terminalOrdinals.size() + migrationColumns.size();
        if (expectedActionCount == 0) {
            return Collections.emptyList();
        }
        if (updateSetMapping == null) {
            throw optimizerMappingError(target, updateColumns.get(0), "missing update SET mapping");
        }

        // Phase 3: translate SET ordinals through updateSetMapping and attach the ownership intent selected before
        // Writer traversal. RowWriteBinding positions are executor-row slots, never JDBC parameter keys.
        final List<RowWriteBinding> result = new ArrayList<>(expectedActionCount);
        final Set<Integer> targetRowIndices = new TreeSet<>();
        for (Integer ordinal : terminalOrdinals) {
            final String updateColumn = updateColumns.get(ordinal);
            final ColumnMeta terminalContent = targetMeta.getColumnByMappingName(updateColumn);
            final int rowIndex = requireMappedRowSlot(updateSetMapping, ordinal, updateColumn, target,
                logicalRowWidth);
            if (!targetRowIndices.add(rowIndex)) {
                throw optimizerMappingError(target, updateColumn, "duplicate target row slot " + rowIndex);
            }
            final RowWriteBinding.WriteIntent writeIntent = terminalWriteIntent(terminalContent.getName(),
                storageOwner, target, materializeNewColumns, reuseExistingAddrColumns);
            result.add(RowWriteBinding.terminal(storageOwner.left, storageOwner.right,
                terminalContent.getName(), rowIndex, logicalRowWidth, writeIntent));
        }
        for (String contentColumn : migrationColumns) {
            final int sourceOrdinal = migrationContentOrdinals.get(contentColumn).get(0);
            final int targetOrdinal = migrationAddrOrdinals.get(contentColumn).get(0);
            final int sourceRowIndex = requireMappedRowSlot(updateSetMapping, sourceOrdinal, contentColumn, target,
                logicalRowWidth);
            final String addrColumn = updateColumns.get(targetOrdinal);
            final int targetRowIndex = requireMappedRowSlot(updateSetMapping, targetOrdinal, addrColumn, target,
                logicalRowWidth);
            if (sourceRowIndex == targetRowIndex) {
                throw optimizerMappingError(target, contentColumn,
                    "migration source/target row slot alias " + sourceRowIndex);
            }
            if (!targetRowIndices.add(targetRowIndex)) {
                throw optimizerMappingError(target, addrColumn, "duplicate target row slot " + targetRowIndex);
            }
            final RowWriteBinding.WriteIntent writeIntent = terminalWriteIntent(contentColumn, storageOwner,
                target, materializeNewColumns, reuseExistingAddrColumns);
            result.add(RowWriteBinding.migration(storageOwner.left, storageOwner.right,
                canonicalContentNames.get(contentColumn), sourceRowIndex, targetRowIndex, logicalRowWidth,
                writeIntent));
        }

        if (result.size() != expectedActionCount) {
            throw optimizerMappingError(target, updateColumns.get(0),
                "expected " + expectedActionCount + " exact-row actions but produced " + result.size());
        }
        // Phase 4: reject cross-binding write-after-read aliases. The executor applies bindings to one copied row;
        // allowing one action's target to overwrite another action's source would make result depend on loop order.
        for (int i = 0; i < result.size(); i++) {
            for (int j = 0; j < result.size(); j++) {
                if (i != j && result.get(i).getTargetRowIndex() == result.get(j).getSourceRowIndex()) {
                    throw optimizerMappingError(target, result.get(i).getContentColumnName(),
                        "cross-action target/source row slot alias " + result.get(i).getTargetRowIndex()
                            + " with " + result.get(j).getContentColumnName());
                }
            }
        }
        return result;
    }

    private static int requireMappedRowSlot(Mapping updateSetMapping, int ordinal, String column,
                                            Pair<String, String> target, int logicalRowWidth) {
        if (ordinal < 0 || ordinal >= updateSetMapping.getSourceCount()) {
            throw optimizerMappingError(target, column,
                "update SET ordinal " + ordinal + " outside mapping source range "
                    + updateSetMapping.getSourceCount());
        }
        final int rowIndex = updateSetMapping.getTargetOpt(ordinal);
        if (rowIndex < 0) {
            throw optimizerMappingError(target, column, "unmapped update SET slot at ordinal " + ordinal);
        }
        if (rowIndex >= updateSetMapping.getTargetCount()) {
            throw optimizerMappingError(target, column,
                "row slot " + rowIndex + " outside mapping target range " + updateSetMapping.getTargetCount());
        }
        if (rowIndex >= logicalRowWidth) {
            throw optimizerMappingError(target, column,
                "row slot " + rowIndex + " outside logical row width " + logicalRowWidth);
        }
        return rowIndex;
    }

    /**
     * Compile externalized bindings from one concrete INSERT leaf's DynamicValues tuple.
     *
     * <p>RexDynamicParam indices in this tuple are zero-based positions in the already evaluated executor row. They
     * are not JDBC parameter keys. Terminal columns map one row slot to a physical addr; migration columns map the
     * logical content source and appended addr target separately. The selected write intent decides whether this leaf
     * owns materialization, reuses an old address, or consumes the primary owner's canonical address.</p>
     */
    private static List<RowWriteBinding> buildInsertTransforms(InsertWriter writer,
                                                               Pair<String, String> storageOwner,
                                                               int logicalRowWidth,
                                                               Set<String> materializeNewColumns,
                                                               Set<String> reuseExistingAddrColumns,
                                                               boolean primaryReinsertLeaf,
                                                               boolean primaryRowRelocates,
                                                               ExecutionContext ec) {
        final LogicalInsert insert = writer.getInsert();
        final Pair<String, String> target = RelUtils.getQualifiedTableName(writer.getTargetTable());
        final TableMeta targetMeta = ec.getSchemaManager(target.left).getTable(target.right);
        final List<String> fieldNames = insert.getInsertRowType().getFieldNames();
        final LogicalDynamicValues values = RelUtils.getRelInput(insert);
        if (values.getTuples().isEmpty()) {
            return Collections.emptyList();
        }

        // Build the physical INSERT-column -> executor-row-slot bridge from the template tuple. Literal expressions
        // are not valid exact-row sources here because the parent Handler has already produced the final row values.
        final List<RexNode> tuple = values.getTuples().get(0);
        final Map<String, Integer> rowIndexByColumn = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < fieldNames.size(); i++) {
            RexNode value = tuple.get(i);
            if (value instanceof RexDynamicParam) {
                // These dynamic-param indices address the executor row produced for this relocate branch. They are
                // zero-based row slots, not the one-based JDBC parameter keys used by ParameterWriteBinding.
                rowIndexByColumn.put(fieldNames.get(i), ((RexDynamicParam) value).getIndex());
            }
        }

        final List<RowWriteBinding> result = new ArrayList<>();
        // Terminal EXTERNALIZED columns have one renamed physical target. Their content value and resulting BlobRef
        // therefore occupy the same executor-row slot.
        for (String fieldName : fieldNames) {
            final ColumnMeta content = targetMeta.getColumnIgnoreCase(fieldName);
            if (content == null || content.getMappingName() == null
                || targetMeta.getColumnMceState(content.getName()) != ColumnMceState.EXTERNALIZED) {
                continue;
            }
            final Integer rowIndex = rowIndexByColumn.get(fieldName);
            if (rowIndex == null) {
                throw optimizerMappingError(target, fieldName, "terminal INSERT value is not a dynamic row slot");
            }
            final RowWriteBinding.WriteIntent writeIntent = terminalWriteIntent(content.getName(), storageOwner,
                target, materializeNewColumns, reuseExistingAddrColumns, primaryReinsertLeaf,
                primaryRowRelocates);
            result.add(RowWriteBinding.terminal(storageOwner.left, storageOwner.right, content.getName(),
                rowIndex, Math.max(logicalRowWidth, rowIndex + 1), writeIntent));
        }

        // Migration rows retain plaintext content and write the BlobRef into a distinct appended addr slot.
        for (ColumnMeta addrColumn : getAppendAddrColumns(targetMeta)) {
            final String contentColumn = targetMeta.getMceContentColumnByAddr(addrColumn.getName());
            final Integer sourceRowIndex = rowIndexByColumn.get(contentColumn);
            final Integer targetRowIndex = rowIndexByColumn.get(addrColumn.getName());
            if (sourceRowIndex == null || targetRowIndex == null) {
                throw optimizerMappingError(target, addrColumn.getName(),
                    "missing content or addr INSERT row slot");
            }
            final RowWriteBinding.WriteIntent writeIntent = terminalWriteIntent(contentColumn, storageOwner,
                target, materializeNewColumns, reuseExistingAddrColumns, primaryReinsertLeaf,
                primaryRowRelocates);
            result.add(RowWriteBinding.migration(storageOwner.left, storageOwner.right, contentColumn,
                sourceRowIndex, targetRowIndex, Math.max(logicalRowWidth, targetRowIndex + 1), writeIntent));
        }
        return result;
    }

    /**
     * Choose the owner of one terminal BlobRef.
     *
     * <p>Only the primary-table leaf may MATERIALIZE_NEW. A covering GSI for the same logical assignment consumes the
     * canonical address produced by that primary leaf. If the logical value is unchanged, a primary reinsert gets
     * REMATERIALIZE_FOR_PRIMARY_REINSERT and its GSI reinsert consumes the new address; a GSI-only relocate keeps
     * REUSE_EXISTING_ADDR because no primary owner writes a new object. An unclassified column deliberately keeps the
     * legacy per-writer materialization behavior; this is the safe fallback for plan shapes whose raw FETCH_BLOB
     * operand was not recoverable.</p>
     */
    private static RowWriteBinding.WriteIntent terminalWriteIntent(String contentColumn,
                                                                   Pair<String, String> storageOwner,
                                                                   Pair<String, String> target,
                                                                   Set<String> materializeNewColumns,
                                                                   Set<String> reuseExistingAddrColumns) {
        return terminalWriteIntent(contentColumn, storageOwner, target, materializeNewColumns,
            reuseExistingAddrColumns, false, false);
    }

    private static RowWriteBinding.WriteIntent terminalWriteIntent(String contentColumn,
                                                                   Pair<String, String> storageOwner,
                                                                   Pair<String, String> target,
                                                                   Set<String> materializeNewColumns,
                                                                   Set<String> reuseExistingAddrColumns,
                                                                   boolean primaryReinsertLeaf,
                                                                   boolean primaryRowRelocates) {
        // Reuse takes precedence over materialization. A primary DELETE+INSERT cannot keep a terminal address when
        // binlog-compatible reinsert semantics require a new object, while a GSI-only relocate can keep the old one.
        if (reuseExistingAddrColumns.contains(contentColumn)) {
            if (primaryReinsertLeaf) {
                return RowWriteBinding.WriteIntent.REMATERIALIZE_FOR_PRIMARY_REINSERT;
            }
            return !sameQualifiedName(storageOwner, target) && primaryRowRelocates
                ? RowWriteBinding.WriteIntent.CONSUME_CANONICAL_OR_REUSE_EXISTING_ADDR
                : RowWriteBinding.WriteIntent.REUSE_EXISTING_ADDR;
        }
        // A changed logical value is materialized exactly once by its primary storage owner. Covering GSI leaves use
        // the canonical address recorded for the same source-row identity.
        if (materializeNewColumns.contains(contentColumn) && !sameQualifiedName(storageOwner, target)) {
            return RowWriteBinding.WriteIntent.CONSUME_CANONICAL_ADDR;
        }
        // Unclassified shapes intentionally retain the legacy leaf-local materialization behavior. This is safer than
        // guessing that an arbitrary Project value is an existing BlobRef.
        return RowWriteBinding.WriteIntent.MATERIALIZE_NEW;
    }

    private static boolean sameQualifiedName(Pair<String, String> left, Pair<String, String> right) {
        return left.left.equalsIgnoreCase(right.left) && left.right.equalsIgnoreCase(right.right);
    }

    private static void registerTransforms(DistinctWriter writer, List<RowWriteBinding> transforms,
                                           int logicalRowWidth,
                                           IdentityHashMap<DistinctWriter, List<RowWriteBinding>> result) {
        if (transforms.isEmpty()) {
            return;
        }
        final Set<Integer> targetIndices = new TreeSet<>();
        // Validate at registration time while Writer layout metadata is still available. The executor assumes every
        // binding source is readable and every target is unique within one prepared row copy.
        for (RowWriteBinding transform : transforms) {
            if (transform.getSourceRowIndex() < 0 || transform.getSourceRowIndex() >= logicalRowWidth
                || transform.getTargetRowIndex() < 0
                || transform.getTargetRowIndex() >= transform.getRequiredRowWidth()
                || !targetIndices.add(transform.getTargetRowIndex())) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Invalid or duplicate MCE exact-row mapping for writer " + writer.getClass().getSimpleName());
            }
        }
        result.put(writer, Collections.unmodifiableList(new ArrayList<>(transforms)));
    }

    private static TddlRuntimeException optimizerMappingError(Pair<String, String> target, String column,
                                                              String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
            "Invalid MCE exact-row mapping for " + target.left + "." + target.right + "." + column + ": "
                + detail);
    }
}
