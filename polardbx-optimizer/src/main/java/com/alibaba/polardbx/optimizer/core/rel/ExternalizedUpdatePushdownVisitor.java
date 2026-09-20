package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Builds an execution-local LogicalModify for the narrow statement-constant externalized UPDATE pushdown path.
 *
 * <p>For example ({@code ?N} denotes JDBC parameter key N):
 * <pre>{@code
 * Terminal:
 *   before: updateColumns=[body],             sourceExpressions=[?1]
 *   after:  updateColumns=[body_addr_],       sourceExpressions=[?1]
 *
 * MCE dual-write:
 *   before: updateColumns=[body, body_addr_], sourceExpressions=[?1, NULL]
 *   after:  updateColumns=[body, body_addr_], sourceExpressions=[?1, ?3]
 * }</pre>
 *
 * <p>The visitor changes only execution-local SET targets and expressions. It does not materialize {@code ?1} or
 * {@code ?3}; the input hook replaces them with route-owned BlobRefs after the original {@code getInput()} path has
 * selected a physical branch.
 *
 * <p>The cached LogicalModify continues to describe logical content columns. This visitor first preserves the
 * ordinary statement-constant evaluation when the original DML path requires it, then copies that node and changes
 * only externalized SET targets/RHS expressions to their physical addr form. Route-owned BlobRefs are materialized
 * later, inside LogicalModifyView's original physical builder. Ordinary UPDATEs use ReplaceCallWithLiteralVisitor
 * directly and never instantiate this class.
 */
public final class ExternalizedUpdatePushdownVisitor extends ReplaceCallWithLiteralVisitor {

    /**
     * SET-list ordinal -> physical target column. Terminal example: {@code {0 -> "body_addr_"}} changes the target
     * at ordinal 0 from {@code body} to {@code body_addr_}. MCE example: {@code {1 -> "body_addr_"}} identifies the
     * address-column slot already appended at ordinal 1; the visitor does not add that slot.
     */
    private final Map<Integer, String> targetColumnReplacements;

    /**
     * SET-list ordinal -> execution-local RHS expression. Terminal-literal example: {@code {0 -> ?3}} changes
     * {@code SET body = 'hello'} to {@code SET body_addr_ = ?3}. MCE example: {@code {1 -> ?3}} changes the appended
     * {@code body_addr_ = NULL} placeholder to {@code body_addr_ = ?3}.
     */
    private final Map<Integer, RexNode> rhsReplacements;

    // Whether the inherited visitor should first perform the ordinary SET/WHERE consistency evaluation.
    private final boolean evaluateOriginalDml;

    public ExternalizedUpdatePushdownVisitor(Map<Integer, ParameterContext> params,
                                             Function<RexNode, Object> calcValueFunc,
                                             Map<Integer, String> targetColumnReplacements,
                                             Map<Integer, RexNode> rhsReplacements,
                                             boolean evaluateOriginalDml) {
        super(new ArrayList<>(), params, calcValueFunc, true);
        this.targetColumnReplacements = targetColumnReplacements;
        this.rhsReplacements = rhsReplacements;
        this.evaluateOriginalDml = evaluateOriginalDml;
    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof LogicalModify) {
            return visit((LogicalModify) other);
        }
        return super.visit(other);
    }

    @Override
    public LogicalModify visit(LogicalModify logicalModify) {
        LogicalModify evaluated = evaluateOriginalDml ? super.visit(logicalModify) : logicalModify;
        // Copy both lists so execution-specific physical names and parameters cannot leak into a cached plan reused
        // by another execution or by a later MCE metadata state. Apply the external-column projection only after the
        // ordinary visitor has evaluated the input/WHERE tree and all SET expressions that require one CN value.
        List<String> updateColumns = new ArrayList<>(evaluated.getUpdateColumnList());
        List<RexNode> sourceExpressions = new ArrayList<>(evaluated.getSourceExpressionList());
        for (Map.Entry<Integer, String> replacement : targetColumnReplacements.entrySet()) {
            updateColumns.set(replacement.getKey(), replacement.getValue());
        }
        for (Map.Entry<Integer, RexNode> replacement : rhsReplacements.entrySet()) {
            sourceExpressions.set(replacement.getKey(), replacement.getValue());
        }

        return new LogicalModify(evaluated.getCluster(),
            evaluated.getTraitSet(),
            evaluated.getTable(),
            evaluated.getCatalogReader(),
            evaluated.getInput(),
            evaluated.getOperation(),
            updateColumns,
            sourceExpressions,
            evaluated.isFlattened(),
            evaluated.getKeywords(),
            evaluated.getHints(),
            evaluated.getHintContext(),
            evaluated.getTableInfo(),
            evaluated.getExtraTargetTables(),
            evaluated.getExtraTargetColumns(),
            evaluated.getPrimaryModifyWriters(),
            evaluated.getGsiModifyWriters(),
            evaluated.getGsiModifyWritersMap(),
            evaluated.isWithoutPk(),
            evaluated.isModifyForeignKey(),
            evaluated.getModifyTopNInfo(),
            evaluated.getMultiWriteInfo());
    }
}
