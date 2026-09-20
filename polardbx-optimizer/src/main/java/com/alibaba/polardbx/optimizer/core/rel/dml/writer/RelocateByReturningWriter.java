package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.alibaba.polardbx.common.dmlStats.GlobalRelocateReturningStatsSingleton;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.dml.CaseWhenWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.ClassifyResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RowClassifier;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Writer for modify sharding column optimized by returning on logical table, do not take gsi into consideration
 */
public class RelocateByReturningWriter extends AbstractSingleWriter implements CaseWhenWriter {

    private final DistinctWriter deleteWriter;
    private final DistinctWriter insertWriter;
    private final DistinctWriter modifyWriter;

    // the sk is the identifier key, the pk could be the identifier key in ScaleOut/GSI writable phase
    protected final Mapping identifierKeyTargetMapping;
    protected final Mapping identifierKeySourceMapping;
    protected final List<ColumnMeta> identifierKeyMetas;
    protected final boolean modifySkOnly;
    protected final boolean usePartFieldChecker;

    public volatile boolean printed = false;
    List<String> identifierKeyNames = new ArrayList<>();

    public RelocateByReturningWriter(RelOptTable targetTable, DistinctWriter deleteWriter, DistinctWriter insertWriter,
                                     DistinctWriter modifyWriter,
                                     Mapping identifierKeyTargetMapping,
                                     Mapping identifierKeySourceMapping,
                                     List<ColumnMeta> identifierKeyMetas, List<String> identifierKeyNames,
                                     boolean modifySkOnly,
                                     boolean usePartFieldChecker) {
        super(targetTable, Operation.UPDATE);
        this.deleteWriter = deleteWriter;
        this.insertWriter = insertWriter;
        this.modifyWriter = modifyWriter;
        this.identifierKeyTargetMapping = identifierKeyTargetMapping;
        this.identifierKeySourceMapping = identifierKeySourceMapping;
        this.identifierKeyMetas = identifierKeyMetas;
        this.identifierKeyNames = identifierKeyNames;
        this.modifySkOnly = modifySkOnly;
        this.usePartFieldChecker = usePartFieldChecker;
    }

    public DistinctWriter getDeleteWriter() {
        return deleteWriter;
    }

    public DistinctWriter getInsertWriter() {
        return insertWriter;
    }

    public DistinctWriter getModifyWriter() {
        return modifyWriter;
    }

    public Mapping getIdentifierKeyTargetMapping() {
        return identifierKeyTargetMapping;
    }

    public Mapping getIdentifierKeySourceMapping() {
        return identifierKeySourceMapping;
    }

    public List<ColumnMeta> getIdentifierKeyMetas() {
        return identifierKeyMetas;
    }

    public boolean getModifySkOnly() {
        return modifySkOnly;
    }

    public boolean isUsePartFieldChecker() {
        return usePartFieldChecker;
    }

    public List<String> getIdentifierKeyNames() {
        return identifierKeyNames;
    }

    public SourceRows getInput(ExecutionContext ec, ExecutionContext insertEc,
                               Function<DistinctWriter, SourceRows> rowGenerator,
                               RowClassifier classifier, List<RelNode> outDeletePlans,
                               List<RelNode> outInsertPlans, List<RelNode> outModifyPlans,
                               List<RelNode> replicateOutDeletePlans, List<RelNode> replicateOutInsertPlans,
                               List<RelNode> replicateOutModifyPlans) {
        final SourceRows sourceRows = rowGenerator.apply(getDeleteWriter());

        final ClassifyResult classifyResult = classifier.apply(this, sourceRows, new ClassifyResult());
        final List<List<Object>> relocateRows = classifyResult.relocateReturningBeforeRows;
        final List<List<Object>> relocateAfterRows = classifyResult.relocateReturningAfterRows;

        List<RelNode> inputs = getDeleteWriter().getInput(ec, (w) -> relocateRows);
        outDeletePlans.addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isPrimaryWriteRelNode()).collect(
            Collectors.toList()));
        replicateOutDeletePlans
            .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));

        inputs = getInsertWriter().getInput(insertEc, (w) -> relocateAfterRows);
        outInsertPlans.addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isPrimaryWriteRelNode()).collect(
            Collectors.toList()));
        replicateOutInsertPlans
            .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList()));

        return sourceRows;
    }

    /**
     * If the old and new value of all sharding column is identical, use UPDATE/REPLACE instead of DELETE + INSERT.
     * If nothing has changed, just skip this row.
     *
     * @param identicalSk Whether old and new value of all sharding column is identical
     * @param sourceRows Input rows
     */
    @Override
    public ClassifyResult classify(BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> identicalSk,
                                   SourceRows sourceRows, ExecutionContext ec, ClassifyResult result) {
        for (List<Object> row : sourceRows.selectedRows) {
            if (!identicalSk.test(this, Pair.of(row, ImmutableMap.of()))) {
                // set relocateBeforeRows for delete
                result.relocateReturningBeforeRows.add(row);
                // set relocateAfterRows for insert
                result.relocateReturningAfterRows.add(row.subList(row.size() / 2, row.size()));
            }
        }

        GlobalRelocateReturningStatsSingleton.getInstance()
            .incrementRelocatedRows(result.relocateReturningBeforeRows.size());

        return result;
    }

    @Override
    public List<Writer> getInputs() {
        final List<Writer> writerList = Lists.newArrayList();
        writerList.add(insertWriter);
        writerList.add(deleteWriter);
        writerList.add(modifyWriter);
        return writerList;
    }
}
