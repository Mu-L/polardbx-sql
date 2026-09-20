package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.GsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.ClassifyResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RowClassifier;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;

import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author chenmo.cm
 */
public class RelocateGsiByReturningWriter extends RelocateByReturningWriter implements GsiWriter {

    protected final TableMeta gsiMeta;

    private final boolean forceRelocate;

    public RelocateGsiByReturningWriter(RelOptTable targetTable, DistinctWriter deleteWriter,
                                        DistinctWriter insertWriter,
                                        DistinctWriter modifyWriter, Mapping skTargetMapping, Mapping skSourceMapping,
                                        List<ColumnMeta> skMetas, List<String> identifierKeyName, boolean modifySkOnly,
                                        boolean usePartFieldChecker,
                                        TableMeta gsiMeta, boolean forceRelocate) {
        super(targetTable,
            deleteWriter,
            insertWriter,
            modifyWriter,
            skTargetMapping,
            skSourceMapping,
            skMetas,
            identifierKeyName,
            modifySkOnly,
            usePartFieldChecker);
        this.gsiMeta = gsiMeta;
        this.forceRelocate = forceRelocate;
    }

    @Override
    public SourceRows getInput(ExecutionContext ec, ExecutionContext insertEc,
                               Function<DistinctWriter, SourceRows> rowGenerator,
                               RowClassifier classifier, List<RelNode> outDeletePlans,
                               List<RelNode> outInsertPlans, List<RelNode> outModifyPlans,
                               List<RelNode> replicateOutDeletePlans, List<RelNode> replicateOutInsertPlans,
                               List<RelNode> replicateOutModifyPlans) {
        final SourceRows distinctRows = rowGenerator.apply(getDeleteWriter());

        final ClassifyResult result = classifier.apply(this, distinctRows, new ClassifyResult());
        final List<List<Object>> modifyRows = result.modifyBeforeRows;
        final List<List<Object>> relocateRows = result.relocateReturningBeforeRows;
        final List<List<Object>> relocateAfterRows = result.relocateReturningAfterRows;

        if (GlobalIndexMeta.canWrite(ec, gsiMeta)) {
            // WRITE_ONLY or PUBLIC
            List<RelNode> inputs = getModifyWriter().getInput(ec, (w) -> modifyRows);
            outModifyPlans.addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isPrimaryWriteRelNode()).collect(
                Collectors.toList()));
            replicateOutModifyPlans
                .addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList()));

            inputs = getDeleteWriter().getInput(ec, (w) -> relocateRows);
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
        } else if (GlobalIndexMeta.canDelete(ec, gsiMeta)) {
            // DELETE_ONLY
            List<RelNode> inputs = getDeleteWriter().getInput(ec, (w) -> modifyRows);
            outDeletePlans.addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isPrimaryWriteRelNode()).collect(
                Collectors.toList()));
            replicateOutDeletePlans.addAll(
                inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList()));

            inputs = getDeleteWriter().getInput(ec, (w) -> relocateRows);
            outDeletePlans.addAll(inputs.stream().filter(o -> ((BaseQueryOperation) o).isPrimaryWriteRelNode()).collect(
                Collectors.toList()));
            replicateOutDeletePlans.addAll(
                inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList()));
        }

        return distinctRows;
    }

    @Override
    public ClassifyResult classify(BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> identicalSk,
                                   SourceRows sourceRows, ExecutionContext ec, ClassifyResult result) {

        for (List<Object> row : sourceRows.selectedRows) {
            if (!forceRelocate && identicalSk.test(this, Pair.of(row, ImmutableMap.of()))) {
                result.modifyBeforeRows.add(row);
            } else {
                result.relocateReturningBeforeRows.add(row);
                result.relocateReturningAfterRows.add(row.subList(row.size() / 2, row.size()));
            }
        }

        return result;
    }

    @Override
    public TableMeta getGsiMeta() {
        return gsiMeta;
    }

    @Override
    public boolean isGsiPublished(ExecutionContext ec) {
        return GlobalIndexMeta.isPublished(ec, getGsiMeta());
    }
}
