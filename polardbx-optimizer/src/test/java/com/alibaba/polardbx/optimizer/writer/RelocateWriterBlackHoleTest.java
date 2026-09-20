package com.alibaba.polardbx.optimizer.writer;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.ClassifyResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RowClassifier;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RelocateWriterBlackHoleTest {

    /**
     * When modifyBlackHole = true, all rows should go to relocateRows (delete + insert),
     * and modifyRows should be empty. The classifier should NOT be called.
     */
    @Test
    public void testGetInput_modifyBlackHoleTrue_allRowsRelocated() {
        // Setup mocks
        RelOptTable targetTable = mock(RelOptTable.class);
        DistinctWriter deleteWriter = mock(DistinctWriter.class);
        DistinctWriter insertWriter = mock(DistinctWriter.class);
        DistinctWriter modifyWriter = mock(DistinctWriter.class);
        Mapping identifierKeyTargetMapping = Mappings.createIdentity(1);
        Mapping identifierKeySourceMapping = Mappings.createIdentity(1);
        List<ColumnMeta> identifierKeyMetas = Collections.emptyList();

        // Create RelocateWriter with modifyBlackHole = true
        RelocateWriter writer = new RelocateWriter(
            targetTable, deleteWriter, insertWriter, modifyWriter,
            identifierKeyTargetMapping, identifierKeySourceMapping,
            identifierKeyMetas, false, false, true);

        // Prepare source rows
        List<Object> row1 = Arrays.asList("val1", "val2");
        List<Object> row2 = Arrays.asList("val3", "val4");
        List<List<Object>> selectedRows = new ArrayList<>(Arrays.asList(row1, row2));
        SourceRows sourceRows = SourceRows.createFromSelect(selectedRows);

        Function<DistinctWriter, SourceRows> rowGenerator = (w) -> sourceRows;

        // Classifier should not be called when modifyBlackHole=true
        RowClassifier classifier = mock(RowClassifier.class);

        // Mock writer getInput to return empty lists to avoid NPE
        BaseQueryOperation mockModifyOp = mock(BaseQueryOperation.class);
        when(mockModifyOp.isReplicateRelNode()).thenReturn(false);
        when(mockModifyOp.isPrimaryWriteRelNode()).thenReturn(true);
        BaseQueryOperation mockDeleteOp = mock(BaseQueryOperation.class);
        when(mockDeleteOp.isReplicateRelNode()).thenReturn(false);
        when(mockDeleteOp.isPrimaryWriteRelNode()).thenReturn(true);
        BaseQueryOperation mockInsertOp = mock(BaseQueryOperation.class);
        when(mockInsertOp.isReplicateRelNode()).thenReturn(false);
        when(mockInsertOp.isPrimaryWriteRelNode()).thenReturn(true);

        // modifyWriter should receive empty rows
        when(modifyWriter.getInput(any(ExecutionContext.class), any()))
            .thenAnswer(invocation -> {
                Function<DistinctWriter, List<List<Object>>> gen = invocation.getArgument(1);
                List<List<Object>> rows = gen.apply(modifyWriter);
                // modifyRows should be empty when modifyBlackHole=true
                Assert.assertTrue("modifyRows should be empty for black hole", rows.isEmpty());
                return Collections.singletonList(mockModifyOp);
            });

        // deleteWriter should receive all rows (relocateRows)
        when(deleteWriter.getInput(any(ExecutionContext.class), any()))
            .thenAnswer(invocation -> {
                Function<DistinctWriter, List<List<Object>>> gen = invocation.getArgument(1);
                List<List<Object>> rows = gen.apply(deleteWriter);
                // relocateRows should contain all selected rows
                Assert.assertEquals("relocateRows should contain all rows", 2, rows.size());
                return Collections.singletonList(mockDeleteOp);
            });

        // insertWriter should receive all rows (relocateRows)
        when(insertWriter.getInput(any(ExecutionContext.class), any()))
            .thenAnswer(invocation -> {
                Function<DistinctWriter, List<List<Object>>> gen = invocation.getArgument(1);
                List<List<Object>> rows = gen.apply(insertWriter);
                Assert.assertEquals("insertRows should contain all rows", 2, rows.size());
                return Collections.singletonList(mockInsertOp);
            });

        ExecutionContext ec = mock(ExecutionContext.class);
        ExecutionContext insertEc = mock(ExecutionContext.class);

        List<RelNode> outDeletePlans = new ArrayList<>();
        List<RelNode> outInsertPlans = new ArrayList<>();
        List<RelNode> outModifyPlans = new ArrayList<>();
        List<RelNode> replicateOutDeletePlans = new ArrayList<>();
        List<RelNode> replicateOutInsertPlans = new ArrayList<>();
        List<RelNode> replicateOutModifyPlans = new ArrayList<>();

        writer.getInput(ec, insertEc, rowGenerator, classifier,
            outDeletePlans, outInsertPlans, outModifyPlans,
            replicateOutDeletePlans, replicateOutInsertPlans, replicateOutModifyPlans);

        // Classifier should never be called when modifyBlackHole=true
        verify(classifier, never()).apply(any(), any(), any());

        // Verify plans were populated
        Assert.assertEquals(1, outModifyPlans.size());
        Assert.assertEquals(1, outDeletePlans.size());
        Assert.assertEquals(1, outInsertPlans.size());
    }

    /**
     * When modifyBlackHole = false, the classifier should be called to determine
     * which rows are modify and which are relocate.
     */
    @Test
    public void testGetInput_modifyBlackHoleFalse_classifierCalled() {
        // Setup mocks
        RelOptTable targetTable = mock(RelOptTable.class);
        DistinctWriter deleteWriter = mock(DistinctWriter.class);
        DistinctWriter insertWriter = mock(DistinctWriter.class);
        DistinctWriter modifyWriter = mock(DistinctWriter.class);
        Mapping identifierKeyTargetMapping = Mappings.createIdentity(1);
        Mapping identifierKeySourceMapping = Mappings.createIdentity(1);
        List<ColumnMeta> identifierKeyMetas = Collections.emptyList();

        // Create RelocateWriter with modifyBlackHole = false
        RelocateWriter writer = new RelocateWriter(
            targetTable, deleteWriter, insertWriter, modifyWriter,
            identifierKeyTargetMapping, identifierKeySourceMapping,
            identifierKeyMetas, false, false, false);

        // Prepare source rows
        List<Object> row1 = Arrays.asList("val1", "val2");
        List<List<Object>> selectedRows = new ArrayList<>(Collections.singletonList(row1));
        SourceRows sourceRows = SourceRows.createFromSelect(selectedRows);

        Function<DistinctWriter, SourceRows> rowGenerator = (w) -> sourceRows;

        // Classifier puts all into modifyRows (no sk change)
        RowClassifier classifier = (relocateWriter, src, result) -> {
            result.modifyRows.addAll(src.selectedRows);
            return result;
        };

        BaseQueryOperation mockModifyOp = mock(BaseQueryOperation.class);
        when(mockModifyOp.isReplicateRelNode()).thenReturn(false);
        when(mockModifyOp.isPrimaryWriteRelNode()).thenReturn(true);
        BaseQueryOperation mockDeleteOp = mock(BaseQueryOperation.class);
        when(mockDeleteOp.isReplicateRelNode()).thenReturn(false);
        when(mockDeleteOp.isPrimaryWriteRelNode()).thenReturn(true);
        BaseQueryOperation mockInsertOp = mock(BaseQueryOperation.class);
        when(mockInsertOp.isReplicateRelNode()).thenReturn(false);
        when(mockInsertOp.isPrimaryWriteRelNode()).thenReturn(true);

        // modifyWriter should receive 1 row
        when(modifyWriter.getInput(any(ExecutionContext.class), any()))
            .thenAnswer(invocation -> {
                Function<DistinctWriter, List<List<Object>>> gen = invocation.getArgument(1);
                List<List<Object>> rows = gen.apply(modifyWriter);
                Assert.assertEquals(1, rows.size());
                return Collections.singletonList(mockModifyOp);
            });

        // deleteWriter should receive 0 rows (no relocate)
        when(deleteWriter.getInput(any(ExecutionContext.class), any()))
            .thenAnswer(invocation -> {
                Function<DistinctWriter, List<List<Object>>> gen = invocation.getArgument(1);
                List<List<Object>> rows = gen.apply(deleteWriter);
                Assert.assertEquals(0, rows.size());
                return Collections.singletonList(mockDeleteOp);
            });

        when(insertWriter.getInput(any(ExecutionContext.class), any()))
            .thenAnswer(invocation -> {
                Function<DistinctWriter, List<List<Object>>> gen = invocation.getArgument(1);
                List<List<Object>> rows = gen.apply(insertWriter);
                Assert.assertEquals(0, rows.size());
                return Collections.singletonList(mockInsertOp);
            });

        ExecutionContext ec = mock(ExecutionContext.class);
        ExecutionContext insertEc = mock(ExecutionContext.class);

        List<RelNode> outDeletePlans = new ArrayList<>();
        List<RelNode> outInsertPlans = new ArrayList<>();
        List<RelNode> outModifyPlans = new ArrayList<>();
        List<RelNode> replicateOutDeletePlans = new ArrayList<>();
        List<RelNode> replicateOutInsertPlans = new ArrayList<>();
        List<RelNode> replicateOutModifyPlans = new ArrayList<>();

        writer.getInput(ec, insertEc, rowGenerator, classifier,
            outDeletePlans, outInsertPlans, outModifyPlans,
            replicateOutDeletePlans, replicateOutInsertPlans, replicateOutModifyPlans);

        Assert.assertEquals(1, outModifyPlans.size());
    }

    /**
     * When modifyBlackHole = true and there are no selected rows,
     * both modify and relocate should be empty.
     */
    @Test
    public void testGetInput_modifyBlackHoleTrue_emptyRows() {
        RelOptTable targetTable = mock(RelOptTable.class);
        DistinctWriter deleteWriter = mock(DistinctWriter.class);
        DistinctWriter insertWriter = mock(DistinctWriter.class);
        DistinctWriter modifyWriter = mock(DistinctWriter.class);
        Mapping identifierKeyTargetMapping = Mappings.createIdentity(1);
        Mapping identifierKeySourceMapping = Mappings.createIdentity(1);
        List<ColumnMeta> identifierKeyMetas = Collections.emptyList();

        RelocateWriter writer = new RelocateWriter(
            targetTable, deleteWriter, insertWriter, modifyWriter,
            identifierKeyTargetMapping, identifierKeySourceMapping,
            identifierKeyMetas, false, false, true);

        // Empty source rows
        SourceRows sourceRows = SourceRows.createFromSelect(new ArrayList<>());
        Function<DistinctWriter, SourceRows> rowGenerator = (w) -> sourceRows;
        RowClassifier classifier = mock(RowClassifier.class);

        BaseQueryOperation mockOp = mock(BaseQueryOperation.class);
        when(mockOp.isReplicateRelNode()).thenReturn(false);
        when(mockOp.isPrimaryWriteRelNode()).thenReturn(true);

        when(modifyWriter.getInput(any(ExecutionContext.class), any()))
            .thenAnswer(invocation -> {
                Function<DistinctWriter, List<List<Object>>> gen = invocation.getArgument(1);
                List<List<Object>> rows = gen.apply(modifyWriter);
                Assert.assertTrue(rows.isEmpty());
                return Collections.singletonList(mockOp);
            });

        when(deleteWriter.getInput(any(ExecutionContext.class), any()))
            .thenAnswer(invocation -> {
                Function<DistinctWriter, List<List<Object>>> gen = invocation.getArgument(1);
                List<List<Object>> rows = gen.apply(deleteWriter);
                Assert.assertTrue(rows.isEmpty());
                return Collections.singletonList(mockOp);
            });

        when(insertWriter.getInput(any(ExecutionContext.class), any()))
            .thenAnswer(invocation -> {
                Function<DistinctWriter, List<List<Object>>> gen = invocation.getArgument(1);
                List<List<Object>> rows = gen.apply(insertWriter);
                Assert.assertTrue(rows.isEmpty());
                return Collections.singletonList(mockOp);
            });

        ExecutionContext ec = mock(ExecutionContext.class);
        ExecutionContext insertEc = mock(ExecutionContext.class);

        List<RelNode> outDeletePlans = new ArrayList<>();
        List<RelNode> outInsertPlans = new ArrayList<>();
        List<RelNode> outModifyPlans = new ArrayList<>();

        writer.getInput(ec, insertEc, rowGenerator, classifier,
            outDeletePlans, outInsertPlans, outModifyPlans,
            new ArrayList<>(), new ArrayList<>(), new ArrayList<>());

        verify(classifier, never()).apply(any(), any(), any());
    }
}
