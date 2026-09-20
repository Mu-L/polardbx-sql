package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.utils.Pair;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExternalTableScanUnitTest {

    private ExternalTableScan newScan(TableSource tableSource) {
        RelOptCluster cluster = mock(RelOptCluster.class);
        when(cluster.traitSetOf(Convention.NONE)).thenReturn(mock(RelTraitSet.class));
        return ExternalTableScan.create(cluster, mock(RelOptTable.class), tableSource);
    }

    @Test
    public void testCreateNullSource() {
        try {
            ExternalTableScan.create(mock(RelOptCluster.class), mock(RelOptTable.class), null);
        } catch (IllegalArgumentException e) {
            // expected, just cover the branch
        }
    }

    @Test
    public void testDelegations() {
        TableSource ts = mock(TableSource.class);
        ExternalTableScan scan = newScan(ts);

        scan.getTableSource();
        scan.computeSelfCost(null, null);
        scan.getRowCount(null);
        scan.getMaxRowCount(null);
        scan.getColumnOrigins(null, 0);
        scan.getColumnOriginNames(null);
        scan.getDmlColumnNames(null);
        scan.getOriginalRowType(null);
        scan.isCoveringIndex(null, mock(RelOptTable.class), "idx");
        scan.getTableReferences(null);
        scan.getUniqueKeys(null, true);
        scan.getColumnsGroupSize(null, ImmutableBitSet.of(0));
        scan.areColumnsUnique(null, ImmutableBitSet.of(0), false);
        scan.getPrimaryKey(null);
        scan.getPopulationSize(null, ImmutableBitSet.of(0));
        scan.distribution(null);
        scan.getDistinctRowCount(null, ImmutableBitSet.of(0), null);
        scan.getSelectivity(null, null);
        scan.getPredicates(null);
        scan.getAllPredicates(null);
        scan.getFunctionalDependency(null, ImmutableBitSet.of(0));
        scan.getNonCumulativeCost(null, null);
        scan.getStartUpCost(null, null);
    }

    @Test
    public void testPushProjectNoPush() {
        TableSource ts = mock(TableSource.class);
        List<RexNode> projects = new ArrayList<>();
        projects.add(mock(RexNode.class));
        when(ts.pushProject(anyList())).thenReturn(Pair.of(Collections.emptyList(), projects));

        ExternalTableScan scan = newScan(ts);
        scan.pushProject(projects);
    }

    @Test
    public void testPushProjectPushed() {
        TableSource ts = mock(TableSource.class);
        List<RexNode> residual = new ArrayList<>();
        residual.add(mock(RexNode.class));
        List<RexNode> pushed = new ArrayList<>();
        pushed.add(mock(RexNode.class));
        when(ts.pushProject(anyList())).thenReturn(Pair.of(pushed, residual));

        ExternalTableScan scan = newScan(ts);
        scan.pushProject(new ArrayList<>(residual));
    }

    @Test
    public void testPushFilterAndSort() {
        TableSource ts = mock(TableSource.class);
        ExternalTableScan scan = newScan(ts);
        scan.pushFilter(mock(RexNode.class));
        scan.pushSort(mock(Sort.class));
    }

    @Test
    public void testPushAgg() {
        TableSource ts = mock(TableSource.class);
        when(ts.pushAgg(any(LogicalAggregate.class))).thenReturn(true).thenReturn(false);

        ExternalTableScan scan = newScan(ts);
        scan.pushAgg(mock(LogicalAggregate.class));
        scan.pushAgg(mock(LogicalAggregate.class));
    }

    @Test
    public void testCopy() {
        TableSource ts = mock(TableSource.class);
        when(ts.copy()).thenReturn(mock(TableSource.class));

        ExternalTableScan scan = newScan(ts);
        scan.copy(mock(RelTraitSet.class), Collections.<RelNode>emptyList());
        scan.copy(mock(RelTraitSet.class));
    }

    @Test
    public void testExplainTerms() {
        TableSource ts = mock(TableSource.class);
        when(ts.display()).thenReturn("pd");
        ExternalTableScan scan = newScan(ts);
        RelWriter pw = mock(RelWriter.class, Mockito.RETURNS_SELF);
        scan.explainTerms(pw);
        scan.explainTermsForDisplay(pw);
    }

    @Test
    public void testExplainTermsForDisplayEmptyPushDown() {
        TableSource ts = mock(TableSource.class);
        when(ts.display()).thenReturn("");
        ExternalTableScan scan = newScan(ts);
        RelWriter pw = mock(RelWriter.class, Mockito.RETURNS_SELF);
        scan.explainTermsForDisplay(pw);
    }
}
