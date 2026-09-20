package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.util.ImmutableBitSet;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TableSourceTest {

    public static class TestTableSource extends TableSource {
        public TestTableSource(RelOptTable table) {
            super(table);
        }

        public TestTableSource() {
            super(null);
        }

        @Override
        public String connectorType() {
            return "test";
        }

        @Override
        public Pair<List<RexNode>, List<RexNode>> pushProject(List<RexNode> projects) {
            return new Pair<>(projects, Collections.emptyList());
        }

        @Override
        public Pair<Boolean, List<RexNode>> pushFilter(RexNode condition) {
            return new Pair<>(false, Collections.emptyList());
        }

        @Override
        public boolean pushSort(Sort sort) {
            return false;
        }

        @Override
        public boolean pushAgg(LogicalAggregate agg) {
            return false;
        }

        @Override
        public TableSource copy() {
            return new TestTableSource(table);
        }

        @Override
        public String display() {
            return "";
        }

        @Override
        public Map<String, Object> toJson() {
            Map<String, Object> json = new HashMap<>();
            json.put("class", TestTableSource.class.getName());
            return json;
        }

        @Override
        public void fromJsonState(Map<String, Object> json, RelOptTable table) {
        }
    }

    private RelOptTable mockTable() {
        RelOptTable table = mock(RelOptTable.class);
        RelDataType rowType = mock(RelDataType.class);
        RelDataTypeField field = mock(RelDataTypeField.class);
        when(field.getIndex()).thenReturn(0);
        when(rowType.getFieldList()).thenReturn(Collections.singletonList(field));
        when(table.getRowType()).thenReturn(rowType);
        when(table.getRowCount()).thenReturn(100.0);
        return table;
    }

    @Test
    public void testConstructorAndGetTable() {
        RelOptTable table = mockTable();
        TestTableSource source = new TestTableSource(table);
        assertEquals(table, source.getTable());
    }

    @Test
    public void testGetOptions() {
        TestTableSource source = new TestTableSource(mockTable());
        assertTrue(source.getOptions().isEmpty());
    }

    @Test
    public void testGetRowType() {
        RelOptTable table = mockTable();
        TestTableSource source = new TestTableSource(table);
        assertEquals(table.getRowType(), source.getRowType());
    }

    @Test
    public void testGetRowCount() {
        RelOptTable table = mockTable();
        TestTableSource source = new TestTableSource(table);
        assertEquals(100.0, source.getRowCount(mock(RelMetadataQuery.class)), 0.001);
    }

    @Test
    public void testGetMaxRowCount() {
        TestTableSource source = new TestTableSource(mockTable());
        assertEquals(Double.POSITIVE_INFINITY, source.getMaxRowCount(null), 0.001);
    }

    @Test
    public void testGetColumnOrigins() {
        RelOptTable table = mockTable();
        TestTableSource source = new TestTableSource(table);
        Set<?> origins = source.getColumnOrigins(null, 0);
        assertEquals(1, origins.size());
    }

    @Test
    public void testGetColumnOriginNames() {
        RelOptTable table = mockTable();
        TestTableSource source = new TestTableSource(table);
        List<Set<org.apache.calcite.rel.metadata.RelColumnOrigin>> names =
            source.getColumnOriginNames(null);
        assertEquals(1, names.size());
    }

    @Test
    public void testGetDmlColumnNames() {
        RelOptTable table = mockTable();
        TestTableSource source = new TestTableSource(table);
        assertNotNull(source.getDmlColumnNames(null));
    }

    @Test
    public void testGetOriginalRowType() {
        RelOptTable table = mockTable();
        TestTableSource source = new TestTableSource(table);
        assertEquals(table.getRowType(), source.getOriginalRowType(null));
    }

    @Test
    public void testIsCoveringIndexReturnsNull() {
        TestTableSource source = new TestTableSource(mockTable());
        assertNull(source.isCoveringIndex(null, null, null));
    }

    @Test
    public void testGetTableReferences() {
        RelOptTable table = mockTable();
        TestTableSource source = new TestTableSource(table);
        Set<RexTableInputRef.RelTableRef> refs = source.getTableReferences(null);
        assertEquals(1, refs.size());
    }

    @Test
    public void testNullReturningMethods() {
        TestTableSource source = new TestTableSource(mockTable());
        assertNull(source.getUniqueKeys(null, false));
        assertNull(source.getColumnsGroupSize(null, null));
        assertNull(source.areColumnsUnique(null, null, false));
        assertNull(source.getPrimaryKey(null));
    }

    @Test
    public void testDistribution() {
        TestTableSource source = new TestTableSource(mockTable());
        assertEquals(RelDistributions.ANY, source.distribution(null));
    }

    @Test
    public void testGetDistinctRowCount() {
        RelOptTable table = mockTable();
        TestTableSource source = new TestTableSource(table);
        assertEquals(1.0, source.getDistinctRowCount(null, ImmutableBitSet.of(), null), 0.001);
        assertEquals(100.0, source.getDistinctRowCount(null, ImmutableBitSet.of(0), null), 0.001);
    }

    @Test
    public void testGetSelectivity() {
        TestTableSource source = new TestTableSource(mockTable());
        assertEquals(1.0, source.getSelectivity(null, null), 0.001);
    }

    @Test
    public void testGetPredicates() {
        TestTableSource source = new TestTableSource(mockTable());
        assertEquals(RelOptPredicateList.EMPTY, source.getPredicates(null));
        assertEquals(RelOptPredicateList.EMPTY, source.getAllPredicates(null));
    }

    @Test
    public void testGetFunctionalDependency() {
        TestTableSource source = new TestTableSource(mockTable());
        assertTrue(source.getFunctionalDependency(null, ImmutableBitSet.of()).isEmpty());
    }

    @Test
    public void testGetNonCumulativeCost() {
        RelOptTable table = mockTable();
        RelOptPlanner planner = mock(RelOptPlanner.class);
        RelOptCostFactory factory = mock(RelOptCostFactory.class);
        RelOptCost cost = mock(RelOptCost.class);
        when(planner.getCostFactory()).thenReturn(factory);
        when(factory.makeCost(anyDouble(), anyDouble(), anyDouble(), anyDouble(), anyDouble()))
            .thenReturn(cost);

        TestTableSource source = new TestTableSource(table);
        assertEquals(cost, source.getNonCumulativeCost(planner, null));
    }

    @Test
    public void testGetStartUpCost() {
        RelOptPlanner planner = mock(RelOptPlanner.class);
        RelOptCostFactory factory = mock(RelOptCostFactory.class);
        RelOptCost cost = mock(RelOptCost.class);
        when(planner.getCostFactory()).thenReturn(factory);
        when(factory.makeTinyCost()).thenReturn(cost);

        TestTableSource source = new TestTableSource(mockTable());
        assertEquals(cost, source.getStartUpCost(planner, null));
    }

    @Test
    public void testCreate() {
        RelOptTable table = mockTable();
        ConnectorRegistry mockRegistry = mock(ConnectorRegistry.class);
        ConnectorDescriptor mockDesc = mock(ConnectorDescriptor.class);
        TestTableSource mockSource = new TestTableSource(table);

        Map<String, String> options = new HashMap<>();
        options.put(ExternalCatalogConstants.OPTION_CONNECTOR, "test");

        try (MockedStatic<ConnectorRegistry> crMock = mockStatic(ConnectorRegistry.class)) {
            crMock.when(ConnectorRegistry::getInstance).thenReturn(mockRegistry);
            when(mockRegistry.getOrNull("test")).thenReturn(mockDesc);
            when(mockDesc.createTableSource(any(), any())).thenReturn(mockSource);

            TableSource result = TableSource.create(options, table);
            assertEquals(mockSource, result);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCreateNullOptions() {
        TableSource.create(null, mockTable());
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCreateMissingConnector() {
        TableSource.create(new HashMap<>(), mockTable());
    }

    @Test
    public void testFromJsonNull() {
        assertNull(TableSource.fromJson(null, mockTable()));
        assertNull(TableSource.fromJson(new HashMap<>(), mockTable()));
    }

    @Test
    public void testFromJsonNoClass() {
        Map<String, Object> json = new HashMap<>();
        json.put("key", "value");
        assertNull(TableSource.fromJson(json, mockTable()));
    }

    @Test
    public void testFromJsonWithClass() {
        Map<String, Object> json = new HashMap<>();
        json.put("class", TestTableSource.class.getName());
        TableSource result = TableSource.fromJson(json, mockTable());
        assertNotNull(result);
        assertTrue(result instanceof TestTableSource);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testFromJsonInvalidClass() {
        Map<String, Object> json = new HashMap<>();
        json.put("class", "com.nonexistent.Class");
        TableSource.fromJson(json, mockTable());
    }

}
