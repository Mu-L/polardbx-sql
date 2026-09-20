package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.optimizer.core.rel.TableSource;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockTableSourceTest {

    @Test
    public void testAllMethods() {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getRowCount()).thenReturn(10D);

        MockTableSource ts = new MockTableSource(null, table);
        ts.connectorType();
        ts.getOptions();
        List<RexNode> projects = new ArrayList<>();
        projects.add(mock(RexNode.class));
        ts.pushProject(projects);
        ts.pushFilter(mock(RexNode.class));
        ts.pushSort(mock(Sort.class));
        ts.pushAgg(mock(LogicalAggregate.class));
        TableSource copied = ts.copy();
        ts.display();
        ts.getRowCount(null);
        ts.toJson();
        ts.fromJsonState(new HashMap<String, Object>(), table);
        copied.getOptions();
    }

    @Test
    public void testWithOptions() {
        RelOptTable table = mock(RelOptTable.class);
        Map<String, String> options = new HashMap<>();
        options.put("mock.databases", "db1");
        options.put("connector", "mock");

        MockTableSource ts = new MockTableSource(options, table);
        ts.getOptions();
        ts.toJson();
    }
}
