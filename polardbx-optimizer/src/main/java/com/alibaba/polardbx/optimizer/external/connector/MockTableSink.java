package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.optimizer.core.rel.TableSink;
import org.apache.calcite.plan.RelOptTable;

/**
 * No-op {@link TableSink} for mock connector.
 * Enables EXPLAIN INSERT to pass through optimizer without NPE,
 * allowing privilege checks to be exercised in IT tests.
 */
public class MockTableSink extends TableSink {
    public MockTableSink(RelOptTable table) {
        super(table);
    }

    @Override
    public String connectorType() {
        return MockConnectorDescriptor.TYPE;
    }

    @Override
    public String display() {
        return "MockTableSink{table=" + table.getQualifiedName() + "}";
    }
}
