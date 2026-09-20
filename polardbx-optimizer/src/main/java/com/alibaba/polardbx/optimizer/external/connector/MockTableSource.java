package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.rel.TableSource;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mock {@link TableSource} for integration testing.
 * Holds options containing mock data declarations; the actual data reading
 * is performed by {@code MockScanHandler} in the executor module.
 */
public class MockTableSource extends TableSource {

    private final Map<String, String> options;
    private final EnumSet<PushdownCapability> capabilities;

    public MockTableSource(Map<String, String> options, RelOptTable table) {
        super(table);
        this.options = options != null ? new HashMap<>(options) : new HashMap<>();
        this.capabilities = MockConnectorDescriptor.parseCapabilities(this.options);
    }

    private MockTableSource(Map<String, String> options, RelOptTable table,
                            EnumSet<PushdownCapability> capabilities) {
        super(table);
        this.options = new HashMap<>(options);
        this.capabilities = EnumSet.copyOf(capabilities);
    }

    @Override
    public String connectorType() {
        return MockConnectorDescriptor.TYPE;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public Pair<List<RexNode>, List<RexNode>> pushProject(List<RexNode> projects) {
        // Mock connector does not push down projections — CN handles them above the scan.
        return Pair.of(Collections.emptyList(), projects);
    }

    @Override
    public Pair<Boolean, List<RexNode>> pushFilter(RexNode condition) {
        // Mock connector does not push down filters — CN handles them above the scan.
        return Pair.of(false, Collections.singletonList(condition));
    }

    @Override
    public boolean pushSort(Sort sort) {
        // Mock connector does not push down sort — CN handles it above the scan.
        return false;
    }

    @Override
    public boolean pushAgg(LogicalAggregate agg) {
        // Mock connector does not push down aggregation — CN handles it above the scan.
        return false;
    }

    @Override
    public TableSource copy() {
        return new MockTableSource(options, table, capabilities);
    }

    @Override
    public String display() {
        return "";
    }

    @Override
    public double getRowCount(RelMetadataQuery mq) {
        return table.getRowCount();
    }

    @Override
    public Map<String, Object> toJson() {
        Map<String, Object> json = new HashMap<>();
        json.put("options", new HashMap<>(options));
        json.put("type", MockConnectorDescriptor.TYPE);
        return json;
    }

    @Override
    public void fromJsonState(Map<String, Object> json, RelOptTable table) {
        // no-op for mock
    }
}
