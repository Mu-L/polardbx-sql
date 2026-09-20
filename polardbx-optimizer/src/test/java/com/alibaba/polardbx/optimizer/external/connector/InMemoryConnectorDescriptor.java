package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.alibaba.polardbx.optimizer.core.rel.TableSource;
import com.alibaba.polardbx.optimizer.external.schema.ColumnDef;
import com.alibaba.polardbx.optimizer.external.schema.InferredSchema;
import org.apache.calcite.plan.RelOptTable;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Test-only {@link ConnectorDescriptor} that produces {@link InMemoryConnectorMetadata}
 * and {@link InMemoryTableSource} instances.
 * <p>
 * The factory holds a shared {@link InMemoryConnectorMetadata} so that all
 * {@code createMetadata} calls return the same instance, and table definitions
 * registered before the test are visible during planning.
 */
public class InMemoryConnectorDescriptor implements ConnectorDescriptor {

    public static final String TYPE = ExternalCatalogConstants.CONNECTOR_MOCK;

    private final String type;
    private final InMemoryConnectorMetadata metadata;
    private final EnumSet<PushdownCapability> capabilities;
    private final long defaultRowCount;
    private final Map<String, ConnectorColumnStatistics> columnStats;

    /**
     * Minimal constructor for tests that only need a factory with a custom type.
     */
    public InMemoryConnectorDescriptor(String type) {
        this(type, new InMemoryConnectorMetadata(),
            EnumSet.of(PushdownCapability.PROJECT, PushdownCapability.FILTER),
            -1, Collections.emptyMap());
    }

    public InMemoryConnectorDescriptor(InMemoryConnectorMetadata metadata) {
        this(TYPE, metadata,
            EnumSet.of(PushdownCapability.PROJECT, PushdownCapability.FILTER),
            -1, Collections.emptyMap());
    }

    public InMemoryConnectorDescriptor(InMemoryConnectorMetadata metadata,
                                       EnumSet<PushdownCapability> capabilities,
                                       long defaultRowCount,
                                       Map<String, ConnectorColumnStatistics> columnStats) {
        this(TYPE, metadata, capabilities, defaultRowCount, columnStats);
    }

    public InMemoryConnectorDescriptor(String type, InMemoryConnectorMetadata metadata,
                                       EnumSet<PushdownCapability> capabilities,
                                       long defaultRowCount,
                                       Map<String, ConnectorColumnStatistics> columnStats) {
        this.type = type;
        this.metadata = metadata;
        this.capabilities = capabilities != null
            ? EnumSet.copyOf(capabilities)
            : EnumSet.noneOf(PushdownCapability.class);
        this.defaultRowCount = defaultRowCount;
        this.columnStats = columnStats != null
            ? new HashMap<>(columnStats)
            : Collections.emptyMap();
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public ConnectorMetadata createMetadata(Map<String, String> catalogProps,
                                            SecretBundle secret) {
        return metadata;
    }

    @Override
    public TableSource createTableSource(Map<String, String> options, RelOptTable table) {
        // Try to look up table-level statistics from metadata
        long rowCount = defaultRowCount;
        Map<String, ConnectorColumnStatistics> colStats = this.columnStats;

        String dbName = options.get("db");
        String tableName = options.get("table");
        if (dbName != null && tableName != null) {
            Optional<ConnectorTableStatistics> stats = metadata.getTableStatistics(dbName.toLowerCase(),
                tableName.toLowerCase());
            if (stats.isPresent()) {
                ConnectorTableStatistics ts = stats.get();
                if (ts.getRowCount() > 0) {
                    rowCount = ts.getRowCount();
                }
                if (!ts.getColumnStats().isEmpty()) {
                    colStats = new HashMap<>(ts.getColumnStats());
                }
            }
        }

        return new InMemoryTableSource(type, options, table, capabilities, rowCount, colStats);
    }

    @Override
    public InferredSchema inferFilesSchema(Map<String, String> options) throws IOException {
        String columnsStr = options.get("mock.columns");
        if (columnsStr == null || columnsStr.isEmpty()) {
            throw new IOException("Mock connector: 'mock.columns' property is required for schema inference");
        }
        List<ColumnDef> columns = MockConnectorMetadata.parseColumnDefs(columnsStr);
        return new InferredSchema(columns);
    }

    @Override
    public EnumSet<PushdownCapability> capabilities() {
        return EnumSet.copyOf(capabilities);
    }
}
