package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.alibaba.polardbx.optimizer.core.rel.TableSink;
import com.alibaba.polardbx.optimizer.core.rel.TableSource;
import com.alibaba.polardbx.optimizer.external.schema.ColumnDef;
import com.alibaba.polardbx.optimizer.external.schema.InferredSchema;
import org.apache.calcite.plan.RelOptTable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Built-in mock connector for testing. Controlled by ENABLE_MOCK_CONNECTOR switch.
 * <p>
 * Schema and data are declared entirely via SQL properties (mock.* keys),
 * enabling zero-dependency integration tests in CN.
 */
public class MockConnectorDescriptor implements ConnectorDescriptor {

    public static final String TYPE = ExternalCatalogConstants.CONNECTOR_MOCK;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public List<PropertyDefinition> secretDefinitions() {
        // Mock connector uses relaxed mode: no required keys, allows any unknown keys
        return Collections.singletonList(new PropertyDefinition(
            TYPE,
            Collections.emptySet(),
            new HashSet<>(Arrays.asList("user", "password")),
            new HashSet<>(Arrays.asList("password")),
            true  // allowUnknownKeys
        ));
    }

    @Override
    public ConnectorMetadata createMetadata(Map<String, String> catalogProps, SecretBundle secret) {
        checkEnabled();
        return new MockConnectorMetadata(catalogProps);
    }

    @Override
    public InferredSchema inferFilesSchema(Map<String, String> options) throws IOException {
        checkEnabled();
        String columnsStr = options.get("mock.columns");
        if (columnsStr == null || columnsStr.isEmpty()) {
            throw new IOException("Mock connector: 'mock.columns' property is required for schema inference");
        }
        List<ColumnDef> columns = MockConnectorMetadata.parseColumnDefs(columnsStr);
        return new InferredSchema(columns);
    }

    @Override
    public TableSource createTableSource(Map<String, String> options, RelOptTable table) {
        checkEnabled();
        return new MockTableSource(options, table);
    }

    @Override
    public Optional<TableSink> createTableSink(
        Map<String, String> options, RelOptTable table) {
        checkEnabled();
        return Optional.of(new MockTableSink(table));
    }

    @Override
    public EnumSet<PushdownCapability> capabilities() {
        return EnumSet.of(PushdownCapability.PROJECT, PushdownCapability.FILTER);
    }

    /**
     * Parse capabilities from properties if specified.
     */
    public static EnumSet<PushdownCapability> parseCapabilities(Map<String, String> options) {
        String capsStr = options.get("mock.capabilities");
        if (capsStr == null || capsStr.isEmpty()) {
            return EnumSet.of(PushdownCapability.PROJECT, PushdownCapability.FILTER);
        }
        EnumSet<PushdownCapability> caps = EnumSet.noneOf(PushdownCapability.class);
        for (String cap : capsStr.split(",")) {
            try {
                caps.add(PushdownCapability.valueOf(cap.trim().toUpperCase()));
            } catch (IllegalArgumentException ignored) {
                // skip unknown capabilities
            }
        }
        return caps.isEmpty()
            ? EnumSet.of(PushdownCapability.PROJECT, PushdownCapability.FILTER)
            : caps;
    }

    private void checkEnabled() {
        if (!DynamicConfig.getInstance().isEnableMockConnector()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Mock connector is disabled. Set ENABLE_MOCK_CONNECTOR=true to enable.");
        }
    }
}
