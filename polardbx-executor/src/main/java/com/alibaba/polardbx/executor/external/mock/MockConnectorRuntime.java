package com.alibaba.polardbx.executor.external.mock;

import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.executor.external.ConnectorRuntime;
import com.alibaba.polardbx.executor.operator.external.ExternalTableScanHandler;
import com.alibaba.polardbx.executor.operator.external.MockScanHandler;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import com.alibaba.polardbx.optimizer.core.rel.TableSink;
import com.alibaba.polardbx.optimizer.core.rel.TableSource;
import com.alibaba.polardbx.optimizer.external.connector.MockConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.MockTableSource;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.PushdownCapability;
import com.alibaba.polardbx.optimizer.external.schema.InferredSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Executor-level wrapper for MockConnectorDescriptor that implements ConnectorRuntime,
 * providing createScanHandler() for mock data reading.
 */
public class MockConnectorRuntime implements ConnectorRuntime {

    private final MockConnectorDescriptor delegate = new MockConnectorDescriptor();

    @Override
    public String type() {
        return MockConnectorDescriptor.TYPE;
    }

    @Override
    public List<PropertyDefinition> secretDefinitions() {
        return delegate.secretDefinitions();
    }

    @Override
    public ConnectorMetadata createMetadata(Map<String, String> catalogProps, SecretBundle secret) {
        return delegate.createMetadata(catalogProps, secret);
    }

    @Override
    public InferredSchema inferFilesSchema(Map<String, String> options) throws IOException {
        return delegate.inferFilesSchema(options);
    }

    @Override
    public TableSource createTableSource(Map<String, String> options, RelOptTable table) {
        return delegate.createTableSource(options, table);
    }

    @Override
    public Optional<TableSink> createTableSink(
        Map<String, String> options, RelOptTable table) {
        return delegate.createTableSink(options, table);
    }

    @Override
    public EnumSet<PushdownCapability> capabilities() {
        return delegate.capabilities();
    }

    @Override
    public ExternalTableScanHandler createScanHandler(
        TableSource source, ExternalTableScan scan, ExecutionContext context) {
        Map<String, String> options = source.getOptions();
        List<DataType> outputTypes = scan.getRowType().getFieldList().stream()
            .map(RelDataTypeField::getType)
            .map(t -> {
                // Map RelDataType to DataType
                return DataTypeUtil.calciteToDrdsType(t);
            })
            .collect(Collectors.toList());
        return new MockScanHandler(options, outputTypes);
    }
}
