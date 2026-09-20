package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.external.schema.ColumnDef;
import com.alibaba.polardbx.optimizer.external.schema.InferredSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * In-memory implementation of {@link ConnectorMetadata} for mock connector.
 * Schema and data are configured entirely via properties (mock.* keys).
 */
public class MockConnectorMetadata implements ConnectorMetadata {

    private final Map<String, String> props;

    public MockConnectorMetadata(Map<String, String> props) {
        this.props = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (props != null) {
            this.props.putAll(props);
        }
    }

    @Override
    public List<String> listDatabases() {
        String simulateError = props.get("simulate_error");
        if ("AUTH_FAIL".equalsIgnoreCase(simulateError)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Access denied for user 'mock'@'localhost'");
        }
        String dbs = props.get("mock.databases");
        if (dbs == null || dbs.isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(dbs.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    }

    @Override
    public List<String> listTables(String db) {
        String tables = props.get("mock." + db + ".tables");
        if (tables == null || tables.isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(tables.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    }

    @Override
    public Optional<ConnectorTable> getTable(String db, String table) {
        String columnsStr = props.get("mock." + db + "." + table + ".columns");
        if (columnsStr == null || columnsStr.isEmpty()) {
            return Optional.empty();
        }
        List<ColumnMeta> columns = parseColumns(table, columnsStr);
        ConnectorTable ct = ConnectorTable.builder(columns).build();
        return Optional.of(ct);
    }

    @Override
    public Optional<ConnectorTableStatistics> getTableStatistics(String db, String table) {
        return Optional.empty();
    }

    @Override
    public InferredSchema doInferQuerySchema(String sql) throws IOException {
        String columnsStr = props.get("mock.native_query.columns");
        if (columnsStr == null || columnsStr.isEmpty()) {
            throw new IOException("Mock connector: mock.native_query.columns not configured");
        }
        List<ColumnDef> columns = parseColumnDefs(columnsStr);
        return new InferredSchema(columns);
    }

    // ===== Utility methods (package-private for MockConnectorDescriptor) =====

    static List<ColumnMeta> parseColumns(String tableName, String columnsStr) {
        List<ColumnMeta> result = new ArrayList<>();
        for (String colDef : columnsStr.split(",")) {
            String[] parts = colDef.trim().split(":");
            String name = parts[0].trim();
            DataType type = parts.length > 1 ? resolveType(parts[1].trim()) : DataTypes.VarcharType;
            Field field = new Field(tableName, name, type);
            result.add(new ColumnMeta(tableName, name, null, field));
        }
        return result;
    }

    public static List<ColumnDef> parseColumnDefs(String columnsStr) {
        List<ColumnDef> result = new ArrayList<>();
        for (String colDef : columnsStr.split(",")) {
            String[] parts = colDef.trim().split(":");
            String name = parts[0].trim();
            DataType type = parts.length > 1 ? resolveType(parts[1].trim()) : DataTypes.VarcharType;
            result.add(new ColumnDef(name, type));
        }
        return result;
    }

    static DataType resolveType(String typeName) {
        switch (typeName.toLowerCase()) {
        case "int":
        case "integer":
            return DataTypes.IntegerType;
        case "bigint":
        case "long":
            return DataTypes.LongType;
        case "varchar":
        case "string":
            return DataTypes.VarcharType;
        case "double":
            return DataTypes.DoubleType;
        case "datetime":
        case "timestamp":
            return DataTypes.TimestampType;
        default:
            return DataTypes.VarcharType;
        }
    }

    Map<String, String> getProps() {
        return props;
    }
}
