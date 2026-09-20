package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlJsonTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public class JsonTableNamespace extends AbstractNamespace {

    private final SqlJsonTable sqlJsonTable;

    public JsonTableNamespace(SqlValidatorImpl validator, SqlJsonTable sqlJsonTable) {
        super(validator, sqlJsonTable);
        this.sqlJsonTable = sqlJsonTable;
    }

    @Override
    public RelDataType validateImpl(RelDataType targetRowType) {
        // Get the columns from SqlJsonTable
        if (sqlJsonTable == null) {
            throw new RuntimeException("JSON_TABLE expression cannot be null");
        }

        // Build the row type from the columns defined in JSON_TABLE
        RelDataTypeFactory typeFactory = validator.getTypeFactory();
        RelDataTypeFactory.Builder builder = typeFactory.builder();

        // Process each column definition recursively
        for (SqlJsonTable.JsonTableColumn column : sqlJsonTable.getColumns()) {
            processColumnRecursively(column, builder, typeFactory);
        }

        // If no columns are defined, create a default structure
        if (sqlJsonTable.getColumns().isEmpty()) {
            builder.add("JSON_VALUE", typeFactory.createSqlType(SqlTypeName.VARCHAR, 4000));
        }

        return builder.build();
    }

    /**
     * Recursively processes a JSON_TABLE column and its nested columns,
     * flattening all columns into the builder
     */
    private void processColumnRecursively(SqlJsonTable.JsonTableColumn column,
                                          RelDataTypeFactory.Builder builder,
                                          RelDataTypeFactory typeFactory) {
        // Add the current column to the builder
        String columnName = column.getName() != null ? column.getName().getSimple() : "COLUMN";
        RelDataType columnType = deriveColumnType(column, typeFactory);
        builder.add(columnName, columnType);

        // Recursively process nested columns
        if (column.getNestedColumns() != null && !column.getNestedColumns().isEmpty()) {
            for (SqlJsonTable.JsonTableColumn nestedColumn : column.getNestedColumns()) {
                processColumnRecursively(nestedColumn, builder, typeFactory);
            }
        }
    }

    /**
     * Derives the RelDataType for a JSON_TABLE column based on its definition
     */
    private RelDataType deriveColumnType(SqlJsonTable.JsonTableColumn column, RelDataTypeFactory typeFactory) {
        SqlDataTypeSpec dataType = column.getDataType();

        if (dataType != null) {
            // Use SqlDataTypeSpec to derive the RelDataType
            return dataType.deriveType(typeFactory);
        }

        // Handle special column types
        if (column.isOrdinality()) {
            return typeFactory.createSqlType(SqlTypeName.INTEGER);
        }

        if (column.isExists()) {
            return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        }

        // Default to VARCHAR for JSON values
        return typeFactory.createSqlType(SqlTypeName.VARCHAR, 4000);
    }

    @Override
    public SqlNode getNode() {
        return sqlJsonTable;
    }
}