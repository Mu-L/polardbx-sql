package com.alibaba.polardbx.optimizer.json;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJsonTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.JsonTableNamespace;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for JsonTableNamespace class.
 * <p>
 * Tests the validation and type derivation functionality for JSON_TABLE expressions.
 */
@RunWith(MockitoJUnitRunner.class)
public class JsonTableNamespaceTest {

    @Mock
    private SqlValidatorImpl mockValidator;

    @Mock
    private SqlJsonTable mockSqlJsonTable;

    @Mock
    private RelDataTypeFactory mockTypeFactory;

    @Mock
    private RelDataType mockTargetRowType;

    @Mock
    private RelDataType mockColumnType;

    private RelDataTypeFactory.FieldInfoBuilder mockBuilder;

    @InjectMocks
    private JsonTableNamespace target;

    @Before
    public void setUp() {
        mockBuilder = mock(RelDataTypeFactory.FieldInfoBuilder.class);
        when(mockValidator.getTypeFactory()).thenReturn(mockTypeFactory);
        doReturn(mockBuilder).when(mockTypeFactory).builder();
        when(mockBuilder.build()).thenReturn(mockTargetRowType);
        doReturn(mockBuilder).when(mockBuilder).add(anyString(), any(RelDataType.class));
    }

    @Test
    public void testValidateImplWithNormalColumns() {
        // Arrange
        List<SqlJsonTable.JsonTableColumn> columns = createMockColumns();
        when(mockSqlJsonTable.getColumns()).thenReturn(columns);

        SqlDataTypeSpec mockDataTypeSpec = mock(SqlDataTypeSpec.class);
        when(mockDataTypeSpec.deriveType(mockTypeFactory)).thenReturn(mockColumnType);
        when(columns.get(0).getDataType()).thenReturn(mockDataTypeSpec);
        when(columns.get(0).getName()).thenReturn(new SqlIdentifier("test_column", SqlParserPos.ZERO));

        // Act
        RelDataType result = target.validateImpl(mockTargetRowType);

        // Assert
        assertNotNull("Result should not be null", result);
        verify(mockTypeFactory).builder();
        verify(mockBuilder).add(eq("test_column"), eq(mockColumnType));
        verify(mockBuilder).build();
    }

    @Test
    public void testValidateImplWithEmptyColumns() {
        // Arrange
        when(mockSqlJsonTable.getColumns()).thenReturn(Collections.emptyList());
        RelDataType mockVarcharType = mock(RelDataType.class);
        when(mockTypeFactory.createSqlType(SqlTypeName.VARCHAR, 4000)).thenReturn(mockVarcharType);

        // Act
        RelDataType result = target.validateImpl(mockTargetRowType);

        // Assert
        assertNotNull("Result should not be null", result);
        verify(mockTypeFactory).createSqlType(SqlTypeName.VARCHAR, 4000);
        verify(mockBuilder).add(eq("JSON_VALUE"), eq(mockVarcharType));
        verify(mockBuilder).build();
    }

    @Test
    public void testValidateImplWithOrdinalityColumn() {
        // Arrange
        List<SqlJsonTable.JsonTableColumn> columns = createMockColumns();
        when(mockSqlJsonTable.getColumns()).thenReturn(columns);
        when(columns.get(0).getName()).thenReturn(new SqlIdentifier("ord_column", SqlParserPos.ZERO));
        when(columns.get(0).getDataType()).thenReturn(null);
        when(columns.get(0).isOrdinality()).thenReturn(true);

        RelDataType mockIntegerType = mock(RelDataType.class);
        when(mockTypeFactory.createSqlType(SqlTypeName.INTEGER)).thenReturn(mockIntegerType);

        // Act
        RelDataType result = target.validateImpl(mockTargetRowType);

        // Assert
        assertNotNull("Result should not be null", result);
        verify(mockTypeFactory).createSqlType(SqlTypeName.INTEGER);
        verify(mockBuilder).add(eq("ord_column"), eq(mockIntegerType));
    }

    @Test
    public void testValidateImplWithExistsColumn() {
        // Arrange
        List<SqlJsonTable.JsonTableColumn> columns = createMockColumns();
        when(mockSqlJsonTable.getColumns()).thenReturn(columns);
        when(columns.get(0).getName()).thenReturn(new SqlIdentifier("exists_column", SqlParserPos.ZERO));
        when(columns.get(0).getDataType()).thenReturn(null);
        when(columns.get(0).isOrdinality()).thenReturn(false);
        when(columns.get(0).isExists()).thenReturn(true);

        RelDataType mockBooleanType = mock(RelDataType.class);
        when(mockTypeFactory.createSqlType(SqlTypeName.BOOLEAN)).thenReturn(mockBooleanType);

        // Act
        RelDataType result = target.validateImpl(mockTargetRowType);

        // Assert
        assertNotNull("Result should not be null", result);
        verify(mockTypeFactory).createSqlType(SqlTypeName.BOOLEAN);
        verify(mockBuilder).add(eq("exists_column"), eq(mockBooleanType));
    }

    @Test
    public void testValidateImplWithDefaultVarcharColumn() {
        // Arrange
        List<SqlJsonTable.JsonTableColumn> columns = createMockColumns();
        when(mockSqlJsonTable.getColumns()).thenReturn(columns);
        when(columns.get(0).getName()).thenReturn(new SqlIdentifier("default_column", SqlParserPos.ZERO));
        when(columns.get(0).getDataType()).thenReturn(null);
        when(columns.get(0).isOrdinality()).thenReturn(false);
        when(columns.get(0).isExists()).thenReturn(false);

        RelDataType mockVarcharType = mock(RelDataType.class);
        when(mockTypeFactory.createSqlType(SqlTypeName.VARCHAR, 4000)).thenReturn(mockVarcharType);

        // Act
        RelDataType result = target.validateImpl(mockTargetRowType);

        // Assert
        assertNotNull("Result should not be null", result);
        verify(mockTypeFactory).createSqlType(SqlTypeName.VARCHAR, 4000);
        verify(mockBuilder).add(eq("default_column"), eq(mockVarcharType));
    }

    @Test
    public void testValidateImplWithNullColumnName() {
        // Arrange
        List<SqlJsonTable.JsonTableColumn> columns = createMockColumns();
        when(mockSqlJsonTable.getColumns()).thenReturn(columns);
        when(columns.get(0).getName()).thenReturn(null);
        when(columns.get(0).getDataType()).thenReturn(null);
        when(columns.get(0).isOrdinality()).thenReturn(false);
        when(columns.get(0).isExists()).thenReturn(false);

        RelDataType mockVarcharType = mock(RelDataType.class);
        when(mockTypeFactory.createSqlType(SqlTypeName.VARCHAR, 4000)).thenReturn(mockVarcharType);

        // Act
        RelDataType result = target.validateImpl(mockTargetRowType);

        // Assert
        assertNotNull("Result should not be null", result);
        verify(mockBuilder).add(eq("COLUMN"), eq(mockVarcharType));
    }

    @Test(expected = RuntimeException.class)
    public void testValidateImplWithNullSqlJsonTable() {
        // Arrange
        target = new JsonTableNamespace(mockValidator, null);

        // Act
        target.validateImpl(mockTargetRowType);

        // Assert - Exception should be thrown
    }

    @Test
    public void testValidateImplWithMultipleColumns() {
        // Arrange
        List<SqlJsonTable.JsonTableColumn> columns = new ArrayList<>();

        // First column with data type
        SqlJsonTable.JsonTableColumn column1 = mock(SqlJsonTable.JsonTableColumn.class);
        SqlDataTypeSpec mockDataTypeSpec1 = mock(SqlDataTypeSpec.class);
        when(column1.getName()).thenReturn(new SqlIdentifier("col1", SqlParserPos.ZERO));
        when(column1.getDataType()).thenReturn(mockDataTypeSpec1);
        when(mockDataTypeSpec1.deriveType(mockTypeFactory)).thenReturn(mockColumnType);

        // Second column - ordinality
        SqlJsonTable.JsonTableColumn column2 = mock(SqlJsonTable.JsonTableColumn.class);
        when(column2.getName()).thenReturn(new SqlIdentifier("col2", SqlParserPos.ZERO));
        when(column2.getDataType()).thenReturn(null);
        when(column2.isOrdinality()).thenReturn(true);

        // Third column - exists
        SqlJsonTable.JsonTableColumn column3 = mock(SqlJsonTable.JsonTableColumn.class);
        when(column3.getName()).thenReturn(new SqlIdentifier("col3", SqlParserPos.ZERO));
        when(column3.getDataType()).thenReturn(null);
        when(column3.isExists()).thenReturn(true);

        columns.add(column1);
        columns.add(column2);
        columns.add(column3);

        when(mockSqlJsonTable.getColumns()).thenReturn(columns);

        RelDataType mockIntegerType = mock(RelDataType.class);
        RelDataType mockBooleanType = mock(RelDataType.class);
        when(mockTypeFactory.createSqlType(SqlTypeName.INTEGER)).thenReturn(mockIntegerType);
        when(mockTypeFactory.createSqlType(SqlTypeName.BOOLEAN)).thenReturn(mockBooleanType);

        // Act
        RelDataType result = target.validateImpl(mockTargetRowType);

        // Assert
        assertNotNull("Result should not be null", result);
        verify(mockBuilder).add(eq("col1"), eq(mockColumnType));
        verify(mockBuilder).add(eq("col2"), eq(mockIntegerType));
        verify(mockBuilder).add(eq("col3"), eq(mockBooleanType));
    }

    @Test
    public void testGetNode() {
        // Act
        SqlNode result = target.getNode();

        // Assert
        assertSame("Should return the same SqlJsonTable instance", mockSqlJsonTable, result);
    }

    @Test
    public void testDeriveColumnTypeWithDataTypeSpec() {
        // Arrange
        SqlJsonTable.JsonTableColumn mockColumn = mock(SqlJsonTable.JsonTableColumn.class);
        SqlDataTypeSpec mockDataTypeSpec = mock(SqlDataTypeSpec.class);
        when(mockColumn.getDataType()).thenReturn(mockDataTypeSpec);
        when(mockDataTypeSpec.deriveType(mockTypeFactory)).thenReturn(mockColumnType);

        // Use reflection to access private method or test through validateImpl
        List<SqlJsonTable.JsonTableColumn> columns = Collections.singletonList(mockColumn);
        when(mockSqlJsonTable.getColumns()).thenReturn(columns);
        when(mockColumn.getName()).thenReturn(new SqlIdentifier("test", SqlParserPos.ZERO));

        // Act
        target.validateImpl(mockTargetRowType);

        // Assert
        verify(mockDataTypeSpec).deriveType(mockTypeFactory);
    }

    @Test
    public void testDeriveColumnTypeForOrdinalityColumn() {
        // Arrange
        SqlJsonTable.JsonTableColumn mockColumn = mock(SqlJsonTable.JsonTableColumn.class);
        when(mockColumn.getDataType()).thenReturn(null);
        when(mockColumn.isOrdinality()).thenReturn(true);
        when(mockColumn.getName()).thenReturn(new SqlIdentifier("ord", SqlParserPos.ZERO));

        RelDataType mockIntegerType = mock(RelDataType.class);
        when(mockTypeFactory.createSqlType(SqlTypeName.INTEGER)).thenReturn(mockIntegerType);

        List<SqlJsonTable.JsonTableColumn> columns = Collections.singletonList(mockColumn);
        when(mockSqlJsonTable.getColumns()).thenReturn(columns);

        // Act
        target.validateImpl(mockTargetRowType);

        // Assert
        verify(mockTypeFactory).createSqlType(SqlTypeName.INTEGER);
    }

    @Test
    public void testDeriveColumnTypeForExistsColumn() {
        // Arrange
        SqlJsonTable.JsonTableColumn mockColumn = mock(SqlJsonTable.JsonTableColumn.class);
        when(mockColumn.getDataType()).thenReturn(null);
        when(mockColumn.isOrdinality()).thenReturn(false);
        when(mockColumn.isExists()).thenReturn(true);
        when(mockColumn.getName()).thenReturn(new SqlIdentifier("exists", SqlParserPos.ZERO));

        RelDataType mockBooleanType = mock(RelDataType.class);
        when(mockTypeFactory.createSqlType(SqlTypeName.BOOLEAN)).thenReturn(mockBooleanType);

        List<SqlJsonTable.JsonTableColumn> columns = Collections.singletonList(mockColumn);
        when(mockSqlJsonTable.getColumns()).thenReturn(columns);

        // Act
        target.validateImpl(mockTargetRowType);

        // Assert
        verify(mockTypeFactory).createSqlType(SqlTypeName.BOOLEAN);
    }

    @Test
    public void testDeriveColumnTypeDefaultVarchar() {
        // Arrange
        SqlJsonTable.JsonTableColumn mockColumn = mock(SqlJsonTable.JsonTableColumn.class);
        when(mockColumn.getDataType()).thenReturn(null);
        when(mockColumn.isOrdinality()).thenReturn(false);
        when(mockColumn.isExists()).thenReturn(false);
        when(mockColumn.getName()).thenReturn(new SqlIdentifier("default", SqlParserPos.ZERO));

        RelDataType mockVarcharType = mock(RelDataType.class);
        when(mockTypeFactory.createSqlType(SqlTypeName.VARCHAR, 4000)).thenReturn(mockVarcharType);

        List<SqlJsonTable.JsonTableColumn> columns = Collections.singletonList(mockColumn);
        when(mockSqlJsonTable.getColumns()).thenReturn(columns);

        // Act
        target.validateImpl(mockTargetRowType);

        // Assert
        verify(mockTypeFactory).createSqlType(SqlTypeName.VARCHAR, 4000);
    }

    /**
     * Helper method to create mock columns for testing.
     */
    private List<SqlJsonTable.JsonTableColumn> createMockColumns() {
        List<SqlJsonTable.JsonTableColumn> columns = new ArrayList<>();
        SqlJsonTable.JsonTableColumn mockColumn = mock(SqlJsonTable.JsonTableColumn.class);
        columns.add(mockColumn);
        return columns;
    }
}