package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr;

import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for MySqlJSONTableExpr class
 * Tests cover visitor pattern, getter/setter methods, and column operations
 */
@RunWith(MockitoJUnitRunner.class)
public class MySqlJSONTableExprTest {

    @Mock
    private MySqlASTVisitor mockVisitor;

    @Mock
    private SQLExpr mockExpr;

    @Mock
    private SQLExpr mockPath;

    @Mock
    private SQLName mockName;

    @Mock
    private SQLDataType mockDataType;

    @Mock
    private SQLExpr mockColumnPath;

    @Mock
    private SQLExpr mockOnError;

    @Mock
    private SQLExpr mockOnEmpty;

    private MySqlJSONTableExpr target;

    @Before
    public void setUp() {
        target = new MySqlJSONTableExpr();
    }

    /**
     * Test comprehensive functionality including visitor pattern and all getter/setter methods
     * This test covers the main MySqlJSONTableExpr class methods and visitor acceptance
     */
    @Test
    public void testComprehensiveFunctionality() {
        // Test initial state
        Assert.assertNull(target.getExpr());
        Assert.assertNull(target.getPath());
        Assert.assertNotNull(target.getColumns());
        Assert.assertTrue(target.getColumns().isEmpty());
        Assert.assertNull(target.getChildren());

        // Test setExpr with null
        target.setExpr(null);
        Assert.assertNull(target.getExpr());

        // Test setExpr with non-null value
        target.setExpr(mockExpr);
        Assert.assertEquals(mockExpr, target.getExpr());
        verify(mockExpr).setParent(target);

        // Test setPath with null
        target.setPath(null);
        Assert.assertNull(target.getPath());

        // Test setPath with non-null value
        target.setPath(mockPath);
        Assert.assertEquals(mockPath, target.getPath());
        verify(mockPath).setParent(target);

        // Test addColumn functionality
        MySqlJSONTableExpr.Column column = new MySqlJSONTableExpr.Column();
        target.addColumn(column);
        Assert.assertEquals(1, target.getColumns().size());
        Assert.assertEquals(column, target.getColumns().get(0));
        Assert.assertEquals(target, column.getParent());

        // Test visitor pattern - visitor returns true
        when(mockVisitor.visit(target)).thenReturn(true);
        target.accept0(mockVisitor);
        verify(mockVisitor).visit(target);
        verify(mockVisitor).endVisit(target);
    }

    /**
     * Test Column inner class functionality with all properties and visitor pattern
     * This test covers all Column class methods including nested columns and boolean flags
     */
    @Test
    public void testColumnComprehensiveFunctionality() {
        MySqlJSONTableExpr.Column column = new MySqlJSONTableExpr.Column();

        // Test initial state
        Assert.assertNull(column.getName());
        Assert.assertNull(column.getDataType());
        Assert.assertNull(column.getPath());
        Assert.assertFalse(column.isOrdinality());
        Assert.assertFalse(column.isExists());
        Assert.assertNull(column.getOnError());
        Assert.assertNull(column.getOnEmpty());
        Assert.assertNotNull(column.getNestedColumns());
        Assert.assertTrue(column.getNestedColumns().isEmpty());

        // Test setName with null and non-null
        column.setName(null);
        Assert.assertNull(column.getName());
        column.setName(mockName);
        Assert.assertEquals(mockName, column.getName());
        verify(mockName).setParent(column);

        // Test setDataType with null and non-null
        column.setDataType(null);
        Assert.assertNull(column.getDataType());
        column.setDataType(mockDataType);
        Assert.assertEquals(mockDataType, column.getDataType());
        verify(mockDataType).setParent(column);

        // Test setPath with null and non-null
        column.setPath(null);
        Assert.assertNull(column.getPath());
        column.setPath(mockColumnPath);
        Assert.assertEquals(mockColumnPath, column.getPath());
        verify(mockColumnPath).setParent(column);

        // Test boolean properties
        column.setOrdinality(true);
        Assert.assertTrue(column.isOrdinality());
        column.setOrdinality(false);
        Assert.assertFalse(column.isOrdinality());

        column.setExists(true);
        Assert.assertTrue(column.isExists());
        column.setExists(false);
        Assert.assertFalse(column.isExists());

        // Test setOnError with null and non-null
        column.setOnError(null);
        Assert.assertNull(column.getOnError());
        column.setOnError(mockOnError);
        Assert.assertEquals(mockOnError, column.getOnError());
        verify(mockOnError).setParent(column);

        // Test setOnEmpty with null and non-null
        column.setOnEmpty(null);
        Assert.assertNull(column.getOnEmpty());
        column.setOnEmpty(mockOnEmpty);
        Assert.assertEquals(mockOnEmpty, column.getOnEmpty());
        verify(mockOnEmpty).setParent(column);

        // Test nested columns functionality
        MySqlJSONTableExpr.Column nestedColumn = new MySqlJSONTableExpr.Column();
        column.addNestedColumn(nestedColumn);
        Assert.assertEquals(1, column.getNestedColumns().size());
        Assert.assertEquals(nestedColumn, column.getNestedColumns().get(0));
        Assert.assertEquals(column, nestedColumn.getParent());

        // Test visitor pattern - visitor returns true
        when(mockVisitor.visit(column)).thenReturn(true);
        column.accept0(mockVisitor);
        verify(mockVisitor).visit(column);
        verify(mockVisitor).endVisit(column);
    }
}