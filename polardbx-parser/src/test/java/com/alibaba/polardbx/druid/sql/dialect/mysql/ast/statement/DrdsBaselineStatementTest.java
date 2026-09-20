package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for DrdsBaselineStatement
 *
 * @author copilot
 */
public class DrdsBaselineStatementTest {

    private DrdsBaselineStatement statement;

    @Before
    public void setUp() {
        statement = new DrdsBaselineStatement();
    }

    /**
     * Test operation getter and setter
     */
    @Test
    public void testOperationGetterSetter() {
        Assert.assertNull("Initial operation should be null", statement.getOperation());

        statement.setOperation("PERSIST");
        Assert.assertEquals("PERSIST", statement.getOperation());

        statement.setOperation("DELETE");
        Assert.assertEquals("DELETE", statement.getOperation());

        statement.setOperation(null);
        Assert.assertNull(statement.getOperation());
    }

    /**
     * Test baseline IDs list operations
     */
    @Test
    public void testBaselineIds() {
        List<Long> ids = statement.getBaselineIds();
        Assert.assertNotNull("Baseline IDs list should not be null", ids);
        Assert.assertTrue("Baseline IDs list should be empty initially", ids.isEmpty());

        // Test addBaselineId
        statement.addBaselineId(1L);
        Assert.assertEquals(1, statement.getBaselineIds().size());
        Assert.assertEquals(Long.valueOf(1L), statement.getBaselineIds().get(0));

        statement.addBaselineId(2L);
        statement.addBaselineId(3L);
        Assert.assertEquals(3, statement.getBaselineIds().size());
        Assert.assertEquals(Long.valueOf(2L), statement.getBaselineIds().get(1));
        Assert.assertEquals(Long.valueOf(3L), statement.getBaselineIds().get(2));
    }

    /**
     * Test select getter and setter
     */
    @Test
    public void testSelectGetterSetter() {
        Assert.assertNull("Initial select should be null", statement.getSelect());

        SQLSelect select = new SQLSelect();
        statement.setSelect(select);
        Assert.assertSame(select, statement.getSelect());

        statement.setSelect(null);
        Assert.assertNull(statement.getSelect());
    }

    /**
     * Test subStatement getter and setter
     */
    @Test
    public void testSubStatementGetterSetter() {
        Assert.assertNull("Initial subStatement should be null", statement.getSubStatement());

        SQLStatement subStmt = Mockito.mock(SQLStatement.class);
        statement.setSubStatement(subStmt);
        Assert.assertSame(subStmt, statement.getSubStatement());

        statement.setSubStatement(null);
        Assert.assertNull(statement.getSubStatement());
    }

    /**
     * Test targetSql getter and setter
     */
    @Test
    public void testTargetSqlGetterSetter() {
        Assert.assertNull("Initial targetSql should be null", statement.getTargetSql());

        statement.setTargetSql("SELECT * FROM test");
        Assert.assertEquals("SELECT * FROM test", statement.getTargetSql());

        statement.setTargetSql("");
        Assert.assertEquals("", statement.getTargetSql());

        statement.setTargetSql(null);
        Assert.assertNull(statement.getTargetSql());
    }

    /**
     * Test expr getter and setter
     */
    @Test
    public void testExprGetterSetter() {
        Assert.assertNull("Initial expr should be null", statement.getExpr());

        SQLExpr expr = new SQLIdentifierExpr("test");
        statement.setExpr(expr);
        Assert.assertSame(expr, statement.getExpr());

        statement.setExpr(null);
        Assert.assertNull(statement.getExpr());
    }

    /**
     * Test grayRatio getter and setter
     */
    @Test
    public void testGrayRatioGetterSetter() {
        Assert.assertNull("Initial grayRatio should be null", statement.getGrayRatio());

        statement.setGrayRatio(50);
        Assert.assertEquals(Integer.valueOf(50), statement.getGrayRatio());

        statement.setGrayRatio(0);
        Assert.assertEquals(Integer.valueOf(0), statement.getGrayRatio());

        statement.setGrayRatio(100);
        Assert.assertEquals(Integer.valueOf(100), statement.getGrayRatio());

        statement.setGrayRatio(null);
        Assert.assertNull(statement.getGrayRatio());
    }

    /**
     * Test inlineHint getter and setter
     */
    @Test
    public void testInlineHintGetterSetter() {
        Assert.assertNull("Initial inlineHint should be null", statement.getInlineHint());

        List<SQLCommentHint> hints = new ArrayList<>();
        statement.setInlineHint(hints);
        Assert.assertSame(hints, statement.getInlineHint());

        SQLCommentHint hint = Mockito.mock(SQLCommentHint.class);
        hints.add(hint);
        Assert.assertEquals(1, statement.getInlineHint().size());

        statement.setInlineHint(null);
        Assert.assertNull(statement.getInlineHint());
    }

    /**
     * Test getSqlType method
     */
    @Test
    public void testGetSqlType() {
        Assert.assertNull("getSqlType should return null", statement.getSqlType());
    }

    /**
     * Test accept0 method with visitor
     */
    @Test
    public void testAccept0() {
        MySqlASTVisitor visitor = Mockito.mock(MySqlASTVisitor.class);

        statement.accept0(visitor);

        // Verify visitor methods were called
        Mockito.verify(visitor, Mockito.times(1)).visit(statement);
        Mockito.verify(visitor, Mockito.times(1)).endVisit(statement);
    }

    /**
     * Test creating a complete statement with all fields
     */
    @Test
    public void testCompleteStatement() {
        // Set all fields
        statement.setOperation("PERSIST");
        statement.addBaselineId(100L);
        statement.addBaselineId(200L);

        SQLSelect select = new SQLSelect();
        statement.setSelect(select);

        SQLStatement subStmt = Mockito.mock(SQLStatement.class);
        statement.setSubStatement(subStmt);

        statement.setTargetSql("SELECT * FROM orders WHERE id > 100");

        SQLExpr expr = new SQLIdentifierExpr("test_expr");
        statement.setExpr(expr);

        statement.setGrayRatio(80);

        List<SQLCommentHint> hints = new ArrayList<>();
        hints.add(Mockito.mock(SQLCommentHint.class));
        statement.setInlineHint(hints);

        // Verify all fields
        Assert.assertEquals("PERSIST", statement.getOperation());
        Assert.assertEquals(2, statement.getBaselineIds().size());
        Assert.assertEquals(Long.valueOf(100L), statement.getBaselineIds().get(0));
        Assert.assertEquals(Long.valueOf(200L), statement.getBaselineIds().get(1));
        Assert.assertSame(select, statement.getSelect());
        Assert.assertSame(subStmt, statement.getSubStatement());
        Assert.assertEquals("SELECT * FROM orders WHERE id > 100", statement.getTargetSql());
        Assert.assertSame(expr, statement.getExpr());
        Assert.assertEquals(Integer.valueOf(80), statement.getGrayRatio());
        Assert.assertSame(hints, statement.getInlineHint());
        Assert.assertEquals(1, statement.getInlineHint().size());
    }

    /**
     * Test that DrdsBaselineStatement extends MySqlStatementImpl
     */
    @Test
    public void testClassHierarchy() {
        Assert.assertTrue(
            "DrdsBaselineStatement should extend MySqlStatementImpl",
            MySqlStatementImpl.class.isAssignableFrom(DrdsBaselineStatement.class)
        );
        Assert.assertTrue(
            "DrdsBaselineStatement should implement SQLStatement",
            SQLStatement.class.isAssignableFrom(DrdsBaselineStatement.class)
        );
    }

    /**
     * Test multiple baseline IDs
     */
    @Test
    public void testMultipleBaselineIds() {
        for (long i = 1; i <= 10; i++) {
            statement.addBaselineId(i);
        }

        Assert.assertEquals(10, statement.getBaselineIds().size());
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(Long.valueOf(i + 1), statement.getBaselineIds().get(i));
        }
    }

    /**
     * Test gray ratio boundary values
     */
    @Test
    public void testGrayRatioBoundaryValues() {
        // Test minimum value
        statement.setGrayRatio(0);
        Assert.assertEquals(Integer.valueOf(0), statement.getGrayRatio());

        // Test maximum value
        statement.setGrayRatio(100);
        Assert.assertEquals(Integer.valueOf(100), statement.getGrayRatio());

        // Test mid value
        statement.setGrayRatio(50);
        Assert.assertEquals(Integer.valueOf(50), statement.getGrayRatio());
    }
}
