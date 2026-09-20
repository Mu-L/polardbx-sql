package com.alibaba.polardbx.optimizer.json;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlJsonTable;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Regression test for AONE-80988337: JSON_TABLE reports ClassCastException
 * when the first argument (JSON data source) or path arguments are string literals.
 *
 * <p>Root cause: {@code FastSqlToCalciteNodeVisitor} incorrectly casts the
 * converted SqlNode to {@code SqlIdentifier} (line 6207) and {@code SqlDynamicParam}
 * (lines 6208-6210, 13367), but {@code convertToSqlNode} returns
 * {@code SqlCharStringLiteral} when the argument is a string literal.</p>
 *
 * <p>These tests must FAIL (throw ClassCastException) before the fix,
 * and PASS after the fix.</p>
 */
public class JsonTableLiteralTest {

    /**
     * JSON_TABLE with a string literal as the JSON data source must not throw
     * ClassCastException.
     *
     * <p>Before fix, line 6207 of FastSqlToCalciteNodeVisitor does:</p>
     * <pre>
     *   SqlIdentifier jsonExprNode = (SqlIdentifier) convertToSqlNode(jsonTableExpr);
     * </pre>
     * When {@code jsonTableExpr} is a string literal, {@code convertToSqlNode} returns
     * {@code SqlCharStringLiteral} (a {@code SqlLiteral}), and casting it to
     * {@code SqlIdentifier} throws {@code ClassCastException}.
     */
    @Test
    public void testJsonTableWithStringLiteralAsJsonSource() {
        String sql = "SELECT * FROM JSON_TABLE("
            + "'[{\"id\":1,\"name\":\"A\"},{\"id\":2,\"name\":\"B\"}]',"
            + " '$[*]' COLUMNS ("
            + "   id INT PATH '$.id',"
            + "   name VARCHAR(20) PATH '$.name'"
            + " )"
            + ") AS jt";

        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, null, false);
        SQLStatement statement = result.getAst();
        assertNotNull("Druid parser must succeed for JSON_TABLE with string literal", statement);

        ContextParameters contextParameters = new ContextParameters(false);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);

        // Before fix: throws ClassCastException on line 6207 of FastSqlToCalciteNodeVisitor:
        //   SqlIdentifier jsonExprNode = (SqlIdentifier) convertToSqlNode(jsonTableExpr);
        // SqlCharStringLiteral is not a SqlIdentifier.
        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        assertNotNull("Converted SqlNode must not be null", sqlNode);
        assertTrue("Top-level node must be SqlSelect", sqlNode instanceof SqlSelect);
    }

    /**
     * JSON_TABLE column PATH clauses that are string literals must not throw
     * ClassCastException.
     *
     * <p>Before fix, line 13367 of FastSqlToCalciteNodeVisitor does:</p>
     * <pre>
     *   SqlDynamicParam path = (SqlDynamicParam) convertToSqlNode(druidColumn.getPath());
     * </pre>
     * When the column path (e.g. {@code '$.id'}) is a string literal,
     * {@code convertToSqlNode} returns {@code SqlCharStringLiteral}, which is not a
     * {@code SqlDynamicParam}, causing {@code ClassCastException}.
     */
    @Test
    public void testJsonTableColumnPathIsStringLiteral() {
        String sql = "SELECT jt.id, jt.name FROM JSON_TABLE("
            + "'[{\"id\":1}]',"
            + " '$[*]' COLUMNS ("
            + "   id INT PATH '$.id',"
            + "   name VARCHAR(50) PATH '$.name'"
            + " )"
            + ") AS jt";

        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, null, false);
        SQLStatement statement = result.getAst();
        assertNotNull("Druid parser must succeed", statement);

        ContextParameters contextParameters = new ContextParameters(false);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);

        // Before fix: throws ClassCastException on line 13367:
        //   SqlDynamicParam path = (SqlDynamicParam) convertToSqlNode(druidColumn.getPath());
        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        assertNotNull("Converted SqlNode must not be null", sqlNode);
    }

    /**
     * After a successful conversion, the JSON expression in the resulting
     * {@link SqlJsonTable} must be a {@link SqlLiteral}, not null and not a column reference.
     */
    @Test
    public void testJsonTableJsonExprIsLiteralAfterConversion() {
        String sql = "SELECT * FROM JSON_TABLE("
            + "'[{\"id\":1}]',"
            + " '$[*]' COLUMNS (id INT PATH '$.id')"
            + ") AS jt";

        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, null, false);
        SQLStatement statement = result.getAst();

        ContextParameters contextParameters = new ContextParameters(false);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);

        // Before fix: ClassCastException; after fix: completes normally.
        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        assertNotNull("Converted SqlNode must not be null", sqlNode);

        SqlSelect select = (SqlSelect) sqlNode;
        SqlNode from = select.getFrom();
        assertNotNull("FROM clause must not be null", from);

        // The FROM clause should ultimately contain a SqlJsonTable.
        // Unwrap AS alias if present.
        if (from instanceof org.apache.calcite.sql.SqlBasicCall) {
            org.apache.calcite.sql.SqlBasicCall asCall = (org.apache.calcite.sql.SqlBasicCall) from;
            SqlNode tableNode = asCall.operands[0];
            assertTrue("Inner FROM node must be SqlJsonTable", tableNode instanceof SqlJsonTable);

            SqlJsonTable jsonTable = (SqlJsonTable) tableNode;
            SqlNode jsonExpr = jsonTable.getJsonExpr();
            assertNotNull("jsonExpr must not be null", jsonExpr);
            assertTrue(
                "jsonExpr must be a SqlLiteral (string literal), not SqlIdentifier",
                jsonExpr instanceof SqlLiteral);
        }
    }

    /**
     * JSON_TABLE with dynamic parameter (?) as JSON data source should still work.
     * This tests the original SqlDynamicParam scenario to ensure the fix doesn't
     * break existing functionality.
     */
    @Test
    public void testJsonTableWithDynamicParamAsJsonSource() {
        String sql = "SELECT * FROM JSON_TABLE("
            + "?,"
            + " '$[*]' COLUMNS ("
            + "   id INT PATH '$.id'"
            + " )"
            + ") AS jt";

        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, null, false);
        SQLStatement statement = result.getAst();
        assertNotNull("Druid parser must succeed for JSON_TABLE with dynamic param", statement);

        ContextParameters contextParameters = new ContextParameters(false);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);

        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        assertNotNull("Converted SqlNode must not be null", sqlNode);
        assertTrue("Top-level node must be SqlSelect", sqlNode instanceof SqlSelect);
    }

    /**
     * JSON_TABLE with dynamic parameter (?) as path should still work.
     * This tests the original SqlDynamicParam path scenario.
     */
    @Test
    public void testJsonTableWithDynamicParamAsPath() {
        String sql = "SELECT * FROM JSON_TABLE("
            + "'[{\"id\":1}]',"
            + " ? COLUMNS ("
            + "   id INT PATH '$.id'"
            + " )"
            + ") AS jt";

        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, null, false);
        SQLStatement statement = result.getAst();
        assertNotNull("Druid parser must succeed", statement);

        ContextParameters contextParameters = new ContextParameters(false);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);

        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        assertNotNull("Converted SqlNode must not be null", sqlNode);

        SqlSelect select = (SqlSelect) sqlNode;
        SqlNode from = select.getFrom();
        if (from instanceof org.apache.calcite.sql.SqlBasicCall) {
            org.apache.calcite.sql.SqlBasicCall asCall = (org.apache.calcite.sql.SqlBasicCall) from;
            SqlNode tableNode = asCall.operands[0];
            assertTrue("Inner FROM node must be SqlJsonTable", tableNode instanceof SqlJsonTable);

            SqlJsonTable jsonTable = (SqlJsonTable) tableNode;
            SqlNode path = jsonTable.getPathExpr();
            assertNotNull("path must not be null", path);
            // After fix: path should still be SqlDynamicParam when input was ?
            assertTrue("path should be SqlDynamicParam when input was ?", path instanceof SqlDynamicParam);
        }
    }

    /**
     * JSON_TABLE with dynamic parameter (?) as column PATH should still work.
     * This tests that the fix doesn't break the original SqlDynamicParam column path scenario.
     */
    @Test
    public void testJsonTableColumnPathWithDynamicParam() {
        String sql = "SELECT * FROM JSON_TABLE("
            + "'[{\"id\":1}]',"
            + " '$[*]' COLUMNS ("
            + "   id INT PATH ?"
            + " )"
            + ") AS jt";

        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, null, false);
        SQLStatement statement = result.getAst();
        assertNotNull("Druid parser must succeed", statement);

        ContextParameters contextParameters = new ContextParameters(false);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);

        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        assertNotNull("Converted SqlNode must not be null", sqlNode);
    }

    /**
     * JSON_TABLE with both JSON source and path as string literals.
     * This is the most comprehensive literal scenario test.
     */
    @Test
    public void testJsonTableWithAllLiterals() {
        String sql = "SELECT jt.id, jt.value FROM JSON_TABLE("
            + "'[{\"id\":1,\"value\":\"foo\"},{\"id\":2,\"value\":\"bar\"}]',"
            + " '$[*]' COLUMNS ("
            + "   id INT PATH '$.id',"
            + "   value VARCHAR(100) PATH '$.value'"
            + " )"
            + ") AS jt WHERE jt.id > 0";

        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, null, false);
        SQLStatement statement = result.getAst();
        assertNotNull("Druid parser must succeed for JSON_TABLE with all literals", statement);

        ContextParameters contextParameters = new ContextParameters(false);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);

        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        assertNotNull("Converted SqlNode must not be null", sqlNode);
        assertTrue("Top-level node must be SqlSelect", sqlNode instanceof SqlSelect);

        SqlSelect select = (SqlSelect) sqlNode;
        assertNotNull("FROM clause must not be null", select.getFrom());
        assertNotNull("WHERE clause must not be null", select.getWhere());
    }

    /**
     * JSON_TABLE with table alias and complex column definitions.
     * Tests that the fix works correctly with more realistic SQL patterns.
     */
    @Test
    public void testJsonTableWithComplexColumns() {
        String sql = "SELECT t.id, t.name, t.nested_val FROM JSON_TABLE("
            + "'[{\"id\":1,\"name\":\"test\",\"nested\":{\"val\":\"hello\"}}]',"
            + " '$[*]' COLUMNS ("
            + "   id INT PATH '$.id',"
            + "   name VARCHAR(50) PATH '$.name',"
            + "   nested_val VARCHAR(100) PATH '$.nested.val'"
            + " )"
            + ") AS t";

        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, null, false);
        SQLStatement statement = result.getAst();
        assertNotNull("Druid parser must succeed", statement);

        ContextParameters contextParameters = new ContextParameters(false);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);

        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        assertNotNull("Converted SqlNode must not be null", sqlNode);
    }

    /**
     * JSON_TABLE with nested columns (JSON NESTED PATH syntax).
     * Tests the nested column handling in convertJsonTableColumn.
     */
    @Test
    public void testJsonTableWithNestedColumns() {
        String sql = "SELECT t.id, t.items.item_name FROM JSON_TABLE("
            + "'[{\"id\":1,\"items\":[{\"name\":\"a\"},{\"name\":\"b\"}]}]',"
            + " '$[*]' COLUMNS ("
            + "   id INT PATH '$.id',"
            + "   NESTED PATH '$.items[*]' COLUMNS ("
            + "     item_name VARCHAR(50) PATH '$.name'"
            + "   )"
            + " )"
            + ") AS t";

        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, null, false);
        SQLStatement statement = result.getAst();
        assertNotNull("Druid parser must succeed for JSON_TABLE with nested columns", statement);

        ContextParameters contextParameters = new ContextParameters(false);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);

        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        assertNotNull("Converted SqlNode must not be null", sqlNode);
    }
}
