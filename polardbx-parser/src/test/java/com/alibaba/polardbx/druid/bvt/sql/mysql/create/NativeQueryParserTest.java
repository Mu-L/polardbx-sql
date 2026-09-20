package com.alibaba.polardbx.druid.bvt.sql.mysql.create;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNativeQueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NativeQueryParserTest {

    private static final SQLParserFeature[] FEATURES = {
        SQLParserFeature.TDDLHint,
        SQLParserFeature.EnableCurrentUserExpr,
        SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DrdsMisc,
        SQLParserFeature.DRDSBaseline,
        SQLParserFeature.DrdsGSI,
        SQLParserFeature.DrdsCCL
    };

    private String parse(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql, FEATURES);
        List<SQLStatement> stmts = parser.parseStatementList();
        assertFalse(stmts.isEmpty());
        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(sb);
        stmts.get(0).accept(visitor);
        return sb.toString();
    }

    private int countOccurrences(String text, String needle) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(needle, index)) >= 0) {
            count++;
            index += needle.length();
        }
        return count;
    }

    @Test
    public void testBasicNativeQuery() {
        String sql = "SELECT * FROM TABLE (mycat.native_query('SELECT id FROM t')) nq";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain native_query, got: " + output, lower.contains("native_query("));
        assertTrue(lower.contains("mycat"));
        assertTrue(lower.contains("nq"));
    }

    @Test
    public void testNativeQueryWithSingleQuoteInSql() {
        // SQL contains single quotes: WHERE name = 'Alice'
        // In parser input, single quotes inside string literal are escaped as ''
        String sql = "SELECT * FROM TABLE (mycat.native_query('SELECT * FROM t WHERE name = ''Alice''')) nq";
        String output = parse(sql);
        // OutputVisitor must properly escape the inner quotes
        assertTrue("Output must contain escaped single quotes, got: " + output,
            output.contains("'SELECT * FROM t WHERE name = ''Alice'''"));
    }

    @Test
    public void testNativeQueryWithoutAlias() {
        String sql = "SELECT * FROM TABLE (mycat.native_query('SELECT 1'))";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue(lower.contains("native_query("));
        assertTrue(output.contains("'SELECT 1'"));
    }

    @Test
    public void testNativeQueryWithAsAlias() {
        String sql = "SELECT * FROM TABLE (mycat.native_query('SELECT 1')) AS q";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue(lower.contains("native_query("));
        assertTrue(lower.contains("q"));
    }

    @Test
    public void testNativeQueryUpperCase() {
        String sql = "SELECT * FROM TABLE (mycat.native_query('SELECT id FROM t')) nq";
        MySqlStatementParser parser = new MySqlStatementParser(sql, FEATURES);
        List<SQLStatement> stmts = parser.parseStatementList();
        assertFalse(stmts.isEmpty());
        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(sb);
        visitor.setUppCase(true);
        stmts.get(0).accept(visitor);
        String output = sb.toString();
        assertTrue(output.contains("NATIVE_QUERY("));
        assertTrue(output.contains("TABLE ("));
    }

    @Test
    public void testNativeQueryAsJoinRightTable() {
        String sql = "SELECT * FROM t JOIN TABLE (mycat.native_query('SELECT id FROM remote_t')) nq ON t.id = nq.id";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain native_query in JOIN, got: " + output, lower.contains("native_query("));
        assertTrue(lower.contains("join"));
        assertTrue(lower.contains("nq"));
    }

    @Test
    public void testNativeQueryAsCommaJoinTable() {
        String sql = "SELECT * FROM t, TABLE (mycat.native_query('SELECT id FROM remote_t')) nq";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain native_query in comma join, got: " + output, lower.contains("native_query("));
        assertTrue(lower.contains("nq"));
    }

    @Test
    public void testNativeQueryAsParenthesizedTableSource() {
        String sql = "SELECT * FROM (TABLE (mycat.native_query('SELECT id FROM remote_t'))) wrapped";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain native_query in parenthesized table source, got: " + output,
            lower.contains("native_query("));
        assertTrue(lower.contains("wrapped"));
    }

    @Test
    public void testNativeQueryAsJoinLeftTable() {
        String sql = "SELECT * FROM TABLE (mycat.native_query('SELECT id FROM remote_t')) nq "
            + "LEFT JOIN local_t lt ON nq.id = lt.id WHERE lt.id IS NOT NULL";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain native_query on JOIN left side, got: " + output, lower.contains("native_query("));
        assertTrue(lower.contains("left join"));
        assertTrue(lower.contains("where"));
    }

    @Test
    public void testNativeQueryOnBothSidesOfJoin() {
        String sql = "SELECT * FROM TABLE (cat1.native_query('SELECT id FROM a')) a "
            + "JOIN TABLE (cat2.native_query('SELECT id FROM b')) b ON a.id = b.id";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should preserve both native_query sources, got: " + output,
            countOccurrences(lower, "native_query(") == 2);
        assertTrue(lower.contains("cat1"));
        assertTrue(lower.contains("cat2"));
    }

    @Test
    public void testNativeQueryInsideSubqueryJoin() {
        String sql = "SELECT * FROM (SELECT t.id FROM local_t t "
            + "JOIN TABLE (mycat.native_query('SELECT id FROM remote_t')) nq ON t.id = nq.id "
            + "WHERE nq.id > 0) s ORDER BY s.id";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain native_query inside subquery JOIN, got: " + output,
            lower.contains("native_query("));
        assertTrue(lower.contains("order by"));
        assertTrue(lower.contains("s"));
    }

    @Test
    public void testNativeQueryInsideCteJoin() {
        String sql = "WITH cte AS (SELECT t.id FROM local_t t "
            + "JOIN TABLE (mycat.native_query('SELECT id FROM remote_t')) nq ON t.id = nq.id) "
            + "SELECT * FROM cte WHERE id > 0";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain native_query inside CTE JOIN, got: " + output,
            lower.contains("native_query("));
        assertTrue(lower.contains("with"));
        assertTrue(lower.contains("cte"));
    }

    @Test
    public void testNativeQueryJoinFiles() {
        String sql = "SELECT * FROM TABLE (mycat.native_query('SELECT id FROM remote_t')) nq "
            + "JOIN FILES('connector'='mock', 'mock.columns'='id:int') f ON nq.id = f.id";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain native_query joined with FILES, got: " + output,
            lower.contains("native_query("));
        assertTrue(lower.contains("files("));
        assertTrue(lower.contains("join"));
    }

    @Test
    public void testFilesJoinNativeQuery() {
        String sql = "SELECT * FROM FILES('connector'='mock', 'mock.columns'='id:int') f "
            + "LEFT JOIN TABLE (mycat.native_query('SELECT id FROM remote_t')) nq ON f.id = nq.id";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain FILES joined with native_query, got: " + output,
            lower.contains("files("));
        assertTrue(lower.contains("native_query("));
        assertTrue(lower.contains("left join"));
    }

    @Test
    public void testNativeQueryAndFilesInsideCte() {
        String sql = "WITH cte AS (SELECT nq.id FROM TABLE (mycat.native_query('SELECT id FROM remote_t')) nq "
            + "JOIN FILES('connector'='mock', 'mock.columns'='id:int') f ON nq.id = f.id) "
            + "SELECT * FROM cte WHERE id > 0";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain native_query and FILES inside CTE, got: " + output,
            lower.contains("native_query("));
        assertTrue(lower.contains("files("));
        assertTrue(lower.contains("with"));
    }

    @Test
    public void testNormalTableNativeQueryFilesAndThreePartTableTogether() {
        String sql = "SELECT * FROM local_t l "
            + "JOIN TABLE (mycat.native_query('SELECT id FROM remote_t')) nq ON l.id = nq.id "
            + "JOIN FILES('connector'='mock', 'mock.columns'='id:int') f ON nq.id = f.id "
            + "JOIN ext_cat.ext_db.orders o ON f.id = o.id "
            + "WHERE l.id > 0";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain normal table, got: " + output, lower.contains("local_t"));
        assertTrue("Should contain native_query, got: " + output, lower.contains("native_query("));
        assertTrue("Should contain FILES, got: " + output, lower.contains("files("));
        assertTrue("Should contain three-part table, got: " + output, lower.contains("ext_cat.ext_db.orders"));
        assertTrue(lower.contains("where"));
    }

    @Test
    public void testNormalTableNativeQueryFilesAndThreePartTableInsideCte() {
        String sql = "WITH cte AS (SELECT l.id FROM local_t l "
            + "JOIN TABLE (mycat.native_query('SELECT id FROM remote_t')) nq ON l.id = nq.id "
            + "JOIN FILES('connector'='mock', 'mock.columns'='id:int') f ON nq.id = f.id "
            + "JOIN ext_cat.ext_db.orders o ON f.id = o.id) "
            + "SELECT * FROM cte WHERE id > 0";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain normal table inside CTE, got: " + output, lower.contains("local_t"));
        assertTrue("Should contain native_query inside CTE, got: " + output, lower.contains("native_query("));
        assertTrue("Should contain FILES inside CTE, got: " + output, lower.contains("files("));
        assertTrue("Should contain three-part table inside CTE, got: " + output,
            lower.contains("ext_cat.ext_db.orders"));
        assertTrue(lower.contains("with"));
    }

    @Test
    public void testIntegratedNativeQueryInSubqueryWithExists() {
        String sql = "SELECT * FROM local_t l WHERE EXISTS (SELECT 1 "
            + "FROM TABLE (mycat.native_query('SELECT id FROM remote_t')) nq "
            + "JOIN FILES('connector'='mock', 'mock.columns'='id:int') f ON nq.id = f.id "
            + "JOIN ext_cat.ext_db.orders o ON f.id = o.id "
            + "WHERE o.id = l.id)";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain outer normal table, got: " + output, lower.contains("local_t"));
        assertTrue("Should contain native_query in EXISTS subquery, got: " + output,
            lower.contains("native_query("));
        assertTrue("Should contain FILES in EXISTS subquery, got: " + output, lower.contains("files("));
        assertTrue("Should contain three-part table in EXISTS subquery, got: " + output,
            lower.contains("ext_cat.ext_db.orders"));
        assertTrue(lower.contains("exists"));
    }

    @Test
    public void testIntegratedNativeQueryWithAggregateHaving() {
        String sql = "SELECT l.id, COUNT(*) cnt FROM local_t l "
            + "JOIN TABLE (mycat.native_query('SELECT id FROM remote_t')) nq ON l.id = nq.id "
            + "JOIN FILES('connector'='mock', 'mock.columns'='id:int') f ON nq.id = f.id "
            + "JOIN ext_cat.ext_db.orders o ON f.id = o.id "
            + "GROUP BY l.id HAVING COUNT(*) > 1";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain native_query in aggregate query, got: " + output,
            lower.contains("native_query("));
        assertTrue("Should contain FILES in aggregate query, got: " + output, lower.contains("files("));
        assertTrue("Should contain three-part table in aggregate query, got: " + output,
            lower.contains("ext_cat.ext_db.orders"));
        assertTrue(lower.contains("group by"));
        assertTrue(lower.contains("having"));
    }

    @Test
    public void testIntegratedNativeQueryWithOrderByAndLimit() {
        String sql = "SELECT l.id FROM local_t l "
            + "JOIN TABLE (mycat.native_query('SELECT id FROM remote_t')) nq ON l.id = nq.id "
            + "JOIN FILES('connector'='mock', 'mock.columns'='id:int') f ON nq.id = f.id "
            + "JOIN ext_cat.ext_db.orders o ON f.id = o.id "
            + "ORDER BY l.id DESC LIMIT 10";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain native_query in sorted query, got: " + output,
            lower.contains("native_query("));
        assertTrue("Should contain FILES in sorted query, got: " + output, lower.contains("files("));
        assertTrue("Should contain three-part table in sorted query, got: " + output,
            lower.contains("ext_cat.ext_db.orders"));
        assertTrue(lower.contains("order by"));
        assertTrue(lower.contains("limit"));
    }

    @Test
    public void testNativeQueryAfterCommaThenJoin() {
        String sql = "SELECT * FROM base b, TABLE (mycat.native_query('SELECT id FROM remote_t')) nq "
            + "JOIN dim d ON nq.id = d.id WHERE b.id = d.id";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should contain native_query after comma and before JOIN, got: " + output,
            lower.contains("native_query("));
        assertTrue(lower.contains("join"));
        assertTrue(lower.contains("where"));
    }

    @Test
    public void testFallbackToAdhocTableSourceInFrom() {
        String sql = "SELECT * FROM TABLE temp_1 (id int, name varchar(20))";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should fall back to adhoc table source, got: " + output, lower.contains("temp_1"));
        assertFalse("Should not be parsed as native_query, got: " + output, lower.contains("native_query("));
    }

    @Test
    public void testFallbackToAdhocTableSourceInJoinRight() {
        String sql = "SELECT * FROM local_t l JOIN TABLE temp_1 (id int) t ON l.id = t.id";
        String output = parse(sql);
        String lower = output.toLowerCase();
        assertTrue("Should fall back to adhoc table source on join right side, got: " + output,
            lower.contains("temp_1"));
        assertFalse("Should not be parsed as native_query, got: " + output, lower.contains("native_query("));
    }

    @Test
    public void testNativeQueryRejectsUnquotedArgument() {
        String sql = "SELECT * FROM TABLE (mycat.native_query(SELECT 1)) nq";
        MySqlStatementParser parser = new MySqlStatementParser(sql, FEATURES);
        try {
            parser.parseStatementList();
            fail("unquoted native_query argument should be rejected");
        } catch (ParserException e) {
            assertTrue("Error should mention native_query, got: " + e.getMessage(),
                e.getMessage().contains("native_query"));
        }
    }

    private SQLNativeQueryTableSource nativeQueryOf(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql, FEATURES);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        return (SQLNativeQueryTableSource) stmt.getSelect().getQueryBlock().getFrom();
    }

    @Test
    public void testNativeQueryBacktickCatalogNameKeptRaw() {
        String sql = "SELECT * FROM TABLE (`hive`.native_query('SELECT 1')) nq";
        assertEquals("Quote characters must survive in the AST so the visitor can print them back",
            "`hive`", nativeQueryOf(sql).getCatalogName());
    }

    @Test
    public void testNativeQueryBacktickAliasKeptRaw() {
        String sql = "SELECT * FROM TABLE (mycat.native_query('SELECT 1')) `t`";
        assertEquals("Quote characters must survive in the AST so the visitor can print them back",
            "`t`", nativeQueryOf(sql).getAlias());
    }

    @Test
    public void testQuotedCatalogAndAliasRoundTrip() {
        // '-' is a legal external catalog character, so such a name can only be written
        // quoted; dropping the quotes on output would produce SQL that cannot re-parse.
        String sql = "SELECT * FROM TABLE (`my-cat`.native_query('SELECT 1')) AS `my alias`";
        String output = parse(sql);
        assertTrue("Quoted catalog must be printed back quoted, got: " + output,
            output.contains("`my-cat`"));
        assertTrue("Quoted alias must be printed back quoted, got: " + output,
            output.contains("`my alias`"));

        SQLNativeQueryTableSource reparsed = nativeQueryOf(output);
        assertEquals("`my-cat`", reparsed.getCatalogName());
        assertEquals("`my alias`", reparsed.getAlias());
        assertEquals("SELECT 1", reparsed.getSql());
    }

    @Test
    public void testNativeQueryRejectsNumericArgument() {
        String sql = "SELECT * FROM TABLE (mycat.native_query(123)) nq";
        MySqlStatementParser parser = new MySqlStatementParser(sql, FEATURES);
        try {
            parser.parseStatementList();
            fail("numeric native_query argument should be rejected");
        } catch (ParserException e) {
            assertTrue("Error should mention native_query, got: " + e.getMessage(),
                e.getMessage().contains("native_query"));
        }
    }
}
