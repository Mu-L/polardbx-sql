package com.alibaba.polardbx.druid.bvt.sql.mysql.create;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLFilesTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNativeQueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class FilesParserTest {
    @Test
    public void testFilesParseStatementList() {
        String sql = "SELECT * FROM FILES('path'=>'/tmp/x','format'=>'csv') f";
        MySqlStatementParser parser = new MySqlStatementParser(sql,
            SQLParserFeature.TDDLHint,
            SQLParserFeature.EnableCurrentUserExpr,
            SQLParserFeature.DRDSAsyncDDL,
            SQLParserFeature.DrdsMisc,
            SQLParserFeature.DRDSBaseline,
            SQLParserFeature.DrdsGSI,
            SQLParserFeature.DrdsMisc,
            SQLParserFeature.DrdsCCL);
        List<SQLStatement> stmts = parser.parseStatementList();
        assertFalse(stmts.isEmpty());
        System.out.println(stmts.get(0).toString());
    }

    @Test
    public void testFilesParseWithByteString() {
        String sql = "SELECT * FROM FILES('path'=>'/tmp/x','format'=>'csv') f";
        ByteString bs =
            ByteString.from(sql);
        MySqlStatementParser parser = new MySqlStatementParser(bs,
            SQLParserFeature.TDDLHint,
            SQLParserFeature.EnableCurrentUserExpr,
            SQLParserFeature.DRDSAsyncDDL,
            SQLParserFeature.DrdsMisc,
            SQLParserFeature.DRDSBaseline,
            SQLParserFeature.DrdsGSI,
            SQLParserFeature.DrdsMisc,
            SQLParserFeature.DrdsCCL);
        List<SQLStatement> stmts = parser.parseStatementList();
        assertFalse(stmts.isEmpty());
        System.out.println(stmts.get(0).toString());
    }

    @Test
    public void testFilesParseWithOptimizedForParameterized() {
        // This is the feature set used by SqlParameterizeUtils (the CN runtime path)
        String sql = "SELECT * FROM FILES('path'=>'/tmp/x','format'=>'csv') f";
        ByteString bs =
            ByteString.from(sql);
        MySqlStatementParser parser = new MySqlStatementParser(bs,
            SQLParserFeature.EnableSQLBinaryOpExprGroup,
            SQLParserFeature.OptimizedForParameterized,
            SQLParserFeature.TDDLHint,
            SQLParserFeature.EnableCurrentUserExpr,
            SQLParserFeature.DRDSAsyncDDL,
            SQLParserFeature.DRDSBaseline,
            SQLParserFeature.DrdsMisc,
            SQLParserFeature.DrdsGSI,
            SQLParserFeature.DrdsCCL);
        List<SQLStatement> stmts = parser.parseStatementList();
        assertFalse(stmts.isEmpty());

        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor outputVisitor =
            new MySqlOutputVisitor(sb);
        stmts.get(0).accept(outputVisitor);
        String output = sb.toString();
        System.out.println("OutputVisitor: [" + output + "]");
        assertTrue("Should contain FILES", output.toUpperCase().contains("FILES("));
        assertTrue("Should contain path", output.contains("path"));
    }

    @Test
    public void testFilesWithAsAlias() {
        String sql = "SELECT * FROM FILES('path'=>'/tmp/x','format'=>'csv') AS myAlias";
        MySqlStatementParser parser = new MySqlStatementParser(sql,
            SQLParserFeature.TDDLHint,
            SQLParserFeature.EnableCurrentUserExpr,
            SQLParserFeature.DRDSAsyncDDL,
            SQLParserFeature.DrdsMisc,
            SQLParserFeature.DRDSBaseline,
            SQLParserFeature.DrdsGSI,
            SQLParserFeature.DrdsCCL);
        List<SQLStatement> stmts = parser.parseStatementList();
        assertFalse(stmts.isEmpty());

        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(sb);
        stmts.get(0).accept(visitor);
        String output = sb.toString().toLowerCase();
        assertTrue("Should contain alias 'myalias', got: " + output,
            output.contains("myalias"));
    }

    @Test
    public void testFilesBacktickAliasKeptRaw() {
        String sql = "SELECT * FROM FILES('path'=>'/tmp/x','format'=>'csv') AS `f`";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        SQLFilesTableSource files = (SQLFilesTableSource)
            stmt.getSelect().getQueryBlock().getFrom();
        assertEquals("Quote characters must survive in the AST so the visitor can print them back",
            "`f`", files.getAlias());
    }

    @Test
    public void testFilesBacktickAliasWithoutAsKeptRaw() {
        String sql = "SELECT * FROM FILES('path'=>'/tmp/x','format'=>'csv') `f`";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        SQLFilesTableSource files = (SQLFilesTableSource)
            stmt.getSelect().getQueryBlock().getFrom();
        assertEquals("Quote characters must survive in the AST so the visitor can print them back",
            "`f`", files.getAlias());
    }

    @Test
    public void testFilesBacktickAliasInJoinKeptRaw() {
        String sql = "SELECT * FROM t1 JOIN FILES('path'=>'/tmp/x','format'=>'csv') `f` ON t1.id = f.id";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        SQLJoinTableSource join = (SQLJoinTableSource) stmt.getSelect().getQueryBlock().getFrom();
        SQLFilesTableSource files = (SQLFilesTableSource) join.getRight();
        assertEquals("Quote characters must survive in the AST so the visitor can print them back",
            "`f`", files.getAlias());
    }

    @Test
    public void testFilesQuotedAliasRoundTrip() {
        // An alias containing a space can only be written quoted; dropping the quotes on
        // output would produce SQL that cannot re-parse.
        String sql = "SELECT * FROM FILES('path'=>'/tmp/x','format'=>'csv') AS `my alias`";
        StringBuilder sb = new StringBuilder();
        new MySqlStatementParser(sql).parseStatementList().get(0)
            .accept(new MySqlOutputVisitor(sb));
        String output = sb.toString();
        assertTrue("Quoted alias must be printed back quoted, got: " + output,
            output.contains("`my alias`"));

        SQLSelectStatement reparsed =
            (SQLSelectStatement) new MySqlStatementParser(output).parseStatementList().get(0);
        SQLFilesTableSource files = (SQLFilesTableSource)
            reparsed.getSelect().getQueryBlock().getFrom();
        assertEquals("`my alias`", files.getAlias());
        assertEquals("/tmp/x", files.getProperties().get("path"));
    }

    @Test
    public void testFilesOutputQuotesAndEscapesRawStringLiterals() {
        SQLFilesTableSource filesSource =
            new SQLFilesTableSource();
        filesSource.getProperties().put("path", "/tmp/it's_here");
        filesSource.getProperties().put("format", "csv");

        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(sb);
        filesSource.accept(visitor);
        String output = sb.toString();
        assertTrue("Should quote escaped path literal, got: " + output,
            output.contains("'path'='/tmp/it''s_here'"));
        assertTrue("Should quote format literal, got: " + output,
            output.contains("'format'='csv'"));
    }

    @Test
    public void testNativeQueryOutputQuotesAndEscapesRawSqlLiteral() {
        SQLNativeQueryTableSource source =
            new SQLNativeQueryTableSource();
        source.setCatalogName("cat1");
        source.setSql("SELECT 'x' AS c");

        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(sb);
        source.accept(visitor);
        String output = sb.toString();
        assertTrue("Should quote escaped native SQL literal, got: " + output,
            output.toLowerCase().contains("native_query('select ''x'' as c')"));
    }

    @Test
    public void testNativeQueryBackslashRoundTrip() {
        String sql = "SELECT * FROM TABLE(cat1.native_query('a\\\\b'))";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        SQLNativeQueryTableSource src = (SQLNativeQueryTableSource)
            stmt.getSelect().getQueryBlock().getFrom();
        String originalSql = src.getSql();

        String output = stmt.toString();
        SQLSelectStatement stmt2 = (SQLSelectStatement)
            new MySqlStatementParser(output).parseStatementList().get(0);
        SQLNativeQueryTableSource src2 = (SQLNativeQueryTableSource)
            stmt2.getSelect().getQueryBlock().getFrom();
        assertEquals("native_query SQL must round-trip with backslash, output: " + output,
            originalSql, src2.getSql());
    }

    @Test
    public void testFilesBackslashRoundTrip() {
        String sql = "SELECT * FROM FILES(path => 'a\\\\b', format => 'csv') f";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        SQLFilesTableSource files = (SQLFilesTableSource)
            stmt.getSelect().getQueryBlock().getFrom();
        String originalPath = files.getProperties().get("path");

        String output = stmt.toString();
        SQLSelectStatement stmt2 = (SQLSelectStatement)
            new MySqlStatementParser(output).parseStatementList().get(0);
        SQLFilesTableSource files2 = (SQLFilesTableSource)
            stmt2.getSelect().getQueryBlock().getFrom();
        assertEquals("FILES path must round-trip with backslash, output: " + output,
            originalPath, files2.getProperties().get("path"));
    }

    @Test
    public void testFilesNumericValue() {
        String sql = "SELECT * FROM FILES(path => '/tmp/x', rows => 100) f";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmts = parser.parseStatementList();
        SQLSelectStatement select = (SQLSelectStatement) stmts.get(0);
        SQLFilesTableSource files =
            (SQLFilesTableSource) select.getSelect().getQueryBlock().getFrom();
        assertEquals("/tmp/x", files.getProperties().get("path"));
        assertEquals("100", files.getProperties().get("rows"));
    }

    @Test
    public void testFilesRejectsMissingComma() {
        String sql = "SELECT * FROM FILES('a'=>'1' 'b'=>'2') f";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatementList();
            fail("missing comma between FILES properties should be rejected");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testFilesInJoinNumericValue() {
        String sql = "SELECT * FROM t1 JOIN FILES('path'=>'/tmp/x', 'rows'=>10) f ON t1.id = f.id";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmts = parser.parseStatementList();
        SQLSelectStatement select = (SQLSelectStatement) stmts.get(0);
        SQLJoinTableSource join = (SQLJoinTableSource) select.getSelect().getQueryBlock().getFrom();
        SQLFilesTableSource files = (SQLFilesTableSource) join.getRight();
        assertEquals("/tmp/x", files.getProperties().get("path"));
        assertEquals("10", files.getProperties().get("rows"));
    }

    @Test
    public void testFilesInJoinIsLeftAssociative() {
        String sql = "SELECT * FROM t1 JOIN FILES(path => 'x', format => 'csv') f JOIN t2 ON 1=1";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        SQLJoinTableSource top = (SQLJoinTableSource) stmt.getSelect().getQueryBlock().getFrom();
        // Left-associative: (t1 JOIN FILES f) JOIN t2 ON 1=1
        assertTrue("top right should be t2 (SQLExprTableSource), got: "
                + top.getRight().getClass().getSimpleName(),
            top.getRight() instanceof SQLExprTableSource);
        assertTrue("top left should be inner join (t1 JOIN FILES), got: "
                + top.getLeft().getClass().getSimpleName(),
            top.getLeft() instanceof SQLJoinTableSource);
        SQLJoinTableSource inner = (SQLJoinTableSource) top.getLeft();
        assertTrue("inner right should be FILES, got: "
                + inner.getRight().getClass().getSimpleName(),
            inner.getRight() instanceof SQLFilesTableSource);
    }

    @Test
    public void testFilesAfterCommaJoinBindsFirst() {
        // After the fix FILES no longer swallows the trailing JOIN into the right
        // subtree, so it behaves like a normal table: (t1 , FILES f) JOIN t2 ON 1=1.
        String sql = "SELECT * FROM t1, FILES(path => 'x', format => 'csv') f JOIN t2 ON 1=1";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        SQLJoinTableSource top = (SQLJoinTableSource) stmt.getSelect().getQueryBlock().getFrom();
        assertNotEquals("top should not be COMMA (trailing JOIN must bind at top), got: "
                + top.getJoinType(),
            SQLJoinTableSource.JoinType.COMMA, top.getJoinType());
        assertTrue("top right should be t2 (SQLExprTableSource), got: "
                + top.getRight().getClass().getSimpleName(),
            top.getRight() instanceof SQLExprTableSource);
        assertTrue("top left should be (t1 COMMA FILES), got: "
                + top.getLeft().getClass().getSimpleName(),
            top.getLeft() instanceof SQLJoinTableSource);
        SQLJoinTableSource inner = (SQLJoinTableSource) top.getLeft();
        assertEquals(SQLJoinTableSource.JoinType.COMMA, inner.getJoinType());
        assertTrue("inner right should be FILES, got: "
                + inner.getRight().getClass().getSimpleName(),
            inner.getRight() instanceof SQLFilesTableSource);
    }

    @Test
    public void testFilesInJoinRejectsMissingComma() {
        String sql = "SELECT * FROM t1 JOIN FILES('a'=>'1' 'b'=>'2') f ON t1.id = f.id";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatementList();
            fail("missing comma between FILES properties should be rejected");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testFilesTableSourceClone() {
        SQLFilesTableSource source = new SQLFilesTableSource();
        source.setAlias("f");
        source.getProperties().put("path", "/tmp/x");
        source.getProperties().put("format", "csv");

        SQLFilesTableSource cloned = source.clone();
        assertEquals("f", cloned.getAlias());
        assertEquals("/tmp/x", cloned.getProperties().get("path"));
        assertEquals("csv", cloned.getProperties().get("format"));

        cloned.getProperties().put("path", "/tmp/y");
        assertEquals("/tmp/x", source.getProperties().get("path"));
    }

    @Test
    public void testNativeQueryTableSourceClone() {
        SQLNativeQueryTableSource source =
            new SQLNativeQueryTableSource();
        source.setAlias("n");
        source.setCatalogName("cat1");
        source.setSql("SELECT 1");

        SQLNativeQueryTableSource cloned = source.clone();
        assertEquals("n", cloned.getAlias());
        assertEquals("cat1", cloned.getCatalogName());
        assertEquals("SELECT 1", cloned.getSql());

        cloned.setSql("SELECT 2");
        assertEquals("SELECT 1", source.getSql());
    }

    @Test
    public void testSelectStatementWithFilesClone() {
        String sql = "SELECT * FROM FILES('path'=>'/tmp/x','format'=>'csv') f";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);

        SQLSelectStatement cloned = stmt.clone();
        SQLFilesTableSource clonedFrom = (SQLFilesTableSource) cloned.getSelect().getQueryBlock().getFrom();
        assertEquals("f", clonedFrom.getAlias());
        assertEquals("/tmp/x", clonedFrom.getProperties().get("path"));
        assertEquals(stmt.toString(), cloned.toString());
    }

    @Test
    public void testSelectStatementWithNativeQueryClone() {
        String sql = "SELECT * FROM TABLE(cat1.native_query('SELECT 1')) n";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);

        SQLSelectStatement cloned = stmt.clone();
        SQLNativeQueryTableSource clonedFrom =
            (SQLNativeQueryTableSource) cloned.getSelect()
                .getQueryBlock().getFrom();
        assertEquals("n", clonedFrom.getAlias());
        assertEquals("cat1", clonedFrom.getCatalogName());
        assertEquals("SELECT 1", clonedFrom.getSql());
        assertEquals(stmt.toString(), cloned.toString());
    }

    @Test
    public void testFilesBacktickPropertyKeysNormalized() {
        String sql = "SELECT * FROM FILES(`path` => '/tmp/x', `format` => 'csv') f";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        SQLFilesTableSource files = (SQLFilesTableSource)
            stmt.getSelect().getQueryBlock().getFrom();
        assertEquals("backtick key `path` should normalize to bare 'path'",
            "/tmp/x", files.getProperties().get("path"));
        assertEquals("backtick key `format` should normalize to bare 'format'",
            "csv", files.getProperties().get("format"));
        assertFalse("backtick wrapper must not leak into property key",
            files.getProperties().containsKey("`path`"));
    }

    @Test
    public void testFilesSingleQuotedPropertyKeysNormalized() {
        String sql = "SELECT * FROM FILES('path' => '/tmp/x', 'format' => 'csv') f";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        SQLFilesTableSource files = (SQLFilesTableSource)
            stmt.getSelect().getQueryBlock().getFrom();
        assertEquals("/tmp/x", files.getProperties().get("path"));
        assertEquals("csv", files.getProperties().get("format"));
    }

    @Test
    public void testFilesBacktickValueNormalized() {
        String sql = "SELECT * FROM FILES(`connector` => `mock`, `mock.columns` => 'id:int') f";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        SQLFilesTableSource files = (SQLFilesTableSource)
            stmt.getSelect().getQueryBlock().getFrom();
        assertEquals("backtick value `mock` should normalize to bare 'mock'",
            "mock", files.getProperties().get("connector"));
        assertFalse("backtick wrapper must not leak into value",
            files.getProperties().containsValue("`mock`"));
    }

    @Test
    public void testFilesMixedCasePropertyKeysNormalized() {
        String sql = "SELECT * FROM FILES('Path' => '/tmp/x', 'FORMAT' => 'csv') f";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        SQLFilesTableSource files = (SQLFilesTableSource)
            stmt.getSelect().getQueryBlock().getFrom();
        assertEquals("/tmp/x", files.getProperties().get("path"));
        assertEquals("csv", files.getProperties().get("format"));
        assertFalse(files.getProperties().containsKey("Path"));
        assertFalse(files.getProperties().containsKey("FORMAT"));
    }

    @Test
    public void testFilesInJoinMixedCasePropertyKeysNormalized() {
        String sql = "SELECT * FROM t1 JOIN FILES('Path' => '/tmp/x', 'FORMAT' => 'csv') f ON t1.id = f.id";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatementList().get(0);
        SQLJoinTableSource join = (SQLJoinTableSource) stmt.getSelect().getQueryBlock().getFrom();
        SQLFilesTableSource files = (SQLFilesTableSource) join.getRight();
        assertEquals("JOIN-right 'Path' should normalize to 'path'",
            "/tmp/x", files.getProperties().get("path"));
        assertEquals("JOIN-right 'FORMAT' should normalize to 'format'",
            "csv", files.getProperties().get("format"));
        assertFalse("Original mixed-case key must not survive", files.getProperties().containsKey("Path"));
        assertFalse("Original mixed-case key must not survive", files.getProperties().containsKey("FORMAT"));
    }
}
