package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DdlHyphenCommentNewlineTest {

    private SqlAlterTable buildAlterTable(String sourceSql, String tableName) {
        SqlIdentifier name = new SqlIdentifier(Collections.singletonList(tableName), SqlParserPos.ZERO);
        return SqlDdlNodes.alterTable(null, name, null, sourceSql, null,
            new ArrayList<>(), SqlParserPos.ZERO);
    }

    private String renderNativeLine(String comment, String lineEnding) {
        String sql = "/*DDL_ID=1*/" + lineEnding
            + comment + lineEnding
            + "/*+TDDL:cmd_extra(X=true)*/" + lineEnding
            + "ALTER TABLE `t` ADD COLUMN c TINYINT NOT NULL DEFAULT 0";
        return RelUtils.toNativeSqlLine(buildAlterTable(sql, "t"));
    }

    private void assertAlterTablePreserved(String comment, String lineEnding) {
        String nativeLineSql = renderNativeLine(comment, lineEnding);
        List<SQLStatement> statements = FastsqlUtils.parseSql(nativeLineSql);
        Assert.assertEquals("Expected one ALTER TABLE statement. sql=[" + nativeLineSql + "]", 1, statements.size());
        Assert.assertTrue(statements.get(0).toString().toUpperCase().contains("ALTER TABLE"));
        Assert.assertTrue("Line comment should be rendered as a block comment. sql=[" + nativeLineSql + "]",
            nativeLineSql.contains("/* section comment*/"));
    }

    @Test
    public void testHyphenCommentWithLf() {
        assertAlterTablePreserved("-- section comment", "\n");
    }

    @Test
    public void testHashCommentWithLf() {
        assertAlterTablePreserved("# section comment", "\n");
    }

    @Test
    public void testHyphenCommentWithCrLf() {
        assertAlterTablePreserved("-- section comment", "\r\n");
    }

    @Test
    public void testHashCommentWithCrLf() {
        assertAlterTablePreserved("# section comment", "\r\n");
    }

    @Test
    public void testHyphenCommentWithCr() {
        assertAlterTablePreserved("-- section comment", "\r");
    }

    @Test
    public void testHashCommentWithCr() {
        assertAlterTablePreserved("# section comment", "\r");
    }

    @Test
    public void testAlterTableWithoutLineCommentKeepsExistingOutput() {
        String sql = "ALTER TABLE `t` ADD COLUMN c INT, ALGORITHM=INPLACE";
        String expected = SQLUtils.parseStatementsWithDefaultFeatures(sql, JdbcConstants.MYSQL).get(0).toString();
        String actual = buildAlterTable(sql, "t").toString();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testTrailingCommentDoesNotRestoreSqlKeyword() {
        String sql = "ALTER TABLE t ADD COLUMN c INT; -- do not DROP TABLE audit";
        String rendered = buildAlterTable(sql, "t").toString();
        Assert.assertFalse(rendered.toUpperCase().contains("DROP TABLE"));
        Assert.assertEquals(1, FastsqlUtils.parseSql(rendered).size());
    }

    @Test
    public void testQuotedHyphensDoNotEnableLineCommentOutput() {
        String sql = "ALTER TABLE t ADD COLUMN c VARCHAR(20) COMMENT 'a -- b'";
        String expected = SQLUtils.parseStatementsWithDefaultFeatures(sql, JdbcConstants.MYSQL).get(0).toString();
        String actual = buildAlterTable(sql, "t").toString();
        Assert.assertEquals(expected, actual);
    }
}
