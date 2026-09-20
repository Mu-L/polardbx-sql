/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.parser;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import junit.framework.TestCase;
import org.junit.Assert;

public class HintsTest extends TestCase {
    public void test_hints_0() throws Exception {
        String sql = "CREATE /*!32302 TEMPORARY */ TABLE t (a INT);";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("CREATE /*!32302 TEMPORARY */ TABLE t (\n\ta INT\n);", output);
    }

    public void test_hints_1() throws Exception {
        String sql = "SELECT /*! STRAIGHT_JOIN */ col1 FROM table1,table2";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SELECT /*! STRAIGHT_JOIN */ col1\nFROM table1, table2", output);
    }

    public void test_hints_none() throws Exception {
        String sql = "SELECT /* STRAIGHT_JOIN */ col1 FROM table1,table2";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SELECT col1\nFROM table1, table2", output);
    }

    public void test_hints_head() throws Exception {
        String sql = "/* STRAIGHT_JOIN */ SELECT col1 FROM table1,table2";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("/* STRAIGHT_JOIN */\n" + "SELECT col1\n" + "FROM table1, table2", output);
    }

    public void test_hints_schema_name() throws Exception {
        String sql = HintUtil.buildPushdown("t1", null, "test-schema");
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SELECT *\n" +
            "FROM t1", output);
    }

    /**
     * buildPushdown() must backtick-quote table names that contain special characters
     * (e.g. hyphens). Without quoting, the parser treats "test-table" as the arithmetic
     * expression "test MINUS table", causing a parse failure or semantic error at runtime.
     */
    public void test_buildPushdown_table_name_with_hyphen() throws Exception {
        String sql = HintUtil.buildPushdown("test-table", null, null);
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt;
        try {
            stmt = parser.parseStatementList().get(0);
        } catch (Exception e) {
            Assert.fail("buildPushdown generated SQL that cannot be parsed: '" + sql + "'. Error: " + e.getMessage());
            return;
        }
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertTrue(
            "Table name with hyphen must be backtick-quoted in generated SQL, got: " + output,
            output.contains("`test-table`"));
    }

    /**
     * Same as above but with a WHERE condition — the condition must still be appended
     * after the backtick-quoted table name.
     */
    public void test_buildPushdown_table_name_with_hyphen_and_condition() throws Exception {
        String sql = HintUtil.buildPushdown("test-table", "pk = 1", null);
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt;
        try {
            stmt = parser.parseStatementList().get(0);
        } catch (Exception e) {
            Assert.fail("buildPushdown generated SQL that cannot be parsed: '" + sql + "'. Error: " + e.getMessage());
            return;
        }
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertTrue(
            "Table name with hyphen must be backtick-quoted in generated SQL, got: " + output,
            output.contains("`test-table`"));
        Assert.assertTrue("WHERE condition must be preserved, got: " + output, output.contains("pk = 1"));
    }

    /**
     * A table name that already starts with a backtick must not be double-wrapped.
     */
    public void test_buildPushdown_already_backticked_table() throws Exception {
        String sql = HintUtil.buildPushdown("`test-table`", null, null);
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt;
        try {
            stmt = parser.parseStatementList().get(0);
        } catch (Exception e) {
            Assert.fail("buildPushdown generated SQL that cannot be parsed: '" + sql + "'. Error: " + e.getMessage());
            return;
        }
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertTrue(
            "Already-backticked table name must not be double-wrapped, got: " + output,
            output.contains("`test-table`") && !output.contains("``"));
    }

    /**
     * schema.table form: when table contains a dot, both parts should be individually quoted.
     */
    public void test_buildPushdown_schema_dot_table_with_hyphen() throws Exception {
        String sql = HintUtil.buildPushdown("test-schema.test-table", null, null);
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt;
        try {
            stmt = parser.parseStatementList().get(0);
        } catch (Exception e) {
            Assert.fail("buildPushdown generated SQL that cannot be parsed: '" + sql + "'. Error: " + e.getMessage());
            return;
        }
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertTrue(
            "schema.table with hyphens must be backtick-quoted, got: " + output,
            output.contains("`test-schema`") && output.contains("`test-table`"));
    }

    /**
     * A table parameter containing spaces (e.g. table with alias) must be passed through
     * as-is without quoting, since it represents a complex FROM clause fragment.
     */
    public void test_buildPushdown_table_with_alias_not_quoted() throws Exception {
        String sql = HintUtil.buildPushdown("test_table_a a", null, null);
        Assert.assertEquals("SELECT * FROM test_table_a a", sql);
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        parser.parseStatementList().get(0);
    }

    /**
     * A table name with a space that is already backtick-quoted must remain valid.
     */
    public void test_buildPushdown_backticked_table_with_space() throws Exception {
        String sql = HintUtil.buildPushdown("`test table`", null, null);
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt;
        try {
            stmt = parser.parseStatementList().get(0);
        } catch (Exception e) {
            Assert.fail("buildPushdown generated SQL that cannot be parsed: '" + sql + "'. Error: " + e.getMessage());
            return;
        }
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertTrue(
            "Backtick-quoted table name with space must remain valid, got: " + output,
            output.contains("`test table`"));
    }

    /**
     * A comma-separated multi-table expression must be passed through as-is.
     */
    public void test_buildPushdown_multi_table_not_quoted() throws Exception {
        String sql = HintUtil.buildPushdown("test_table_a a, test_table_b b", null, null);
        Assert.assertEquals("SELECT * FROM test_table_a a, test_table_b b", sql);
    }

    /**
     * A safe table name (only alphanumeric and underscore) must NOT be backtick-quoted.
     */
    public void test_buildPushdown_safe_table_name_no_quoting() throws Exception {
        String sql = HintUtil.buildPushdown("my_table_123", null, null);
        Assert.assertEquals("SELECT * FROM my_table_123", sql);
    }

    /**
     * When table is null, buildPushdown should generate SELECT * FROM DUAL.
     */
    public void test_buildPushdown_null_table_produces_dual() throws Exception {
        String sql = HintUtil.buildPushdown(null, null, null);
        Assert.assertEquals("SELECT * FROM DUAL", sql);
    }
}
