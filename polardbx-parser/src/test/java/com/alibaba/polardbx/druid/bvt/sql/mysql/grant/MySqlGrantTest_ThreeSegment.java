/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.mysql.grant;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import org.junit.Assert;

import java.util.List;

/**
 * Verify Parser produces correct AST structure for three-segment GRANT syntax:
 * catalog.db.* and catalog.db.table and *.*.*
 * <p>
 * This is critical for the catalog privilege implementation which relies on
 * detecting nested SQLPropertyExpr in PolarHandlerCommon.getGrantees().
 */
public class MySqlGrantTest_ThreeSegment extends MysqlTest {

    /**
     * GRANT SELECT ON hive.warehouse.* TO user
     * Expected AST:
     * SQLExprTableSource.getExpr() = SQLPropertyExpr(name="*")
     * .getOwner() = SQLPropertyExpr(name="warehouse")
     * .getOwner() = SQLIdentifierExpr("hive")
     */
    public void test_catalog_db_wildcard() throws Exception {
        String sql = "GRANT SELECT ON hive.warehouse.* TO 'testuser'@'%'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        Assert.assertEquals(1, statementList.size());

        SQLGrantStatement stmt = (SQLGrantStatement) statementList.get(0);
        SQLExprTableSource resource = (SQLExprTableSource) stmt.getResource();

        // Outermost: SQLPropertyExpr with name="*"
        Assert.assertTrue("resource expr should be SQLPropertyExpr",
            resource.getExpr() instanceof SQLPropertyExpr);
        SQLPropertyExpr outerExpr = (SQLPropertyExpr) resource.getExpr();
        Assert.assertEquals("*", outerExpr.getName());

        // Middle: owner should be SQLPropertyExpr with name="warehouse"
        Assert.assertTrue("owner of outer should be SQLPropertyExpr",
            outerExpr.getOwner() instanceof SQLPropertyExpr);
        SQLPropertyExpr middleExpr = (SQLPropertyExpr) outerExpr.getOwner();
        Assert.assertEquals("warehouse", middleExpr.getName());

        // Innermost: owner should be SQLIdentifierExpr("hive")
        Assert.assertTrue("owner of middle should be SQLIdentifierExpr",
            middleExpr.getOwner() instanceof SQLIdentifierExpr);
        SQLIdentifierExpr catalogExpr = (SQLIdentifierExpr) middleExpr.getOwner();
        Assert.assertEquals("hive", catalogExpr.getName());

        // Verify getOwnerName() shortcut
        Assert.assertEquals("hive", middleExpr.getOwnernName());

        // Verify round-trip serialization
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("GRANT SELECT ON hive.warehouse.* TO 'testuser'@'%'", output);
    }

    /**
     * GRANT SELECT ON hive.warehouse.orders TO user
     * Expected AST:
     * SQLExprTableSource.getExpr() = SQLPropertyExpr(name="orders")
     * .getOwner() = SQLPropertyExpr(name="warehouse")
     * .getOwner() = SQLIdentifierExpr("hive")
     */
    public void test_catalog_db_table() throws Exception {
        String sql = "GRANT SELECT ON hive.warehouse.orders TO 'testuser'@'%'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        Assert.assertEquals(1, statementList.size());

        SQLGrantStatement stmt = (SQLGrantStatement) statementList.get(0);
        SQLExprTableSource resource = (SQLExprTableSource) stmt.getResource();

        SQLPropertyExpr outerExpr = (SQLPropertyExpr) resource.getExpr();
        Assert.assertEquals("orders", outerExpr.getName());

        SQLPropertyExpr middleExpr = (SQLPropertyExpr) outerExpr.getOwner();
        Assert.assertEquals("warehouse", middleExpr.getName());

        SQLIdentifierExpr catalogExpr = (SQLIdentifierExpr) middleExpr.getOwner();
        Assert.assertEquals("hive", catalogExpr.getName());

        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("GRANT SELECT ON hive.warehouse.orders TO 'testuser'@'%'", output);
    }

    /**
     * GRANT SELECT ON *.*.* TO user (global wildcard)
     * Expected AST:
     * SQLExprTableSource.getExpr() = SQLPropertyExpr(name="*")
     * .getOwner() = SQLPropertyExpr(name="*")
     * .getOwner() = SQLAllColumnExpr
     */
    public void test_global_wildcard() throws Exception {
        String sql = "GRANT SELECT ON *.*.* TO 'testuser'@'%'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        Assert.assertEquals(1, statementList.size());

        SQLGrantStatement stmt = (SQLGrantStatement) statementList.get(0);
        SQLExprTableSource resource = (SQLExprTableSource) stmt.getResource();

        // Outermost: name="*"
        Assert.assertTrue("resource expr should be SQLPropertyExpr",
            resource.getExpr() instanceof SQLPropertyExpr);
        SQLPropertyExpr outerExpr = (SQLPropertyExpr) resource.getExpr();
        Assert.assertEquals("*", outerExpr.getName());

        // Middle: owner should be SQLPropertyExpr with name="*"
        Assert.assertTrue("owner of outer should be SQLPropertyExpr for *.*.*",
            outerExpr.getOwner() instanceof SQLPropertyExpr);
        SQLPropertyExpr middleExpr = (SQLPropertyExpr) outerExpr.getOwner();
        Assert.assertEquals("*", middleExpr.getName());

        // Innermost: SQLAllColumnExpr (the first * is parsed as SQLAllColumnExpr, not SQLIdentifierExpr)
        // Note: getOwnernName() returns null for SQLAllColumnExpr,
        // so we check owner type and toString() instead
        Assert.assertTrue("owner of middle should be SQLAllColumnExpr for *.*.*",
            middleExpr.getOwner() instanceof SQLAllColumnExpr);
        Assert.assertEquals("*", middleExpr.getOwner().toString());

        // Verify round-trip serialization
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("GRANT SELECT ON *.*.* TO 'testuser'@'%'", output);
    }

    /**
     * GRANT SELECT ON hive.*.* TO user (catalog-level wildcard)
     */
    public void test_catalog_wildcard() throws Exception {
        String sql = "GRANT SELECT ON hive.*.* TO 'testuser'@'%'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        Assert.assertEquals(1, statementList.size());

        SQLGrantStatement stmt = (SQLGrantStatement) statementList.get(0);
        SQLExprTableSource resource = (SQLExprTableSource) stmt.getResource();

        SQLPropertyExpr outerExpr = (SQLPropertyExpr) resource.getExpr();
        Assert.assertEquals("*", outerExpr.getName());

        Assert.assertTrue(outerExpr.getOwner() instanceof SQLPropertyExpr);
        SQLPropertyExpr middleExpr = (SQLPropertyExpr) outerExpr.getOwner();
        Assert.assertEquals("*", middleExpr.getName());
        Assert.assertEquals("hive", middleExpr.getOwnernName());

        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("GRANT SELECT ON hive.*.* TO 'testuser'@'%'", output);
    }

    /**
     * REVOKE SELECT ON hive.warehouse.* FROM user (REVOKE also uses three-segment)
     */
    public void test_revoke_catalog_db_wildcard() throws Exception {
        String sql = "REVOKE SELECT ON hive.warehouse.* FROM 'testuser'@'%'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        Assert.assertEquals(1, statementList.size());

        // Verify it parses without error and round-trips correctly
        String output = SQLUtils.toMySqlString(statementList.get(0));
        Assert.assertEquals("REVOKE SELECT ON hive.warehouse.* FROM 'testuser'@'%'", output);
    }

    /**
     * REVOKE ALL ON *.*.* FROM user
     */
    public void test_revoke_all_global_wildcard() throws Exception {
        String sql = "REVOKE ALL ON *.*.* FROM 'testuser'@'%'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        Assert.assertEquals(1, statementList.size());

        String output = SQLUtils.toMySqlString(statementList.get(0));
        Assert.assertEquals("REVOKE ALL ON *.*.* FROM 'testuser'@'%'", output);
    }

    /**
     * GRANT with WITH GRANT OPTION on three-segment
     */
    public void test_catalog_with_grant_option() throws Exception {
        String sql = "GRANT SELECT ON hive.warehouse.* TO 'testuser'@'%' WITH GRANT OPTION";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        Assert.assertEquals(1, statementList.size());

        SQLGrantStatement stmt = (SQLGrantStatement) statementList.get(0);
        Assert.assertTrue(stmt.getWithGrantOption());

        SQLExprTableSource resource = (SQLExprTableSource) stmt.getResource();
        SQLPropertyExpr outerExpr = (SQLPropertyExpr) resource.getExpr();
        Assert.assertEquals("*", outerExpr.getName());
        Assert.assertTrue(outerExpr.getOwner() instanceof SQLPropertyExpr);

        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("GRANT SELECT ON hive.warehouse.* TO 'testuser'@'%' WITH GRANT OPTION", output);
    }

    /**
     * Verify that the traditional two-segment GRANT still produces the
     * expected AST (regression check).
     * GRANT SELECT ON mydb.* TO user → owner is SQLIdentifierExpr, not nested SQLPropertyExpr
     */
    public void test_two_segment_regression() throws Exception {
        String sql = "GRANT SELECT ON mydb.* TO 'testuser'@'%'";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        Assert.assertEquals(1, statementList.size());

        SQLGrantStatement stmt = (SQLGrantStatement) statementList.get(0);
        SQLExprTableSource resource = (SQLExprTableSource) stmt.getResource();

        SQLPropertyExpr outerExpr = (SQLPropertyExpr) resource.getExpr();
        Assert.assertEquals("*", outerExpr.getName());

        // For two-segment, owner should be SQLIdentifierExpr (NOT SQLPropertyExpr)
        Assert.assertTrue("two-segment owner should be SQLIdentifierExpr",
            outerExpr.getOwner() instanceof SQLIdentifierExpr);
        Assert.assertEquals("mydb", ((SQLIdentifierExpr) outerExpr.getOwner()).getName());
    }
}
