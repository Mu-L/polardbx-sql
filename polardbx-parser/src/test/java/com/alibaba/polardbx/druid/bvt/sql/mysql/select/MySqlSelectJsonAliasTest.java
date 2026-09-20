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
package com.alibaba.polardbx.druid.bvt.sql.mysql.select;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import org.junit.Assert;

import java.util.List;

/**
 * Regression test for AONE-76682281: JSON path operator (->/->>)
 * with implicit quoted string alias was incorrectly concatenating
 * the alias literal into the JSON path.
 */
public class MySqlSelectJsonAliasTest extends MysqlTest {

    public void test_jsonExtractWithQuotedAlias() throws Exception {
        String sql = "SELECT json_data ->> '$.categories' 'm1' FROM t";
        List<SQLStatement> stmts = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement ss = (SQLSelectStatement) stmts.get(0);
        SQLSelectItem item = ss.getSelect().getQueryBlock().getSelectList().get(0);

        // as() wraps LITERAL_CHARS alias values in single quotes: 'm1' → "'m1'"
        Assert.assertNotNull("Alias should not be null - quoted string alias was consumed into JSON path",
            item.getAlias());
        // JSON path should NOT include the alias text
        Assert.assertFalse("JSON path should not contain alias text 'm1'",
            item.getExpr().toString().contains("m1"));
    }

    public void test_jsonArrowWithQuotedAlias() throws Exception {
        String sql = "SELECT json_data -> '$.key' 'alias1' FROM t";
        List<SQLStatement> stmts = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement ss = (SQLSelectStatement) stmts.get(0);
        SQLSelectItem item = ss.getSelect().getQueryBlock().getSelectList().get(0);

        Assert.assertNotNull("Alias should not be null for quoted alias with ->",
            item.getAlias());
        Assert.assertFalse("JSON path should not contain alias text",
            item.getExpr().toString().contains("alias1"));
    }

    public void test_jsonExtractWithAsAlias() throws Exception {
        // AS alias form should continue to work
        String sql = "SELECT json_data ->> '$.categories' AS m1 FROM t";
        List<SQLStatement> stmts = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement ss = (SQLSelectStatement) stmts.get(0);
        SQLSelectItem item = ss.getSelect().getQueryBlock().getSelectList().get(0);

        Assert.assertEquals("m1", item.getAlias());
    }

    public void test_jsonExtractWithUnquotedAlias() throws Exception {
        // Unquoted alias (identifier) should continue to work
        String sql = "SELECT json_data ->> '$.categories' m1 FROM t";
        List<SQLStatement> stmts = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement ss = (SQLSelectStatement) stmts.get(0);
        SQLSelectItem item = ss.getSelect().getQueryBlock().getSelectList().get(0);

        Assert.assertEquals("m1", item.getAlias());
    }
}
