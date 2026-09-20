package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

/**
 * Tests for EXPLAIN DDL_DAG syntax parsing
 */
public class MysqlExplainTest_ddl_dag extends MysqlTest {

    public void test_explain_ddl_dag_alter_table() throws Exception {
        String sql = "explain ddl_dag alter table t1 add column c1 int";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertTrue(stmt instanceof MySqlExplainStatement);
        assertTrue(((MySqlExplainStatement) stmt).getType().equalsIgnoreCase("DDL_DAG"));
    }

    public void test_explain_ddl_dag_create_table() throws Exception {
        String sql = "explain ddl_dag create table t1 (id int primary key, name varchar(20))";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertTrue(stmt instanceof MySqlExplainStatement);
        assertTrue(((MySqlExplainStatement) stmt).getType().equalsIgnoreCase("DDL_DAG"));
    }

    public void test_explain_ddl_dag_drop_table() throws Exception {
        String sql = "explain ddl_dag drop table t1";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertTrue(stmt instanceof MySqlExplainStatement);
        assertTrue(((MySqlExplainStatement) stmt).getType().equalsIgnoreCase("DDL_DAG"));
    }

    public void test_explain_ddl_dag_create_index() throws Exception {
        String sql = "explain ddl_dag create index idx_name on t1(name)";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertTrue(stmt instanceof MySqlExplainStatement);
        assertTrue(((MySqlExplainStatement) stmt).getType().equalsIgnoreCase("DDL_DAG"));
    }

    public void test_explain_ddl_dag_case_insensitive() throws Exception {
        String sql = "EXPLAIN DDL_DAG ALTER TABLE t1 DROP COLUMN c1";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertTrue(stmt instanceof MySqlExplainStatement);
        assertTrue(((MySqlExplainStatement) stmt).getType().equalsIgnoreCase("DDL_DAG"));
    }
}
