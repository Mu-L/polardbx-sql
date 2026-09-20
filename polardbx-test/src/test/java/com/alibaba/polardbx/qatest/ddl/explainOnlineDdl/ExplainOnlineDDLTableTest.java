package com.alibaba.polardbx.qatest.ddl.explainOnlineDdl;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExplainOnlineDDLTableTest extends ExplainOnlineDDLBaseTest {
    String tableName = "explain_online_ddl_table";
    String localIndexName = "idx_b";
    String globalIndexName = "gsi_b";

    @Before
    public void prepare() {
        String createTableSql = String.format(
            "create table %s ("
                + "a int, "
                + "b varchar(10), "
                + "c text, "
                + "index %s(b), "
                + "global index %s(a,b) partition by key(a)"
                + ") partition by key(a)",
            tableName, localIndexName, globalIndexName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        setSupportInstant(tddlConnection);
    }

    @After
    public void clean() {
        String dropTableSql = String.format("drop table %s", tableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testModifyRowFormat() {
        String sql =
            String.format("explain online_ddl alter table %s ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testModifyOptions() {
        String sql = String.format("explain online_ddl alter table %s comment \"test table\"", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testOptimizeTable() {
        String sql = String.format("explain online_ddl optimize table %s", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testAnalyzeTable() {
        String sql = String.format("explain online_ddl analyze table %s", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testRenameTable() {
        String sql = String.format("explain online_ddl rename table %s to rename_test_o", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
    }

    @Test
    public void testDefaultCharset() {
        String sql = String.format("explain online_ddl alter table %s default character set utf8", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testConvertCharset() {
        String sql = String.format("explain online_ddl alter table %s convert to character set utf8", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testTruncateTableWithGsi() {
        String sql = String.format("explain online_ddl truncate table %s", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.DEFAULT);
    }

    @Test
    public void testCreateTable() {
        String sql = "explain online_ddl create table xxxttt1(a int)";
        assertOnlineDdlResult(tddlConnection, sql, DdlType.NONE, DdlAlgorithm.DEFAULT);
    }

    @Test
    public void testTruncateTableWithOutGsi() {
        String localTableName = "explain_truncate_without_gsi";
        String createTableSql =
            String.format("create table %s (a int, b varchar(10), c text, index %s(b)) partition by key(a)",
                localTableName, localIndexName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        String sql = String.format("explain online_ddl truncate table %s", localTableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.DEFAULT);
        dropTable(localTableName);
    }
}
