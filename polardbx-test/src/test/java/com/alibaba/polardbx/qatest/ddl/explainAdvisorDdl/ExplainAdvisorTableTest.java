package com.alibaba.polardbx.qatest.ddl.explainAdvisorDdl;

import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.ddl.explainOnlineDdl.ExplainOnlineDDLBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExplainAdvisorTableTest extends ExplainOnlineDDLBaseTest {
    String tableName = "explain_advisor_table";
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

        if (!isMySQL80()) {
            JdbcUtil.executeSuccess(tddlConnection, "set FORCE_USING_OMC_30 = true");
        }
    }

    @After
    public void clean() {
        String dropTableSql = String.format("drop table if exists %s", tableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testModifyRowFormat() {
        String sql = String.format("explain advisor alter table %s ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4", tableName);
        String expectedSql = String.format("alter table %s ROW_FORMAT = COMPRESSED KEY_BLOCK_SIZE = 4", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testModifyOptions() {
        String sql = String.format("explain advisor alter table %s comment 'test table'", tableName);
        String expectedSql = String.format("alter table %s comment = 'test table'", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testOptimizeTable() {
        String sql = String.format("explain advisor optimize table %s", tableName);
        String expectedSql = String.format("optimize table %s", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testAnalyzeTable() {
        String sql = String.format("explain advisor analyze table %s", tableName);
        String expectedSql = String.format("analyze table %s", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testRenameTable() {
        String sql = String.format("explain advisor rename table %s to rename_test_adv", tableName);
        String expectedSql = String.format("rename table %s to rename_test_adv", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.META_ONLY);
    }

    @Test
    public void testDefaultCharset() {
        String sql = String.format("explain advisor alter table %s default character set utf8", tableName);
        String expectedSql = String.format("alter table %s character set = utf8", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @ReplicaIgnore(ignoreReason = "set session variables")
    @Test
    public void testConvertCharset() {
        String sql = String.format("explain advisor alter table %s convert to character set utf8", tableName);
        String expectedSql = String.format("alter table %s convert to character set utf8, algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);
    }

    @Test
    public void testTruncateTableWithGsi() {
        String sql = String.format("explain advisor truncate table %s", tableName);
        String expectedSql = String.format("truncate table %s", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.DEFAULT);
    }

    @Test
    public void testTruncateTableWithOutGsi() {
        String localTableName = "explain_truncate_without_gsi";
        String createTableSql =
            String.format("create table %s (a int, b varchar(10), c text, index %s(b)) partition by key(a)",
                localTableName, localIndexName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        String sql = String.format("explain advisor truncate table %s", localTableName);
        String expectedSql = String.format("truncate table %s", localTableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.DEFAULT);
        dropTable(localTableName);
    }
}
