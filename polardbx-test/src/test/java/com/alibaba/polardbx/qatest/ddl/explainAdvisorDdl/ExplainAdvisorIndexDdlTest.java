package com.alibaba.polardbx.qatest.ddl.explainAdvisorDdl;

import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.ddl.explainOnlineDdl.ExplainOnlineDDLBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExplainAdvisorIndexDdlTest extends ExplainOnlineDDLBaseTest {

    String tableName = "explain_advisor_index_ddl";
    String localIndexName = "idx_b";
    String globalIndexName = "gsi_b";

    @Before
    public void prepare() {
        String createTableSql = String.format(
            "create table %s ("
                + "a int, "
                + "b varchar(10), "
                + "c text, "
                + "index %s(b),"
                + "global index %s(b) partition by key(b)"
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
        String dropTableSql = String.format("drop table %s", tableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
    }

    @Test
    public void testCreateDropLocalIndex() {
        String sql = String.format("explain advisor create local index test_local on %s(a)", tableName);
        String expectedSql = String.format("create local index test_local on %s (a)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);

        sql = String.format("explain advisor drop index %s on %s", localIndexName, tableName);
        expectedSql = String.format("drop index %s on %s", localIndexName, tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testAlterAddDropLocalIndex() {
        String sql = String.format("explain advisor alter table %s add local index test_local(a)", tableName);
        String expectedSql = String.format("alter table %s add local index test_local (a)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);

        sql = String.format("explain advisor alter table %s drop index %s", tableName, localIndexName);
        expectedSql = String.format("alter table %s drop index %s", tableName, localIndexName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testRenameLocalIndex() {
        String sql =
            String.format("explain advisor alter table %s rename index %s to test_local", tableName, localIndexName);
        String expectedSql = String.format("alter table %s rename index %s to test_local", tableName, localIndexName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @ReplicaIgnore(ignoreReason = "set session variables")
    @Test
    public void testFullTextIndex() {
        String sql = String.format("explain advisor alter table %s add fulltext index test_local(c)", tableName);
        String expectedSql =
            String.format("alter table %s add fulltext index test_local (c), algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);
    }

    @Test
    public void testCreateDropGlobalIndex() {
        String sql =
            String.format("explain advisor create global index test_gsi on %s(a) partition by key(a)", tableName);
        String expectedSql = String.format("create global index test_gsi on %s (a) partition by key (a)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OSC);

        sql = String.format("explain advisor drop index %s on %s", globalIndexName, tableName);
        expectedSql = String.format("drop index %s on %s", globalIndexName, tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OSC);
    }

    @Test
    public void testAlterAddDropGlobalIndex() {
        String sql =
            String.format("explain advisor alter table %s add global index test_gsi(a) partition by key(a)", tableName);
        String expectedSql =
            String.format("alter table %s add global index test_gsi (a) partition by key (a)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OSC);

        sql = String.format("explain advisor alter table %s drop index %s", tableName, globalIndexName);
        expectedSql = String.format("alter table %s drop index %s", tableName, globalIndexName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OSC);
    }

    @Test
    public void testAlterRenameGlobalIndex() {
        String sql =
            String.format("explain advisor alter table %s rename index %s to test_gsi", tableName, globalIndexName);
        String expectedSql =
            String.format("alter table %s rename index %s to test_gsi", tableName, globalIndexName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.META_ONLY);
    }

    @Test
    public void testCreateDropIndexWithAutoPartition() {
        String tableName1 = "explain_auto_partition";
        String indexName1 = "idx_b1";
        String createTableSql =
            String.format("create table %s (a int, b varchar(10), c text, index %s(b))", tableName1, indexName1);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        String sql = String.format("explain advisor create index test_idx on %s(a)", tableName1);
        String expectedSql = String.format("create index test_idx on %s (a)", tableName1);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OSC);

        sql = String.format("explain advisor drop index %s on %s", indexName1, tableName1);
        expectedSql = String.format("drop index %s on %s", indexName1, tableName1);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OSC);
        dropTable(tableName1);
    }

    @Test
    public void testAlterAddDropIndexWithAutoPartition() {
        String tableName1 = "explain_auto_partition2";
        String indexName1 = "idx_b2";
        String createTableSql =
            String.format("create table %s (a int, b varchar(10), c text, index %s(b))", tableName1, indexName1);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        String sql = String.format("explain advisor alter table %s add index test_idx(a)", tableName1);
        String expectedSql = String.format("alter table %s add index test_idx (a)", tableName1);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OSC);

        sql = String.format("explain advisor alter table %s drop index %s", tableName1, indexName1);
        expectedSql = String.format("alter table %s drop index %s", tableName1, indexName1);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OSC);
        dropTable(tableName1);
    }

    @Test
    public void testAlterRenameIndexWithAutoPartition() {
        String tableName1 = "explain_auto_partition3";
        String indexName1 = "idx_b3";
        String createTableSql =
            String.format("create table %s (a int, b varchar(10), c text, index %s(b))", tableName1, indexName1);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        String sql = String.format("explain advisor alter table %s rename index %s to xxxx", tableName1, indexName1);
        String expectedSql = String.format("alter table %s rename index %s to xxxx", tableName1, indexName1);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.META_ONLY);
        dropTable(tableName1);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
