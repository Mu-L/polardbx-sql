package com.alibaba.polardbx.qatest.ddl.explainOnlineDdl;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExplainOnlineDDLIndexTest extends ExplainOnlineDDLBaseTest {

    String tableName = "explain_online_ddl_index";
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
    public void testCreateDropLocalIndex() {
        String sql = String.format("explain online_ddl create local index test_local on %s(a)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);

        sql = String.format("explain online_ddl drop index %s on %s", localIndexName, tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testAlterAddDropLocalIndex() {
        String sql = String.format("explain online_ddl alter table %s add local index test_local(a)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);

        sql = String.format("explain online_ddl alter table %s drop index %s", tableName, localIndexName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testRenameLocalIndex() {
        String sql =
            String.format("explain online_ddl alter table %s rename index %s to test_local", tableName, localIndexName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testFullTextIndex() {
        String sql = String.format("explain online_ddl alter table %s add fulltext index test_local(c)", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.COPY : DdlAlgorithm.INPLACE;
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, expectAlgorithm);
    }

    @Test
    public void testCreateDropGlobalIndex() {
        String sql =
            String.format("explain online_ddl create global index test_gsi on %s(a) partition by key(a)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);

        sql = String.format("explain online_ddl drop index %s on %s", globalIndexName, tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testAlterAddDropGlobalIndex() {
        String sql = String.format("explain online_ddl alter table %s add global index test_gsi(a) partition by key(a)",
            tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);

        sql = String.format("explain online_ddl alter table %s drop index %s", tableName, globalIndexName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testAlterRenameGlobalIndex() {
        String sql =
            String.format("explain online_ddl alter table %s rename index %s to test_gsi", tableName, globalIndexName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
    }

    @Test
    public void testCreateDropIndexWithAutoPartition() {
        String tableName1 = "explain_auto_partition";
        String indexName1 = "idx_b1";
        String createTableSql =
            String.format("create table %s (a int, b varchar(10), c text, index %s(b))", tableName1, indexName1);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        String sql = String.format("explain online_ddl create index test_idx on %s(a)", tableName1);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);

        sql = String.format("explain online_ddl drop index %s on %s", indexName1, tableName1);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
        dropTable(tableName1);
    }

    @Test
    public void testAlterAddDropIndexWithAutoPartition() {
        String tableName1 = "explain_auto_partition2";
        String indexName1 = "idx_b2";
        String createTableSql =
            String.format("create table %s (a int, b varchar(10), c text, index %s(b))", tableName1, indexName1);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        String sql = String.format("explain online_ddl alter table %s add index test_idx(a)", tableName1);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);

        sql = String.format("explain online_ddl alter table %s drop index %s", tableName1, indexName1);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
        dropTable(tableName1);
    }

    @Test
    public void testAlterRenameIndexWithAutoPartition() {
        String tableName1 = "explain_auto_partition3";
        String indexName1 = "idx_b3";
        String createTableSql =
            String.format("create table %s (a int, b varchar(10), c text, index %s(b))", tableName1, indexName1);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        String sql = String.format("explain online_ddl alter table %s rename index %s to xxxx", tableName1, indexName1);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
        dropTable(tableName1);
    }
}
