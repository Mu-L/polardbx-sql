package com.alibaba.polardbx.qatest.ddl.explainDdlDag;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Integration tests for EXPLAIN DDL_DAG syntax.
 * Verifies that EXPLAIN DDL_DAG returns JOB_ID and DAG (Graphviz DOT format)
 * for various DDL statements without actually executing them.
 */
public class ExplainDdlDagTest extends DDLBaseNewDBTestCase {

    private static final String TABLE_NAME = "explain_ddl_dag_test";
    private static final String TABLE_RANGE = "explain_ddl_dag_range";
    private static final String TABLE_INDEX = "explain_ddl_dag_idx";

    @Before
    public void prepare() {
        // Simple partitioned table
        JdbcUtil.executeSuccess(tddlConnection, String.format(
            "create table if not exists %s (a int, b varchar(10), c int) partition by key(a) partitions 3",
            TABLE_NAME));

        // Range partitioned table
        JdbcUtil.executeSuccess(tddlConnection, String.format(
            "create table if not exists %s (a int, b varchar(10), c int) "
                + "PARTITION BY RANGE(a) ("
                + "PARTITION p1 VALUES LESS THAN(20),"
                + "PARTITION p2 VALUES LESS THAN(100),"
                + "PARTITION p3 VALUES LESS THAN(200))", TABLE_RANGE));

        // Table for index tests
        JdbcUtil.executeSuccess(tddlConnection, String.format(
            "create table if not exists %s (a int, b varchar(10), c int, index idx_b(b)) partition by key(a) partitions 5",
            TABLE_INDEX));
    }

    @After
    public void clean() {
        JdbcUtil.executeSuccess(tddlConnection, String.format("drop table if exists %s", TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format("drop table if exists %s", TABLE_RANGE));
        JdbcUtil.executeSuccess(tddlConnection, String.format("drop table if exists %s", TABLE_INDEX));
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    // ===== Result structure validation =====

    @Test
    public void testResultColumns() {
        String sql = String.format("explain ddl_dag alter table %s add column f int", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            ResultSetMetaData meta = rs.getMetaData();
            Assert.assertEquals("Should have 2 columns", 2, meta.getColumnCount());
            Assert.assertEquals("JOB_ID", meta.getColumnName(1).toUpperCase());
            Assert.assertEquals("DAG", meta.getColumnName(2).toUpperCase());
        } catch (SQLException e) {
            Assert.fail("EXPLAIN DDL_DAG failed: " + e.getMessage());
        }
    }

    // ===== ALTER TABLE tests =====

    @Test
    public void testAlterTableAddColumn() {
        String sql = String.format("explain ddl_dag alter table %s add column f varchar(20)", TABLE_NAME);
        assertDdlDagResult(tddlConnection, sql);
    }

    @Test
    public void testAlterTableDropColumn() {
        String sql = String.format("explain ddl_dag alter table %s drop column c", TABLE_NAME);
        assertDdlDagResult(tddlConnection, sql);
    }

    @Test
    public void testAlterTableModifyColumn() {
        String sql = String.format("explain ddl_dag alter table %s modify column b varchar(20)", TABLE_NAME);
        assertDdlDagResult(tddlConnection, sql);
    }

    @Test
    public void testAlterTableAddIndex() {
        String sql = String.format("explain ddl_dag alter table %s add index idx_c(c)", TABLE_NAME);
        assertDdlDagResult(tddlConnection, sql);
    }

    // ===== Partition DDL tests =====

    @Test
    public void testSplitPartition() {
        String sql = String.format(
            "explain ddl_dag alter table %s split partition p1 into "
                + "(PARTITION p4 VALUES LESS THAN(10), PARTITION p5 VALUES LESS THAN(20))",
            TABLE_RANGE);
        assertDdlDagResult(tddlConnection, sql);
    }

    @Test
    public void testMergePartition() {
        String sql = String.format("explain ddl_dag alter table %s merge partitions p2,p3 to p4", TABLE_RANGE);
        assertDdlDagResult(tddlConnection, sql);
    }

    @Test
    public void testAddPartition() {
        String sql = String.format(
            "explain ddl_dag alter table %s add partition (PARTITION p4 VALUES LESS THAN(400))", TABLE_RANGE);
        assertDdlDagResult(tddlConnection, sql);
    }

    @Test
    public void testDropPartition() {
        String sql = String.format("explain ddl_dag alter table %s drop partition p2", TABLE_RANGE);
        assertDdlDagResult(tddlConnection, sql);
    }

    // ===== CREATE / DROP tests =====

    @Test
    public void testCreateTable() {
        String newTable = TABLE_NAME + "_new";
        JdbcUtil.executeSuccess(tddlConnection, String.format("drop table if exists %s", newTable));
        try {
            String sql = String.format(
                "explain ddl_dag create table %s (id int, name varchar(20)) partition by key(id)", newTable);
            assertDdlDagResult(tddlConnection, sql);
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, String.format("drop table if exists %s", newTable));
        }
    }

    @Test
    public void testCreateIndex() {
        String sql = String.format("explain ddl_dag create index idx_c on %s(c)", TABLE_INDEX);
        assertDdlDagResult(tddlConnection, sql);
    }

    @Test
    public void testDropIndex() {
        String sql = String.format("explain ddl_dag drop index idx_b on %s", TABLE_INDEX);
        assertDdlDagResult(tddlConnection, sql);
    }

    // ===== Case insensitivity =====

    @Test
    public void testCaseInsensitive() {
        String sql = String.format("EXPLAIN DDL_DAG ALTER TABLE %s ADD COLUMN g INT", TABLE_NAME);
        assertDdlDagResult(tddlConnection, sql);

        sql = String.format("explain Ddl_Dag alter table %s add column h int", TABLE_NAME);
        assertDdlDagResult(tddlConnection, sql);
    }

    // ===== DAG content validation =====

    @Test
    public void testDagContainsTaskNodes() {
        String sql = String.format("explain ddl_dag alter table %s add column f int", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue("Should have at least one row", rs.next());
            String dag = rs.getString("DAG");

            // DAG should contain task nodes with shape=record
            Assert.assertTrue("DAG should contain task nodes", dag.contains("shape=record"));
            // DAG should contain taskId labels
            Assert.assertTrue("DAG should contain taskId labels", dag.contains("taskId:"));
            // DAG should contain state info
            Assert.assertTrue("DAG should contain state info", dag.contains("state:"));
        } catch (SQLException e) {
            Assert.fail("EXPLAIN DDL_DAG failed: " + e.getMessage());
        }
    }

    @Test
    public void testDagContainsEdges() {
        // Partition split should produce a complex DAG with edges
        String sql = String.format(
            "explain ddl_dag alter tablegroup by table %s split partition p1 into "
                + "(PARTITION p4 VALUES LESS THAN(10), PARTITION p5 VALUES LESS THAN(20))",
            TABLE_RANGE);
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue("Should have at least one row", rs.next());
            String dag = rs.getString("DAG");

            // Complex DDLs should produce DAGs with edges (->)
            Assert.assertTrue("DAG should contain edges", dag.contains("->"));
        } catch (SQLException e) {
            Assert.fail("EXPLAIN DDL_DAG failed: " + e.getMessage());
        }
    }

    @Test
    public void testDdlNotActuallyExecuted() {
        // After EXPLAIN DDL_DAG add column, the column should NOT exist
        String sql = String.format("explain ddl_dag alter table %s add column z_not_exist int", TABLE_NAME);
        assertDdlDagResult(tddlConnection, sql);

        // Verify the column was NOT added
        try (ResultSet rs = JdbcUtil.executeQuery(
            String.format("show columns from %s like 'z_not_exist'", TABLE_NAME), tddlConnection)) {
            Assert.assertFalse("Column should NOT exist after EXPLAIN DDL_DAG", rs.next());
        } catch (SQLException e) {
            Assert.fail("Verify column failed: " + e.getMessage());
        }
    }

    // ===== Helper method =====

    /**
     * Execute EXPLAIN DDL_DAG and verify:
     * 1. ResultSet has one row
     * 2. JOB_ID is a positive number
     * 3. DAG is a valid Graphviz DOT format string
     */
    private void assertDdlDagResult(Connection connection, String sql) {
        try (ResultSet rs = JdbcUtil.executeQuery(sql, connection)) {
            Assert.assertTrue("EXPLAIN DDL_DAG should return at least one row", rs.next());

            long jobId = rs.getLong("JOB_ID");
            String dag = rs.getString("DAG");

            Assert.assertTrue("JOB_ID should be positive, got: " + jobId, jobId > 0);
            Assert.assertNotNull("DAG should not be null", dag);
            Assert.assertTrue("DAG should start with 'digraph G {'", dag.startsWith("digraph G {"));
            Assert.assertTrue("DAG should end with '}'", dag.trim().endsWith("}"));
            Assert.assertTrue("DAG should contain at least one task node",
                dag.contains("[shape=record"));
        } catch (SQLException e) {
            Assert.fail("EXPLAIN DDL_DAG failed for SQL: " + sql + "\n" + e.getMessage());
        }
    }
}
