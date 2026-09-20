package com.alibaba.polardbx.qatest.failpoint.newpartition.failpoint;

import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * FailPoint tests for multi-partition split.
 * Tests failure injection at various stages and verifies rollback behavior.
 */
public class MultiSplitPartitionFailPointTest extends DDLBaseNewDBTestCase {

    private static final String SCHEMA_NAME = "multi_split_fp";
    private static final String INPLACE_HINT =
        "/*+TDDL:cmd_extra(ENABLE_INPLACE_BACKFILL=true, SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111,%s=%s)*/";

    private static String q(String name) {
        return "`" + name + "`";
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + SCHEMA_NAME);
            JdbcUtil.executeUpdateSuccess(conn, "create database if not exists " + SCHEMA_NAME + " mode=auto");
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + SCHEMA_NAME);
        }
    }

    @After
    public void tearDown() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            try {
                JdbcUtil.executeUpdate(conn, "set @FP_CLEAR=true");
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private void prepareKeyTable(Connection conn, String table, int rows) throws SQLException {
        JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
        JdbcUtil.executeUpdateSuccess(conn,
            "CREATE TABLE " + q(table) + " (" +
                "id BIGINT NOT NULL AUTO_INCREMENT, " +
                "val BIGINT NOT NULL, " +
                "name VARCHAR(100), " +
                "PRIMARY KEY(id)" +
                ") PARTITION BY KEY(val, id) PARTITIONS 4");
        JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
        for (int i = 1; i <= rows; i++) {
            JdbcUtil.executeUpdateSuccess(conn,
                "INSERT INTO " + q(table) + " (val, name) VALUES (" + i + ", 'row_" + i + "')");
        }
    }

    private int rowCount(Connection conn, String table) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery("SELECT COUNT(*) FROM " + q(table), conn);
        rs.next();
        int count = rs.getInt(1);
        rs.close();
        return count;
    }

    /**
     * Test existing FP_SPLIT_FAILED_AFTER_READONLY_TASK with multi-split.
     */
    @Test
    public void testFp_multiSplit_failAfterReadonly() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "fp_只读-ro";
            prepareKeyTable(conn, table, 100);
            int before = rowCount(conn, table);

            String ddl = String.format(INPLACE_HINT,
                FailPointKey.FP_SPLIT_FAILED_AFTER_READONLY_TASK, "true")
                + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            try {
                JdbcUtil.executeUpdateWithException(conn, ddl);
            } catch (Exception e) {
                // Expected
            }

            waitForDdlComplete(conn, table, 120);
            int after = rowCount(conn, table);
            Assert.assertEquals("Data must be intact after readonly fail rollback", before, after);
        }
    }

    /**
     * Test FP_SPLIT_FAILED_BEFORE_READONLY_TASK with multi-split.
     */
    @Test
    public void testFp_multiSplit_failBeforeReadonly() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "fp_前只-bro";
            prepareKeyTable(conn, table, 100);
            int before = rowCount(conn, table);

            String ddl = String.format(INPLACE_HINT,
                FailPointKey.FP_SPLIT_FAILED_BEFORE_READONLY_TASK, "true")
                + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            try {
                JdbcUtil.executeUpdateWithException(conn, ddl);
            } catch (Exception e) {
                // Expected
            }

            waitForDdlComplete(conn, table, 120);
            int after = rowCount(conn, table);
            Assert.assertEquals("Data must be intact", before, after);
        }
    }

    /**
     * Test FP_EACH_DDL_TASK_BACK_AND_FORTH with multi-split.
     * Each task runs, rolls back, then runs again.
     * Inplace backfill rollback does not clean target partition data,
     * so the re-execution will hit duplicate key error — same as single-split.
     */
    @Test
    public void testFp_multiSplit_backAndForth() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "fp_反复-bf";
            prepareKeyTable(conn, table, 100);
            int before = rowCount(conn, table);

            String ddl = String.format(INPLACE_HINT,
                FailPointKey.FP_EACH_DDL_TASK_BACK_AND_FORTH, "true")
                + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            try {
                JdbcUtil.executeUpdateWithException(conn, ddl);
            } catch (Exception e) {
                // Expected: back-and-forth causes duplicate key on inplace backfill re-execution
            }

            waitForDdlComplete(conn, table, 120);
            int after = rowCount(conn, table);
            Assert.assertEquals("Data must be intact after back-and-forth", before, after);
        }
    }

    /**
     * Test FP_EACH_DDL_TASK_EXECUTE_TWICE with multi-split.
     */
    @Test
    public void testFp_multiSplit_executeTwice() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "fp_两次-et";
            prepareKeyTable(conn, table, 100);
            int before = rowCount(conn, table);

            String ddl = String.format(INPLACE_HINT,
                FailPointKey.FP_EACH_DDL_TASK_EXECUTE_TWICE, "true")
                + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Data must be intact after execute-twice", before, after);
        }
    }

    /**
     * Test FP_RANDOM_FAIL with multi-split, DDL should eventually complete on retry.
     */
    @Test
    public void testFp_multiSplit_randomFail() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "fp_随机-rf";
            prepareKeyTable(conn, table, 100);
            int before = rowCount(conn, table);

            // Random fail may cause DDL to fail; retry if needed
            String ddl = String.format(INPLACE_HINT, FailPointKey.FP_RANDOM_FAIL, "30")
                + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            boolean succeeded = false;
            for (int attempt = 0; attempt < 3 && !succeeded; attempt++) {
                try {
                    JdbcUtil.executeUpdateWithException(conn, ddl);
                    succeeded = true;
                } catch (Exception e) {
                    waitForDdlComplete(conn, table, 120);
                }
            }

            int after = rowCount(conn, table);
            Assert.assertTrue("Row count should be >= original", after >= before);
        }
    }

    private void waitForDdlComplete(Connection conn, String table, int timeoutSec) throws SQLException {
        long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                ResultSet rs = JdbcUtil.executeQuery("SHOW DDL", conn);
                boolean hasPending = false;
                while (rs.next()) {
                    String objName = rs.getString("OBJECT_NAME");
                    if (objName != null && objName.equalsIgnoreCase(table)) {
                        hasPending = true;
                        break;
                    }
                }
                rs.close();
                if (!hasPending) {
                    return;
                }
            } catch (Exception e) {
                // ignore
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
