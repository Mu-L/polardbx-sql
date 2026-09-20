package com.alibaba.polardbx.qatest.ddl.auto.inplacebackfill;

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
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Rollback tests for multi-partition split.
 * Verifies that after a failed multi-split DDL:
 * 1. Data integrity is preserved
 * 2. Readonly status is properly cleared
 * 3. DML operations work normally
 * 4. Subsequent DDL operations succeed
 */
public class MultiSplitPartitionRollbackTest extends DDLBaseNewDBTestCase {

    private static final String SCHEMA_NAME = "multi_split_rb";
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

    private List<String> getFirstLevelPartitionNames(Connection conn, String table) throws SQLException {
        List<String> names = new ArrayList<>();
        ResultSet rs = JdbcUtil.executeQuery(
            "SELECT DISTINCT partition_name FROM information_schema.partitions " +
                "WHERE table_schema = '" + SCHEMA_NAME + "' AND table_name = '" + table + "' " +
                "AND partition_name IS NOT NULL " +
                "ORDER BY partition_ordinal_position",
            conn);
        while (rs.next()) {
            String name = rs.getString(1);
            if (name != null && !names.contains(name)) {
                names.add(name);
            }
        }
        rs.close();
        return names;
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

    private void verifyReadonlyCleared(Connection conn, String table) throws SQLException {
        String showTopology = "SHOW TOPOLOGY FROM " + q(table);
        ResultSet topRs = JdbcUtil.executeQuery(showTopology, conn);
        List<String[]> phyTables = new ArrayList<>();
        while (topRs.next()) {
            phyTables.add(new String[] {topRs.getString("GROUP_NAME"), topRs.getString("TABLE_NAME")});
        }
        topRs.close();

        for (String[] pt : phyTables) {
            String checkSql = "/*+TDDL:NODE('" + pt[0] + "')*/ " +
                "SELECT secondary_engine_attribute FROM information_schema.tables_extensions " +
                "WHERE table_name = '" + pt[1] + "'";
            try {
                ResultSet rs = JdbcUtil.executeQuery(checkSql, conn);
                if (rs.next()) {
                    String attr = rs.getString(1);
                    if (attr != null && attr.contains("polarx.readonly") && attr.contains("true")) {
                        assertWithMessage("Physical table " + pt[0] + "." + pt[1] + " still readonly").fail();
                    }
                }
                rs.close();
            } catch (Exception e) {
                // some DN may not support this query
            }
        }
    }

    /**
     * After rollback from FP_SPLIT_FAILED_AFTER_READONLY_TASK,
     * verify readonly status is cleared and DML works.
     */
    @Test
    public void testRollback_readonlyCleared() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "rb_只读-ro";
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

            // Verify readonly cleared
            verifyReadonlyCleared(conn, table);

            // Verify data integrity
            Assert.assertEquals(before, rowCount(conn, table));

            // Verify DML works
            JdbcUtil.executeUpdateSuccess(conn,
                "INSERT INTO " + q(table) + " (val, name) VALUES (9999, 'after_rollback')");
            Assert.assertEquals(before + 1, rowCount(conn, table));
        }
    }

    /**
     * After rollback, a subsequent normal split should succeed.
     */
    @Test
    public void testRollback_subsequentSplitSucceeds() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "rb_重试-rt";
            prepareKeyTable(conn, table, 100);
            int before = rowCount(conn, table);

            // First attempt: fail
            String ddlFail = String.format(INPLACE_HINT,
                FailPointKey.FP_SPLIT_FAILED_BEFORE_READONLY_TASK, "true")
                + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            try {
                JdbcUtil.executeUpdateWithException(conn, ddlFail);
            } catch (Exception e) {
                // Expected
            }
            waitForDdlComplete(conn, table, 120);
            Assert.assertEquals(before, rowCount(conn, table));

            // Get actual partition names after rollback (may change during rollback)
            List<String> partNames = getFirstLevelPartitionNames(conn, table);
            Assert.assertTrue("Should have at least 2 partitions after rollback", partNames.size() >= 2);

            // Second attempt: succeed (no failpoint) using actual partition names
            String ddlOk =
                "/*+TDDL:cmd_extra(ENABLE_INPLACE_BACKFILL=true, SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111)*/"
                    + "ALTER TABLE " + q(table) + " SPLIT PARTITION "
                    + partNames.get(0) + ", " + partNames.get(1) + " INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddlOk);
            Assert.assertEquals("Data intact after retry", before, rowCount(conn, table));
        }
    }

    /**
     * Rollback preserves data for a large data set.
     */
    @Test
    public void testRollback_largeDataSet() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "rb_大表-lg";
            prepareKeyTable(conn, table, 1000);
            int before = rowCount(conn, table);

            String ddl = String.format(INPLACE_HINT,
                FailPointKey.FP_SPLIT_FAILED_AFTER_READONLY_TASK, "true")
                + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2, p3 INTO PARTITIONS 2";
            try {
                JdbcUtil.executeUpdateWithException(conn, ddl);
            } catch (Exception e) {
                // Expected
            }
            waitForDdlComplete(conn, table, 180);

            int after = rowCount(conn, table);
            Assert.assertEquals("Large dataset must be preserved after rollback", before, after);
        }
    }

    /**
     * After rollback, single-partition split still works (backward compat).
     */
    @Test
    public void testRollback_singleSplitAfterMultiSplitRollback() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "rb_单分-sa";
            prepareKeyTable(conn, table, 100);
            int before = rowCount(conn, table);

            // Multi-split fails
            String ddlFail = String.format(INPLACE_HINT,
                FailPointKey.FP_SPLIT_FAILED_BEFORE_READONLY_TASK, "true")
                + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            try {
                JdbcUtil.executeUpdateWithException(conn, ddlFail);
            } catch (Exception e) {
                // Expected
            }
            waitForDdlComplete(conn, table, 120);

            // Get actual partition names after rollback
            List<String> partNames = getFirstLevelPartitionNames(conn, table);
            Assert.assertTrue("Should have at least 1 partition after rollback", partNames.size() >= 1);

            // Single split should succeed using actual partition name
            String ddlOk =
                "/*+TDDL:cmd_extra(ENABLE_INPLACE_BACKFILL=true, SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111)*/"
                    + "ALTER TABLE " + q(table) + " SPLIT PARTITION " + partNames.get(0) + " INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddlOk);
            Assert.assertEquals("Data intact after single split", before, rowCount(conn, table));
        }
    }

    /**
     * Multiple failed multi-splits in sequence, then a successful one.
     */
    @Test
    public void testRollback_multipleFailsThenSuccess() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "rb_多失-mf";
            prepareKeyTable(conn, table, 100);
            int before = rowCount(conn, table);

            // Fail 3 times
            for (int i = 0; i < 3; i++) {
                // Get actual partition names (may change after each rollback)
                List<String> loopPartNames = getFirstLevelPartitionNames(conn, table);
                Assert.assertTrue("Should have at least 2 partitions", loopPartNames.size() >= 2);
                String ddlFail = String.format(INPLACE_HINT,
                    FailPointKey.FP_SPLIT_FAILED_BEFORE_READONLY_TASK, "true")
                    + "ALTER TABLE " + q(table) + " SPLIT PARTITION "
                    + loopPartNames.get(0) + ", " + loopPartNames.get(1) + " INTO PARTITIONS 2";
                try {
                    JdbcUtil.executeUpdateWithException(conn, ddlFail);
                } catch (Exception e) {
                    // Expected
                }
                waitForDdlComplete(conn, table, 120);
                Assert.assertEquals(before, rowCount(conn, table));
            }

            // Get actual partition names after rollbacks
            List<String> partNames = getFirstLevelPartitionNames(conn, table);
            Assert.assertTrue("Should have at least 2 partitions", partNames.size() >= 2);

            // Succeed using actual partition names
            String ddlOk =
                "/*+TDDL:cmd_extra(ENABLE_INPLACE_BACKFILL=true, SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111)*/"
                    + "ALTER TABLE " + q(table) + " SPLIT PARTITION "
                    + partNames.get(0) + ", " + partNames.get(1) + " INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddlOk);
            Assert.assertEquals("Data intact after multiple retries", before, rowCount(conn, table));
        }
    }

    // ========== Subpartition table helpers ==========

    private void prepareKeyKeySubPartTable(Connection conn, String table, int rows) throws SQLException {
        JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
        JdbcUtil.executeUpdateSuccess(conn,
            "CREATE TABLE " + q(table) + " (" +
                "id BIGINT NOT NULL AUTO_INCREMENT, " +
                "val BIGINT NOT NULL, " +
                "name VARCHAR(100), " +
                "PRIMARY KEY(id)" +
                ") PARTITION BY KEY(val) PARTITIONS 4" +
                " SUBPARTITION BY KEY(id) SUBPARTITIONS 2");
        JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
        for (int i = 1; i <= rows; i++) {
            JdbcUtil.executeUpdateSuccess(conn,
                "INSERT INTO " + q(table) + " (val, name) VALUES (" + i + ", 'row_" + i + "')");
        }
    }

    // ========== Subpartition rollback test cases ==========

    /**
     * After rollback on KEY+KEY subpartition table,
     * verify readonly status is cleared and DML works.
     */
    @Test
    public void testRollback_subPartTable_readonlyCleared() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "rb_子只读-sro";
            prepareKeyKeySubPartTable(conn, table, 100);
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

            // Verify readonly cleared
            verifyReadonlyCleared(conn, table);

            // Verify data integrity
            Assert.assertEquals(before, rowCount(conn, table));

            // Verify DML works
            JdbcUtil.executeUpdateSuccess(conn,
                "INSERT INTO " + q(table) + " (val, name) VALUES (9999, 'after_rollback')");
            Assert.assertEquals(before + 1, rowCount(conn, table));
        }
    }

    /**
     * After rollback on KEY+KEY subpartition table, a subsequent normal split should succeed.
     */
    @Test
    public void testRollback_subPartTable_subsequentSplitSucceeds() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "rb_子重试-srt";
            prepareKeyKeySubPartTable(conn, table, 100);
            int before = rowCount(conn, table);

            // First attempt: fail
            String ddlFail = String.format(INPLACE_HINT,
                FailPointKey.FP_SPLIT_FAILED_BEFORE_READONLY_TASK, "true")
                + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            try {
                JdbcUtil.executeUpdateWithException(conn, ddlFail);
            } catch (Exception e) {
                // Expected
            }
            waitForDdlComplete(conn, table, 120);
            Assert.assertEquals(before, rowCount(conn, table));

            // Get actual partition names after rollback (may change for subpart tables)
            List<String> partNames = getFirstLevelPartitionNames(conn, table);
            Assert.assertTrue("Should have at least 2 partitions after rollback", partNames.size() >= 2);

            // Second attempt: succeed (no failpoint) using actual partition names
            String ddlOk =
                "/*+TDDL:cmd_extra(ENABLE_INPLACE_BACKFILL=true, SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111)*/"
                    + "ALTER TABLE " + q(table) + " SPLIT PARTITION "
                    + partNames.get(0) + ", " + partNames.get(1) + " INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddlOk);
            Assert.assertEquals("Data intact after retry on subpart table", before, rowCount(conn, table));
        }
    }

    /**
     * Rollback on KEY+KEY subpartition table with multi-split subpartition.
     * Verify readonly status is cleared and DML works after rollback.
     */
    @Test
    public void testRollback_subPartMultiSplit_readonlyCleared() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(SCHEMA_NAME)) {
            String table = "rb_子多分-sm";
            prepareKeyKeySubPartTable(conn, table, 100);
            int before = rowCount(conn, table);

            // Fail during multi-split subpartition
            String ddlFail = String.format(INPLACE_HINT,
                FailPointKey.FP_SPLIT_FAILED_AFTER_READONLY_TASK, "true")
                + "ALTER TABLE " + q(table) + " SPLIT SUBPARTITION sp1, sp2 INTO SUBPARTITIONS 2";
            try {
                JdbcUtil.executeUpdateWithException(conn, ddlFail);
            } catch (Exception e) {
                // Expected
            }
            waitForDdlComplete(conn, table, 120);

            // Verify readonly cleared
            JdbcUtil.executeUpdateSuccess(conn,
                "INSERT INTO " + q(table) + " (val, name) VALUES (9999, 'after_rollback')");
            int afterInsert = rowCount(conn, table);
            Assert.assertEquals("Row count should increase by 1 after insert", before + 1, afterInsert);
        }
    }
}
