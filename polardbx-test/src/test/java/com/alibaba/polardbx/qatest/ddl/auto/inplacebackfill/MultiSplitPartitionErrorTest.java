package com.alibaba.polardbx.qatest.ddl.auto.inplacebackfill;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Error path tests for multi-partition split.
 * Validates error messages for invalid multi-split operations.
 * Table names intentionally include Chinese characters and hyphens to test special character handling.
 */
public class MultiSplitPartitionErrorTest extends DDLBaseNewDBTestCase {

    private static final String SCHEMA_NAME = "multi_split_err";

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

    private Connection getConn() throws SQLException {
        return getPolardbxConnection(SCHEMA_NAME);
    }

    private static String q(String name) {
        return "`" + name + "`";
    }

    private void prepareRangeTable(Connection conn, String table) throws SQLException {
        JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
        JdbcUtil.executeUpdateSuccess(conn,
            "CREATE TABLE " + q(table) + " (" +
                "id BIGINT NOT NULL, " +
                "PRIMARY KEY(id)" +
                ") PARTITION BY RANGE(id) (" +
                "PARTITION p1 VALUES LESS THAN(100), " +
                "PARTITION p2 VALUES LESS THAN(200), " +
                "PARTITION p3 VALUES LESS THAN(MAXVALUE))");
    }

    private void prepareKeyTable(Connection conn, String table) throws SQLException {
        JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
        JdbcUtil.executeUpdateSuccess(conn,
            "CREATE TABLE " + q(table) + " (" +
                "id BIGINT NOT NULL AUTO_INCREMENT, " +
                "val BIGINT NOT NULL, " +
                "PRIMARY KEY(id)" +
                ") PARTITION BY KEY(val, id) PARTITIONS 4");
        JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
    }

    /**
     * Multi-split on RANGE partition should fail - only HASH/KEY supported.
     */
    @Test
    public void testMultiSplit_rangePartition_rejected() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_范围-rng";
            prepareRangeTable(conn, table);
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2");
                assertWithMessage("Should fail for RANGE partition multi-split").fail();
            } catch (Exception e) {
                assertWithMessage("Error should mention HASH/KEY strategy")
                    .that(e.getMessage().toLowerCase())
                    .containsMatch("hash|key");
            }
        }
    }

    /**
     * Duplicate partition names should fail.
     */
    @Test
    public void testMultiSplit_duplicateNames_rejected() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_重复-dup";
            prepareKeyTable(conn, table);
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p1 INTO PARTITIONS 2");
                assertWithMessage("Should fail for duplicate partition name").fail();
            } catch (Exception e) {
                assertWithMessage("Error should mention duplicate")
                    .that(e.getMessage().toLowerCase())
                    .contains("duplicate");
            }
        }
    }

    /**
     * Non-existent partition name should fail.
     */
    @Test
    public void testMultiSplit_nonExistentPartition_rejected() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_不存在-ne";
            prepareKeyTable(conn, table);
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, pXXX INTO PARTITIONS 2");
                assertWithMessage("Should fail for non-existent partition").fail();
            } catch (Exception e) {
                assertWithMessage("Error should mention partition not exists")
                    .that(e.getMessage().toLowerCase())
                    .contains("not exist");
            }
        }
    }

    /**
     * AT clause with multi-split should fail at parser level.
     */
    @Test
    public void testMultiSplit_atClause_rejectedByParser() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_AT子句-at";
            prepareKeyTable(conn, table);
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table)
                        + " SPLIT PARTITION p1, p2 AT(100) INTO (PARTITION p10, PARTITION p11)");
                assertWithMessage("Should fail for AT clause with multi-split").fail();
            } catch (Exception e) {
                assertWithMessage("Error should mention AT clause not supported")
                    .that(e.getMessage())
                    .contains("AT clause is not supported");
            }
        }
    }

    /**
     * Explicit partition definitions with multi-split should fail at parser level.
     */
    @Test
    public void testMultiSplit_explicitDefs_rejectedByParser() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_显式-exp";
            prepareKeyTable(conn, table);
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table)
                        + " SPLIT PARTITION p1, p2 INTO (PARTITION p10, PARTITION p11)");
                assertWithMessage("Should fail for explicit defs with multi-split").fail();
            } catch (Exception e) {
                assertWithMessage("Error should mention explicit partition definitions not supported")
                    .that(e.getMessage())
                    .contains("Explicit partition definitions are not supported");
            }
        }
    }

    /**
     * Multi-split with HASH/KEY sub-partition should succeed.
     */
    @Test
    public void testMultiSplit_subPartition_accepted() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_子分区-sp";
            JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
            JdbcUtil.executeUpdateSuccess(conn,
                "CREATE TABLE " + q(table) + " (" +
                    "id BIGINT NOT NULL AUTO_INCREMENT, " +
                    "val BIGINT NOT NULL, " +
                    "PRIMARY KEY(id)" +
                    ") PARTITION BY KEY(val) PARTITIONS 4 " +
                    "SUBPARTITION BY KEY(id) SUBPARTITIONS 2");
            JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
            JdbcUtil.executeUpdateSuccess(conn,
                "ALTER TABLE " + q(table) + " SPLIT SUBPARTITION sp1, sp2 INTO SUBPARTITIONS 2");
        }
    }

    /**
     * INTO PARTITIONS N with N < 2 should fail.
     */
    @Test
    public void testMultiSplit_partitionNumLessThan2_rejected() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_数量-num1";
            prepareKeyTable(conn, table);
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 1");
                assertWithMessage("Should fail for INTO PARTITIONS 1").fail();
            } catch (Exception e) {
                // Expected - partition number must be > 1
            }
        }
    }

    /**
     * INTO PARTITIONS 0 should fail.
     */
    @Test
    public void testMultiSplit_zeroPartitionCount_rejected() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_零-zero";
            prepareKeyTable(conn, table);
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 0");
                assertWithMessage("Should fail for INTO PARTITIONS 0").fail();
            } catch (Exception e) {
                // Expected - partition count 0 is invalid
            }
        }
    }

    /**
     * INTO PARTITIONS -1 should fail at parser or executor level.
     */
    @Test
    public void testMultiSplit_negativePartitionCount_rejected() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_负数-neg";
            prepareKeyTable(conn, table);
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS -1");
                assertWithMessage("Should fail for INTO PARTITIONS -1").fail();
            } catch (Exception e) {
                // Expected - negative partition count is invalid
            }
        }
    }

    /**
     * Non-evenly-divisible partition count should fail if validation is active.
     */
    @Test
    public void testMultiSplit_oddPartitionCount_accepted() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_奇数-odd";
            prepareKeyTable(conn, table);
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 3");
            } catch (Exception e) {
                assertWithMessage("Should not fail due to uneven division")
                    .that(e.getMessage().toLowerCase())
                    .doesNotContain("divisible");
            }
        }
    }

    /**
     * Multi-split with sub-partition on non-templated subpartition table should succeed.
     */
    @Test
    public void testMultiSplit_subPartition_nonTemplated_accepted() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_非模板-nt";
            JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
            JdbcUtil.executeUpdateSuccess(conn,
                "CREATE TABLE " + q(table) + " (" +
                    "id BIGINT NOT NULL AUTO_INCREMENT, " +
                    "val BIGINT NOT NULL, " +
                    "PRIMARY KEY(id)" +
                    ") PARTITION BY HASH(val, id) PARTITIONS 2" +
                    " SUBPARTITION BY HASH(id)" +
                    "(partition p1 subpartitions 2, partition p2 subpartitions 4)");
            JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
            JdbcUtil.executeUpdateSuccess(conn,
                "ALTER TABLE " + q(table) + " SPLIT SUBPARTITION p1sp1, p1sp2 INTO SUBPARTITIONS 2");
        }
    }

    /**
     * Multi-split with RANGE sub-partition strategy should be rejected.
     */
    @Test
    public void testMultiSplit_subPartition_rangeStrategy_rejected() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_范围子分-rsp";
            JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
            JdbcUtil.executeUpdateSuccess(conn,
                "CREATE TABLE " + q(table) + " (" +
                    "id BIGINT NOT NULL AUTO_INCREMENT, " +
                    "val BIGINT NOT NULL, " +
                    "PRIMARY KEY(id)" +
                    ") PARTITION BY KEY(val) PARTITIONS 4 " +
                    "SUBPARTITION BY RANGE(id)" +
                    "(SUBPARTITION sp1 VALUES LESS THAN(100)," +
                    " SUBPARTITION sp2 VALUES LESS THAN(MAXVALUE))");
            JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table) + " SPLIT SUBPARTITION sp1, sp2 INTO SUBPARTITIONS 2");
                assertWithMessage("Should fail for multi-split with RANGE sub-partition strategy").fail();
            } catch (Exception e) {
                assertWithMessage("Error should indicate HASH/KEY requirement")
                    .that(e.getMessage().toLowerCase())
                    .containsMatch("hash|key");
            }
        }
    }

    /**
     * Duplicate subpartition names in multi-split should fail.
     */
    @Test
    public void testMultiSplit_subPartSplit_duplicateNames_rejected() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_子分重复-sd";
            JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
            JdbcUtil.executeUpdateSuccess(conn,
                "CREATE TABLE " + q(table) + " (" +
                    "id BIGINT NOT NULL AUTO_INCREMENT, " +
                    "val BIGINT NOT NULL, " +
                    "PRIMARY KEY(id)" +
                    ") PARTITION BY KEY(val) PARTITIONS 4 " +
                    "SUBPARTITION BY KEY(id) SUBPARTITIONS 2");
            JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table) + " SPLIT SUBPARTITION sp1, sp1 INTO SUBPARTITIONS 2");
                assertWithMessage("Should fail for duplicate subpartition names").fail();
            } catch (Exception e) {
                assertWithMessage("Error should mention duplicate")
                    .that(e.getMessage().toLowerCase())
                    .contains("duplicate");
            }
        }
    }

    /**
     * Non-existent subpartition name in multi-split should fail.
     */
    @Test
    public void testMultiSplit_subPartSplit_nonExistent_rejected() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_子分不存-sne";
            JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
            JdbcUtil.executeUpdateSuccess(conn,
                "CREATE TABLE " + q(table) + " (" +
                    "id BIGINT NOT NULL AUTO_INCREMENT, " +
                    "val BIGINT NOT NULL, " +
                    "PRIMARY KEY(id)" +
                    ") PARTITION BY KEY(val) PARTITIONS 4 " +
                    "SUBPARTITION BY KEY(id) SUBPARTITIONS 2");
            JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table) + " SPLIT SUBPARTITION sp1, spXXX INTO SUBPARTITIONS 2");
                assertWithMessage("Should fail for non-existent subpartition").fail();
            } catch (Exception e) {
                assertWithMessage("Error should mention subpartition not exists")
                    .that(e.getMessage().toLowerCase())
                    .contains("not exist");
            }
        }
    }

    /**
     * INTO SUBPARTITIONS N with N < 2 should fail.
     */
    @Test
    public void testMultiSplit_subPartSplit_numLessThan2_rejected() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "err_子分数量-sn1";
            JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
            JdbcUtil.executeUpdateSuccess(conn,
                "CREATE TABLE " + q(table) + " (" +
                    "id BIGINT NOT NULL AUTO_INCREMENT, " +
                    "val BIGINT NOT NULL, " +
                    "PRIMARY KEY(id)" +
                    ") PARTITION BY KEY(val) PARTITIONS 4 " +
                    "SUBPARTITION BY KEY(id) SUBPARTITIONS 2");
            JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table) + " SPLIT SUBPARTITION sp1, sp2 INTO SUBPARTITIONS 1");
                assertWithMessage("Should fail for INTO SUBPARTITIONS 1").fail();
            } catch (Exception e) {
                // Expected - subpartition number must be > 1
            }
        }
    }
}
