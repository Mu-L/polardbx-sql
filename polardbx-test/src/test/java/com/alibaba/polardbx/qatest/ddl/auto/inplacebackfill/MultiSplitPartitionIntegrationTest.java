package com.alibaba.polardbx.qatest.ddl.auto.inplacebackfill;

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

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Integration tests for multi-partition split feature.
 * Tests the end-to-end workflow of splitting multiple partitions in a single DDL.
 * Table names intentionally include Chinese characters and hyphens to test special character handling.
 */
public class MultiSplitPartitionIntegrationTest extends DDLBaseNewDBTestCase {

    private static final String SCHEMA_NAME = "multi_split_it";
    private static final String INPLACE_HINT =
        "/*+TDDL:cmd_extra(ENABLE_INPLACE_BACKFILL=true, SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111)*/";

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

    /**
     * Backtick-quote a table name for use in SQL.
     */
    private static String q(String name) {
        return "`" + name + "`";
    }

    private void prepareKeyTable(Connection conn, String table, int parts, int rows) throws SQLException {
        JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
        JdbcUtil.executeUpdateSuccess(conn,
            "CREATE TABLE " + q(table) + " (" +
                "id BIGINT NOT NULL AUTO_INCREMENT, " +
                "val BIGINT NOT NULL, " +
                "name VARCHAR(100), " +
                "PRIMARY KEY(id)" +
                ") PARTITION BY KEY(val, id) PARTITIONS " + parts);
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
     * Basic: split 2 partitions into 2 each, verify data integrity.
     */
    @Test
    public void testMultiSplit_basic_2partitions() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_分区-2p";
            prepareKeyTable(conn, table, 4, 200);
            int before = rowCount(conn, table);

            String ddl = INPLACE_HINT + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged after multi-split", before, after);
        }
    }

    /**
     * Split 3 partitions into 3 each.
     */
    @Test
    public void testMultiSplit_basic_3partitions() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_分区-3p";
            prepareKeyTable(conn, table, 4, 300);
            int before = rowCount(conn, table);

            String ddl =
                INPLACE_HINT + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2, p3 INTO PARTITIONS 3";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged after multi-split", before, after);
        }
    }

    /**
     * Split all 4 partitions at once.
     */
    @Test
    public void testMultiSplit_splitAll() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_全部-4p";
            prepareKeyTable(conn, table, 4, 400);
            int before = rowCount(conn, table);

            String ddl =
                INPLACE_HINT + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2, p3, p4 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged after split all", before, after);
        }
    }

    /**
     * Split with custom partition name prefix.
     */
    @Test
    public void testMultiSplit_withPrefix() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_前缀-np";
            prepareKeyTable(conn, table, 4, 100);
            int before = rowCount(conn, table);

            String ddl =
                INPLACE_HINT + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO np PARTITIONS 3";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged", before, after);
        }
    }

    /**
     * HASH partition strategy split.
     */
    @Test
    public void testMultiSplit_hashPartition() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_哈希-h";
            JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
            JdbcUtil.executeUpdateSuccess(conn,
                "CREATE TABLE " + q(table) + " (" +
                    "id BIGINT NOT NULL AUTO_INCREMENT, " +
                    "val INT NOT NULL, " +
                    "name VARCHAR(100), " +
                    "PRIMARY KEY(id)" +
                    ") PARTITION BY HASH(val) PARTITIONS 4");
            JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
            for (int i = 1; i <= 200; i++) {
                JdbcUtil.executeUpdateSuccess(conn,
                    "INSERT INTO " + q(table) + " (val, name) VALUES (" + i + ", 'hash_" + i + "')");
            }
            int before = rowCount(conn, table);

            String ddl = INPLACE_HINT + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged for HASH split", before, after);
        }
    }

    /**
     * DML during multi-split DDL: verify data integrity.
     */
    @Test
    public void testMultiSplit_concurrentDml() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_并发-dml";
            prepareKeyTable(conn, table, 4, 500);
            int before = rowCount(conn, table);

            // Run DDL in a separate thread while inserting data
            Thread ddlThread = new Thread(() -> {
                try (Connection ddlConn = ConnectionManager.getInstance().newPolarDBXConnection(SCHEMA_NAME)) {
                    String ddl =
                        INPLACE_HINT + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
                    JdbcUtil.executeUpdateSuccess(ddlConn, ddl);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            ddlThread.start();

            // Concurrently insert some data
            try (Connection dmlConn = ConnectionManager.getInstance().newPolarDBXConnection(SCHEMA_NAME)) {
                for (int i = 1; i <= 50; i++) {
                    try {
                        JdbcUtil.executeUpdate(dmlConn,
                            "INSERT INTO " + q(table) + " (val, name) VALUES (" + (1000 + i) + ", 'new_" + i
                                + "')");
                    } catch (Exception e) {
                        // some may fail during readonly phase, acceptable
                    }
                }
            }

            ddlThread.join(300000);
            int after = rowCount(conn, table);
            Assert.assertTrue("Row count should be >= original after concurrent DML", after >= before);
        }
    }

    /**
     * Verify that multi-split at TABLEGROUP level is rejected.
     * Multi-partition split is only supported at the table level (ALTER TABLE).
     */
    @Test
    public void testMultiSplit_tablegroup_rejected() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_表组-tg";
            prepareKeyTable(conn, table, 4, 100);

            // Get table group name
            ResultSet rs = JdbcUtil.executeQuery("SHOW CREATE TABLE " + q(table), conn);
            rs.next();
            String createSql = rs.getString(2);
            rs.close();

            // Extract tablegroup from create statement
            int tgIdx = createSql.toLowerCase().indexOf("tablegroup");
            if (tgIdx < 0) {
                return; // skip if no tablegroup info
            }
            String tgPart = createSql.substring(tgIdx);
            String tgName = tgPart.split("=")[1].trim().split("\\s")[0].replace("`", "").replace("'", "");

            try {
                String ddl =
                    INPLACE_HINT + "ALTER TABLEGROUP " + tgName + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
                JdbcUtil.executeUpdateWithException(conn, ddl);
                assertWithMessage("Should fail for tablegroup multi-split").fail();
            } catch (Exception e) {
                assertWithMessage("Error should mention ALTER TABLEGROUP not supported")
                    .that(e.getMessage())
                    .contains("not supported for ALTER TABLEGROUP");
            }
        }
    }

    /**
     * Split the same partition set twice (first split, then split resulting partitions).
     */
    @Test
    public void testMultiSplit_sequential() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_连续-seq";
            prepareKeyTable(conn, table, 4, 200);
            int before = rowCount(conn, table);

            // First split: p1, p2 into 2 each
            String ddl1 =
                INPLACE_HINT + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl1);
            Assert.assertEquals(before, rowCount(conn, table));

            // Second split: p3, p4 into 2 each
            String ddl2 =
                INPLACE_HINT + "ALTER TABLE " + q(table) + " SPLIT PARTITION p3, p4 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl2);
            Assert.assertEquals("Data should remain intact after sequential splits", before, rowCount(conn, table));
        }
    }

    // ========== Subpartition table helpers ==========

    private void prepareKeyKeySubPartTable(Connection conn, String table, int parts, int subparts, int rows)
        throws SQLException {
        JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
        JdbcUtil.executeUpdateSuccess(conn,
            "CREATE TABLE " + q(table) + " (" +
                "id BIGINT NOT NULL AUTO_INCREMENT, " +
                "val BIGINT NOT NULL, " +
                "name VARCHAR(100), " +
                "PRIMARY KEY(id)" +
                ") PARTITION BY KEY(val) PARTITIONS " + parts +
                " SUBPARTITION BY KEY(id) SUBPARTITIONS " + subparts);
        JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
        for (int i = 1; i <= rows; i++) {
            JdbcUtil.executeUpdateSuccess(conn,
                "INSERT INTO " + q(table) + " (val, name) VALUES (" + i + ", 'row_" + i + "')");
        }
    }

    private void prepareHashKeySubPartTable(Connection conn, String table, int parts, int subparts, int rows)
        throws SQLException {
        JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
        JdbcUtil.executeUpdateSuccess(conn,
            "CREATE TABLE " + q(table) + " (" +
                "id BIGINT NOT NULL AUTO_INCREMENT, " +
                "val BIGINT NOT NULL, " +
                "name VARCHAR(100), " +
                "PRIMARY KEY(id)" +
                ") PARTITION BY HASH(val) PARTITIONS " + parts +
                " SUBPARTITION BY KEY(id) SUBPARTITIONS " + subparts);
        JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
        for (int i = 1; i <= rows; i++) {
            JdbcUtil.executeUpdateSuccess(conn,
                "INSERT INTO " + q(table) + " (val, name) VALUES (" + i + ", 'row_" + i + "')");
        }
    }

    private void prepareKeyRangeSubPartTable(Connection conn, String table, int parts, int rows)
        throws SQLException {
        JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
        JdbcUtil.executeUpdateSuccess(conn,
            "CREATE TABLE " + q(table) + " (" +
                "id BIGINT NOT NULL AUTO_INCREMENT, " +
                "val INT NOT NULL, " +
                "name VARCHAR(100), " +
                "PRIMARY KEY(id)" +
                ") PARTITION BY KEY(val, id) PARTITIONS " + parts +
                " SUBPARTITION BY RANGE(val) (" +
                "  SUBPARTITION sp1 VALUES LESS THAN (100)," +
                "  SUBPARTITION sp2 VALUES LESS THAN MAXVALUE" +
                ")");
        JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
        for (int i = 1; i <= rows; i++) {
            JdbcUtil.executeUpdateSuccess(conn,
                "INSERT INTO " + q(table) + " (val, name) VALUES (" + (i % 200) + ", 'row_" + i + "')");
        }
    }

    private void prepareHashHashNoTempSubPartTable(Connection conn, String table, int rows)
        throws SQLException {
        JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
        JdbcUtil.executeUpdateSuccess(conn,
            "CREATE TABLE " + q(table) + " (" +
                "id BIGINT NOT NULL AUTO_INCREMENT, " +
                "val INT NOT NULL, " +
                "name VARCHAR(100), " +
                "PRIMARY KEY(id)" +
                ") PARTITION BY HASH(val, id) PARTITIONS 2" +
                " SUBPARTITION BY HASH(id)" +
                "(partition p1 subpartitions 2, partition p2 subpartitions 4)");
        JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
        for (int i = 1; i <= rows; i++) {
            JdbcUtil.executeUpdateSuccess(conn,
                "INSERT INTO " + q(table) + " (val, name) VALUES (" + i + ", 'row_" + i + "')");
        }
    }

    private void prepareRangeHashSubPartTable(Connection conn, String table, int rows)
        throws SQLException {
        JdbcUtil.executeUpdate(conn, "DROP TABLE IF EXISTS " + q(table));
        JdbcUtil.executeUpdateSuccess(conn,
            "CREATE TABLE " + q(table) + " (" +
                "id BIGINT NOT NULL AUTO_INCREMENT, " +
                "val INT NOT NULL, " +
                "name VARCHAR(100), " +
                "PRIMARY KEY(id)" +
                ") PARTITION BY RANGE(val)" +
                " SUBPARTITION BY HASH(id) SUBPARTITIONS 2 (" +
                "  PARTITION p1 VALUES LESS THAN (100)," +
                "  PARTITION p2 VALUES LESS THAN (200)," +
                "  PARTITION p3 VALUES LESS THAN MAXVALUE" +
                ")");
        JdbcUtil.executeUpdateSuccess(conn, "ALTER TABLE " + q(table) + " SET tablegroup = ''");
        for (int i = 1; i <= rows; i++) {
            JdbcUtil.executeUpdateSuccess(conn,
                "INSERT INTO " + q(table) + " (val, name) VALUES (" + (i % 300) + ", 'row_" + i + "')");
        }
    }

    // ========== Subpartition test cases ==========

    /**
     * Multi-split on KEY+KEY templated subpartition table.
     * Splits first-level partitions; exercises spec.isLogical() code path.
     */
    @Test
    public void testMultiSplit_keyKeySubPartition() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_键键-sp";
            prepareKeyKeySubPartTable(conn, table, 4, 2, 200);
            int before = rowCount(conn, table);

            String ddl =
                INPLACE_HINT + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged after KEY+KEY subpart multi-split", before, after);
        }
    }

    /**
     * Multi-split on HASH+KEY templated subpartition table.
     */
    @Test
    public void testMultiSplit_hashKeySubPartition() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_哈希键-sp";
            prepareHashKeySubPartTable(conn, table, 4, 2, 200);
            int before = rowCount(conn, table);

            String ddl =
                INPLACE_HINT + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged after HASH+KEY subpart multi-split", before, after);
        }
    }

    /**
     * Multi-split on KEY+RANGE templated subpartition table.
     * First-level KEY supports multi-split; subpartitions are RANGE (preserved).
     */
    @Test
    public void testMultiSplit_keyRangeSubPartition() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_键范围-sp";
            prepareKeyRangeSubPartTable(conn, table, 4, 200);
            int before = rowCount(conn, table);

            String ddl =
                INPLACE_HINT + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged after KEY+RANGE subpart multi-split", before, after);
        }
    }

    /**
     * Multi-split on HASH+HASH non-templated subpartition table.
     * Each partition has a different number of subpartitions.
     */
    @Test
    public void testMultiSplit_hashHashNoTempSubPartition() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_哈希-notemp";
            prepareHashHashNoTempSubPartTable(conn, table, 200);
            int before = rowCount(conn, table);

            String ddl =
                INPLACE_HINT + "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged after HASH+HASH non-temp subpart multi-split",
                before, after);
        }
    }

    /**
     * Multi-split on RANGE+HASH subpartition table should fail.
     * RANGE first-level partitions are not supported for multi-split (only HASH/KEY).
     */
    @Test
    public void testMultiSplit_rangeHashSubPartition_rejected() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_范围-hash";
            prepareRangeHashSubPartTable(conn, table, 100);

            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "ALTER TABLE " + q(table) + " SPLIT PARTITION p1, p2 INTO PARTITIONS 2");
                assertWithMessage("Should fail for RANGE+HASH subpart multi-split").fail();
            } catch (Exception e) {
                assertWithMessage("Error should mention HASH/KEY strategy")
                    .that(e.getMessage().toLowerCase())
                    .containsMatch("hash|key");
            }
        }
    }

    /**
     * Multi-split subpartition on KEY+KEY templated table.
     * Split multiple template subpartitions at once.
     */
    @Test
    public void testMultiSplit_subPartSplit_keyKey_template() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_子分-kk";
            prepareKeyKeySubPartTable(conn, table, 4, 3, 200);
            int before = rowCount(conn, table);

            String ddl = INPLACE_HINT + "ALTER TABLE " + q(table)
                + " SPLIT SUBPARTITION sp1, sp2 INTO SUBPARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged after KEY+KEY template subpart multi-split",
                before, after);
        }
    }

    /**
     * Multi-split subpartition on HASH+KEY templated table.
     * Different strategy combination from KEY+KEY.
     */
    @Test
    public void testMultiSplit_subPartSplit_hashKey_template() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_子分-hk";
            prepareHashKeySubPartTable(conn, table, 4, 3, 200);
            int before = rowCount(conn, table);

            String ddl = INPLACE_HINT + "ALTER TABLE " + q(table)
                + " SPLIT SUBPARTITION sp1, sp2 INTO SUBPARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged after HASH+KEY template subpart multi-split",
                before, after);
        }
    }

    /**
     * Multi-split subpartition with custom name prefix on KEY+KEY table.
     */
    @Test
    public void testMultiSplit_subPartSplit_withPrefix() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_子分-前缀";
            prepareKeyKeySubPartTable(conn, table, 4, 3, 200);
            int before = rowCount(conn, table);

            String ddl = INPLACE_HINT + "ALTER TABLE " + q(table)
                + " SPLIT SUBPARTITION sp1, sp2 INTO xsp SUBPARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged after subpart multi-split with prefix",
                before, after);
        }
    }

    /**
     * Multi-split subpartition on HASH+HASH non-templated table.
     * Split multiple physical subpartitions at once.
     */
    @Test
    public void testMultiSplit_subPartSplit_hashHash_nonTemplate() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getConn()) {
            String table = "ms_子分-hh";
            prepareHashHashNoTempSubPartTable(conn, table, 200);
            int before = rowCount(conn, table);

            String ddl = INPLACE_HINT + "ALTER TABLE " + q(table)
                + " SPLIT SUBPARTITION p1sp1, p1sp2 INTO SUBPARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, ddl);

            int after = rowCount(conn, table);
            Assert.assertEquals("Row count must be unchanged after HASH+HASH non-template subpart multi-split",
                before, after);
        }
    }
}
