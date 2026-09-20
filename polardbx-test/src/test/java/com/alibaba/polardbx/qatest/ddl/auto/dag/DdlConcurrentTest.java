package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * IT-2: 并发 DDL 无 table too old — 对同一表（或同一 TableGroup）并发执行多个互相兼容的 DDL，
 * 验证全部 DDL 成功执行且无 "table too old" 错误。
 * <p>
 * Phase 1 锁序列化访问，让每个 DDL 依次完成，后执行的 DDL 看到最新表版本，不会出现 "table too old"。
 */
public class DdlConcurrentTest extends BaseDdlEngineTestCase {

    private static final String BASE_TABLE = "ddl_conc_tbl";
    private static final String BASE_TG = "ddl_conc_tg";

    @Before
    public void setUp() {
        cleanUp();
    }

    @After
    public void tearDown() {
        cleanUp();
    }

    private void cleanUp() {
        dropTableIfExists(BASE_TABLE + "_2way");
        dropTableIfExists(BASE_TABLE + "_3way");
        dropTableIfExists(BASE_TABLE + "_ci");
        dropTableIfExists(BASE_TABLE + "_dc");
        dropTableIfExists(BASE_TABLE + "_tr");
        dropTableIfExists(BASE_TABLE + "_opt");
        dropTableIfExists(BASE_TABLE + "_tg_split");
        dropTableIfExists(BASE_TABLE + "_tg_reorg");
        dropTableIfExists(BASE_TABLE + "_tg_rename");
        dropTableIfExists(BASE_TABLE + "_tg_merge");
        dropTableIfExists(BASE_TABLE + "_tg_3way");
        dropTableIfExists(BASE_TABLE + "_stg");
        dropTableIfExists(BASE_TABLE + "_stg2");
        dropTableGroupIfExists(BASE_TG + "_split");
        dropTableGroupIfExists(BASE_TG + "_reorg");
        dropTableGroupIfExists(BASE_TG + "_rename");
        dropTableGroupIfExists(BASE_TG + "_merge");
        dropTableGroupIfExists(BASE_TG + "_3way");
        dropTableGroupIfExists(BASE_TG + "_stg");
        dropTableGroupIfExists(BASE_TG + "_stg2");
    }

    // ======================== Helper Methods ========================

    protected void assertNoInitialRecords(String objectName) {
        waiting(500);
        String sql = "/*TDDL:NODE='__META_DB__'*/SELECT COUNT(*) FROM ddl_engine "
            + "WHERE state='INITIAL' AND object_name='" + objectName + "'";
        try (java.sql.ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            if (rs.next()) {
                org.junit.Assert
                    .assertEquals("Found INITIAL record(s) for object '" + objectName + "'", 0, rs.getInt(1));
            }
        } catch (Exception e) {
            org.junit.Assert.assertFalse("show ddl should not contain INITIAL for " + objectName,
                showDdl().contains(objectName));
        }
    }

    protected void createPartitionedTable(String tableName, int partitions) {
        JdbcUtil.executeSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), c0 INT, PRIMARY KEY(id)) "
                + "PARTITION BY HASH(id) PARTITIONS %d",
            tableName, partitions));
    }

    protected void createTableGroup(String tgName) {
        JdbcUtil.executeSuccess(tddlConnection, "CREATE TABLEGROUP " + tgName);
    }

    protected void createRangeTableInTG(String tableName, String tgName) {
        createTableGroup(tgName);
        JdbcUtil.executeSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
                + "PARTITION BY RANGE (id) ("
                + "PARTITION p1 VALUES LESS THAN (100),"
                + "PARTITION p2 VALUES LESS THAN (200),"
                + "PARTITION p3 VALUES LESS THAN (MAXVALUE)"
                + ") TABLEGROUP=%s",
            tableName, tgName));
    }

    protected void createRangeTableInTG5Parts(String tableName, String tgName) {
        createTableGroup(tgName);
        JdbcUtil.executeSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
                + "PARTITION BY RANGE (id) ("
                + "PARTITION p1 VALUES LESS THAN (50),"
                + "PARTITION p2 VALUES LESS THAN (100),"
                + "PARTITION p3 VALUES LESS THAN (200),"
                + "PARTITION p4 VALUES LESS THAN (300),"
                + "PARTITION p5 VALUES LESS THAN (MAXVALUE)"
                + ") TABLEGROUP=%s",
            tableName, tgName));
    }

    /**
     * Run concurrent DDLs and assert all succeed.
     */
    protected void runConcurrentAndAssertAllSuccess(String[] ddls, String assertObjectName) throws Exception {
        final int DDL_COUNT = ddls.length;
        ExecutorService pool = Executors.newFixedThreadPool(DDL_COUNT);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(DDL_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        List<String> errors = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < DDL_COUNT; i++) {
            final String ddl = ddls[i];
            final int delay = i * 100;
            pool.submit(() -> {
                try (Connection conn = getPolardbxConnection()) {
                    start.await();
                    Thread.sleep(delay);
                    try {
                        JdbcUtil.executeSuccess(conn, ddl);
                        successCount.incrementAndGet();
                    } catch (Throwable e) {
                        errors.add("DDL failed: " + ddl + " -> " + e.getMessage());
                    }
                } catch (Throwable e) {
                    errors.add("Unexpected: " + e.getMessage());
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        org.junit.Assert.assertTrue("DDL did not complete in time — possible permanent block",
            done.await(12000, TimeUnit.SECONDS));
        pool.shutdown();

        org.junit.Assert.assertEquals(
            "All " + DDL_COUNT + " DDLs should succeed, but only "
                + successCount.get() + " succeeded. Errors: " + errors,
            DDL_COUNT, successCount.get());
        if (assertObjectName != null) {
            assertNoInitialRecords(assertObjectName);
        }
    }

    // ======================== A. Same Table Concurrent DDLs ========================

    @Test
    public void testConcurrent_AlterTableAddColumn_TwoWay() throws Exception {
        String tableName = BASE_TABLE + "_2way";
        try {
            createPartitionedTable(tableName, 4);
            runConcurrentAndAssertAllSuccess(new String[] {
                "ALTER TABLE " + tableName + " ADD COLUMN c1 INT",
                "ALTER TABLE " + tableName + " ADD COLUMN c2 INT",
            }, tableName);
            // Verify both columns exist
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT c1, c2 FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testConcurrent_AlterTableAddColumn_ThreeWay() throws Exception {
        String tableName = BASE_TABLE + "_3way";
        try {
            createPartitionedTable(tableName, 4);
            runConcurrentAndAssertAllSuccess(new String[] {
                "ALTER TABLE " + tableName + " ADD COLUMN c1 INT",
                "ALTER TABLE " + tableName + " ADD COLUMN c2 INT",
                "ALTER TABLE " + tableName + " ADD COLUMN c3 INT",
            }, tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT c1, c2, c3 FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testConcurrent_AlterTableAddColumn_CreateIndex() throws Exception {
        String tableName = BASE_TABLE + "_ci";
        try {
            createPartitionedTable(tableName, 4);
            runConcurrentAndAssertAllSuccess(new String[] {
                "ALTER TABLE " + tableName + " ADD COLUMN c1 INT",
                "CREATE INDEX idx_ci ON " + tableName + "(id)",
            }, tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT c1 FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testConcurrent_AlterTableAddColumn_DropColumn() throws Exception {
        String tableName = BASE_TABLE + "_dc";
        try {
            createPartitionedTable(tableName, 4);
            runConcurrentAndAssertAllSuccess(new String[] {
                "ALTER TABLE " + tableName + " ADD COLUMN c1 INT",
                "ALTER TABLE " + tableName + " DROP COLUMN c0",
            }, tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT c1 FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testConcurrent_AlterTableAddColumn_Truncate() throws Exception {
        String tableName = BASE_TABLE + "_tr";
        try {
            createPartitionedTable(tableName, 4);
            // Insert some data before truncate
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + tableName + " (id, name) VALUES (1, 'a'), (2, 'b')");
            runConcurrentAndAssertAllSuccess(new String[] {
                "ALTER TABLE " + tableName + " ADD COLUMN c1 INT",
                "TRUNCATE TABLE " + tableName,
            }, tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT c1 FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testConcurrent_AlterTableAddColumn_OptimizeTable() throws Exception {
        String tableName = BASE_TABLE + "_opt";
        try {
            createPartitionedTable(tableName, 4);
            runConcurrentAndAssertAllSuccess(new String[] {
                "ALTER TABLE " + tableName + " ADD COLUMN c1 INT",
                "OPTIMIZE TABLE " + tableName,
            }, tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT c1 FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    // ======================== B. Same TableGroup Concurrent DDLs ========================

    @Test
    public void testConcurrent_TG_Split_DifferentPartitions() throws Exception {
        String tgName = BASE_TG + "_split";
        String tableName = BASE_TABLE + "_tg_split";
        try {
            createRangeTableInTG(tableName, tgName);
            runConcurrentAndAssertAllSuccess(new String[] {
                "ALTER TABLEGROUP " + tgName
                    + " SPLIT PARTITION p1 INTO (PARTITION p1a VALUES LESS THAN (50), PARTITION p1b VALUES LESS THAN (100))",
                "ALTER TABLEGROUP " + tgName
                    + " SPLIT PARTITION p2 INTO (PARTITION p2a VALUES LESS THAN (150), PARTITION p2b VALUES LESS THAN (200))",
            }, tgName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableGroupIfExists(tgName);
        }
    }

//    @Test
//    public void testConcurrent_TG_Reorg_Optimize_DifferentPartitions() throws Exception {
//        String tgName = BASE_TG + "_reorg";
//        String tableName = BASE_TABLE + "_tg_reorg";
//        try {
//            createRangeTableInTG5Parts(tableName, tgName);
//            runConcurrentAndAssertAllSuccess(new String[] {
//                "ALTER TABLEGROUP " + tgName
//                    + " REORGANIZE PARTITION p1,p2 INTO (PARTITION p1a VALUES LESS THAN (75),"
//                    + " PARTITION p2a VALUES LESS THAN (100))",
//                "ALTER TABLE " + tableName + " OPTIMIZE PARTITION p4",
//            }, tgName);
//            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
//        } finally {
//            dropTableIfExists(tableName);
//            dropTableGroupIfExists(tgName);
//        }
//    }

    @Test
    public void testConcurrent_TG_Rename_DifferentPartitions() throws Exception {
        String tgName = BASE_TG + "_rename";
        String tableName = BASE_TABLE + "_tg_rename";
        try {
            createRangeTableInTG(tableName, tgName);
            runConcurrentAndAssertAllSuccess(new String[] {
                "ALTER TABLEGROUP " + tgName + " RENAME PARTITION p1 TO p1_new",
                "ALTER TABLEGROUP " + tgName + " RENAME PARTITION p2 TO p2_new",
            }, tgName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableGroupIfExists(tgName);
        }
    }

    @Test
    public void testConcurrent_TG_Merge_Split_DifferentPartitions() throws Exception {
        String tgName = BASE_TG + "_merge";
        String tableName = BASE_TABLE + "_tg_merge";
        try {
            createRangeTableInTG5Parts(tableName, tgName);
            runConcurrentAndAssertAllSuccess(new String[] {
                "ALTER TABLEGROUP " + tgName + " MERGE PARTITIONS p4,p5 TO pm45",
                "ALTER TABLEGROUP " + tgName
                    + " SPLIT PARTITION p1 INTO (PARTITION p1a VALUES LESS THAN (25), PARTITION p1b VALUES LESS THAN (50))",
            }, tgName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableGroupIfExists(tgName);
        }
    }

    @Test
    public void testConcurrent_TG_ThreeWay_Split() throws Exception {
        String tgName = BASE_TG + "_3way";
        String tableName = BASE_TABLE + "_tg_3way";
        try {
            createRangeTableInTG5Parts(tableName, tgName);
            runConcurrentAndAssertAllSuccess(new String[] {
                "ALTER TABLEGROUP " + tgName
                    + " SPLIT PARTITION p1 INTO (PARTITION p1a VALUES LESS THAN (25), PARTITION p1b VALUES LESS THAN (50))",
                "ALTER TABLEGROUP " + tgName
                    + " SPLIT PARTITION p2 INTO (PARTITION p2a VALUES LESS THAN (75), PARTITION p2b VALUES LESS THAN (100))",
                "ALTER TABLEGROUP " + tgName
                    + " SPLIT PARTITION p3 INTO (PARTITION p3a VALUES LESS THAN (150), PARTITION p3b VALUES LESS THAN (200))",
            }, tgName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableGroupIfExists(tgName);
        }
    }

    // ======================== C. Cross-Handler Type Concurrent DDLs ========================

//    @Test
//    public void testConcurrent_AlterTable_SetTableGroup_AlterTableGroupRename() throws Exception {
//        String tgName = BASE_TG + "_stg";
//        String tgName2 = BASE_TG + "_stg2";
//        String tableName = BASE_TABLE + "_stg";
//        String tableName2 = BASE_TABLE + "_stg2";
//        try {
//            createRangeTableInTG(tableName, tgName);
//            createTableGroup(tgName2);
//            // tableName2 reuses existing tgName, skip createTableGroup
//            JdbcUtil.executeSuccess(tddlConnection, String.format(
//                "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
//                    + "PARTITION BY RANGE (id) ("
//                    + "PARTITION p1 VALUES LESS THAN (100),"
//                    + "PARTITION p2 VALUES LESS THAN (200),"
//                    + "PARTITION p3 VALUES LESS THAN (MAXVALUE)"
//                    + ") TABLEGROUP=%s",
//                tableName2, tgName));
//            runConcurrentAndAssertAllSuccess(new String[] {
//                "ALTER TABLE " + tableName2 + " SET TABLEGROUP = " + tgName2,
//                "ALTER TABLEGROUP " + tgName + " RENAME PARTITION p1 TO p1_new",
//            }, null);
//            assertNoInitialRecords(tableName2);
//            assertNoInitialRecords(tgName);
//        } finally {
//            dropTableIfExists(tableName);
//            dropTableIfExists(tableName2);
//            dropTableGroupIfExists(tgName);
//            dropTableGroupIfExists(tgName2);
//        }
//    }
}
