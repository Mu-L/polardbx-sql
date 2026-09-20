package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * IT-3: 死锁预防验证 — 构造 Phase 1 获取不同资源、Phase 2 想要获取对方资源的场景，
 * 验证无永久死锁（所有 DDL 在超时内完成 — 要么成功，要么报错返回），
 * 且至少一个 DDL 成功执行。
 */
public class DdlDeadlockTest extends BaseDdlEngineTestCase {

    private static final String BASE_TABLE = "ddl_dl_tbl";
    private static final String BASE_TG = "ddl_dl_tg";

    @Before
    public void setUp() {
        cleanUp();
    }

    @After
    public void tearDown() {
        cleanUp();
    }

    private void cleanUp() {
        dropTableIfExists(BASE_TABLE + "_a");
        dropTableIfExists(BASE_TABLE + "_b");
        dropTableIfExists(BASE_TABLE + "_src");
        dropTableIfExists(BASE_TABLE + "_tgt");
        dropTableIfExists(BASE_TABLE + "_rn_old");
        dropTableIfExists(BASE_TABLE + "_rn_new");
        dropTableIfExists(BASE_TABLE + "_opt1");
        dropTableIfExists(BASE_TABLE + "_opt2");
        dropTableIfExists(BASE_TABLE + "_3a");
        dropTableIfExists(BASE_TABLE + "_3b");
        dropTableIfExists(BASE_TABLE + "_3c");
        dropTableGroupIfExists(BASE_TG + "_cross");
        dropTableGroupIfExists(BASE_TG + "_exch_split");
        dropTableGroupIfExists(BASE_TG + "_3way");
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
            "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
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
                + "PARTITION p2 VALUES LESS THAN (200)"
                + ") TABLEGROUP=%s",
            tableName, tgName));
    }

    /**
     * Create a range partitioned table in an existing tablegroup (skip createTableGroup).
     */
    protected void createRangeTableInExistingTG(String tableName, String tgName) {
        JdbcUtil.executeSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
                + "PARTITION BY RANGE (id) ("
                + "PARTITION p1 VALUES LESS THAN (100),"
                + "PARTITION p2 VALUES LESS THAN (200)"
                + ") TABLEGROUP=%s",
            tableName, tgName));
    }

    protected void createSingleTable(String tableName) {
        JdbcUtil.executeSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) SINGLE",
            tableName));
    }

    protected String describeError(AtomicReference<Throwable> errorRef) {
        Throwable e = errorRef.get();
        return e == null ? "null" : e.getClass().getSimpleName() + ": " + e.getMessage();
    }

    /**
     * Run two concurrent DDLs that may deadlock. Assert:
     * 1. No permanent deadlock (both complete within timeout)
     * 2. At least one DDL succeeds
     */
    protected void runTwoWayAndAssertNoDeadlock(String ddlA, String ddlB,
                                                String assertObjectNameA, String assertObjectNameB)
        throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicReference<Throwable> errorA = new AtomicReference<>();
        AtomicReference<Throwable> errorB = new AtomicReference<>();

        // DDL A (no delay, starts first)
        pool.submit(() -> {
            try (Connection conn = getPolardbxConnection()) {
                start.await();
                try {
                    JdbcUtil.executeSuccess(conn, ddlA);
                    successCount.incrementAndGet();
                } catch (Throwable e) {
                    errorA.compareAndSet(null, e);
                }
            } catch (Throwable e) {
                errorA.compareAndSet(null, e);
            } finally {
                done.countDown();
            }
        });

        // DDL B (100ms delay to ensure different jobId)
        pool.submit(() -> {
            try (Connection conn = getPolardbxConnection()) {
                start.await();
                Thread.sleep(100);
                try {
                    JdbcUtil.executeSuccess(conn, ddlB);
                    successCount.incrementAndGet();
                } catch (Throwable e) {
                    errorB.compareAndSet(null, e);
                }
            } catch (Throwable e) {
                errorB.compareAndSet(null, e);
            } finally {
                done.countDown();
            }
        });

        start.countDown();
        org.junit.Assert.assertTrue("DDL did not complete in time — possible permanent deadlock",
            done.await(120, TimeUnit.SECONDS));
        pool.shutdown();

        org.junit.Assert.assertTrue(
            "At least one DDL should succeed, but both failed: A=" + describeError(errorA)
                + ", B=" + describeError(errorB),
            successCount.get() >= 1);

        if (assertObjectNameA != null) {
            assertNoInitialRecords(assertObjectNameA);
        }
        if (assertObjectNameB != null) {
            assertNoInitialRecords(assertObjectNameB);
        }
    }

    // ======================== Test Cases ========================

    /**
     * 1. Same table, different DDLs — Phase 1 = {t} for both.
     * Not a true deadlock, just serialization. Verify no permanent block.
     */
    @Test
    public void testDeadlock_SameTable_DifferentDdl() throws Exception {
        String tableName = BASE_TABLE + "_a";
        try {
            createPartitionedTable(tableName, 4);
            JdbcUtil.executeSuccess(tddlConnection,
                "ALTER TABLE " + tableName + " ADD COLUMN c0 INT");
            runTwoWayAndAssertNoDeadlock(
                "ALTER TABLE " + tableName + " ADD COLUMN c1 INT",
                "ALTER TABLE " + tableName + " DROP COLUMN c0",
                tableName, tableName);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    /**
     * 2. Cross-table, same TableGroup — Phase 1 = {tA} and {tB}, Phase 2 both lock TG.
     */
    @Test
    public void testDeadlock_CrossTable_SameTableGroup() throws Exception {
        String tgName = BASE_TG + "_cross";
        String tableA = BASE_TABLE + "_a";
        String tableB = BASE_TABLE + "_b";
        try {
            createRangeTableInTG(tableA, tgName);
            createRangeTableInExistingTG(tableB, tgName);
            runTwoWayAndAssertNoDeadlock(
                "ALTER TABLE " + tableA + " ADD COLUMN c1 INT",
                "ALTER TABLE " + tableB + " ADD COLUMN c2 INT",
                tableA, tableB);
        } finally {
            dropTableIfExists(tableA);
            dropTableIfExists(tableB);
            dropTableGroupIfExists(tgName);
        }
    }

    /**
     * 4. EXCHANGE PARTITION vs SPLIT PARTITION — Phase 1 = {src, tgt} vs {tg}.
     * Phase 2: src table is in the tablegroup, creating resource crossing.
     */
    @Test
    public void testDeadlock_ExchangeVsSplit() throws Exception {
        String tgName = BASE_TG + "_exch_split";
        String srcTable = BASE_TABLE + "_src";
        String tgtTable = BASE_TABLE + "_tgt";
        try {
            createRangeTableInTG(srcTable, tgName);
            createSingleTable(tgtTable);
            // Insert data into target so exchange has something to swap
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + tgtTable + " VALUES (1, 'a')");
            runTwoWayAndAssertNoDeadlock(
                "ALTER TABLE " + srcTable + " EXCHANGE PARTITION p1 WITH TABLE " + tgtTable,
                "ALTER TABLEGROUP " + tgName
                    + " SPLIT PARTITION p2 INTO (PARTITION p2a VALUES LESS THAN (150), PARTITION p2b VALUES LESS THAN (200))",
                srcTable, tgName);
        } finally {
            dropTableIfExists(srcTable);
            dropTableIfExists(tgtTable);
            dropTableGroupIfExists(tgName);
        }
    }

    /**
     * 6. RENAME TABLE vs ALTER TABLE — Phase 1 = {old, new} vs {old}.
     * Both lock the same table 'old', creating resource crossing.
     */
    @Test
    public void testDeadlock_RenameTableVsAlterTable() throws Exception {
        String tableName = BASE_TABLE + "_rn_old";
        String newName = BASE_TABLE + "_rn_new";
        try {
            createPartitionedTable(tableName, 4);
            runTwoWayAndAssertNoDeadlock(
                "RENAME TABLE " + tableName + " TO " + newName,
                "ALTER TABLE " + tableName + " ADD COLUMN c1 INT",
                tableName, tableName);
        } finally {
            dropTableIfExists(tableName);
            dropTableIfExists(newName);
        }
    }

    /**
     * 7. OPTIMIZE TABLE (multi-table) vs ALTER TABLE — Phase 1 = {t1, t2} vs {t2}.
     * t2 resource crossing.
     */
    @Test
    public void testDeadlock_OptimizeVsAlterTable() throws Exception {
        String table1 = BASE_TABLE + "_opt1";
        String table2 = BASE_TABLE + "_opt2";
        try {
            createPartitionedTable(table1, 4);
            createPartitionedTable(table2, 4);
            runTwoWayAndAssertNoDeadlock(
                "OPTIMIZE TABLE " + table1 + ", " + table2,
                "ALTER TABLE " + table2 + " ADD COLUMN c1 INT",
                table1, table2);
        } finally {
            dropTableIfExists(table1);
            dropTableIfExists(table2);
        }
    }

    /**
     * 8. Three-way: 3 tables in same TableGroup, concurrent ALTER TABLE ADD COLUMN.
     * Phase 1 = {tA}, {tB}, {tC}. Phase 2 all lock TG.
     */
    @Test
    public void testDeadlock_ThreeWay_SameTableGroup() throws Exception {
        String tgName = BASE_TG + "_3way";
        String tableA = BASE_TABLE + "_3a";
        String tableB = BASE_TABLE + "_3b";
        String tableC = BASE_TABLE + "_3c";
        try {
            createRangeTableInTG(tableA, tgName);
            createRangeTableInExistingTG(tableB, tgName);
            createRangeTableInExistingTG(tableC, tgName);

            final int DDL_COUNT = 3;
            ExecutorService pool = Executors.newFixedThreadPool(DDL_COUNT);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(DDL_COUNT);
            AtomicInteger successCount = new AtomicInteger(0);
            java.util.List<String> errors =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());

            String[] ddls = {
                "ALTER TABLE " + tableA + " ADD COLUMN c1 INT",
                "ALTER TABLE " + tableB + " ADD COLUMN c2 INT",
                "ALTER TABLE " + tableC + " ADD COLUMN c3 INT",
            };

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
            org.junit.Assert.assertTrue("DDL did not complete in time — possible permanent deadlock",
                done.await(120, TimeUnit.SECONDS));
            pool.shutdown();

            org.junit.Assert.assertTrue(
                "At least one DDL should succeed, but only " + successCount.get()
                    + " succeeded. Errors: " + errors,
                successCount.get() >= 1);

            assertNoInitialRecords(tableA);
            assertNoInitialRecords(tableB);
            assertNoInitialRecords(tableC);
        } finally {
            dropTableIfExists(tableA);
            dropTableIfExists(tableB);
            dropTableIfExists(tableC);
            dropTableGroupIfExists(tgName);
        }
    }
}
