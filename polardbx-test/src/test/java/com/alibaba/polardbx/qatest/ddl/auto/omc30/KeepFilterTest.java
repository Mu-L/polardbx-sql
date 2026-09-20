package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Integration tests for REBUILD_TABLE_KEEP_FILTER feature in OMC 3.0.
 * Tests cover:
 * - Backfill: only rows matching keep filter are migrated
 * - Changeset catchup: incremental INSERT/UPDATE/DELETE with filter
 * - Validation: illegal filter expressions are rejected
 */
@CdcIgnore(ignoreReason = "REBUILD_TABLE_KEEP_FILTER 不产生逐行 DELETE binlog，CDC 下游数据校验会不一致")
@NotThreadSafe
public class KeepFilterTest extends DDLBaseNewDBTestCase {

    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/ ";
    }

    private static String buildAlterTableEngineOmc(String tableName) {
        return String.format("ALTER TABLE %s ENGINE=InnoDB, ALGORITHM=OMC", tableName);
    }

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    // ==================== Backfill (存量) Tests ====================

    /**
     * Basic backfill test: only rows matching keep filter are retained.
     * Table has rows with balance=0 and balance!=0, only balance!=0 should remain.
     */
    @Test
    public void testBackfillKeepFilterBasic() {
        String tableName = "kf_backfill_basic" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int, name varchar(32)) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Insert rows: some with balance=0, some with balance!=0
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (1, 0, 'zero_a')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (2, 100, 'hundred')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (3, 0, 'zero_b')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (4, 200, 'two_hundred')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (5, 0, 'zero_c')", tableName));

        // ALTER TABLE ENGINE=InnoDB with keep filter
        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"balance != 0\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);

        // Verify: only rows with balance!=0 should remain
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select id, balance, name from %s order by id", tableName));
        try {
            List<String> results = new ArrayList<>();
            while (rs.next()) {
                results.add(rs.getInt("id") + ":" + rs.getInt("balance") + ":" + rs.getString("name"));
            }
            Assert.assertEquals("Should have 2 rows remaining", 2, results.size());
            Assert.assertEquals("2:100:hundred", results.get(0));
            Assert.assertEquals("4:200:two_hundred", results.get(1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Backfill test: all rows match keep filter, no data should be lost.
     */
    @Test
    public void testBackfillKeepFilterAllMatch() {
        String tableName = "kf_backfill_all_match" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // All rows have balance != 0
        for (int i = 1; i <= 10; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("insert into %s values (%d, %d)", tableName, i, i * 10));
        }

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"balance != 0\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);

        // Verify: all 10 rows should remain
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s", tableName));
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("All rows should be kept", 10, rs.getInt(1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Backfill test: no rows match keep filter, all data should be cleaned.
     */
    @Test
    public void testBackfillKeepFilterNoneMatch() {
        String tableName = "kf_backfill_none_match" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // All rows have balance = 0
        for (int i = 1; i <= 5; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("insert into %s values (%d, 0)", tableName, i));
        }

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"balance != 0\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);

        // Verify: no rows should remain
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s", tableName));
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("No rows should be kept", 0, rs.getInt(1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Backfill test: keep filter with string comparison.
     */
    @Test
    public void testBackfillKeepFilterStringCondition() {
        String tableName = "kf_backfill_str" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, status varchar(32)) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (1, 'active')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (2, 'deleted')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (3, 'active')", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (4, 'pending')", tableName));

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"status = 'active'\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);

        // Verify: only 'active' rows remain
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select id from %s order by id", tableName));
        try {
            List<Integer> results = new ArrayList<>();
            while (rs.next()) {
                results.add(rs.getInt(1));
            }
            Assert.assertEquals("Should have 2 rows remaining", 2, results.size());
            Assert.assertEquals(Integer.valueOf(1), results.get(0));
            Assert.assertEquals(Integer.valueOf(3), results.get(1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ==================== Changeset (增量) Tests ====================

    /**
     * Incremental INSERT during OMC: new row matches filter → should be kept.
     */
    @Test
    public void testIncrementalInsertMatchesFilter() throws Exception {
        String tableName = "kf_incr_insert_match" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Pre-fill some rows
        for (int i = 1; i <= 5; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("insert into %s values (%d, %d)", tableName, i, i * 100));
        }

        // Concurrent DDL + DML
        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"balance != 0\"");

        AtomicBoolean ddlDone = new AtomicBoolean(false);

        Callable<Void> ddlTask = () -> {
            Connection conn = getPolardbxConnection();
            try {
                String alterSql = hint + buildAlterTableEngineOmc(tableName);
                execDdlWithRetry(tddlDatabase1, tableName, alterSql, conn);
            } finally {
                ddlDone.set(true);
                conn.close();
            }
            return null;
        };

        Callable<Void> dmlTask = () -> {
            Connection conn = getPolardbxConnection();
            try {
                // Insert a row with balance != 0 → should be kept
                Thread.sleep(2000);
                JdbcUtil.executeUpdateSuccess(conn,
                    String.format("insert into %s values (100, 999)", tableName));
            } catch (Exception e) {
                // DDL may block DML, retry
                System.out.println("DML error (expected during DDL): " + e.getMessage());
            } finally {
                conn.close();
            }
            return null;
        };

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<Void> ddlFuture = pool.submit(ddlTask);
        Future<Void> dmlFuture = pool.submit(dmlTask);

        try {
            ddlFuture.get();
            dmlFuture.get();
        } finally {
            pool.shutdown();
        }

        // Verify: row 100 with balance=999 should exist
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = 100 and balance = 999", tableName));
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("Inserted row matching filter should be kept", 1, rs.getInt(1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Incremental UPDATE during OMC: row changes from matching to not matching filter.
     * Uses FP_OMC_BEFORE_CUTOVER_SUSPEND to ensure DML happens during DDL execution.
     * After OMC, the row should be removed from target (balance=0 doesn't match filter).
     */
    @Test
    public void testIncrementalUpdateFromMatchToNotMatch() throws Exception {
        String tableName = "kf_incr_upd_match2not" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Row 1: balance=100 (matches filter)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (1, 100)", tableName));
        // Row 2: balance=200 (matches filter, control row)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (2, 200)", tableName));

        // FP_OMC_BEFORE_CUTOVER_SUSPEND suspends DDL before cutover for 15s,
        // giving the DML thread time to execute during OMC changeset catchup phase.
        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"balance != 0\"",
            "FP_OMC_BEFORE_CUTOVER_SUSPEND=15000");

        AtomicBoolean ddlStarted = new AtomicBoolean(false);

        Thread ddlThread = new Thread(() -> {
            try (Connection conn = getPolardbxConnection()) {
                ddlStarted.set(true);
                String alterSql = hint + buildAlterTableEngineOmc(tableName);
                JdbcUtil.executeUpdateSuccess(conn, alterSql);
            } catch (Exception e) {
                System.out.println("DDL error: " + e.getMessage());
            }
        });
        ddlThread.start();

        // Wait for DDL to start, then execute DML during the suspend window
        while (!ddlStarted.get()) {
            Thread.sleep(100);
        }
        // Sleep to let DDL progress past backfill into the suspend window
        Thread.sleep(5000);

        // Update row 1: balance 100 → 0 (no longer matches filter)
        // This DML happens during DDL execution and will be captured by changeset.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("update %s set balance = 0 where id = 1", tableName));

        ddlThread.join(60000);

        // Verify: row 1 should no longer exist (balance=0, doesn't match keepFilter)
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Row updated to not match filter should be removed", 0, rs.getInt(1));
        // Row 2 (control) should still exist
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = 2", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Control row should still exist", 1, rs.getInt(1));
    }

    /**
     * Incremental UPDATE during OMC: row changes from not matching to matching filter.
     * Uses FP_OMC_BEFORE_CUTOVER_SUSPEND to ensure DML happens during DDL execution.
     * After OMC, the row should be present in target (balance=100 matches filter).
     */
    @Test
    public void testIncrementalUpdateFromNotMatchToMatch() throws Exception {
        String tableName = "kf_incr_upd_not2match" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Row 1: balance=0 (does not match filter — backfill will skip it)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (1, 0)", tableName));
        // Row 2: balance=100 (matches filter, control row)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (2, 100)", tableName));

        // FP_OMC_BEFORE_CUTOVER_SUSPEND suspends DDL before cutover for 15s,
        // giving the DML thread time to execute during OMC changeset catchup phase.
        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"balance != 0\"",
            "FP_OMC_BEFORE_CUTOVER_SUSPEND=15000");

        AtomicBoolean ddlStarted = new AtomicBoolean(false);

        Thread ddlThread = new Thread(() -> {
            try (Connection conn = getPolardbxConnection()) {
                ddlStarted.set(true);
                String alterSql = hint + buildAlterTableEngineOmc(tableName);
                JdbcUtil.executeUpdateSuccess(conn, alterSql);
            } catch (Exception e) {
                System.out.println("DDL error: " + e.getMessage());
            }
        });
        ddlThread.start();

        // Wait for DDL to start, then execute DML during the suspend window
        while (!ddlStarted.get()) {
            Thread.sleep(100);
        }
        // Sleep to let DDL progress past backfill into the suspend window
        Thread.sleep(5000);

        // Update row 1: balance 0 → 100 (now matches filter)
        // This DML happens during DDL execution and will be captured by changeset.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("update %s set balance = 100 where id = 1", tableName));

        ddlThread.join(60000);

        // Verify: row 1 should exist with balance=100 (changeset INSERT applied with keepFilter)
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select balance from %s where id = 1", tableName));
        Assert.assertTrue("Row updated to match filter should be kept", rs.next());
        Assert.assertEquals("balance should be 100", 100, rs.getInt("balance"));
        // Row 2 (control) should also exist
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = 2", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Control row should still exist", 1, rs.getInt(1));
    }

    /**
     * Incremental DELETE during OMC: row that matches filter is deleted.
     * After OMC, the row should not exist in target.
     */
    @Test
    public void testIncrementalDelete() throws Exception {
        String tableName = "kf_incr_delete" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Row with balance != 0 (matches filter)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (1, 100)", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (2, 200)", tableName));

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"balance != 0\"");

        Callable<Void> ddlTask = () -> {
            Connection conn = getPolardbxConnection();
            try {
                String alterSql = hint + buildAlterTableEngineOmc(tableName);
                execDdlWithRetry(tddlDatabase1, tableName, alterSql, conn);
            } finally {
                conn.close();
            }
            return null;
        };

        Callable<Void> dmlTask = () -> {
            Connection conn = getPolardbxConnection();
            try {
                // Delete row 1 during OMC
                Thread.sleep(2000);
                JdbcUtil.executeUpdateSuccess(conn,
                    String.format("delete from %s where id = 1", tableName));
            } catch (Exception e) {
                System.out.println("DML error (expected during DDL): " + e.getMessage());
            } finally {
                conn.close();
            }
            return null;
        };

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<Void> ddlFuture = pool.submit(ddlTask);
        Future<Void> dmlFuture = pool.submit(dmlTask);

        try {
            ddlFuture.get();
            dmlFuture.get();
        } finally {
            pool.shutdown();
        }

        // Verify: row 1 should be gone, row 2 should remain
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select id from %s order by id", tableName));
        try {
            List<Integer> results = new ArrayList<>();
            while (rs.next()) {
                results.add(rs.getInt(1));
            }
            Assert.assertEquals("Should have 1 row remaining", 1, results.size());
            Assert.assertEquals(Integer.valueOf(2), results.get(0));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ==================== Validation Tests ====================

    /**
     * Filter with subquery should be rejected.
     */
    @Test
    public void testValidationRejectSubquery() {
        String tableName = "kf_val_subquery" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            String.format("REBUILD_TABLE_KEEP_FILTER=\"id in (select id from %s)\"", tableName));
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alterSql, "subqueries are not allowed");
    }

    /**
     * Filter with non-deterministic function NOW() should be rejected.
     */
    @Test
    public void testValidationRejectNow() {
        String tableName = "kf_val_now" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, gmt_created timestamp) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"gmt_created > NOW()\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alterSql, "non-deterministic function");
    }

    /**
     * Filter with non-deterministic function RAND() should be rejected.
     */
    @Test
    public void testValidationRejectRand() {
        String tableName = "kf_val_rand" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"balance > RAND() * 100\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alterSql, "non-deterministic function");
    }

    /**
     * Filter with non-existent column should be rejected.
     */
    @Test
    public void testValidationRejectNonExistentColumn() {
        String tableName = "kf_val_no_col" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"nonexistent_col > 0\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alterSql, "does not exist");
    }

    /**
     * Filter with syntax error should be rejected.
     */
    @Test
    public void testValidationRejectSyntaxError() {
        String tableName = "kf_val_syntax" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"balance !! 0\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alterSql, "Invalid REBUILD_TABLE_KEEP_FILTER");
    }

    /**
     * Filter with UNIX_TIMESTAMP(constant) should be allowed (deterministic with arg).
     */
    @Test
    public void testValidationAllowUnixTimestampWithArg() {
        String tableName = "kf_val_unix_ts" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, gmt_created int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // UNIX_TIMESTAMP with a constant arg is deterministic
        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"gmt_created > UNIX_TIMESTAMP('2024-01-01')\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);
    }

    /**
     * Filter with EXISTS subquery should be rejected.
     */
    @Test
    public void testValidationRejectExists() {
        String tableName = "kf_val_exists" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            String.format("REBUILD_TABLE_KEEP_FILTER=\"exists (select 1 from %s where id = 1)\"", tableName));
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alterSql, "subqueries are not allowed");
    }

    /**
     * Filter with UNIX_TIMESTAMP() without args should be rejected (non-deterministic).
     */
    @Test
    public void testValidationRejectUnixTimestampNoArg() {
        String tableName = "kf_val_unix_noarg" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, gmt_created int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // UNIX_TIMESTAMP() without args is non-deterministic
        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"gmt_created > UNIX_TIMESTAMP()\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alterSql, "non-deterministic function");
    }

    /**
     * Filter with SYSDATE() should be rejected (non-deterministic).
     */
    @Test
    public void testValidationRejectSysdate() {
        String tableName = "kf_val_sysdate" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, gmt_created timestamp) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"gmt_created > SYSDATE()\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alterSql, "non-deterministic function");
    }

    /**
     * Filter with UUID() should be rejected (non-deterministic).
     */
    @Test
    public void testValidationRejectUuid() {
        String tableName = "kf_val_uuid" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, uid varchar(64)) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"uid != UUID()\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alterSql, "non-deterministic function");
    }

    /**
     * Filter with compound AND/OR conditions should work correctly.
     */
    @Test
    public void testBackfillKeepFilterCompoundCondition() {
        String tableName = "kf_backfill_compound" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int, status varchar(32)) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Insert rows with various combinations
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (1, 0, 'active')", tableName));    // balance=0, active -> filtered
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (2, 100, 'deleted')", tableName)); // balance!=0, deleted -> filtered
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (3, 100, 'active')", tableName));  // balance!=0, active -> kept
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values (4, 200, 'active')", tableName));  // balance!=0, active -> kept

        // Keep filter: balance != 0 AND status = 'active'
        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"balance != 0 AND status = 'active'\"");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);

        // Verify: only rows 3 and 4 should remain
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select id from %s order by id", tableName));
        try {
            List<Integer> results = new ArrayList<>();
            while (rs.next()) {
                results.add(rs.getInt(1));
            }
            Assert.assertEquals("Should have 2 rows remaining", 2, results.size());
            Assert.assertEquals(Integer.valueOf(3), results.get(0));
            Assert.assertEquals(Integer.valueOf(4), results.get(1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * No filter (empty) should work normally - all rows kept.
     */
    @Test
    public void testNoFilterAllRowsKept() {
        String tableName = "kf_no_filter" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, balance int) partition by hash(`id`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 1; i <= 5; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("insert into %s values (%d, 0)", tableName, i));
        }

        // ALTER TABLE without keep filter
        String hint = buildCmdExtra("FORCE_USING_OMC_30=TRUE");
        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);

        // All 5 rows should remain
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s", tableName));
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("All rows should be kept without filter", 5, rs.getInt(1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
