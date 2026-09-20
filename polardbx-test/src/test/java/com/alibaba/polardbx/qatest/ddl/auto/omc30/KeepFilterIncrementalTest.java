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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Integration tests for REBUILD_TABLE_KEEP_FILTER with incremental DML during OMC 3.0.
 * Uses million-level data to ensure changeset catchup is triggered,
 * covering OmcChangeSetApplier.generateSql branches:
 * - REPLACE SELECT with keepFilter (insert=true, keepFilter not empty)
 * - DELETE with FORCE INDEX (insert=false, connection=null, non-lock catchup)
 * - DELETE without FORCE INDEX (insert=false, connection!=null, lock-table catchup)
 */
@CdcIgnore(ignoreReason = "REBUILD_TABLE_KEEP_FILTER 不产生逐行 DELETE binlog，CDC 下游数据校验会不一致")
@NotThreadSafe
public class KeepFilterIncrementalTest extends DDLBaseNewDBTestCase {

    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());

    private static final int TOTAL_ROWS = 1_000_000;
    private static final int BATCH_SIZE = 5000;

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

    /**
     * Load million-level data: rows with id in [0, TOTAL_ROWS).
     * balance = id % 100, so ~99% rows have balance != 0, ~1% have balance = 0.
     */
    private void loadBulkData(String tableName, Connection conn) throws Exception {
        String insertSql = String.format(
            "insert into %s(id, balance, name) values(?, ?, ?)", tableName);
        for (int offset = 0; offset < TOTAL_ROWS; offset += BATCH_SIZE) {
            List<List<Object>> params = new ArrayList<>(BATCH_SIZE);
            for (int j = 0; j < BATCH_SIZE; j++) {
                int id = offset + j;
                List<Object> param = new ArrayList<>(3);
                param.add(id);
                param.add(id % 100);
                param.add("row_" + id);
                params.add(param);
            }
            JdbcUtil.updateDataBatch(conn, insertSql, params);
        }
    }

    /**
     * Test 1: Incremental INSERT during OMC.
     * Inserts rows during DDL suspension window; some match filter, some don't.
     * Verifies that only matching rows exist after OMC.
     */
    @Test
    public void testIncrementalInsertWithKeepFilter() throws Exception {
        String tableName = "kf_incr_bulk_ins" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "create table %s(id int primary key, balance int, name varchar(32)) "
                + "partition by hash(`id`) partitions 3", tableName));

        loadBulkData(tableName, tddlConnection);

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

        while (!ddlStarted.get()) {
            Thread.sleep(100);
        }
        Thread.sleep(5000);

        // Insert a row matching filter (balance=999)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values(%d, %d, 'match_insert')",
                tableName, TOTAL_ROWS + 1, 999));
        // Insert a row NOT matching filter (balance=0)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values(%d, %d, 'nomatch_insert')",
                tableName, TOTAL_ROWS + 2, 0));

        ddlThread.join(120000);

        // Row with balance=999 should exist
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = %d", tableName, TOTAL_ROWS + 1));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Inserted row matching filter should be kept", 1, rs.getInt(1));

        // Row with balance=0 should NOT exist
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = %d", tableName, TOTAL_ROWS + 2));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Inserted row not matching filter should be removed", 0, rs.getInt(1));
    }

    /**
     * Test 2: Incremental UPDATE during OMC — row changes from match to not-match.
     * Updates a row's balance from non-zero to 0 during DDL.
     * Verifies the row is removed from target after OMC.
     */
    @Test
    public void testIncrementalUpdateMatchToNotMatch() throws Exception {
        String tableName = "kf_incr_upd_m2n" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "create table %s(id int primary key, balance int, name varchar(32)) "
                + "partition by hash(`id`) partitions 3", tableName));

        loadBulkData(tableName, tddlConnection);

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

        while (!ddlStarted.get()) {
            Thread.sleep(100);
        }
        Thread.sleep(5000);

        // Update row id=1: balance 1%100=1 → 0 (no longer matches filter)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("update %s set balance = 0 where id = 1", tableName));

        ddlThread.join(120000);

        // Row id=1 should not exist (balance=0, doesn't match keepFilter)
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Row updated to not match filter should be removed", 0, rs.getInt(1));

        // Control: row id=2 (balance=2) should still exist
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = 2", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Control row should still exist", 1, rs.getInt(1));
    }

    /**
     * Test 3: Incremental UPDATE during OMC — row changes from not-match to match.
     * Updates a row's balance from 0 to non-zero during DDL.
     * Verifies the row is present in target after OMC.
     */
    @Test
    public void testIncrementalUpdateNotMatchToMatch() throws Exception {
        String tableName = "kf_incr_upd_n2m" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "create table %s(id int primary key, balance int, name varchar(32)) "
                + "partition by hash(`id`) partitions 3", tableName));

        loadBulkData(tableName, tddlConnection);

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

        while (!ddlStarted.get()) {
            Thread.sleep(100);
        }
        Thread.sleep(5000);

        // Update row id=100: balance 100%100=0 → 999 (now matches filter)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("update %s set balance = 999 where id = 100", tableName));

        ddlThread.join(120000);

        // Row id=100 should exist with balance=999
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select balance from %s where id = 100", tableName));
        Assert.assertTrue("Row updated to match filter should be kept", rs.next());
        Assert.assertEquals(999, rs.getInt("balance"));
    }

    /**
     * Test 4: Incremental DELETE during OMC.
     * Deletes a row that matches filter during DDL.
     * Verifies the row is gone after OMC.
     */
    @Test
    public void testIncrementalDeleteMatchingRow() throws Exception {
        String tableName = "kf_incr_del_match" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "create table %s(id int primary key, balance int, name varchar(32)) "
                + "partition by hash(`id`) partitions 3", tableName));

        loadBulkData(tableName, tddlConnection);

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

        while (!ddlStarted.get()) {
            Thread.sleep(100);
        }
        Thread.sleep(5000);

        // Delete row id=1 (balance=1, matches filter)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("delete from %s where id = 1", tableName));

        ddlThread.join(120000);

        // Row id=1 should not exist
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Deleted matching row should not exist", 0, rs.getInt(1));
    }

    /**
     * Test 5: Mixed incremental DML during OMC.
     * Executes INSERT, UPDATE, DELETE concurrently during DDL suspension.
     * Verifies all operations are correctly handled by changeset with keepFilter.
     */
    @Test
    public void testMixedIncrementalDml() throws Exception {
        String tableName = "kf_incr_mixed" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "create table %s(id int primary key, balance int, name varchar(32)) "
                + "partition by hash(`id`) partitions 3", tableName));

        loadBulkData(tableName, tddlConnection);

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

        while (!ddlStarted.get()) {
            Thread.sleep(100);
        }
        Thread.sleep(5000);

        // 1. INSERT matching row
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values(%d, 888, 'mixed_ins_match')", tableName, TOTAL_ROWS + 10));
        // 2. INSERT non-matching row
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("insert into %s values(%d, 0, 'mixed_ins_nomatch')", tableName, TOTAL_ROWS + 11));
        // 3. UPDATE match → not match
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("update %s set balance = 0 where id = 3", tableName));
        // 4. UPDATE not match → match (id=200, balance 200%100=0 → 777)
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("update %s set balance = 777 where id = 200", tableName));
        // 5. DELETE matching row
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("delete from %s where id = 5", tableName));

        ddlThread.join(120000);

        // 1. INSERT matching → should exist
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = %d", tableName, TOTAL_ROWS + 10));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Mixed: inserted matching row should exist", 1, rs.getInt(1));

        // 2. INSERT non-matching → should NOT exist
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = %d", tableName, TOTAL_ROWS + 11));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Mixed: inserted non-matching row should not exist", 0, rs.getInt(1));

        // 3. UPDATE match → not match → should NOT exist
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = 3", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Mixed: row updated to not match should not exist", 0, rs.getInt(1));

        // 4. UPDATE not match → match → should exist with balance=777
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select balance from %s where id = 200", tableName));
        Assert.assertTrue("Mixed: row updated to match should exist", rs.next());
        Assert.assertEquals("Mixed: balance should be 777", 777, rs.getInt("balance"));

        // 5. DELETE matching → should NOT exist
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where id = 5", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Mixed: deleted matching row should not exist", 0, rs.getInt(1));
    }

    /**
     * Test 6: Verify bulk data integrity after OMC with keepFilter.
     * After OMC, all rows with balance != 0 should be present,
     * and all rows with balance = 0 should be absent.
     */
    @Test
    public void testBulkDataIntegrityWithKeepFilter() throws Exception {
        String tableName = "kf_bulk_integrity" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "create table %s(id int primary key, balance int, name varchar(32)) "
                + "partition by hash(`id`) partitions 3", tableName));

        loadBulkData(tableName, tddlConnection);

        // Count rows before OMC
        int totalBefore = 0;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s", tableName));
        Assert.assertTrue(rs.next());
        totalBefore = rs.getInt(1);

        // Count rows with balance != 0 before OMC
        int matchBefore = 0;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where balance != 0", tableName));
        Assert.assertTrue(rs.next());
        matchBefore = rs.getInt(1);

        // Count rows with balance = 0 before OMC
        int noMatchBefore = 0;
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where balance = 0", tableName));
        Assert.assertTrue(rs.next());
        noMatchBefore = rs.getInt(1);

        String hint = buildCmdExtra(
            "FORCE_USING_OMC_30=TRUE",
            "REBUILD_TABLE_KEEP_FILTER=\"balance != 0\"");

        String alterSql = hint + buildAlterTableEngineOmc(tableName);
        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);

        // After OMC: total should equal matchBefore
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Total rows after OMC should equal match count before",
            matchBefore, rs.getInt(1));

        // No rows with balance = 0 should remain
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("select count(*) from %s where balance = 0", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("No rows with balance=0 should remain after OMC", 0, rs.getInt(1));

        System.out.println(String.format(
            "Bulk integrity: total=%d, match=%d, nomatch=%d, after=%d",
            totalBefore, matchBefore, noMatchBefore, matchBefore));
    }
}
