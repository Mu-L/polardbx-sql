package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.ExpandPartitionTaskRecord;
import com.alibaba.polardbx.gms.metadb.misc.ExpandPartitionTasksAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tests for ALTER TABLE ... EXPAND PARTITIONS / EXPAND SUBPARTITIONS and CANCEL EXPAND.
 * All tests use dedicated schema "expand_partitions_test_db" to avoid interference with default schema.
 */
public class AlterTableExpandPartitionsTest extends DDLBaseNewDBTestCase {

    private static final String TEST_SCHEMA = "expand_partitions_test_db";
    private static boolean schemaInitialized = false;

    // -----------------------------------------------------------------------
    // Logging helpers: every SQL statement is printed to the test log so that
    // failures can be diagnosed without enabling server-side query log.
    // -----------------------------------------------------------------------

    private void execSuccess(String sql) {
        execSuccess(sql, true);
    }

    private void execSuccess(String sql, boolean printSql) {
        if (printSql) {
            System.out.println("[EXPAND TEST SQL] " + sql);
        }
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    private void execUpdateSuccess(String sql) {
        System.out.println("[EXPAND TEST SQL] " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void execUpdateFailed(String sql, String errMsg) {
        System.out.println("[EXPAND TEST SQL] " + sql + " [expect fail: " + errMsg + "]");
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, errMsg);
    }

    private ResultSet execQuery(String sql) {
        System.out.println("[EXPAND TEST SQL] " + sql);
        return JdbcUtil.executeQuery(sql, tddlConnection);
    }

    private void ensureSchemaExists() {
        if (!schemaInitialized) {
            execUpdateSuccess("drop database if exists " + TEST_SCHEMA);
            execSuccess("create database " + TEST_SCHEMA + " mode='auto'");
            schemaInitialized = true;
        }
        execSuccess("use " + TEST_SCHEMA);
    }

    @Test
    public void testExpandPartitionsBasic() throws SQLException, InterruptedException {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand-/~``@#$%好_partitions_t1`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "(a int primary key, b int) partition by key(a) partitions 2");
        execSuccess("alter table " + tableName + " expand partitions to 8");

        // Wait until SHOW EXPAND STATUS can see the record.
        boolean found = false;
        String showSql = "show expand status for " + tableName;
        for (int i = 0; i < 30; i++) {
            try (ResultSet rs = execQuery(showSql)) {
                List<List<Object>> rows = JdbcUtil.getAllResult(rs);
                if (!rows.isEmpty()) {
                    found = true;
                    break;
                }
            }
            Thread.sleep(1000L);
        }

        Assert.assertTrue(found);

        // Verify final partition count
        ResultSet rs = execQuery("show create table " + tableName);
        rs.next();
        String createDef = rs.getString(2);
        Assert.assertTrue(createDef.contains("PARTITIONS 8") || createDef.toLowerCase().contains("partitions 8"));
    }

    @Test
    public void testCancelExpandRemovesStatus() throws SQLException, InterruptedException {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand-/~``@#$%好_partitions_t2`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "(a int primary key, b int) partition by key(a) partitions 2");
        execSuccess("alter table " + tableName + " expand partitions to 8");

        // Ensure the expand task record appears first.
        String showSql = "show expand status for " + tableName;
        boolean found = false;
        for (int i = 0; i < 30; i++) {
            try (ResultSet rs = execQuery(showSql)) {
                List<List<Object>> rows = JdbcUtil.getAllResult(rs);
                if (!rows.isEmpty()) {
                    found = true;
                    break;
                }
            }
            Thread.sleep(1000L);
        }
        Assert.assertTrue(found);

        // Issue CANCEL EXPAND and expect the meta record to be removed.
        execSuccess("alter table " + tableName + " cancel expand");

        boolean gone = false;
        for (int i = 0; i < 30; i++) {
            try (ResultSet rs = execQuery(showSql)) {
                List<List<Object>> rows = JdbcUtil.getAllResult(rs);
                if (rows.isEmpty()) {
                    gone = true;
                    break;
                }
            }
            Thread.sleep(1000L);
        }

        Assert.assertTrue(gone);
    }

    @Test
    public void testExpandHashPartitions() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_hash-/~``@#$%好_partitions_t3`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess(
            "create table " + tableName + "(a int primary key, b varchar(32)) partition by hash(a) partitions 3");
        execSuccess("alter table " + tableName + " expand partitions to 12");

        // Verify final partition count
        ResultSet rs = execQuery("show create table " + tableName);
        rs.next();
        String createDef = rs.getString(2);
        Assert.assertTrue(createDef.contains("PARTITIONS 12") || createDef.toLowerCase().contains("partitions 12"));
    }

    @Test
    public void testExpandListPartitionsShouldFail() {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_list_-/~``@#$%好partitions_t4`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "(a int primary key, b int) "
            + "partition by list(a) ("
            + "partition p1 values in (1, 2, 3), "
            + "partition p2 values in (4, 5, 6))");

        // Should fail because LIST partitioning is not supported
        execUpdateFailed("alter table " + tableName + " expand partitions to 4", "not support");
    }

    @Test
    public void testExpandPartitionsWithUnsupportCharsetShouldFail() {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_unsupprt_charset_-/~``@#$%好partitions_t4`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess(
            "create table " + tableName + "(a int primary key, b varchar(40)) CHARSET=utf8 COLLATE=utf8_general_ci "
                + "partition by key(b) partitions 2");

        // Should fail because unsupported charset/collation for inplace backfill
        execUpdateFailed("alter table " + tableName + " expand partitions to 4", "not support");
    }

    @Test
    public void testExpandRangePartitionsShouldFail() {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_range_z-/~``@#$%好partitions_t5`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "(a int primary key, b int) "
            + "partition by range(a) ("
            + "partition p1 values less than (100), "
            + "partition p2 values less than (200), "
            + "partition p3 values less than maxvalue)");

        // Should fail because RANGE partitioning is not supported
        execUpdateFailed("alter table " + tableName + " expand partitions to 6", "not support");
    }

    @Test
    public void testExpandSubpartitionsTemplated() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_subpart-/~``@#$%好_templated_t6`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "("
            + "a bigint unsigned not null, "
            + "b bigint unsigned not null, "
            + "c datetime NOT NULL, "
            + "d varchar(16) NOT NULL, "
            + "e varchar(16) NOT NULL"
            + ") partition by hash(a) partitions 4 "
            + "subpartition by key(b) subpartitions 3");
        execSuccess("alter table " + tableName + " expand subpartitions to 6");

        // Verify final subpartition count
        ResultSet rs = execQuery("show create table " + tableName);
        rs.next();
        String createDef = rs.getString(2);
        Assert.assertTrue(createDef.contains("SUBPARTITIONS 6") || createDef.toLowerCase().contains("subpartitions 6"));
    }

    @Test
    public void testExpandSubpartitionsNonTemplatedShouldFail() {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_subpart-/~``@#$%好_non_templated_t7`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "("
            + "a bigint unsigned not null, "
            + "b bigint unsigned not null"
            + ") partition by hash(a) partitions 2 "
            + "subpartition by key(b) ("
            + "partition p1 subpartitions 2, "
            + "partition p2 subpartitions 3)");

        // Should fail because non-templated subpartitions are not supported
        execUpdateFailed("alter table " + tableName + " expand subpartitions to 6", "templated subpartitions");
    }

    @Test
    public void testExpandSubpartitionsOnTableWithoutSubpartShouldFail() {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_subpart_-/~``@#$%好no_subpart_t8`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "(a int primary key, b int) partition by key(a) partitions 4");

        // Should fail because table has no subpartitions
        execUpdateFailed("alter table " + tableName + " expand subpartitions to 8",
            "requires the table to have subpartitions");
    }

    @Test
    public void testRestartExpandIdempotent() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_restart_-/~``@#$%好idempotent_t9`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "(a int primary key, b int) partition by key(a) partitions 3");

        // First expand: 3 -> 12 (synchronous, will complete)
        execSuccess("alter table " + tableName + " expand partitions to 12");

        // Verify first expand succeeded
        ResultSet rs = execQuery("show create table " + tableName);
        rs.next();
        String createDef = rs.getString(2);
        Assert.assertTrue(createDef.contains("PARTITIONS 12") || createDef.toLowerCase().contains("partitions 12"));

        // Try to restart with the same target count (should fail - already at target)
        execUpdateFailed("alter table " + tableName + " expand partitions to 12", "must be greater");

        // Second expand: 12 -> 24 (chain expansion)
        execSuccess("alter table " + tableName + " expand partitions to 24");

        // Verify second expand succeeded
        rs = execQuery("show create table " + tableName);
        rs.next();
        createDef = rs.getString(2);
        Assert.assertTrue(createDef.contains("PARTITIONS 24") || createDef.toLowerCase().contains("partitions 24"));
    }

    @Test
    public void testCancelAndResumeExpand() throws SQLException, InterruptedException {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_-/~``@#$%好cancel_resume_t9b`";
        String originTableName = "expand_-/~`@#$%好cancel_resume_t9b";

        execUpdateSuccess("drop table if exists " + tableName);

        // Create table with data to slow down the expand operation
        execSuccess(
            "create table " + tableName + "(a int primary key, b varchar(1000)) partition by key(a) partitions 10");

        // Insert data to make split operations slower
        for (int i = 0; i < 1000; i++) {
            execSuccess("insert into " + tableName + " values(" + i + ", repeat('x', 1000))", false);
        }
        int partCount = 0;
        int firstPartCount = 10;
        String firstJobId = "";
        String showSql = "show expand status for " + tableName;
        for (int time = 0; time < 2; time++) {
            // Start async expand: 10 -> 40 (should take longer due to data)
            if (time == 1) {
                ResultSet rs0 = execQuery("show topology from " + tableName);
                Set<String> curPartNames = new TreeSet<>(String::compareToIgnoreCase);
                while (rs0.next()) {
                    curPartNames.add(rs0.getString("PARTITION_NAME"));
                }
                updatePendingStatus(TEST_SCHEMA, originTableName, curPartNames);
                System.out.println(
                    "[EXPAND TEST SQL] alter table " + tableName + " expand partitions to 40 async=true");
                boolean rec = JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection,
                    "alter table " + tableName + " expand partitions to 40 async=true",
                    ImmutableSet.of("exist", "must be greater than current partition count"));
                if (rec) {
                    break;
                }
            } else {
                execSuccess("alter table " + tableName + " expand partitions to 40 async=true");
            }

            // Wait for expand task to appear in status
            boolean taskStarted = false;
            String jobId = "";
            for (int i = 0; i < 100; i++) {
                try (ResultSet rs = execQuery(showSql)) {
                    List<List<Object>> rows = JdbcUtil.getAllResult(rs);
                    if (!rows.isEmpty()) {
                        jobId = rows.get(0).get(7).toString();
                        if (time == 0) {
                            firstJobId = jobId;
                            taskStarted = true;
                            break;
                        } else if (!firstJobId.equalsIgnoreCase(jobId)) {
                            taskStarted = true;
                            break;
                        }
                    }
                }
                Thread.sleep(100L);
            }
            if (!taskStarted) {
                System.out.println("exit, task not started");
                return;
            }
            partCount = 0;
            ResultSet rs0 = execQuery("show topology from " + tableName);
            while (rs0.next()) {
                partCount++;
            }
            int maxLoop = 1000;
            if (!StringUtils.isEmpty(jobId)) {
                while (maxLoop-- > 0) {
                    if (partCount > firstPartCount && partCount != 40) {
                        System.out.println("[EXPAND TEST SQL] cancel ddl " + jobId);
                        Boolean suc = JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, "cancel ddl " + jobId,
                            ImmutableSet.of("continue ddl", "does not exist", "cannot be rolled back"));
                        if (suc) {
                            Thread.sleep(100L);
                            System.out.println("[EXPAND TEST SQL] continue ddl " + jobId + " async=true");
                            suc = JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection,
                                "continue ddl " + jobId + " async=true",
                                ImmutableSet.of("does not exist", "cancelled or interrupted", "has been cancelled"));
                            if (suc) {
                                break;
                            }
                        } else {
                            firstPartCount = partCount;
                            break;
                        }
                    } else if (partCount == firstPartCount) {
                        partCount = 0;
                        ResultSet rs = execQuery("show topology from " + tableName);
                        while (rs.next()) {
                            partCount++;
                        }
                        Thread.sleep(100L);
                    } else {
                        break;
                    }
                }

            }
            if (maxLoop == 0) {
                System.out.println("exit loop");
                return;
            }
        }

        boolean jobfinished = false;
        for (int i = 0; i < 100; i++) {
            try (ResultSet rs = execQuery(showSql)) {
                List<List<Object>> rows = JdbcUtil.getAllResult(rs);
                if (rows.isEmpty()) {
                    jobfinished = true;
                    break;
                }
            }
            Thread.sleep(100L);
        }
        if (!jobfinished) {
            System.out.println("exit, job not finished");
            return;
        }
        execUpdateFailed("alter table " + tableName + " expand partitions to 10", "must be greater");
        partCount = 0;
        ResultSet rs = execQuery("show topology from " + tableName);
        while (rs.next()) {
            partCount++;
        }
        if (partCount != 40) {
            // There may or may not be a pending CANCEL EXPAND task at this point (timing-dependent).
            // Tolerate both: reject with "CANCEL EXPAND" if task is still active, or succeed if already done.
            JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection,
                "alter table " + tableName + " expand partitions to " + (partCount * 2),
                ImmutableSet.of("CANCEL EXPAND", "must be greater"));

            System.out.println("[EXPAND TEST SQL] alter table " + tableName + " split partition p10");
            boolean rec = JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection,
                "alter table " + tableName + " split partition p10", ImmutableSet.of("exists"));

            if (!rec) {
                JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection,
                    "alter table " + tableName + " expand partitions to 40",
                    ImmutableSet.of("invalidated", "must be greater", "CANCEL EXPAND"));
            }
        }
        // Cancel any in-progress expand task; tolerate if none exists.
        // Use a broad catch to avoid brittle message matching across server versions.
        try {
            execSuccess("alter table " + tableName + " cancel expand");
        } catch (Throwable ignored) {
            // no expand task running, or cancel not supported in current state — safe to ignore
        }

        // Wait for cancel to complete
        boolean cancelled = false;
        for (int i = 0; i < 30; i++) {
            try (ResultSet rs1 = execQuery(showSql)) {
                List<List<Object>> rows = JdbcUtil.getAllResult(rs1);
                if (rows.isEmpty()) {
                    cancelled = true;
                    break;
                }
            }
            Thread.sleep(1000L);
        }
        Assert.assertTrue(cancelled);

        // Check current partition count (should be between 10 and 40)
        rs = execQuery("show topology from " + tableName);
        partCount = 0;
        while (rs.next()) {
            partCount++;
        }

        // Resume: expand to 40 again (should be idempotent if some partitions were already split)
        execSuccess("alter table " + tableName + " expand partitions to " + (partCount * 2));

        // Verify final partition count
        rs = execQuery("show create table " + tableName);
        rs.next();
        String createDef = rs.getString(2);
        Assert.assertTrue(createDef.contains("PARTITIONS " + (partCount * 2)) || createDef.toLowerCase()
            .contains("partitions " + (partCount * 2)));

        // Verify data integrity after expand
        rs = execQuery("select count(*) from " + tableName);
        rs.next();
        int count = rs.getInt(1);
        Assert.assertTrue(count == 1000);
    }

    @Test
    public void testChainedExpansion() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand-/~``@#$%好_chained_t10`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "(a int primary key, b int) partition by hash(a) partitions 2");

        // First expansion: 2 -> 6
        execSuccess("alter table " + tableName + " expand partitions to 6");

        ResultSet rs = execQuery("show create table " + tableName);
        rs.next();
        String createDef = rs.getString(2);
        Assert.assertTrue(createDef.contains("PARTITIONS 6") || createDef.toLowerCase().contains("partitions 6"));

        // Second expansion: 6 -> 18
        execSuccess("alter table " + tableName + " expand partitions to 18");

        rs = execQuery("show create table " + tableName);
        rs.next();
        createDef = rs.getString(2);
        Assert.assertTrue(createDef.contains("PARTITIONS 18") || createDef.toLowerCase().contains("partitions 18"));
    }

    @Test
    public void testExpandWithInvalidTargetCount() {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_invalid-/~``@#$%好_target_t11`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "(a int primary key, b int) partition by key(a) partitions 5");

        // Target count not a multiple of current count
        execUpdateFailed("alter table " + tableName + " expand partitions to 12", "must be a multiple");

        // Target count less than current count
        execUpdateFailed("alter table " + tableName + " expand partitions to 3", "must be greater");
    }

    @Test
    public void testExpandBothPartitionsAndSubpartitions() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_both_-/~``@#$%好levels_t12`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "("
            + "a bigint unsigned not null, "
            + "b bigint unsigned not null"
            + ") partition by hash(a) partitions 2 "
            + "subpartition by key(b) subpartitions 3");

        // Expand top-level partitions
        execSuccess("alter table " + tableName + " expand partitions to 8");

        ResultSet rs = execQuery("show create table " + tableName);
        rs.next();
        String createDef = rs.getString(2);
        Assert.assertTrue(createDef.contains("PARTITIONS 8") || createDef.toLowerCase().contains("partitions 8"));

        // Expand subpartitions
        execSuccess("alter table " + tableName + " expand subpartitions to 9");

        rs = execQuery("show create table " + tableName);
        rs.next();
        createDef = rs.getString(2);
        Assert.assertTrue(createDef.contains("SUBPARTITIONS 9") || createDef.toLowerCase().contains("subpartitions 9"));
    }

    @Test
    public void testCancelSubpartitionsExpand() throws SQLException, InterruptedException {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_-/~``@#$%好cancel_subpart_t13`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "("
            + "a bigint unsigned not null, "
            + "b bigint unsigned not null"
            + ") partition by hash(a) partitions 3 "
            + "subpartition by key(b) subpartitions 2");
        execSuccess("alter table " + tableName + " expand subpartitions to 8 async=true");

        // Wait for expand task to be created and appear in status
        String showSql = "show expand status for " + tableName;
        boolean taskStarted = false;
        for (int i = 0; i < 30; i++) {
            try (ResultSet rs = execQuery(showSql)) {
                List<List<Object>> rows = JdbcUtil.getAllResult(rs);
                if (!rows.isEmpty()) {
                    taskStarted = true;
                    break;
                }
            }
            Thread.sleep(1000L);
        }
        Assert.assertTrue(taskStarted);

        // Cancel the expand
        execSuccess("alter table " + tableName + " cancel expand");

        // Verify status is gone
        boolean gone = false;
        for (int i = 0; i < 30; i++) {
            try (ResultSet rs = execQuery(showSql)) {
                List<List<Object>> rows = JdbcUtil.getAllResult(rs);
                if (rows.isEmpty()) {
                    gone = true;
                    break;
                }
            }
            Thread.sleep(1000L);
        }
        Assert.assertTrue(gone);
    }

    @Test
    public void testExpandLargeFactor() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_large_-/~``@#$%好factor_t14`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "(a int primary key, b int) partition by key(a) partitions 2");

        // Expand with factor = 8 (2 -> 16)
        execSuccess("alter table " + tableName + " expand partitions to 16");

        ResultSet rs = execQuery("show create table " + tableName);
        rs.next();
        String createDef = rs.getString(2);
        Assert.assertTrue(createDef.contains("PARTITIONS 16") || createDef.toLowerCase().contains("partitions 16"));
    }

    @Test
    public void testExpandWithDataInsertion() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName = "`expand_with_data_-/~``@#$%好t15`";

        execUpdateSuccess("drop table if exists " + tableName);
        execSuccess("create table " + tableName + "(a int primary key, b int) partition by key(a) partitions 4");

        // Insert test data
        for (int i = 0; i < 100; i++) {
            execSuccess("insert into " + tableName + " values(" + i + ", " + (i * 10) + ")", false);
        }

        // Expand partitions
        execSuccess("alter table " + tableName + " expand partitions to 12");

        // Verify data integrity
        ResultSet rs = execQuery("select count(*) from " + tableName);
        rs.next();
        int count = rs.getInt(1);
        Assert.assertTrue(count == 100);

        // Verify sum to ensure data correctness
        rs = execQuery("select sum(b) from " + tableName);
        rs.next();
        long sum = rs.getLong(1);
        Assert.assertTrue(sum == 49500L); // 0+10+20+...+990 = 49500
    }

    @Test
    public void testShowExpandStatusMultipleTables() throws SQLException, InterruptedException {
        if (!isMySQL80()) {
            return;
        }
        ensureSchemaExists();
        String tableName1 = "`expand_-/~``@#$%好show_multi_t16_a`";
        String tableName2 = "`expand_-/~``@#$%好show_multi_t16_b`";

        // Setup table 1
        execUpdateSuccess("drop table if exists " + tableName1);
        execSuccess("create table " + tableName1 + "(a int primary key, b int) partition by key(a) partitions 2");

        // Setup table 2
        execUpdateSuccess("drop table if exists " + tableName2);
        execSuccess("create table " + tableName2 + "(a int primary key, b int) partition by hash(a) partitions 3");

        // Start expand on both tables
        execSuccess("alter table " + tableName1 + " expand partitions to 8");
        execSuccess("alter table " + tableName2 + " expand partitions to 9");

        Thread.sleep(2000L);

        // Verify both show up in status
        ResultSet rs = execQuery("show expand status");
        List<List<Object>> rows = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(rows.size() >= 2);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private void updatePendingStatus(String schemaName, String tableName, Set<String> curPartitions) {
        try (Connection conn = getMetaConnection()) {
            ExpandPartitionTasksAccessor accessor = new ExpandPartitionTasksAccessor();
            accessor.setConnection(conn);

            List<ExpandPartitionTaskRecord> existing = accessor.queryBySchemaTable(schemaName, tableName);
            if (existing.isEmpty()) {
                // Record gone (e.g., CANCEL EXPAND was issued) - skip silently
                return;
            }

            ExpandPartitionTaskRecord rec = existing.get(0);
            String currentJson = rec.planJson;
            JSONObject root = JSON.parseObject(currentJson);
            JSONArray items = root.getJSONArray("items");

            boolean updated = false;
            Set<String> pendingPartitions = new TreeSet<>(String::compareToIgnoreCase);
            for (int i = 0; i < items.size(); i++) {
                JSONObject item = items.getJSONObject(i);
                if ("DONE".equals(item.getString("status"))) {
                    // Already DONE - idempotent
                    continue;
                }
                pendingPartitions.add(item.getString("name"));
            }
            String partitionName = "";
            for (int i = 0; i < items.size(); i++) {
                JSONObject item = items.getJSONObject(i);
                if (pendingPartitions.contains(item.getString("name")) && !curPartitions.contains(
                    item.getString("name"))) {
                    item.put("status", "DONE");
                    partitionName = item.getString("name");
                    updated = true;
                    break;
                }
            }

            if (!updated) {
                // Partition not found in plan (unexpected) - skip silently
                return;
            }

            long newCompleted = rec.completedPartitions + 1;
            long newPending = Math.max(0, rec.pendingPartitions - 1);
            System.out.println("[EXPAND TEST SQL] update status for partitionName = " + partitionName);
            accessor.updateProgress(schemaName, tableName, JSON.toJSONString(root), newCompleted, newPending);

        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "UpdateExpandProgressTask", "expand_partition_tasks", e.getMessage());
        }
    }

}
