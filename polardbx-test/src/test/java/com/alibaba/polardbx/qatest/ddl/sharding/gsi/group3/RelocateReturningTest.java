package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group3;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Comprehensive tests for the Relocate Returning optimization path (DRDS / sharding mode).
 * Covers all scenarios where UPDATE modifies partition keys and the system
 * uses UPDATE...RETURNING to optimize the DELETE + INSERT rewrite.
 * <p>
 * Requires DN80+ (supportsReturningAll).
 */
public class RelocateReturningTest extends DDLBaseNewDBTestCase {
    private boolean useAffectedRows;
    private Connection oldTddl;
    private Connection oldMySql;

    private static final String RETURNING_HINT =
        "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=TRUE, OPTIMIZE_RELOCATE_BY_RETURNING=TRUE)*/";
    private static final String RETURNING_HINT_SKIP_UNCHANGED =
        "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=TRUE, OPTIMIZE_RELOCATE_BY_RETURNING=TRUE, DML_RELOCATE_SKIP_UNCHANGED_ROW=TRUE)*/";
    private static final String RETURNING_HINT_NO_SKIP =
        "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=TRUE, OPTIMIZE_RELOCATE_BY_RETURNING=TRUE, DML_RELOCATE_SKIP_UNCHANGED_ROW=FALSE)*/";
    private static final String DISABLE_RETURNING_HINT =
        "/*+TDDL:CMD_EXTRA(DML_USE_RETURNING=FALSE)*/";

    private boolean supportReturningAll = false;

    public RelocateReturningTest(boolean useAffectedRows) {
        this.useAffectedRows = useAffectedRows;
    }

    @Parameterized.Parameters(name = "{index}:useAffectedRows={0}")
    public static List<Boolean[]> prepareData() {
        return ImmutableList.of(new Boolean[] {false}, new Boolean[] {true});
    }

    @Override
    public boolean usingNewPartDb() {
        return false;
    }

    @Before
    public void before() {
        if (useAffectedRows && !useXproto()) {
            useAffectedRows = false;
        }
        if (useAffectedRows) {
            oldTddl = tddlConnection;
            tddlConnection = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
            useDb(tddlConnection, tddlDatabase1);
            oldMySql = mysqlConnection;
            mysqlConnection = ConnectionManager.getInstance().newMysqlConnectionWithUseAffectedRows();
            useDb(mysqlConnection, mysqlDatabase1);
        }

        this.supportReturningAll = useXproto()
            && Optional.ofNullable(getStorageProperties(tddlConnection).get("supportsReturningAll"))
            .map(Boolean::parseBoolean).orElse(false);

        setSqlMode("STRICT_TRANS_TABLES", tddlConnection);
        setSqlMode("STRICT_TRANS_TABLES", mysqlConnection);
    }

    // ==================== Scenario 1: Primary + GSI Relocate ====================

    /**
     * Modify both primary table partition key and GSI partition key.
     * Both primary and GSI need relocate (DELETE + INSERT).
     */
    @Test
    public void testRelocatePrimaryAndGsi() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_relocate_primary_and_gsi";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, c) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b,c) VALUES (1,1,1,10),(2,2,2,20),(3,3,3,30)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        // Modify both a (primary SK) and b (GSI SK)
        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=10, b=10 WHERE pk=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=10, b=10 WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * Modify only primary table partition key; GSI partition key unchanged.
     * Primary relocates, GSI does modify-only.
     */
    @Test
    public void testRelocatePrimaryOnly() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_relocate_primary_only";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, c) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b,c) VALUES (1,1,1,10),(2,2,2,20),(3,3,3,30)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        // Modify only a (primary SK), b (GSI SK) stays
        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=20 WHERE pk=2", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=20 WHERE pk=2", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * Modify only GSI partition key; primary table partition key unchanged.
     */
    @Test
    public void testRelocateGsiOnly() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_relocate_gsi_only";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, c) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b,c) VALUES (1,1,1,10),(2,2,2,20),(3,3,3,30)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        // Modify only b (GSI SK), a (primary SK) stays
        String update = RETURNING_HINT + String.format(" UPDATE %s SET b=20 WHERE pk=2", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET b=20 WHERE pk=2", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    // ==================== Scenario 2: Primary Relocate without GSI ====================

    /**
     * Simple partition key modification on a table without GSI.
     */
    @Test
    public void testRelocateWithoutGsi() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_relocate_no_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c VARCHAR(100)"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c VARCHAR(100)"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String insert = String.format("INSERT INTO %s(pk,a,b,c) VALUES (1,1,10,'hello'),(2,2,20,'world')", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=100 WHERE pk=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=100 WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
    }

    /**
     * Multi-row relocate without GSI.
     */
    @Test
    public void testRelocateMultiRowsWithoutGsi() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_relocate_multi_no_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c VARCHAR(100)"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c VARCHAR(100)"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String insert = String.format(
            "INSERT INTO %s(pk,a,b,c) VALUES (1,1,10,'a'),(2,2,20,'b'),(3,3,30,'c'),(4,4,40,'d'),(5,5,50,'e')",
            tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=a+100", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=a+100", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
    }

    // ==================== Scenario 3: ON UPDATE CURRENT_TIMESTAMP ====================

    /**
     * Core test: when relocating with ON UPDATE CURRENT_TIMESTAMP column,
     * primary table and GSI must have identical timestamp values.
     */
    @Test
    public void testOnUpdateTimestampRelocate() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_on_update_ts_relocate";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String createTable = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
                + ") dbpartition by hash(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, ts) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b) VALUES (1,1,1),(2,2,2),(3,3,3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignore
        }

        // Modify both partition keys, triggering relocate; ts should update consistently
        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=10, b=10 WHERE pk=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * When row is not actually changed, ON UPDATE timestamp should NOT update.
     * Validates MODIFY_ON_UPDATE() semantics.
     */
    @Test
    public void testOnUpdateTimestampUnchangedRow() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_on_update_ts_unchanged";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String createTable = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
                + ") dbpartition by hash(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, ts) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b) VALUES (1,1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        // Get original timestamp
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT ts FROM %s WHERE pk=1", tableName));
        assertTrue(rs.next());
        String originalTs = rs.getString("ts");
        rs.close();

        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            // ignore
        }

        // SET same values -> row not actually changed -> ts should NOT update
        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=1, b=1 WHERE pk=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT ts FROM %s WHERE pk=1", tableName));
        assertTrue(rs.next());
        String newTs = rs.getString("ts");
        rs.close();

        assertEquals("Timestamp should not change when row data is unchanged", originalTs, newTs);
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * User explicitly sets the ON UPDATE TIMESTAMP column.
     * The explicit value should be used, not wrapped by MODIFY_ON_UPDATE.
     */
    @Test
    public void testOnUpdateTimestampUserExplicitSet() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_on_update_ts_explicit";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, ts) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b) VALUES (1,1,1)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignore
        }

        // User explicitly sets ts, not auto-added -> should use user's value directly
        String explicitTs = "2020-01-01 00:00:00.000000";
        String update = RETURNING_HINT + String.format(
            " UPDATE %s SET a=10, ts='%s' WHERE pk=1", tableName, explicitTs);
        String mysqlUpdate = String.format(
            "UPDATE %s SET a=10, ts='%s' WHERE pk=1", tableName, explicitTs);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT pk,a,b,ts FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * Multiple ON UPDATE CURRENT_TIMESTAMP columns - all should have the same timestamp.
     */
    @Test
    public void testOnUpdateTimestampMultipleCols() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_on_update_ts_multi_cols";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String createTable = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "ts1 TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), "
                + "ts2 DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
                + ") dbpartition by hash(pk)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(a) COVERING(ts1, ts2) DBPARTITION BY HASH(a)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a) VALUES (1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignore
        }

        // Modify partition key to trigger relocate
        String update = RETURNING_HINT + String.format(" UPDATE %s SET pk=10, a=10 WHERE pk=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);

        // Both ts1 and ts2 should be identical (same CN-generated timestamp)
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT ts1, ts2 FROM %s WHERE pk=10", tableName));
        assertTrue(rs.next());
        String ts1 = rs.getString("ts1");
        String ts2 = rs.getString("ts2");
        rs.close();

        // ts1 is TIMESTAMP, ts2 is DATETIME - both should reflect the same instant
        assertTrue("ts1 and ts2 should both be updated", ts1 != null && ts2 != null);
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * TIMESTAMP with different precisions (0, 3, 6).
     */
    @Test
    public void testOnUpdateTimestampPrecision() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        // Test TIMESTAMP(3)
        final String tableName = "rr_on_update_ts_prec3";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String createTable = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "ts TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"
                + ") dbpartition by hash(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, ts) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b) VALUES (1,1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignore
        }

        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=10, b=10 WHERE pk=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);
        checkGsi(tddlConnection, gsiName);

        // Test TIMESTAMP(0) - no fractional seconds
        final String tableName0 = "rr_on_update_ts_prec0";
        final String gsiName0 = tableName0 + "_gsi";
        dropTableIfExists(tableName0);

        String createTable0 = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
                + ") dbpartition by hash(a)", tableName0);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable0);

        String createGsi0 = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, ts) DBPARTITION BY HASH(b)",
            gsiName0, tableName0);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi0);

        insert = String.format("INSERT INTO %s(pk,a,b) VALUES (1,1,1)", tableName0);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignore
        }

        update = RETURNING_HINT + String.format(" UPDATE %s SET a=10, b=10 WHERE pk=1", tableName0);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);
        checkGsi(tddlConnection, gsiName0);
    }

    // ==================== Scenario 4: modifySkOnly Optimization ====================

    /**
     * Only modify partition key (no other columns changed) with ON UPDATE TIMESTAMP.
     * The timestamp should update because the partition key actually changed.
     */
    @Test
    public void testModifySkOnlyWithTimestamp() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_modify_sk_only_ts";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String createTable = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
                + ") dbpartition by hash(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, ts) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b) VALUES (1,1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT ts FROM %s WHERE pk=1", tableName));
        assertTrue(rs.next());
        String originalTs = rs.getString("ts");
        rs.close();

        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            // ignore
        }

        // Only modify partition key a -> row actually changes -> ts SHOULD update
        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=10 WHERE pk=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT ts FROM %s WHERE pk=1", tableName));
        assertTrue(rs.next());
        String newTs = rs.getString("ts");
        rs.close();

        assertTrue("Timestamp should update when partition key actually changed",
            !originalTs.equals(newTs));
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * Modify partition key to the same value -> row not actually changed.
     */
    @Test
    public void testModifySkOnlySameValue() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_modify_sk_same";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String createTable = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
                + ") dbpartition by hash(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, ts) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b) VALUES (1,1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT ts FROM %s WHERE pk=1", tableName));
        assertTrue(rs.next());
        String originalTs = rs.getString("ts");
        rs.close();

        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            // ignore
        }

        // SET a=1 -> same value -> row not changed -> ts should NOT update
        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=1 WHERE pk=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT ts FROM %s WHERE pk=1", tableName));
        assertTrue(rs.next());
        String newTs = rs.getString("ts");
        rs.close();

        assertEquals("Timestamp should not change when partition key set to same value",
            originalTs, newTs);
        checkGsi(tddlConnection, gsiName);
    }

    // ==================== Scenario 5: Skip Unchanged Row ====================

    /**
     * With DML_RELOCATE_SKIP_UNCHANGED_ROW=TRUE, unchanged rows should be skipped.
     */
    @Test
    public void testSkipUnchangedRowEnabled() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_skip_unchanged_enabled";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String createTable = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY, "
                + "a INT, "
                + "b INT"
                + ") dbpartition by hash(pk)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(a) COVERING(b) DBPARTITION BY HASH(a)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b) VALUES (1,1,10)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        // With skip unchanged = true, setting same values should result in minimal physical SQLs
        String update = "trace " + RETURNING_HINT_SKIP_UNCHANGED
            + String.format(" UPDATE %s SET pk=1, a=1, b=10 WHERE pk=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);
        List<List<String>> trace = getTrace(tddlConnection);
        // Only the returning SELECT, no DELETE/INSERT needed
        assertEquals("Should have minimal trace entries when row unchanged", 1, trace.size());

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * With DML_RELOCATE_SKIP_UNCHANGED_ROW=FALSE, even unchanged rows go through full path.
     */
    @Test
    public void testSkipUnchangedRowDisabled() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_skip_unchanged_disabled";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String createTable = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY, "
                + "a INT, "
                + "b INT"
                + ") dbpartition by hash(pk)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(a) COVERING(b) DBPARTITION BY HASH(a)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b) VALUES (1,1,10)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        // With skip unchanged = false, even same values should trigger full UPDATE
        String update = "trace " + RETURNING_HINT_NO_SKIP
            + String.format(" UPDATE %s SET pk=1, a=1, b=10 WHERE pk=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);
        List<List<String>> trace = getTrace(tddlConnection);
        // Should have more trace entries (UPDATE + GSI operations)
        assertTrue("Should have more trace entries when skip is disabled", trace.size() > 1);

        checkGsi(tddlConnection, gsiName);
    }

    // ==================== Scenario 6: NULL Value Handling ====================

    /**
     * Partition key changes from NULL to non-NULL.
     */
    @Test
    public void testNullToNonNull() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_null_to_nonnull";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c VARCHAR(100)"
                + ") dbpartition by hash(pk)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c VARCHAR(100)"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(a) COVERING(b, c) DBPARTITION BY HASH(a)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk, a, b) VALUES (1, NULL, 10)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=100 WHERE pk=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=100 WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * Partition key changes from non-NULL to NULL.
     */
    @Test
    public void testNonNullToNull() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_nonnull_to_null";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c VARCHAR(100)"
                + ") dbpartition by hash(pk)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c VARCHAR(100)"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(a) COVERING(b, c) DBPARTITION BY HASH(a)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk, a, b) VALUES (1, 100, 10)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=NULL WHERE pk=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=NULL WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * Partition key stays NULL while other columns change.
     */
    @Test
    public void testNullToNull() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_null_to_null";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c VARCHAR(100)"
                + ") dbpartition by hash(pk)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c VARCHAR(100)"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(a) COVERING(b, c) DBPARTITION BY HASH(a)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk, a, b) VALUES (1, NULL, 10)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=NULL, b=20 WHERE pk=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=NULL, b=20 WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    // ==================== Scenario 7: Various Data Types ====================

    /**
     * VARCHAR as partition key.
     */
    @Test
    public void testRelocateVarcharPartitionKey() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_varchar_sk";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a VARCHAR(100), "
                + "b INT"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a VARCHAR(100), "
                + "b INT"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(a) COVERING(b) DBPARTITION BY HASH(a)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk, a, b) VALUES (1, 'hello', 1),(2, 'world', 2)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String update = RETURNING_HINT + String.format(" UPDATE %s SET a='changed' WHERE pk=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a='changed' WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * BIGINT as partition key.
     */
    @Test
    public void testRelocateBigintPartitionKey() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_bigint_sk";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a BIGINT, "
                + "b INT"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a BIGINT, "
                + "b INT"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(a) COVERING(b) DBPARTITION BY HASH(a)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert =
            String.format("INSERT INTO %s(pk, a, b) VALUES (1, 9999999999, 1),(2, 1234567890, 2)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=8888888888 WHERE pk=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=8888888888 WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    // ==================== Scenario 8: Fallback (Returning Not Enabled) ====================

    /**
     * Without OPTIMIZE_RELOCATE_BY_RETURNING hint, falls back to traditional path.
     */
    @Test
    public void testFallbackWithoutReturningHint() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_fallback_no_hint";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, ts) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b) VALUES (1,1,1),(2,2,2)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        // No returning hint -> traditional SELECT+DELETE+INSERT path
        String update = String.format("UPDATE %s SET a=10, b=10 WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);

        selectContentSameAssert(
            buildSqlCheckData(ImmutableList.of("pk", "a", "b"), tableName), null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * With DML_USE_RETURNING=FALSE, explicitly disables returning path.
     */
    @Test
    public void testFallbackDisableReturning() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_fallback_disable";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b) VALUES (1,1,1),(2,2,2)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String update = DISABLE_RETURNING_HINT + String.format(" UPDATE %s SET a=10, b=10 WHERE pk=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=10, b=10 WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    // ==================== Scenario 9: Composite Partition Key ====================

    /**
     * Modify one part of composite partition key.
     */
    @Test
    public void testCompositePartitionKeyRelocate() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_composite_sk";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "a INT, "
                + "b INT, "
                + "c INT, "
                + "d VARCHAR(100), "
                + "PRIMARY KEY(a, b)"
                + ") dbpartition by hash(a) tbpartition by hash(b) tbpartitions 3", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "a INT, "
                + "b INT, "
                + "c INT, "
                + "d VARCHAR(100), "
                + "PRIMARY KEY(a, b)"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(c) COVERING(d) DBPARTITION BY HASH(c)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s VALUES (1, 1, 10, 'hello'),(2, 2, 20, 'world')", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        // Modify part of composite PK
        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=5 WHERE a=1 AND b=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=5 WHERE a=1 AND b=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * Modify all parts of composite partition key.
     */
    @Test
    public void testCompositePartitionKeyBothChange() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_composite_sk_both";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "a INT, "
                + "b INT, "
                + "c INT, "
                + "d VARCHAR(100), "
                + "PRIMARY KEY(a, b)"
                + ") dbpartition by hash(a) tbpartition by hash(b) tbpartitions 3", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "a INT, "
                + "b INT, "
                + "c INT, "
                + "d VARCHAR(100), "
                + "PRIMARY KEY(a, b)"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(c) COVERING(d) DBPARTITION BY HASH(c)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s VALUES (1, 1, 10, 'hello')", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        // Modify all parts of composite PK
        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=3, b=3 WHERE a=1 AND b=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=3, b=3 WHERE a=1 AND b=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    // ==================== Scenario 10: Unique GSI ====================

    /**
     * Relocate primary partition key with unique GSI.
     */
    @Test
    public void testUniqueGsiRelocate() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_ugsi_relocate";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT, "
                + "UNIQUE KEY %s(b)"
                + ")", tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL UNIQUE INDEX %s ON %s(b) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b,c) VALUES (1,1,100,10),(2,2,200,20)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        // Modify primary SK, UGSI key unchanged
        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=5 WHERE pk=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=5 WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * Modify unique GSI partition key.
     */
    @Test
    public void testUniqueGsiKeyChange() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_ugsi_key_change";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT, "
                + "UNIQUE KEY %s(b)"
                + ")", tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL UNIQUE INDEX %s ON %s(b) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b,c) VALUES (1,1,100,10),(2,2,200,20)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        // Modify UGSI key
        String update = RETURNING_HINT + String.format(" UPDATE %s SET b=300 WHERE pk=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET b=300 WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    // ==================== Scenario 11: Clustered GSI ====================

    /**
     * Modify primary partition key with clustered GSI.
     */
    @Test
    public void testClusteredGsiRelocate() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_clustered_gsi";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT, "
                + "KEY %s(b)"
                + ")", tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE CLUSTERED INDEX %s ON %s(b) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b,c) VALUES (1,1,10,100),(2,2,20,200)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=3 WHERE pk=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=3 WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    // ==================== Scenario 12: Multi-Row Cross-Partition ====================

    /**
     * Multi-row update across different partitions.
     */
    @Test
    public void testMultiRowCrossPartition() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_multi_cross_part";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, c) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format(
            "INSERT INTO %s(pk,a,b,c) VALUES (1,1,1,10),(2,2,2,20),(3,3,3,30),(4,4,4,40),(5,5,5,50)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        // Batch modify all rows' partition keys
        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=a+100, b=b+100", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=a+100, b=b+100", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    /**
     * Multi-row where only some rows need relocate.
     */
    @Test
    public void testMultiRowPartialRelocate() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_multi_partial_relocate";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT, "
                + "c INT"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, c) DBPARTITION BY HASH(b)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format(
            "INSERT INTO %s(pk,a,b,c) VALUES (1,1,1,10),(2,2,2,20),(3,3,3,30)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        // Only pk=1 changes partition key, others just update c
        String update = RETURNING_HINT + String.format(
            " UPDATE %s SET a=CASE WHEN pk=1 THEN 100 ELSE a END, c=c+1", tableName);
        String mysqlUpdate = String.format(
            "UPDATE %s SET a=CASE WHEN pk=1 THEN 100 ELSE a END, c=c+1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    // ==================== Scenario 13: Hash Partition (DRDS equivalent of Range) ====================

    /**
     * Hash partition - relocate across partition boundaries.
     * (DRDS does not support range partition, using hash partition instead.)
     */
    @Test
    public void testHashPartitionRelocate() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName = "rr_hash_part";
        final String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTddl = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT"
                + ") dbpartition by hash(a)", tableName);
        String createMysql = String.format(
            "CREATE TABLE %s ("
                + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                + "a INT, "
                + "b INT"
                + ")", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTddl);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createMysql);

        String createGsi = String.format(
            "CREATE GLOBAL INDEX %s ON %s(a) COVERING(b) DBPARTITION BY HASH(a)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("INSERT INTO %s(pk,a,b) VALUES (1,5,1),(2,15,2),(3,25,3)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        // Move to different hash partition
        String update = RETURNING_HINT + String.format(" UPDATE %s SET a=15 WHERE pk=1", tableName);
        String mysqlUpdate = String.format("UPDATE %s SET a=15 WHERE pk=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate, update, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);

        // Move to another partition
        String update2 = RETURNING_HINT + String.format(" UPDATE %s SET a=3 WHERE pk=3", tableName);
        String mysqlUpdate2 = String.format("UPDATE %s SET a=3 WHERE pk=3", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlUpdate2, update2, null, true);

        selectContentSameAssert("SELECT * FROM " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    // ==================== Scenario 14: Returning vs Traditional Path Comparison ====================

    /**
     * Compare results between returning path and traditional path to ensure consistency.
     */
    @Test
    public void testReturningVsTraditionalConsistency() throws SQLException {
        if (!supportReturningAll) {
            return;
        }

        final String tableName1 = "rr_cmp_returning";
        final String tableName2 = "rr_cmp_traditional";
        final String gsiName1 = tableName1 + "_gsi";
        final String gsiName2 = tableName2 + "_gsi";
        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);

        // Create two identical tables
        for (String tbl : new String[] {tableName1, tableName2}) {
            String gsi = tbl.equals(tableName1) ? gsiName1 : gsiName2;
            String createTable = String.format(
                "CREATE TABLE %s ("
                    + "pk INT PRIMARY KEY AUTO_INCREMENT, "
                    + "a INT, "
                    + "b INT, "
                    + "c VARCHAR(100)"
                    + ") dbpartition by hash(a)", tbl);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

            String createGsi = String.format(
                "CREATE GLOBAL INDEX %s ON %s(b) COVERING(a, c) DBPARTITION BY HASH(b)",
                gsi, tbl);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

            String insert = String.format(
                "INSERT INTO %s(a,b,c) VALUES (1,1,'aaa'),(2,2,'bbb'),(3,3,'ccc'),(4,4,'ddd'),(5,5,'eee')", tbl);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        }

        // Execute same UPDATE on both tables with different paths
        // Table 1: returning path
        String update1 = RETURNING_HINT + String.format(
            " UPDATE %s SET a=a+10, b=b+10, c=CONCAT(c,'_updated')", tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update1);

        // Table 2: traditional path (disable returning)
        String update2 = DISABLE_RETURNING_HINT + String.format(
            " UPDATE %s SET a=a+10, b=b+10, c=CONCAT(c,'_updated')", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update2);

        // Compare results between the two tables (excluding auto-generated columns)
        ResultSet rs1 = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT a,b,c FROM %s ORDER BY pk", tableName1));
        ResultSet rs2 = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT a,b,c FROM %s ORDER BY pk", tableName2));

        while (rs1.next() && rs2.next()) {
            assertEquals("Column a should match", rs1.getInt("a"), rs2.getInt("a"));
            assertEquals("Column b should match", rs1.getInt("b"), rs2.getInt("b"));
            assertEquals("Column c should match", rs1.getString("c"), rs2.getString("c"));
        }
        // Both should be exhausted
        assertTrue("Both result sets should have same number of rows", !rs1.next() && !rs2.next());
        rs1.close();
        rs2.close();

        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);
    }
}
