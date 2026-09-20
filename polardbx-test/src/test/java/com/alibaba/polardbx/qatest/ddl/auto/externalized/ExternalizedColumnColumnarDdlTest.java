package com.alibaba.polardbx.qatest.ddl.auto.externalized;

import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Metadata-only CCI compatibility boundary tests for externalized columns.
 *
 * <p>Externalized columns and CCI are mutually exclusive. These cases cover both DDL directions
 * and retain a non-externalized control case so the guard does not affect ordinary CCI tables. CCI
 * creation skips {@code WaitColumnarTableCreationTask}: the DDL still publishes CCI metadata used by
 * the guard, while the test no longer requires an online columnar node.
 */
public class ExternalizedColumnColumnarDdlTest extends ExternalizedColumnTestBase {

    private static final String SKIP_COLUMNAR_CREATION_HINT =
        "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableCreationTask\")*/ ";

    private final String suffix = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private final String tableName = "ext_col_cci_guard_" + suffix;
    private final String dbName = "ext_col_cci_guard_db_" + suffix;

    @Before
    public void setUp() throws SQLException {
        createIsolatedDatabase(dbName);
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(dbName);
        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
    }

    @After
    public void tearDown() {
        if (tddlConnection != null) {
            try {
                JdbcUtil.useDb(tddlConnection, "information_schema");
            } catch (Throwable ignored) {
                // best-effort
            }
            try {
                tddlConnection.close();
            } catch (SQLException ignored) {
                // best-effort
            }
            tddlConnection = null;
        }
        dropIsolatedDatabase(dbName);
    }

    @Test
    public void testInlineCciRejectedOnExternalizedTable() {
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(
            SKIP_COLUMNAR_CREATION_HINT + "CREATE TABLE %s ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "content LONGTEXT EXTERNALIZE,"
                + "CLUSTERED COLUMNAR INDEX cci_inline(id) PARTITION BY KEY(id) PARTITIONS 2"
                + ") PARTITION BY KEY(id) PARTITIONS 2",
            tableName), "table with externalized columns");
    }

    @Test
    public void testCreateCciRejectedOnExternalizedTable() {
        createExternalizedTable();

        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(
            SKIP_COLUMNAR_CREATION_HINT + "CREATE CLUSTERED COLUMNAR INDEX cci_create ON %s (id) "
                + "PARTITION BY KEY(id) PARTITIONS 2",
            tableName), "table with externalized columns");
    }

    @Test
    public void testAlterAddCciRejectedOnExternalizedTable() {
        createExternalizedTable();

        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(
            SKIP_COLUMNAR_CREATION_HINT + "ALTER TABLE %s ADD CLUSTERED COLUMNAR INDEX cci_alter(id) "
                + "PARTITION BY KEY(id) PARTITIONS 2",
            tableName), "table with externalized columns");
    }

    @Test
    public void testAddExternalizedColumnRejectedOnCciTable() {
        createOrdinaryTable();
        createCciOn(tableName, "cci_add_ext");

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s ADD COLUMN extra LONGTEXT EXTERNALIZE", tableName),
            "table with CCI");
    }

    @Test
    public void testMceRejectedOnCciTable() {
        createOrdinaryTable();
        createCciOn(tableName, "cci_mce");

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", tableName),
            "existing CCI");
    }

    @Test
    public void testOrdinaryExtTypeCommentStillAllowsCci() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            SKIP_COLUMNAR_CREATION_HINT + "CREATE TABLE %s ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "payload_addr_ VARCHAR(128) COMMENT 'ext_type:LONGTEXT',"
                + "CLUSTERED COLUMNAR INDEX cci_ordinary(id) PARTITION BY KEY(id) PARTITIONS 2"
                + ") PARTITION BY KEY(id) PARTITIONS 2",
            tableName));

        String showCreate = getFullCreateTable(tableName);
        assertTrue(showCreate.toLowerCase().contains("payload_addr_"));
        assertTrue(showCreate.toLowerCase().contains("cci_ordinary"));
        assertFalse(showCreate.toUpperCase().contains("EXTERNALIZE"));
    }

    private void createExternalizedTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "content LONGTEXT EXTERNALIZE"
                + ") PARTITION BY KEY(id) PARTITIONS 2",
            tableName));
    }

    private void createOrdinaryTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "content LONGTEXT"
                + ") PARTITION BY KEY(id) PARTITIONS 2",
            tableName));
    }

    private void createCciOn(String table, String cciName) {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            SKIP_COLUMNAR_CREATION_HINT + "CREATE CLUSTERED COLUMNAR INDEX %s ON %s (id) "
                + "PARTITION BY KEY(id) PARTITIONS 2",
            cciName, table));
    }

    private String getFullCreateTable(String table) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW FULL CREATE TABLE " + table)) {
            assertTrue("SHOW FULL CREATE TABLE should return a row", rs.next());
            return rs.getString(2);
        }
    }
}
