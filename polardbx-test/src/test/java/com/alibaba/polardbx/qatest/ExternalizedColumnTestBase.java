package com.alibaba.polardbx.qatest;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Shared integration-test support for externalized columns.
 *
 * <p>This base deliberately has no dependency on CCI or a columnar node. Tests that
 * explicitly exercise the columnar read path should extend {@code ColumnarReadBaseTestCase}
 * instead.
 */
public abstract class ExternalizedColumnTestBase extends AutoReadBaseTestCase {

    public enum DatabaseMode {
        AUTO("auto"),
        DRDS("drds");

        private final String ddlMode;

        DatabaseMode(String ddlMode) {
            this.ddlMode = ddlMode;
        }

        public String getDdlMode() {
            return ddlMode;
        }
    }

    protected final DatabaseMode databaseMode;

    protected ExternalizedColumnTestBase() {
        this(DatabaseMode.AUTO);
    }

    protected ExternalizedColumnTestBase(DatabaseMode databaseMode) {
        this.databaseMode = databaseMode;
    }

    protected static List<Object[]> autoAndDrdsModes() {
        return Arrays.asList(new Object[][] {
            {DatabaseMode.AUTO},
            {DatabaseMode.DRDS}
        });
    }

    protected boolean isAutoMode() {
        return databaseMode == DatabaseMode.AUTO;
    }

    protected boolean isDrdsMode() {
        return databaseMode == DatabaseMode.DRDS;
    }

    protected String databaseName(String autoDatabase, String drdsDatabase) {
        return isAutoMode() ? autoDatabase : drdsDatabase;
    }

    protected String tableDistribution(String column, int partitions) {
        return tableDistribution(databaseMode, column, partitions);
    }

    protected static String tableDistribution(DatabaseMode mode, String column, int partitions) {
        if (mode == DatabaseMode.DRDS) {
            return " DBPARTITION BY HASH(" + column + ")"
                + " TBPARTITION BY HASH(" + column + ") TBPARTITIONS " + partitions;
        }
        return " PARTITION BY KEY(" + column + ") PARTITIONS " + partitions;
    }

    /**
     * DRDS externalized tables deliberately require PURGE to bypass the recycle bin. Keep that
     * topology-specific cleanup detail out of individual dual-mode test classes.
     */
    protected void dropTestTable(Connection connection, String tableName) {
        dropTestTable(connection, tableName, databaseMode);
    }

    protected static void dropTestTable(Connection connection, String tableName, DatabaseMode mode) {
        if (mode == DatabaseMode.DRDS) {
            JdbcUtil.executeUpdateSuccess(connection, "DROP TABLE IF EXISTS " + tableName + " PURGE");
        } else {
            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Override
    public boolean usingNewPartDb() {
        // During superclass construction databaseMode is not initialized yet. Treat that brief
        // window as AUTO, which preserves the behavior of AutoReadBaseTestCase.
        return databaseMode != DatabaseMode.DRDS;
    }

    /**
     * Returns a fresh URL-bound TP connection on the given per-test-class isolated database.
     */
    public static Connection getTpConnection(String dbName) throws SQLException {
        Connection connection = ConnectionManager.getInstance().newPolarDBXConnection(dbName);
        JdbcUtil.executeSuccess(connection, "SET SESSION WORKLOAD_TYPE=TP");
        return connection;
    }

    /**
     * Create an isolated AUTO-mode database. Retries up to 3 times on
     * {@code ERR_GMS_GENERIC: Failed to create physical db} (transient DN
     * connectivity flakiness), cleaning any half-created physical groups
     * with a best-effort {@code DROP DATABASE IF EXISTS} between attempts.
     */
    public static void createIsolatedDatabase(String dbName) throws SQLException {
        createIsolatedDatabase(dbName, "auto");
    }

    /**
     * Create an isolated AUTO- or DRDS-mode database with the same bounded retry policy.
     */
    public static void createIsolatedDatabase(String dbName, String mode) throws SQLException {
        if (!"auto".equalsIgnoreCase(mode) && !"drds".equalsIgnoreCase(mode)) {
            throw new IllegalArgumentException("Unsupported database mode: " + mode);
        }
        final String createSql = "CREATE DATABASE IF NOT EXISTS " + dbName + " MODE='" + mode + "'";
        SQLException last = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            try (Connection c = ConnectionManager.getInstance().newPolarDBXConnection();
                Statement s = c.createStatement()) {
                s.execute(createSql);
                return;
            } catch (SQLException e) {
                last = e;
                if (!isRetriableDbDdlFlaky(e)) {
                    throw e;
                }
                System.err.println("[ISOLATED_DB_RETRY] CREATE DATABASE attempt " + attempt
                    + " hit flaky: " + e.getMessage() + "; cleanup and retry. db=" + dbName);
                try (Connection c2 = ConnectionManager.getInstance().newPolarDBXConnection();
                    Statement s2 = c2.createStatement()) {
                    s2.execute("DROP DATABASE IF EXISTS " + dbName);
                } catch (SQLException ignore) {
                    // best-effort cleanup
                }
            }
        }
        throw last;
    }

    public static void createIsolatedDatabase(String dbName, DatabaseMode mode) throws SQLException {
        createIsolatedDatabase(dbName, mode.getDdlMode());
    }

    /**
     * Drop an isolated database. Best-effort: swallows errors so test class
     * teardown never blocks subsequent class runs. Retries on the same
     * flaky-DN-connectivity error class as the create helper.
     */
    public static void dropIsolatedDatabase(String dbName) {
        for (int attempt = 1; attempt <= 3; attempt++) {
            try (Connection c = ConnectionManager.getInstance().newPolarDBXConnection();
                Statement s = c.createStatement()) {
                s.execute("DROP DATABASE IF EXISTS " + dbName);
                return;
            } catch (SQLException e) {
                if (!isRetriableDbDdlFlaky(e) || attempt == 3) {
                    System.err.println("[ISOLATED_DB_RETRY] DROP DATABASE gave up after attempt "
                        + attempt + ": " + e.getMessage() + " db=" + dbName);
                    return;
                }
                System.err.println("[ISOLATED_DB_RETRY] DROP DATABASE attempt " + attempt
                    + " hit flaky: " + e.getMessage() + "; will retry. db=" + dbName);
            }
        }
    }

    /**
     * Execute a CREATE/DROP TABLE statement with retry on the
     * {@code ERR_TABLE_GROUP_NOT_EXISTS} race that occurs when concurrent test
     * classes (or parameterized instances) thrash the same auto-allocated
     * table-group inside one isolated database. Up to 5 attempts; final attempt
     * goes through {@link JdbcUtil#executeUpdateSuccess(Connection, String)} so
     * the canonical failure message is preserved on persistent failure.
     */
    public static void executeDdlWithTgRetry(Connection conn, String sql) {
        final int maxAttempts = 5;
        for (int attempt = 1; attempt < maxAttempts; attempt++) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
                if (attempt > 1) {
                    System.err.println("[EXT_DDL_RETRY_OK] succeeded on attempt " + attempt + " sql=" + sql);
                }
                return;
            } catch (SQLException e) {
                if (!isRetriableTgFlaky(e)) {
                    JdbcUtil.executeUpdateSuccess(conn, sql);
                    return;
                }
                System.err.println("[EXT_DDL_RETRY] attempt " + attempt + " hit flaky: "
                    + e.getMessage() + "; will retry. sql=" + sql);
                try {
                    Thread.sleep(1000L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        // Final attempt: delegate to JdbcUtil so failure (if any) is reported in standard form.
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    private static boolean isRetriableDbDdlFlaky(SQLException e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        return msg.contains("ERR_GMS_GENERIC")
            && (msg.contains("Failed to create physical db")
            || msg.contains("Failed to drop physical db"));
    }

    private static boolean isRetriableTgFlaky(SQLException e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        // [TDDL-9304][ERR_TABLE_GROUP_NOT_EXISTS] table group: XXX not exist
        return msg.contains("ERR_TABLE_GROUP_NOT_EXISTS")
            || (msg.contains("table group:") && msg.contains("not exist"));
    }
}
