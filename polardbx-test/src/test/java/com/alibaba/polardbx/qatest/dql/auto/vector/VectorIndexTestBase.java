package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class for Vector Index tests.
 * Each vector index test class runs in its own AUTO database.
 * Vector capability is exercised through the normal SQL path on MySQL 8.0 DNs.
 */
public abstract class VectorIndexTestBase {

    private static final int MAX_DATABASE_NAME_LENGTH = 64;

    protected Connection tddlConnection;
    protected String databaseName;
    private String independentDatabase;

    protected static void assumeMysql80Dn() {
        String version = null;
        try (Connection dnConnection = ConnectionManager.getInstance().newMysqlConnection();
            Statement stmt = dnConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT VERSION()")) {
            if (rs.next()) {
                version = rs.getString(1);
            }
        } catch (SQLException e) {
            Assume.assumeNoException(e);
        }
        Assume.assumeTrue("Vector tests require a MySQL 8.0 DN, actual version: " + version,
            version != null && version.startsWith("8.0"));
    }

    protected static void createTestDatabase(String databaseName) throws Exception {
        try (Connection adminConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.createPartDatabase(adminConnection, databaseName);
            try (Statement stmt = adminConnection.createStatement()) {
                stmt.execute("SET GLOBAL vidx_disabled = 'OFF'");
            }
        }
    }

    protected static void dropTestDatabase(String databaseName) throws Exception {
        try (Connection adminConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.dropDatabase(adminConnection, databaseName);
        }
    }

    @Before
    public void setUp() throws Exception {
        databaseName = databaseNameFor(getClass());

        // Open a persistent connection to the database for test use
        tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(databaseName);
    }

    @After
    public void tearDown() throws Exception {
        // Close the test connection
        if (tddlConnection != null) {
            try {
                tddlConnection.close();
            } catch (SQLException e) {
                // Ignore
            }
            tddlConnection = null;
        }
        if (independentDatabase != null) {
            try {
                dropTestDatabase(independentDatabase);
            } finally {
                independentDatabase = null;
            }
        }
    }

    protected void switchTestDatabase(String targetDatabase) throws Exception {
        if (tddlConnection != null) {
            tddlConnection.close();
        }
        databaseName = targetDatabase;
        tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(databaseName);
    }

    protected String useIndependentDatabase(String methodName) throws Exception {
        independentDatabase = databaseNameFor(getClass(), methodName);
        dropTestDatabase(independentDatabase);
        createTestDatabase(independentDatabase);
        switchTestDatabase(independentDatabase);
        return independentDatabase;
    }

    protected void restoreClassDatabase(String classDatabase, String independentDatabase) throws Exception {
        switchTestDatabase(classDatabase);
        dropTestDatabase(independentDatabase);
        this.independentDatabase = null;
    }

    protected static String databaseNameFor(Class<?> testClass) {
        return databaseNameFor(testClass, null);
    }

    protected static String databaseNameFor(Class<?> testClass, String methodName) {
        String rawName = "vector_" + testClass.getSimpleName();
        if (methodName != null) {
            rawName += "_" + methodName;
        }
        rawName = rawName.toLowerCase(Locale.ROOT);
        if (rawName.length() <= MAX_DATABASE_NAME_LENGTH) {
            return rawName;
        }

        String hash = Integer.toUnsignedString(rawName.hashCode(), 36);
        int prefixLength = MAX_DATABASE_NAME_LENGTH - hash.length() - 1;
        return rawName.substring(0, prefixLength) + "_" + hash;
    }

    /**
     * Drop a table if it exists in the current database.
     * If there are paused DDL jobs blocking the operation, cancel them first and retry.
     */
    public void dropTableIfExists(String tableName) {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("ERR_PAUSED_DDL_JOB_EXISTS")) {
                // Extract job IDs from error message and cancel them
                cancelPausedDdlJobs(msg);
                // Retry drop table
                try (Statement stmt = tddlConnection.createStatement()) {
                    stmt.execute(sql);
                } catch (SQLException e2) {
                    // Ignore errors on retry (table may not exist)
                }
            }
            // For other errors, ignore - table may not exist
        }
    }

    /**
     * Cancel paused DDL jobs extracted from the error message.
     * Error message format: "Found Paused DDL JOB. You can use 'SHOW DDL <jobId>' ..."
     */
    private void cancelPausedDdlJobs(String errorMsg) {
        // Extract job IDs from error message (format: SHOW DDL 1234567890 or CONTINUE DDL 1234567890)
        Pattern pattern = Pattern.compile("(?:SHOW|CONTINUE|CANCEL) DDL (\\d+)");
        Matcher matcher = pattern.matcher(errorMsg);
        List<Long> jobIds = new ArrayList<>();
        while (matcher.find()) {
            try {
                jobIds.add(Long.parseLong(matcher.group(1)));
            } catch (NumberFormatException ignore) {
            }
        }
        // Remove duplicates
        jobIds = jobIds.stream().distinct().collect(java.util.stream.Collectors.toList());

        try (Connection adminConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            for (Long jobId : jobIds) {
                try (Statement stmt = adminConnection.createStatement()) {
                    stmt.execute("CANCEL DDL " + jobId);
                } catch (SQLException e) {
                    // Ignore cancel errors - job may have already completed/been cancelled
                }
            }
        } catch (Exception e) {
            // Ignore connection errors during cleanup
        }
    }

    /**
     * Drop a table with GSI if it exists.
     */
    public void dropTableWithGsi(String primary, List<String> indexNames) {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + quoteSpecialName(primary));
            if (indexNames != null) {
                for (String gsi : indexNames) {
                    stmt.execute("DROP TABLE IF EXISTS " + quoteSpecialName(gsi));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String quoteSpecialName(String name) {
        if (!name.contains(".")) {
            if (name.contains("`")) {
                name = "`" + name.replaceAll("`", "``") + "`";
            } else {
                name = "`" + name + "`";
            }
        }
        return name;
    }
}
