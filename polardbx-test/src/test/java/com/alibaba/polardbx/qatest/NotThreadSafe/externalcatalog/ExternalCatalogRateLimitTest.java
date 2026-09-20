package com.alibaba.polardbx.qatest.NotThreadSafe.externalcatalog;

import com.alibaba.polardbx.qatest.BaseExternalCatalogTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Integration tests for external catalog connection validation rate limiting.
 * Verifies that repeated authentication failures trigger rate limiting,
 * and that limits reset after expiry or successful connection.
 */
public class ExternalCatalogRateLimitTest extends BaseExternalCatalogTest {

    private static final String SECRET_NAME = "rate_limit_secret";
    private static final String CATALOG_NAME = "rate_limit_cat";

    private Connection conn;

    @Before
    public void setUp() {
        conn = getPolardbxConnection();
    }

    @After
    public void cleanup() {
        dropCatalog(conn, CATALOG_NAME);
        dropSecret(conn, SECRET_NAME);
        cleanupRateLimitRecords();
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
            conn = null;
        }
    }

    @Test
    public void testRateLimitBlocksAfterMaxAttempts() throws SQLException {
        try {
            createMockSecret(conn, SECRET_NAME);

            // Internal rate limit threshold is 5
            for (int i = 0; i < 5; i++) {
                String catalogName = "rate_limit_" + i;
                try {
                    JdbcUtil.executeUpdateWithException(conn,
                        "CREATE EXTERNAL CATALOG " + catalogName
                            + " WITH ('connector'='mock', 'simulate_error'='AUTH_FAIL', 'secret'='" + SECRET_NAME
                            + "')");
                    Assert.fail("Should fail with Access denied");
                } catch (SQLException e) {
                    Assert.assertFalse("Should not be rate limited yet, got: " + e.getMessage(),
                        e.getMessage().contains("Too many failed connection attempts"));
                }
            }

            // The 6th attempt should be blocked by rate limit
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "CREATE EXTERNAL CATALOG rate_limit_blocked"
                        + " WITH ('connector'='mock', 'simulate_error'='AUTH_FAIL', 'secret'='" + SECRET_NAME + "')");
                Assert.fail("Should be blocked by rate limit");
            } catch (SQLException e) {
                Assert.assertTrue("Expected rate limit error but got: " + e.getMessage(),
                    e.getMessage().contains("Too many failed connection attempts"));
            }
        } finally {
            cleanupRateLimitRecords();
        }
    }

    @Test
    public void testRateLimitResetsAfterExpiry() throws SQLException {
        try {
            createMockSecret(conn, SECRET_NAME);

            // Trigger rate limit
            triggerRateLimit();

            // Manually expire the rate limit record
            updateExpireDateToPast();

            // Should no longer be blocked - will fail with auth error, not rate limit
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "CREATE EXTERNAL CATALOG rate_limit_after_expire"
                        + " WITH ('connector'='mock', 'simulate_error'='AUTH_FAIL', 'secret'='" + SECRET_NAME + "')");
                Assert.fail("Should fail with auth error");
            } catch (SQLException e) {
                Assert.assertFalse("Should NOT be rate limited after expiry, but got: " + e.getMessage(),
                    e.getMessage().contains("Too many failed connection attempts"));
            }
            Assert.assertEquals("Expired rate-limit window should restart at one failed attempt",
                1, getRateLimitErrorCount());
        } finally {
            cleanupRateLimitRecords();
        }
    }

    @Test
    public void testSuccessfulConnectionClearsCount() throws SQLException {
        try {
            createMockSecret(conn, SECRET_NAME);

            // Accumulate some failures (less than max)
            for (int i = 0; i < 3; i++) {
                try {
                    JdbcUtil.executeUpdateWithException(conn,
                        "CREATE EXTERNAL CATALOG fail_" + i
                            + " WITH ('connector'='mock', 'simulate_error'='AUTH_FAIL', 'secret'='" + SECRET_NAME
                            + "')");
                    Assert.fail("Should fail with auth error");
                } catch (SQLException e) {
                    // expected
                }
            }

            // Successful creation clears the count
            createMockCatalog(conn, CATALOG_NAME, SECRET_NAME,
                "'mock.databases'='db1','mock.db1.tables'='t1','mock.db1.t1.columns'='id:bigint'");

            // Drop it so we can try again
            dropCatalog(conn, CATALOG_NAME);

            // After clearing, we should be able to fail 5 more times without being blocked
            for (int i = 0; i < 5; i++) {
                try {
                    JdbcUtil.executeUpdateWithException(conn,
                        "CREATE EXTERNAL CATALOG fail_after_" + i
                            + " WITH ('connector'='mock', 'simulate_error'='AUTH_FAIL', 'secret'='" + SECRET_NAME
                            + "')");
                    Assert.fail("Should fail with auth error");
                } catch (SQLException e) {
                    Assert.assertFalse("Should not be rate limited after clear, got: " + e.getMessage(),
                        e.getMessage().contains("Too many failed connection attempts"));
                }
            }
        } finally {
            cleanupRateLimitRecords();
        }
    }

    private void triggerRateLimit() {
        for (int i = 0; i < 5; i++) {
            try {
                JdbcUtil.executeUpdateWithException(conn,
                    "CREATE EXTERNAL CATALOG trigger_limit_" + i
                        + " WITH ('connector'='mock', 'simulate_error'='AUTH_FAIL', 'secret'='" + SECRET_NAME + "')");
                Assert.fail("Should fail with auth error");
            } catch (SQLException e) {
                // expected
            }
        }
    }

    private void cleanupRateLimitRecords() {
        try (Connection metaDb = getMetaConnection()) {
            try (Statement s = metaDb.createStatement()) {
                s.executeUpdate(
                    "DELETE FROM user_login_error_limit WHERE limit_key LIKE 'EXT_CATALOG:%'");
            }
        } catch (Exception e) {
            // ignore cleanup errors
        }
    }

    private int getRateLimitErrorCount() {
        try (Connection metaDb = getMetaConnection()) {
            try (Statement s = metaDb.createStatement();
                ResultSet rs = s.executeQuery(
                    "SELECT error_count FROM user_login_error_limit WHERE limit_key LIKE 'EXT_CATALOG:%'")) {
                if (rs.next()) {
                    return rs.getInt("error_count");
                }
                return -1;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to query error_count", e);
        }
    }

    private void updateExpireDateToPast() {
        try (Connection metaDb = getMetaConnection()) {
            try (Statement s = metaDb.createStatement()) {
                s.executeUpdate(
                    "UPDATE user_login_error_limit SET expire_date = '2000-01-01 00:00:00'"
                        + " WHERE limit_key LIKE 'EXT_CATALOG:%'");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to update expire_date", e);
        }
    }
}
