package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Regression test for AONE-85095101: GmsSyncDataSource establishes sync connections without
 * pooling or retry, so transient network jitter between CN nodes exhausts the DDL engine
 * retries and pauses the DDL job.
 * <p>
 * FP_SYNC_LOCAL_VIA_MANAGER forces the cluster sync to connect the local node through the real
 * JDBC manager-port path; FP_GMS_SYNC_CONN_FAIL_TIMES injects N transient connection-establishment
 * failures. The DDL must survive the jitter: the connection layer is expected to retry and
 * recover instead of letting the job become PAUSED.
 */
@NotThreadSafe
@RunWith(Parameterized.class)
public class SyncConnectionRetryTest extends DDLBaseNewDBTestCase {

    private static final Log log = LogFactory.getLog(SyncConnectionRetryTest.class);

    private static final int INJECTED_CONN_FAIL_TIMES = 5;

    public SyncConnectionRetryTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}
        });
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @After
    public void cleanupFailPoints() {
        JdbcUtil.executeIgnoreErrors(tddlConnection, "SET @FP_GMS_SYNC_CONN_FAIL_TIMES = null");
        JdbcUtil.executeIgnoreErrors(tddlConnection, "SET @FP_SYNC_LOCAL_VIA_MANAGER = null");
        // In the Red state a paused DDL job may remain; drop database would be blocked by it.
        JdbcUtil.executeIgnoreErrors(tddlConnection, "remove ddl all paused");
        cleanDataBase();
    }

    @Test
    public void testDdlSurvivesSyncConnectionJitter() throws SQLException {
        String mytable = schemaPrefix + randomTableName("sync_conn_retry", 4);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }

        // Create the base table before arming any fault injection.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table if not exists " + mytable
                + "(a int, b char, d int, primary key(d)) partition by hash(a) partitions 16");

        // Route cluster sync through the real JDBC manager-port path (including the local node),
        // then inject transient connection-establishment failures to simulate network jitter.
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @FP_SYNC_LOCAL_VIA_MANAGER = 'true'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "SET @FP_GMS_SYNC_CONN_FAIL_TIMES = '" + INJECTED_CONN_FAIL_TIMES + "'");

        // The DDL must survive the jitter: the sync connection layer is expected to retry and
        // recover. Before the fix, every sync attempt fails immediately, the DDL engine exhausts
        // its retries (1 + 3 recovery retries < injected failures) and the job becomes PAUSED,
        // so this statement returns an error.
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table " + mytable + " add column c int");

        // The schema change must really take effect after the DDL completes.
        boolean foundColumn = false;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("desc " + mytable)) {
            while (rs.next()) {
                if ("c".equalsIgnoreCase(rs.getString(1))) {
                    foundColumn = true;
                    break;
                }
            }
        }
        if (!foundColumn) {
            throw new AssertionError("column c is missing after alter table succeeded, table: " + mytable);
        }

        // No paused DDL job may remain for the test schema.
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("show ddl")) {
            while (rs.next()) {
                String state = rs.getString("STATE");
                String objectSchema = rs.getString("OBJECT_SCHEMA");
                if ("PAUSED".equalsIgnoreCase(state) && tddlDatabase1 != null
                    && tddlDatabase1.equalsIgnoreCase(objectSchema)) {
                    throw new AssertionError(
                        "found PAUSED ddl job after jitter was supposed to be absorbed: " + rs.getString("JOB_ID"));
                }
            }
        }
    }
}
