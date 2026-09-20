package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * IT: DDL RW lock FIFO ordering — two DDLs requiring the same resource are blocked by a third-party
 * blocker lock. Once the blocker releases the resource, the two DDLs must be granted the resource
 * in the order they joined the waiting queue (submission order), matching MySQL MDL-like FIFO
 * semantics for {@code ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE}.
 */
public class DdlRwLockFifoOrderTest extends BaseDdlEngineTestCase {

    private static final String TABLE_NAME = "ddl_fifo_order_tbl";

    @Before
    public void setUp() {
        cleanUp();
    }

    @After
    public void tearDown() {
        cleanUp();
    }

    private void cleanUp() {
        clearStaleBlockerLocks();
        dropTableIfExists(TABLE_NAME);
    }

    protected String currentSchema() {
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(tddlDatabase1)) {
            return tddlDatabase1;
        }
        try {
            return tddlConnection.getCatalog();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Insert a blocking EXCLUSIVE lock row into metaDB read_write_lock.
     * Owner must NOT start with "DDL_", otherwise acquireResource treats it as an orphan
     * DDL lock and auto-releases it.
     */
    protected void insertBlockerLock(String schema, String resource, String owner) throws Exception {
        try (Connection meta = getMetaConnection();
            PreparedStatement ps = meta.prepareStatement(
                "insert into read_write_lock(`schema_name`, `owner`, `resource`, `type`) values (?, ?, ?, 'EXCLUSIVE')")) {
            ps.setString(1, schema);
            ps.setString(2, owner);
            ps.setString(3, resource);
            ps.executeUpdate();
        }
    }

    protected void deleteBlockerLock(String owner) {
        try (Connection meta = getMetaConnection();
            PreparedStatement ps = meta.prepareStatement(
                "delete from read_write_lock where `owner` = ?")) {
            ps.setString(1, owner);
            ps.executeUpdate();
        } catch (Exception ignore) {
        }
    }

    /**
     * Clear any stale blocker locks left by a previously crashed run.
     */
    protected void clearStaleBlockerLocks() {
        try (Connection meta = getMetaConnection();
            PreparedStatement ps = meta.prepareStatement(
                "delete from read_write_lock where `owner` like 'TEST_FIFO_BLOCKER_%'")) {
            ps.executeUpdate();
        } catch (Exception ignore) {
        }
    }

    /**
     * Return the owners currently waiting for the given resource, ordered by queue_seq ascending
     * (i.e. FIFO submission order).
     */
    private List<String> queryWaitingOwnersOrderedByQueueSeq(String resource) throws Exception {
        List<String> owners = new ArrayList<>();
        try (Connection meta = getMetaConnection();
            PreparedStatement ps = meta.prepareStatement(
                "select `owner` from read_write_lock_waiting where `resource` = ? order by `queue_seq` asc")) {
            ps.setString(1, resource);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    owners.add(rs.getString(1));
                }
            }
        }
        return owners;
    }

    /**
     * Return the owner currently holding the EXCLUSIVE granted lock for the given resource,
     * or null if nobody holds it.
     */
    private String queryGrantedOwner(String resource) throws Exception {
        try (Connection meta = getMetaConnection();
            PreparedStatement ps = meta.prepareStatement(
                "select `owner` from read_write_lock where `resource` = ? and `type` = 'EXCLUSIVE'")) {
            ps.setString(1, resource);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        }
        return null;
    }

    /**
     * Poll until the waiting queue for the given resource reaches the expected size, or fail after
     * the timeout. Returns the owners in FIFO order once reached.
     */
    private List<String> waitForWaitingQueueSize(String resource, int expectedSize, int timeoutMillis)
        throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        List<String> owners = queryWaitingOwnersOrderedByQueueSeq(resource);
        while (owners.size() < expectedSize && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
            owners = queryWaitingOwnersOrderedByQueueSeq(resource);
        }
        org.junit.Assert.assertEquals(
            "Waiting queue for resource " + resource + " did not reach expected size " + expectedSize
                + " within timeout, actual owners: " + owners,
            expectedSize, owners.size());
        return owners;
    }

    /**
     * Test scenario: DDL A and DDL B both require the same table (blocked by a third-party
     * blocker). A is submitted first and joins the waiting queue first; B is submitted second and
     * joins second. Once the blocker releases the resource, A must be granted the resource (and
     * thus complete) before B is granted the resource — i.e. the two DDLs execute in submission
     * order, matching MySQL MDL FIFO semantics.
     */
    @Test(timeout = 90000)
    public void testTwoWaitingDdlsExecuteInSubmissionOrder() throws Exception {
        String schema = currentSchema();
        String resource = schema + "." + TABLE_NAME;
        String blockerOwner = "TEST_FIFO_BLOCKER_" + UUID.randomUUID().toString().replace("-", "");

        createPartitionedTable(TABLE_NAME, 4);

        Connection connA = getPolardbxConnection();
        Connection connB = getPolardbxConnection();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            insertBlockerLock(schema, resource, blockerOwner);

            // Submit DDL A first; it must join the waiting queue before DDL B.
            Future<Throwable> futureA = executor.submit(() -> {
                try {
                    JdbcUtil.executeUpdateSuccess(connA, "ALTER TABLE " + TABLE_NAME + " ADD COLUMN col_a INT");
                    return null;
                } catch (Throwable e) {
                    return e;
                }
            });

            // Wait until A has actually joined the waiting queue for the table resource before
            // submitting B, so the submission order is deterministic.
            waitForWaitingQueueSize(resource, 1, 30000);

            Future<Throwable> futureB = executor.submit(() -> {
                try {
                    JdbcUtil.executeUpdateSuccess(connB, "ALTER TABLE " + TABLE_NAME + " ADD COLUMN col_b INT");
                    return null;
                } catch (Throwable e) {
                    return e;
                }
            });

            // Capture the FIFO order once both DDLs are queued: owner with smallest queue_seq is A.
            List<String> waitingOrder = waitForWaitingQueueSize(resource, 2, 30000);
            String expectedFirstOwner = waitingOrder.get(0);
            String expectedSecondOwner = waitingOrder.get(1);
            org.junit.Assert.assertNotEquals(
                "The two waiting DDLs must have distinct owners", expectedFirstOwner, expectedSecondOwner);

            // Release the blocker and observe the sequence of EXCLUSIVE holders for the resource
            // until both DDLs complete. Only one owner can hold the EXCLUSIVE lock at a time, so
            // the first-seen distinct owner must match the FIFO order captured above.
            deleteBlockerLock(blockerOwner);

            Set<String> observedOrder = new LinkedHashSet<>();
            long deadline = System.currentTimeMillis() + 60000;
            while (!(futureA.isDone() && futureB.isDone()) && System.currentTimeMillis() < deadline) {
                String holder = queryGrantedOwner(resource);
                if (holder != null) {
                    observedOrder.add(holder);
                }
                Thread.sleep(100);
            }
            // Capture any remaining holder right after both futures complete, in case the poll
            // missed the last window.
            String finalHolder = queryGrantedOwner(resource);
            if (finalHolder != null) {
                observedOrder.add(finalHolder);
            }

            Throwable errorA = futureA.get(30, TimeUnit.SECONDS);
            Throwable errorB = futureB.get(30, TimeUnit.SECONDS);
            org.junit.Assert.assertNull("DDL A should succeed after the blocker is released",
                errorA == null ? null : errorA.toString());
            org.junit.Assert.assertNull("DDL B should succeed after the blocker is released",
                errorB == null ? null : errorB.toString());

            List<String> observedList = new ArrayList<>(observedOrder);
            org.junit.Assert.assertEquals(
                "The resource must be granted to the two DDLs in FIFO submission order, expected "
                    + waitingOrder + " but observed " + observedList,
                waitingOrder, observedList);

            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT col_a, col_b FROM " + TABLE_NAME + " LIMIT 1");
        } finally {
            executor.shutdownNow();
            deleteBlockerLock(blockerOwner);
            try {
                if (!connA.isClosed()) {
                    connA.close();
                }
            } catch (Exception ignore) {
            }
            try {
                if (!connB.isClosed()) {
                    connB.close();
                }
            } catch (Exception ignore) {
            }
            dropTableIfExists(TABLE_NAME);
        }
    }

    protected void createPartitionedTable(String tableName, int partitions) {
        JdbcUtil.executeSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
                + "PARTITION BY HASH(id) PARTITIONS %d",
            tableName, partitions));
    }
}
