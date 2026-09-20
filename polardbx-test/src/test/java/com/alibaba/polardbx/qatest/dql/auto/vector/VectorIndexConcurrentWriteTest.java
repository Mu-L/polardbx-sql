package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for Vector Index write-write concurrency on the same physical partition.
 * <p>
 * Background: DN vector index (HNSW) previously did not support concurrent writes —
 * multiple transactions inserting into the same physical partition would deadlock on
 * HNSW graph node locks. DN has now implemented write-write concurrency.
 * This test validates that concurrent writes to a partitioned table with vector index
 * no longer cause distributed deadlocks from the CN perspective.
 */
public class VectorIndexConcurrentWriteTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexConcurrentWriteTest.class);

    @org.junit.BeforeClass
    public static void setUpDatabase() throws Exception {
        assumeMysql80Dn();
        dropTestDatabase(DATABASE_NAME);
        createTestDatabase(DATABASE_NAME);
    }

    @org.junit.AfterClass
    public static void tearDownDatabase() throws Exception {
        dropTestDatabase(DATABASE_NAME);
    }

    private static final int THREAD_COUNT = 8;
    private static final int OPS_PER_THREAD = 50;
    private static final long TEST_TIMEOUT_SECONDS = 120;

    @Before
    public void init() throws Exception {
        dropTableIfExists("t_vec_cw_single");
        dropTableIfExists("t_vec_cw_batch");
        dropTableIfExists("t_vec_cw_mixed");
        dropTableIfExists("t_vec_cw_same_part");
        dropTableIfExists("t_vec_cw_ann");
        dropTableIfExists("t_vec_cw_txn");
        dropTableIfExists("t_vec_cw_dist_dml");
        dropTableIfExists("t_vec_cw_dist_txn");
        dropTableIfExists("t_vec_cw_dist_heavy");
    }

    private Connection newConnection() throws Exception {
        Connection conn = ConnectionManager.getInstance().newPolarDBXConnection(databaseName);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("SET innodb_lock_wait_timeout = 60");
        }
        return conn;
    }

    private String generateVector(long seed) {
        float v1 = (float) (Math.sin(seed) * 0.5 + 0.5);
        float v2 = (float) (Math.cos(seed) * 0.5 + 0.5);
        float v3 = (float) (Math.sin(seed * 2) * 0.5 + 0.5);
        float v4 = (float) (Math.cos(seed * 2) * 0.5 + 0.5);
        return String.format("[%f,%f,%f,%f]", v1, v2, v3, v4);
    }

    /**
     * Test concurrent INSERTs into a SINGLE table (one physical partition).
     * All writes go to the same physical table — maximum contention on HNSW locks.
     * Asserts: zero deadlocks, all rows inserted, ANN query works after.
     */
    @Test
    public void testConcurrentInsertSamePartition() throws Exception {
        String table = "t_vec_cw_single";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY, "
                    + "name VARCHAR(100), "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") SINGLE", table));
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger deadlockCount = new AtomicInteger(0);
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int i = 0; i < OPS_PER_THREAD; i++) {
                            long id = threadId * 10000L + i + 1;
                            String vec = generateVector(id);
                            String sql = String.format(
                                "INSERT INTO %s (id, name, emb) VALUES (%d, 'thread%d_row%d', VEC_FROMTEXT('%s'))",
                                table, id, threadId, i, vec);
                            try (Statement stmt = conn.createStatement()) {
                                stmt.execute(sql);
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                                errors.add("thread" + threadId + "_row" + i + ": " + e.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            }));
        }

        startLatch.countDown();
        assertTrue("Test timed out", doneLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals("Should have zero deadlocks — DN now supports concurrent vector writes",
            0, deadlockCount.get());
        assertTrue("Should have zero errors: " + errors, errors.isEmpty());

        int expected = THREAD_COUNT * OPS_PER_THREAD;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(rs.next());
            assertEquals("All rows must be present in table", expected, rs.getInt(1));
        }

        // Verify ANN query works correctly after concurrent inserts
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0.5,0.5,0.5,0.5]')) LIMIT 5")) {
            assertTrue("ANN query should return results after concurrent inserts", rs.next());
        }
    }

    /**
     * Test concurrent batch INSERTs (multi-row) into the same partition.
     * Each thread inserts multiple rows per statement, increasing contention.
     */
    @Test
    public void testConcurrentInsertSamePartitionMultiRow() throws Exception {
        String table = "t_vec_cw_batch";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY, "
                    + "name VARCHAR(100), "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") SINGLE", table));
        }

        final int BATCH_SIZE = 10;
        final int BATCHES_PER_THREAD = 10;

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger deadlockCount = new AtomicInteger(0);
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int batch = 0; batch < BATCHES_PER_THREAD; batch++) {
                            StringBuilder sb = new StringBuilder();
                            sb.append("INSERT INTO ").append(table).append(" (id, name, emb) VALUES ");
                            for (int i = 0; i < BATCH_SIZE; i++) {
                                if (i > 0) {
                                    sb.append(",");
                                }
                                long id = threadId * 100000L + batch * BATCH_SIZE + i + 1;
                                String vec = generateVector(id);
                                sb.append(String.format("(%d, 'batch_%d_%d', VEC_FROMTEXT('%s'))",
                                    id, threadId, batch * BATCH_SIZE + i, vec));
                            }
                            try (Statement stmt = conn.createStatement()) {
                                stmt.execute(sb.toString());
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                                errors.add("thread" + threadId + "_batch" + batch + ": " + e.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("Test timed out", doneLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals("Should have zero deadlocks with batch inserts", 0, deadlockCount.get());
        assertTrue("Should have zero errors: " + errors, errors.isEmpty());

        int expectedRows = THREAD_COUNT * BATCHES_PER_THREAD * BATCH_SIZE;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(rs.next());
            assertEquals("All rows must be present in table", expectedRows, rs.getInt(1));
        }
    }

    /**
     * Test concurrent mixed DML (INSERT/UPDATE/DELETE) on the same partition.
     * Verifies that write-write concurrency works for all DML types, not just INSERT.
     */
    @Test
    public void testConcurrentMixedDmlSamePartition() throws Exception {
        String table = "t_vec_cw_mixed";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY, "
                    + "name VARCHAR(100), "
                    + "val INT, "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") SINGLE", table));

            // Pre-insert data for UPDATE/DELETE operations.
            // Each thread owns a non-overlapping range: [threadId * OPS_PER_THREAD, (threadId+1) * OPS_PER_THREAD)
            int totalPreInsert = THREAD_COUNT * OPS_PER_THREAD;
            for (int i = 0; i < totalPreInsert; i++) {
                String vec = generateVector(i);
                stmt.execute(String.format(
                    "INSERT INTO %s (id, name, val, emb) VALUES (%d, 'seed_%d', %d, VEC_FROMTEXT('%s'))",
                    table, i, i, i, vec));
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger deadlockCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int i = 0; i < OPS_PER_THREAD; i++) {
                            int opType = (threadId + i) % 3;
                            String sql;
                            switch (opType) {
                            case 0: // INSERT new rows (unique IDs per thread)
                                long newId = 1000L + threadId * 10000L + i;
                                String vec = generateVector(newId);
                                sql = String.format(
                                    "INSERT INTO %s (id, name, val, emb) VALUES (%d, 'new_%d_%d', %d, VEC_FROMTEXT('%s'))",
                                    table, newId, threadId, i, (int) newId, vec);
                                break;
                            case 1: // UPDATE vector column (each thread targets its own range)
                                int updateId = threadId * OPS_PER_THREAD + i;
                                String newVec = generateVector(updateId + 9999);
                                sql = String.format(
                                    "UPDATE %s SET emb = VEC_FROMTEXT('%s'), name = 'upd_%d' WHERE id = %d",
                                    table, newVec, threadId, updateId);
                                break;
                            default: // DELETE (each thread targets its own range)
                                int delId = threadId * OPS_PER_THREAD + i;
                                sql = String.format("DELETE FROM %s WHERE id = %d", table, delId);
                                break;
                            }
                            try (Statement stmt = conn.createStatement()) {
                                stmt.execute(sql);
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                                errorCount.incrementAndGet();
                                errors.add("t" + threadId + "_i" + i + ": " + e.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("Test timed out", doneLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals("Should have zero deadlocks with mixed DML", 0, deadlockCount.get());
        assertEquals("Should have zero errors (each thread targets non-overlapping rows): " + errors,
            0, errorCount.get());

        // Verify table is still queryable and ANN works
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0.5,0.5,0.5,0.5]')) LIMIT 3")) {
            assertTrue("ANN query should work after concurrent mixed DML", rs.next());
        }
    }

    /**
     * Test concurrent INSERTs on a HASH-partitioned table where all rows
     * are forced into the same partition (same hash bucket).
     * This simulates the exact scenario that caused deadlocks during repartition backfill.
     */
    @Test
    public void testConcurrentInsertPartitionedTableSamePartition() throws Exception {
        String table = "t_vec_cw_same_part";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY, "
                    + "name VARCHAR(100), "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") PARTITION BY HASH(id) PARTITIONS 4", table));
        }

        // All threads insert IDs that hash to the same partition (multiples of 4)
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger deadlockCount = new AtomicInteger(0);
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int i = 0; i < OPS_PER_THREAD; i++) {
                            // Use IDs with same modulo 4 to target the same partition
                            long id = (threadId * OPS_PER_THREAD + i) * 4 + 4;
                            String vec = generateVector(id);
                            String sql = String.format(
                                "INSERT INTO %s (id, name, emb) VALUES (%d, 'same_part_%d_%d', VEC_FROMTEXT('%s'))",
                                table, id, threadId, i, vec);
                            try (Statement stmt = conn.createStatement()) {
                                stmt.execute(sql);
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                                errors.add("thread" + threadId + "_row" + i + ": " + e.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("Test timed out", doneLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals("Should have zero deadlocks on partitioned table with same-partition writes",
            0, deadlockCount.get());
        assertTrue("Should have zero errors: " + errors, errors.isEmpty());

        int expected = THREAD_COUNT * OPS_PER_THREAD;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(rs.next());
            assertEquals("All rows must be present in table", expected, rs.getInt(1));
        }
    }

    /**
     * Test concurrent INSERTs interleaved with ANN queries.
     * Verifies that vector index remains functional and searchable during concurrent writes.
     */
    @Test
    public void testConcurrentInsertWithAnnQuery() throws Exception {
        String table = "t_vec_cw_ann";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY, "
                    + "name VARCHAR(100), "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") SINGLE", table));

            // Seed some initial data so ANN queries return results from the start
            for (int i = 1; i <= 20; i++) {
                String vec = generateVector(i);
                stmt.execute(String.format(
                    "INSERT INTO %s (id, name, emb) VALUES (%d, 'seed_%d', VEC_FROMTEXT('%s'))",
                    table, i, i, vec));
            }
        }

        int writerThreads = 6;
        int readerThreads = 2;
        ExecutorService executor = Executors.newFixedThreadPool(writerThreads + readerThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(writerThreads + readerThreads);
        AtomicInteger deadlockCount = new AtomicInteger(0);
        AtomicInteger querySuccessCount = new AtomicInteger(0);
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

        // Writer threads
        for (int t = 0; t < writerThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int i = 0; i < OPS_PER_THREAD; i++) {
                            long id = 100L + threadId * 10000L + i;
                            String vec = generateVector(id);
                            String sql = String.format(
                                "INSERT INTO %s (id, name, emb) VALUES (%d, 'writer_%d_%d', VEC_FROMTEXT('%s'))",
                                table, id, threadId, i, vec);
                            try (Statement stmt = conn.createStatement()) {
                                stmt.execute(sql);
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                                errors.add("insert_t" + threadId + "_row" + i + ": " + e.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Reader threads (ANN queries)
        for (int t = 0; t < readerThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int i = 0; i < OPS_PER_THREAD; i++) {
                            String vec = generateVector(threadId * 1000L + i);
                            String sql = String.format(
                                "SELECT id FROM %s ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('%s')) LIMIT 5",
                                table, vec);
                            try (Statement stmt = conn.createStatement();
                                ResultSet rs = stmt.executeQuery(sql)) {
                                if (rs.next()) {
                                    querySuccessCount.incrementAndGet();
                                }
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("Test timed out", doneLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals("Should have zero deadlocks during concurrent insert+query", 0, deadlockCount.get());
        assertTrue("Should have zero errors: " + errors, errors.isEmpty());

        // Verify all inserted rows are present (seed 20 + writer threads * OPS_PER_THREAD)
        int expectedInserts = 20 + writerThreads * OPS_PER_THREAD;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(rs.next());
            assertEquals("All rows must be present in table", expectedInserts, rs.getInt(1));
        }
        assertTrue("Should have successful ANN queries during writes", querySuccessCount.get() > 0);
    }

    /**
     * Test concurrent INSERTs within explicit transactions (autoCommit=false)
     * on the same partition. Transactions are held open longer, increasing the
     * window for lock contention on HNSW graph nodes.
     */
    @Test
    public void testConcurrentInsertExplicitTransaction() throws Exception {
        String table = "t_vec_cw_txn";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY, "
                    + "name VARCHAR(100), "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") SINGLE", table));
        }

        final int ROWS_PER_TXN = 10;
        final int TXNS_PER_THREAD = 5;

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger deadlockCount = new AtomicInteger(0);
        AtomicInteger commitCount = new AtomicInteger(0);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        conn.setAutoCommit(false);
                        for (int txn = 0; txn < TXNS_PER_THREAD; txn++) {
                            try {
                                for (int i = 0; i < ROWS_PER_TXN; i++) {
                                    long id = threadId * 100000L + txn * ROWS_PER_TXN + i;
                                    String vec = generateVector(id);
                                    String sql = String.format(
                                        "INSERT INTO %s (id, name, emb) VALUES (%d, 'txn_%d_%d_%d', VEC_FROMTEXT('%s'))",
                                        table, id, threadId, txn, i, vec);
                                    try (Statement stmt = conn.createStatement()) {
                                        stmt.execute(sql);
                                    }
                                }
                                conn.commit();
                                commitCount.incrementAndGet();
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                                try {
                                    conn.rollback();
                                } catch (SQLException ignored) {
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("Test timed out", doneLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals("Should have zero deadlocks with explicit transactions", 0, deadlockCount.get());
        assertEquals("All transactions should commit",
            THREAD_COUNT * TXNS_PER_THREAD, commitCount.get());

        int expectedRows = THREAD_COUNT * TXNS_PER_THREAD * ROWS_PER_TXN;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(rs.next());
            assertEquals(expectedRows, rs.getInt(1));
        }
    }

    /**
     * Test concurrent mixed DML (INSERT/UPDATE/DELETE) + ANN queries on a HASH-partitioned
     * distributed logical table with vector index.
     * Each thread's operations span multiple partitions. This exercises the CN distributed
     * transaction coordinator under concurrent vector DML to verify no deadlocks occur.
     */
    @Test
    public void testConcurrentMixedDmlPartitionedTable() throws Exception {
        String table = "t_vec_cw_dist_dml";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY, "
                    + "name VARCHAR(100), "
                    + "val INT, "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") PARTITION BY HASH(id) PARTITIONS 4", table));

            // Pre-insert data spread across all partitions
            for (int i = 0; i < 400; i++) {
                String vec = generateVector(i);
                stmt.execute(String.format(
                    "INSERT INTO %s (id, name, val, emb) VALUES (%d, 'seed_%d', %d, VEC_FROMTEXT('%s'))",
                    table, i, i, i, vec));
            }
        }

        int writerThreads = 6;
        int readerThreads = 2;
        int totalThreads = writerThreads + readerThreads;
        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalThreads);
        AtomicInteger deadlockCount = new AtomicInteger(0);
        AtomicInteger dmlSuccessCount = new AtomicInteger(0);
        AtomicInteger querySuccessCount = new AtomicInteger(0);

        // Writer threads: mixed INSERT/UPDATE/DELETE across partitions
        for (int t = 0; t < writerThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int i = 0; i < OPS_PER_THREAD; i++) {
                            int opType = (threadId + i) % 3;
                            String sql;
                            switch (opType) {
                            case 0: // INSERT new rows (unique IDs across threads)
                                long newId = 10000L + threadId * 10000L + i;
                                String vec = generateVector(newId);
                                sql = String.format(
                                    "INSERT INTO %s (id, name, val, emb) VALUES (%d, 'new_%d_%d', %d, VEC_FROMTEXT('%s'))",
                                    table, newId, threadId, i, (int) newId, vec);
                                break;
                            case 1: // UPDATE vector column on existing rows
                                int updateId = (threadId * OPS_PER_THREAD + i) % 400;
                                String newVec = generateVector(updateId + 50000);
                                sql = String.format(
                                    "UPDATE %s SET emb = VEC_FROMTEXT('%s'), name = 'upd_%d_%d' WHERE id = %d",
                                    table, newVec, threadId, i, updateId);
                                break;
                            default: // DELETE existing rows
                                int delId = (threadId * OPS_PER_THREAD + i) % 400;
                                sql = String.format("DELETE FROM %s WHERE id = %d", table, delId);
                                break;
                            }
                            try (Statement stmt = conn.createStatement()) {
                                stmt.execute(sql);
                                dmlSuccessCount.incrementAndGet();
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Reader threads: continuous ANN queries during writes
        for (int t = 0; t < readerThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int i = 0; i < OPS_PER_THREAD; i++) {
                            String vec = generateVector(threadId * 1000L + i);
                            String sql = String.format(
                                "SELECT id, VEC_DISTANCE(emb, VEC_FROMTEXT('%s')) AS dist FROM %s "
                                    + "ORDER BY dist LIMIT 5",
                                vec, table);
                            try (Statement stmt = conn.createStatement();
                                ResultSet rs = stmt.executeQuery(sql)) {
                                while (rs.next()) {
                                    rs.getLong("id");
                                    rs.getDouble("dist");
                                }
                                querySuccessCount.incrementAndGet();
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("Test timed out", doneLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals("Should have zero deadlocks with mixed DML on partitioned table", 0, deadlockCount.get());
        assertTrue("DML operations should succeed", dmlSuccessCount.get() > 0);
        assertTrue("ANN queries should succeed during concurrent DML", querySuccessCount.get() > 0);

        // Verify table is still consistent and queryable
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0.5,0.5,0.5,0.5]')) LIMIT 5")) {
            assertTrue("ANN query must work after concurrent mixed DML on partitioned table", rs.next());
        }
    }

    /**
     * Test concurrent explicit transactions with mixed DML + ANN queries on a
     * HASH-partitioned distributed logical table.
     * Each transaction inserts/updates/deletes multiple rows potentially spanning multiple
     * partitions, maximizing cross-partition lock contention.
     */
    @Test
    public void testConcurrentExplicitTxnPartitionedTable() throws Exception {
        String table = "t_vec_cw_dist_txn";
        final int ROWS_PER_TXN = 8;
        final int TXNS_PER_THREAD = 5;
        int writerThreads = 6;
        int readerThreads = 2;
        int totalThreads = writerThreads + readerThreads;

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY, "
                    + "name VARCHAR(100), "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") PARTITION BY HASH(id) PARTITIONS 4", table));

            // Pre-insert data across all partitions.
            // Each writer thread owns a non-overlapping range of TXNS_PER_THREAD * ROWS_PER_TXN rows.
            int rowsPerThread = TXNS_PER_THREAD * ROWS_PER_TXN;
            int totalPreInsert = writerThreads * rowsPerThread;
            for (int i = 0; i < totalPreInsert; i++) {
                String vec = generateVector(i);
                stmt.execute(String.format(
                    "INSERT INTO %s (id, name, emb) VALUES (%d, 'seed_%d', VEC_FROMTEXT('%s'))",
                    table, i, i, vec));
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalThreads);
        AtomicInteger deadlockCount = new AtomicInteger(0);
        AtomicInteger commitCount = new AtomicInteger(0);
        AtomicInteger querySuccessCount = new AtomicInteger(0);

        // Writer threads: explicit transactions with multi-row DML
        for (int t = 0; t < writerThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        conn.setAutoCommit(false);
                        for (int txn = 0; txn < TXNS_PER_THREAD; txn++) {
                            try {
                                for (int i = 0; i < ROWS_PER_TXN; i++) {
                                    int opType = (threadId + txn + i) % 3;
                                    String sql;
                                    switch (opType) {
                                    case 0: // INSERT (unique IDs)
                                        long newId = 10000L + threadId * 100000L + txn * ROWS_PER_TXN + i;
                                        String vec = generateVector(newId);
                                        sql = String.format(
                                            "INSERT INTO %s (id, name, emb) VALUES (%d, 'txn_%d_%d_%d', VEC_FROMTEXT('%s'))",
                                            table, newId, threadId, txn, i, vec);
                                        break;
                                    case 1: // UPDATE (each thread targets its own range)
                                        int updateId = threadId * TXNS_PER_THREAD * ROWS_PER_TXN
                                            + txn * ROWS_PER_TXN + i;
                                        String newVec = generateVector(updateId + 80000);
                                        sql = String.format(
                                            "UPDATE %s SET emb = VEC_FROMTEXT('%s') WHERE id = %d",
                                            table, newVec, updateId);
                                        break;
                                    default: // DELETE (each thread targets its own range)
                                        int delId = threadId * TXNS_PER_THREAD * ROWS_PER_TXN
                                            + txn * ROWS_PER_TXN + i;
                                        sql = String.format("DELETE FROM %s WHERE id = %d", table, delId);
                                        break;
                                    }
                                    try (Statement stmt = conn.createStatement()) {
                                        stmt.execute(sql);
                                    }
                                }
                                conn.commit();
                                commitCount.incrementAndGet();
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                                try {
                                    conn.rollback();
                                } catch (SQLException ignored) {
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Reader threads: ANN queries during transactions
        for (int t = 0; t < readerThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int i = 0; i < OPS_PER_THREAD; i++) {
                            String vec = generateVector(threadId * 1000L + i);
                            String sql = String.format(
                                "SELECT id FROM %s ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('%s')) LIMIT 5",
                                table, vec);
                            try (Statement stmt = conn.createStatement();
                                ResultSet rs = stmt.executeQuery(sql)) {
                                while (rs.next()) {
                                    rs.getLong("id");
                                }
                                querySuccessCount.incrementAndGet();
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("Test timed out", doneLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals("Should have zero deadlocks with explicit txns on partitioned table",
            0, deadlockCount.get());
        assertTrue("Transactions should commit successfully", commitCount.get() > 0);
        assertTrue("ANN queries should succeed during concurrent txns", querySuccessCount.get() > 0);

        // Verify table is consistent
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0.5,0.5,0.5,0.5]')) LIMIT 5")) {
            assertTrue("ANN query must work after concurrent explicit txns on partitioned table", rs.next());
        }
    }

    /**
     * Test heavy concurrent DML load on a HASH-partitioned distributed logical table:
     * simultaneous INSERT, UPDATE, DELETE and ANN queries with more threads and operations.
     * Designed to stress-test the vector index concurrent write path and ensure
     * no deadlocks under high contention across multiple partitions.
     */
    @Test
    public void testHeavyConcurrentDmlPartitionedTable() throws Exception {
        String table = "t_vec_cw_dist_heavy";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT PRIMARY KEY, "
                    + "name VARCHAR(100), "
                    + "val INT, "
                    + "emb VECTOR(4), "
                    + "VECTOR INDEX vi1(emb) DISTANCE=COSINE M=16"
                    + ") PARTITION BY HASH(id) PARTITIONS 4", table));

            // Pre-insert 500 rows spread across all partitions
            for (int i = 0; i < 500; i++) {
                String vec = generateVector(i);
                stmt.execute(String.format(
                    "INSERT INTO %s (id, name, val, emb) VALUES (%d, 'seed_%d', %d, VEC_FROMTEXT('%s'))",
                    table, i, i, i, vec));
            }
        }

        int insertThreads = 4;
        int updateThreads = 3;
        int deleteThreads = 2;
        int queryThreads = 3;
        int totalThreads = insertThreads + updateThreads + deleteThreads + queryThreads;
        int opsPerThread = 40;

        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalThreads);
        AtomicInteger deadlockCount = new AtomicInteger(0);
        AtomicInteger insertSuccessCount = new AtomicInteger(0);
        AtomicInteger updateSuccessCount = new AtomicInteger(0);
        AtomicInteger deleteSuccessCount = new AtomicInteger(0);
        AtomicInteger querySuccessCount = new AtomicInteger(0);
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

        // INSERT threads: insert new rows with unique IDs
        for (int t = 0; t < insertThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int i = 0; i < opsPerThread; i++) {
                            long id = 100000L + threadId * 10000L + i;
                            String vec = generateVector(id);
                            String sql = String.format(
                                "INSERT INTO %s (id, name, val, emb) VALUES (%d, 'ins_%d_%d', %d, VEC_FROMTEXT('%s'))",
                                table, id, threadId, i, (int) (id % 1000), vec);
                            try (Statement stmt = conn.createStatement()) {
                                stmt.execute(sql);
                                insertSuccessCount.incrementAndGet();
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                                errors.add("insert_t" + threadId + "_i" + i + ": " + e.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // UPDATE threads: update vector column on pre-existing rows.
        // Each update thread targets a non-overlapping range: [threadId * opsPerThread, (threadId+1) * opsPerThread)
        for (int t = 0; t < updateThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int i = 0; i < opsPerThread; i++) {
                            int updateId = threadId * opsPerThread + i;
                            String newVec = generateVector(updateId + 70000L + threadId * 10000L);
                            String sql = String.format(
                                "UPDATE %s SET emb = VEC_FROMTEXT('%s'), val = %d WHERE id = %d",
                                table, newVec, i, updateId);
                            try (Statement stmt = conn.createStatement()) {
                                stmt.execute(sql);
                                updateSuccessCount.incrementAndGet();
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                                errors.add("update_t" + threadId + "_i" + i + ": " + e.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // DELETE threads: delete pre-existing rows.
        // Each delete thread targets a non-overlapping range separate from update threads:
        // [(updateThreads + threadId) * opsPerThread, (updateThreads + threadId + 1) * opsPerThread)
        for (int t = 0; t < deleteThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int i = 0; i < opsPerThread; i++) {
                            int delId = (updateThreads + threadId) * opsPerThread + i;
                            String sql = String.format("DELETE FROM %s WHERE id = %d", table, delId);
                            try (Statement stmt = conn.createStatement()) {
                                stmt.execute(sql);
                                deleteSuccessCount.incrementAndGet();
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                                errors.add("delete_t" + threadId + "_i" + i + ": " + e.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // QUERY threads: continuous ANN queries
        for (int t = 0; t < queryThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (Connection conn = newConnection()) {
                        for (int i = 0; i < opsPerThread; i++) {
                            String vec = generateVector(threadId * 10000L + i);
                            String sql = String.format(
                                "SELECT id, VEC_DISTANCE(emb, VEC_FROMTEXT('%s')) AS dist FROM %s "
                                    + "ORDER BY dist LIMIT 10",
                                vec, table);
                            try (Statement stmt = conn.createStatement();
                                ResultSet rs = stmt.executeQuery(sql)) {
                                while (rs.next()) {
                                    rs.getLong("id");
                                    rs.getDouble("dist");
                                }
                                querySuccessCount.incrementAndGet();
                            } catch (SQLException e) {
                                if (e.getMessage().contains("Deadlock found")) {
                                    deadlockCount.incrementAndGet();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("Test timed out", doneLatch.await(TEST_TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals("Should have zero deadlocks under heavy concurrent DML on partitioned table",
            0, deadlockCount.get());
        assertTrue("Should have zero errors: " + errors, errors.isEmpty());

        assertTrue("UPDATE operations should succeed", updateSuccessCount.get() > 0);
        assertTrue("DELETE operations should succeed", deleteSuccessCount.get() > 0);
        assertTrue("ANN queries should succeed during heavy concurrent DML", querySuccessCount.get() > 0);

        // Verify table is consistent and queryable after heavy load
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                assertTrue(rs.next());
                assertTrue("Table should have rows after heavy DML", rs.getInt(1) > 0);
            }
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id FROM " + table
                    + " ORDER BY VEC_DISTANCE(emb, VEC_FROMTEXT('[0.5,0.5,0.5,0.5]')) LIMIT 5")) {
                assertTrue("ANN query must work after heavy concurrent DML on partitioned table", rs.next());
            }
        }
    }
}
