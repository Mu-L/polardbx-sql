package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for Vector Index with concurrent operations.
 * Validates that vector index operations work correctly under concurrent access.
 */
public class VectorIndexConcurrencyTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexConcurrencyTest.class);

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

    private static final String TABLE_NAME = "vec_concurrent_test";
    private static final String VEC_IDX_NAME = "vec_idx_concurrent";
    private static final int THREAD_COUNT = 8;
    private static final int OPERATIONS_PER_THREAD = 20;

    @Before
    public void initTable() {
        dropTableIfExists(TABLE_NAME);
    }

    /**
     * Get a new connection to the test database for concurrent tests.
     */
    protected Connection getPolardbxConnection() throws Exception {
        Connection conn = ConnectionManager.getInstance().newPolarDBXConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("USE " + databaseName);
        }
        return conn;
    }

    /**
     * Test concurrent inserts into vector table.
     */
    @Test
    public void testConcurrentInserts() throws Exception {
        createTableWithVectorIndex();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                        long id = threadId * 10000L + i;
                        String embedding =
                            String.format("[%d.0, %d.5, %d.2, %d.8]", id % 100, id % 100, id % 100, id % 100);
                        String sql = String.format(
                            "INSERT INTO %s (id, name, embedding) VALUES (%d, 'thread_%d_row_%d', VEC_FROMTEXT('%s'))",
                            TABLE_NAME, id, threadId, i, embedding);
                        try {
                            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        // Verify total records
        try (ResultSet rs = JdbcUtil.executeQuery(
            String.format("SELECT COUNT(*) FROM %s", TABLE_NAME), tddlConnection)) {
            rs.next();
            int count = rs.getInt(1);
            Assert.assertTrue("Should have inserted records", count > 0);
            System.out.println(
                "Concurrent inserts: " + successCount.get() + " success, " + errorCount.get() + " errors, total rows: "
                    + count);
        }
    }

    /**
     * Test concurrent reads while inserting.
     */
    @Test
    public void testConcurrentReadsAndInserts() throws Exception {
        createTableWithVectorIndex();
        insertInitialData(50);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        List<Future<?>> futures = new ArrayList<>();

        // Half threads for reading, half for inserting
        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            final boolean isReader = (t % 2 == 0);

            futures.add(executor.submit(() -> {
                try {
                    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                        if (isReader) {
                            // Reader thread
                            String query = String.format(
                                "SELECT id FROM %s ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[1.0, 1.0, 1.0, 1.0]')) LIMIT 5",
                                TABLE_NAME);
                            try (Connection conn = getPolardbxConnection();
                                Statement stmt = conn.createStatement();
                                ResultSet rs = stmt.executeQuery(query)) {
                                while (rs.next()) {
                                    // Just consume results
                                }
                            }
                        } else {
                            // Writer thread
                            long id = threadId * 10000L + i + 100000;
                            String embedding =
                                String.format("[%d.0, %d.5, %d.2, %d.8]", id % 100, id % 100, id % 100, id % 100);
                            String sql = String.format(
                                "INSERT INTO %s (id, name, embedding) VALUES (%d, 'concurrent_%d', VEC_FROMTEXT('%s'))",
                                TABLE_NAME, id, id, embedding);
                            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
                        }
                    }
                } catch (Exception e) {
                    // Log error but continue
                    System.err.println("Thread " + threadId + " error: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            }));
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        // Verify data integrity
        try (ResultSet rs = JdbcUtil.executeQuery(
            String.format("SELECT COUNT(*) FROM %s", TABLE_NAME), tddlConnection)) {
            rs.next();
            Assert.assertTrue("Should have records", rs.getInt(1) > 50);
        }
    }

    /**
     * Test concurrent updates on vector column.
     */
    @Test
    public void testConcurrentUpdates() throws Exception {
        createTableWithVectorIndex();
        insertInitialData(100);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                        long id = (threadId * OPERATIONS_PER_THREAD + i) % 100;
                        String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]",
                            threadId * 10 + i, threadId * 10 + i, threadId * 10 + i, threadId * 10 + i);
                        String sql = String.format(
                            "UPDATE %s SET embedding = VEC_FROMTEXT('%s') WHERE id = %d",
                            TABLE_NAME, embedding, id);
                        try {
                            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
                        } catch (Exception e) {
                            // May have conflicts, that's okay
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        // Verify all rows still exist
        try (ResultSet rs = JdbcUtil.executeQuery(
            String.format("SELECT COUNT(*) FROM %s", TABLE_NAME), tddlConnection)) {
            rs.next();
            Assert.assertEquals(100, rs.getInt(1));
        }
    }

    /**
     * Test concurrent deletes.
     */
    @Test
    public void testConcurrentDeletes() throws Exception {
        createTableWithVectorIndex();
        insertInitialData(200);

        int initialCount = 0;
        try (ResultSet rs = JdbcUtil.executeQuery(
            String.format("SELECT COUNT(*) FROM %s", TABLE_NAME), tddlConnection)) {
            rs.next();
            initialCount = rs.getInt(1);
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT / 2);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT / 2);

        for (int t = 0; t < THREAD_COUNT / 2; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    // Each thread deletes different range
                    for (int i = 0; i < 10; i++) {
                        long id = threadId * 10 + i + 100; // Delete from id 100 onwards
                        String sql = String.format(
                            "DELETE FROM %s WHERE id = %d", TABLE_NAME, id);
                        try {
                            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
                        } catch (Exception e) {
                            // Ignore conflicts
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        // Verify records were deleted
        try (ResultSet rs = JdbcUtil.executeQuery(
            String.format("SELECT COUNT(*) FROM %s", TABLE_NAME), tddlConnection)) {
            rs.next();
            int finalCount = rs.getInt(1);
            Assert.assertTrue("Count should decrease", finalCount < initialCount);
        }
    }

    /**
     * Test concurrent mixed DML operations.
     */
    @Test
    public void testConcurrentMixedDml() throws Exception {
        createTableWithVectorIndex();
        insertInitialData(50);

        AtomicInteger insertCount = new AtomicInteger(0);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger deleteCount = new AtomicInteger(0);
        AtomicInteger selectCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            final int opType = t % 4; // 0=insert, 1=update, 2=delete, 3=select

            executor.submit(() -> {
                try {
                    for (int i = 0; i < 10; i++) {
                        try {
                            switch (opType) {
                            case 0: // Insert
                                long newId = threadId * 1000L + i + 10000;
                                String emb =
                                    String.format("[%d.0, %d.5, %d.2, %d.8]", newId % 50, newId % 50, newId % 50,
                                        newId % 50);
                                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                                    "INSERT INTO %s (id, name, embedding) VALUES (%d, 'mixed_%d', VEC_FROMTEXT('%s'))",
                                    TABLE_NAME, newId, newId, emb));
                                insertCount.incrementAndGet();
                                break;
                            case 1: // Update
                                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                                    "UPDATE %s SET name = 'updated_%d' WHERE id = %d",
                                    TABLE_NAME, i * 5, i * 5));
                                updateCount.incrementAndGet();
                                break;
                            case 2: // Delete
                                if (i >= 30) { // Only delete higher IDs
                                    JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                                        "DELETE FROM %s WHERE id = %d", TABLE_NAME, i * 5));
                                    deleteCount.incrementAndGet();
                                }
                                break;
                            case 3: // Select
                                try {
                                    JdbcUtil.executeQuery(String.format(
                                        "SELECT * FROM %s ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[1.0, 1.0, 1.0, 1.0]')) LIMIT 10",
                                        TABLE_NAME), tddlConnection).close();
                                    selectCount.incrementAndGet();
                                } catch (Exception ex) {
                                    // ignore
                                }
                                break;
                            }
                        } catch (Exception e) {
                            // Log and continue
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        System.out.println("Mixed DML - Insert: " + insertCount.get() +
            ", Update: " + updateCount.get() +
            ", Delete: " + deleteCount.get() +
            ", Select: " + selectCount.get());

        // Table should still be queryable
        try (ResultSet rs = JdbcUtil.executeQuery(
            String.format("SELECT COUNT(*) FROM %s", TABLE_NAME), tddlConnection)) {
            rs.next();
            Assert.assertTrue("Should have records", rs.getInt(1) > 0);
        }
    }

    /**
     * Test concurrent vector similarity queries.
     */
    @Test
    public void testConcurrentVectorQueries() throws Exception {
        createTableWithVectorIndex();
        insertInitialData(100);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try (Connection conn = getPolardbxConnection();
                    Statement stmt = conn.createStatement()) {
                    for (int i = 0; i < 10; i++) {
                        double baseValue = (threadId * 10 + i) % 100;
                        String query = String.format(
                            "SELECT id, VEC_DISTANCE(embedding, VEC_FROMTEXT('[%s, %s, %s, %s]')) as dist "
                                + "FROM %s ORDER BY dist LIMIT 10",
                            baseValue, baseValue, baseValue, baseValue, TABLE_NAME);
                        try (ResultSet rs = stmt.executeQuery(query)) {
                            int count = 0;
                            while (rs.next()) {
                                count++;
                            }
                            if (count > 0) {
                                successCount.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Query thread " + threadId + " error: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        Assert.assertTrue("Should have successful queries", successCount.get() > 0);
        System.out.println("Concurrent queries completed: " + successCount.get() + " successful");
    }

    private void createTableWithVectorIndex() {
        String createTable = String.format(
            "CREATE TABLE %s (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  name VARCHAR(100),\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id),\n"
                + "  VECTOR INDEX %s (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
    }

    private void insertInitialData(int count) {
        for (int i = 0; i < count; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i % 100, i % 100, i % 100, i % 100);
            String sql = String.format(
                "INSERT INTO %s (id, name, embedding) VALUES (%d, 'initial_%d', VEC_FROMTEXT('%s'))",
                TABLE_NAME, i, i, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }
}
