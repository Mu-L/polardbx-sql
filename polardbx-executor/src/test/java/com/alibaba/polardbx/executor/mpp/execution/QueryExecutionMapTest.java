package com.alibaba.polardbx.executor.mpp.execution;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class QueryExecutionMapTest {

    private QueryExecutionMap queryExecutionMap;
    private TestQueryExecution testQueryExecution1;
    private TestQueryExecution testQueryExecution2;
    private TestQueryExecution testQueryExecution3;

    @Before
    public void setUp() {
        queryExecutionMap = new QueryExecutionMap();
        testQueryExecution1 = new TestQueryExecution("query1");
        testQueryExecution2 = new TestQueryExecution("query2");
        testQueryExecution3 = new TestQueryExecution("query3");
    }

    @After
    public void tearDown() {
        queryExecutionMap.clear();
    }

    @Test
    public void testRemoveWithStringKey() {
        String queryId = "testQuery1";
        queryExecutionMap.put(queryId, testQueryExecution1);

        QueryExecution result = queryExecutionMap.remove(queryId);

        assertNotNull(result);
        assertEquals(testQueryExecution1, result);
        assertFalse(queryExecutionMap.containsKey(queryId));
    }

    @Test
    public void testRemoveWithStringKeyNotFound() {
        String queryId = "nonExistentQuery";

        QueryExecution result = queryExecutionMap.remove(queryId);

        assertNull(result);
    }

    @Test
    public void testRemoveWithNonStringKey() {
        Integer intKey = 123;
        queryExecutionMap.put("query1", testQueryExecution1);

        QueryExecution result = queryExecutionMap.remove(intKey);

        assertNull(result);
        // Original entry should still be there
        assertTrue(queryExecutionMap.containsKey("query1"));
    }

    @Test
    public void testRemoveWithKeyValueBothValid() {
        String queryId = "testQuery2";
        queryExecutionMap.put(queryId, testQueryExecution2);

        boolean result = queryExecutionMap.remove(queryId, testQueryExecution2);

        assertTrue(result);
        assertFalse(queryExecutionMap.containsKey(queryId));
    }

    @Test
    public void testRemoveWithKeyValueMismatch() {
        String queryId = "testQuery3";
        queryExecutionMap.put(queryId, testQueryExecution3);

        boolean result = queryExecutionMap.remove(queryId, testQueryExecution1);

        assertFalse(result);
        assertTrue(queryExecutionMap.containsKey(queryId));
        assertEquals(testQueryExecution3, queryExecutionMap.get(queryId));
    }

    @Test
    public void testRemoveWithKeyValueInvalidTypes() {
        Integer intKey = 456;
        String stringValue = "notQueryExecution";

        boolean result = queryExecutionMap.remove(intKey, stringValue);

        assertFalse(result);
    }

    @Test
    public void testRemoveWithStringKeyButInvalidValue() {
        String queryId = "testQuery4";
        queryExecutionMap.put(queryId, testQueryExecution1);

        boolean result = queryExecutionMap.remove(queryId, "invalidValue");

        assertFalse(result);
        assertTrue(queryExecutionMap.containsKey(queryId));
    }

    @Test
    public void testClearWithMultipleEntries() {
        // Setup multiple entries
        queryExecutionMap.put("query1", testQueryExecution1);
        queryExecutionMap.put("query2", testQueryExecution2);
        queryExecutionMap.put("query3", testQueryExecution3);

        queryExecutionMap.clear();

        assertTrue(queryExecutionMap.isEmpty());
        assertEquals(0, queryExecutionMap.size());
    }

    @Test
    public void testClearWithEmptyMap() {
        assertTrue(queryExecutionMap.isEmpty());

        queryExecutionMap.clear();

        assertTrue(queryExecutionMap.isEmpty());
        assertEquals(0, queryExecutionMap.size());
    }

    @Test
    public void testClearWithSingleEntry() {
        // Test with single entry
        queryExecutionMap.put("singleQuery", testQueryExecution1);

        queryExecutionMap.clear();

        assertTrue(queryExecutionMap.isEmpty());
        assertEquals(0, queryExecutionMap.size());
    }

    @Test
    public void testConcurrentRemoveOperations() throws InterruptedException {
        // Test concurrent access to ensure thread safety
        int numThreads = 10;
        int numOperationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);

        // Pre-populate the map
        for (int i = 0; i < numOperationsPerThread; i++) {
            queryExecutionMap.put("query" + i, new TestQueryExecution("query" + i));
        }

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < numOperationsPerThread; j++) {
                        String queryId = "query" + j;
                        if (threadId % 2 == 0) {
                            // Test remove(Object key)
                            queryExecutionMap.remove(queryId);
                        } else {
                            // Test remove(Object key, Object value)
                            QueryExecution qe = queryExecutionMap.get(queryId);
                            if (qe != null) {
                                queryExecutionMap.remove(queryId, qe);
                            }
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        // All entries should be removed
        assertTrue(queryExecutionMap.isEmpty());
    }

    @Test
    public void testConcurrentClearOperations() throws InterruptedException {
        // Test concurrent clear operations
        int numThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    // Each thread adds some entries and then clears
                    for (int j = 0; j < 10; j++) {
                        queryExecutionMap.put("thread" + Thread.currentThread().getId() + "_query" + j,
                            new TestQueryExecution("query" + j));
                    }
                    queryExecutionMap.clear();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        // Map should be empty
        assertTrue(queryExecutionMap.isEmpty());
    }

    @Test
    public void testInheritanceFromConcurrentHashMap() {
        // Test that the class properly inherits from ConcurrentHashMap
        assertTrue(queryExecutionMap instanceof java.util.concurrent.ConcurrentHashMap);

        // Test basic ConcurrentHashMap operations still work
        queryExecutionMap.put("testKey", testQueryExecution1);
        assertTrue(queryExecutionMap.containsKey("testKey"));
        assertEquals(testQueryExecution1, queryExecutionMap.get("testKey"));
        assertEquals(1, queryExecutionMap.size());
    }

    // Simple test implementation of QueryExecution for testing
    private static class TestQueryExecution extends QueryExecution {
        private final String queryId;

        public TestQueryExecution(String queryId) {
            this.queryId = queryId;
        }

        @Override
        public String getQueryId() {
            return queryId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TestQueryExecution that = (TestQueryExecution) obj;
            return queryId != null ? queryId.equals(that.queryId) : that.queryId == null;
        }

        @Override
        public int hashCode() {
            return queryId != null ? queryId.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "TestQueryExecution{queryId='" + queryId + "'}";
        }

        // Minimal implementations for abstract methods - not used in our tests
        @Override
        public boolean isNeedReserveAfterExpired() {
            return false;
        }

        @Override
        public QueryInfo getQueryInfo() {
            return null;
        }

        @Override
        public long getTotalMemoryReservation() {
            return 0;
        }

        @Override
        public io.airlift.units.Duration getTotalCpuTime() {
            return new io.airlift.units.Duration(0, TimeUnit.MILLISECONDS);
        }

        @Override
        public TaskContext getTaskContext() {
            return null;
        }

        @Override
        public void start() {
        }

        @Override
        public void cancelStage(StageId stageId) {
        }

        @Override
        public void close(Throwable throwable) {
        }

        @Override
        public void mergeBloomFilter(
            java.util.List<com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo> filterInfos) {
        }
    }
}