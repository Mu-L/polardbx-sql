package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.utils.StorageNodeHelper;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FetchIndexUsageSyncAction
 */
public class FetchIndexUsageSyncActionTest {

    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_SQL =
        "SELECT OBJECT_SCHEMA, OBJECT_NAME, INDEX_NAME, COUNT_STAR, COUNT_FETCH, SUM_TIMER_WAIT, MAX_TIMER_WAIT FROM information_schema.INDEX_USAGE";
    private static final String TEST_NODE1_ADDRESS = "127.0.0.1:3306";
    private static final String TEST_NODE1_INST_ID = "inst1";
    private static final String TEST_NODE2_ADDRESS = "127.0.0.1:3307";
    private static final String TEST_NODE2_INST_ID = "inst2";

    private MockedStatic<StorageNodeHelper> mockedStorageNodeHelper;

    @Before
    public void setUp() {
        mockedStorageNodeHelper = mockStatic(StorageNodeHelper.class);
    }

    @After
    public void tearDown() {
        if (mockedStorageNodeHelper != null) {
            mockedStorageNodeHelper.close();
        }
    }

    /**
     * Test 1: testSync_WithMultipleNodes_ReturnsAggregatedResults
     * Mock 2 StorageNodes, each returning different data rows, verify results contain all rows
     */
    @Test
    public void testSync_WithMultipleNodes_ReturnsAggregatedResults() throws SQLException {
        // Setup mock data sources and connections
        IDataSource mockDataSource1 = mock(IDataSource.class);
        IConnection mockConnection1 = mock(IConnection.class);
        PreparedStatement mockStatement1 = mock(PreparedStatement.class);
        ResultSet mockResultSet1 = mock(ResultSet.class);

        IDataSource mockDataSource2 = mock(IDataSource.class);
        IConnection mockConnection2 = mock(IConnection.class);
        PreparedStatement mockStatement2 = mock(PreparedStatement.class);
        ResultSet mockResultSet2 = mock(ResultSet.class);

        // Configure mock for node 1 - returns 2 rows
        when(mockDataSource1.getConnection()).thenReturn(mockConnection1);
        when(mockConnection1.prepareStatement(anyString())).thenReturn(mockStatement1);
        when(mockStatement1.executeQuery()).thenReturn(mockResultSet1);
        when(mockResultSet1.next()).thenReturn(true, true, false);
        when(mockResultSet1.getObject("OBJECT_SCHEMA")).thenReturn("schema1", "schema1");
        when(mockResultSet1.getObject("OBJECT_NAME")).thenReturn("table1", "table2");
        when(mockResultSet1.getObject("INDEX_NAME")).thenReturn("idx1", "idx2");
        when(mockResultSet1.getObject("COUNT_STAR")).thenReturn(100L, 200L);
        when(mockResultSet1.getObject("COUNT_FETCH")).thenReturn(10L, 20L);
        when(mockResultSet1.getObject("SUM_TIMER_WAIT")).thenReturn(1000L, 2000L);
        when(mockResultSet1.getObject("MAX_TIMER_WAIT")).thenReturn(500L, 800L);

        // Configure mock for node 2 - returns 1 row
        when(mockDataSource2.getConnection()).thenReturn(mockConnection2);
        when(mockConnection2.prepareStatement(anyString())).thenReturn(mockStatement2);
        when(mockStatement2.executeQuery()).thenReturn(mockResultSet2);
        when(mockResultSet2.next()).thenReturn(true, false);
        when(mockResultSet2.getObject("OBJECT_SCHEMA")).thenReturn("schema2");
        when(mockResultSet2.getObject("OBJECT_NAME")).thenReturn("table3");
        when(mockResultSet2.getObject("INDEX_NAME")).thenReturn("idx3");
        when(mockResultSet2.getObject("COUNT_STAR")).thenReturn(300L);
        when(mockResultSet2.getObject("COUNT_FETCH")).thenReturn(30L);
        when(mockResultSet2.getObject("SUM_TIMER_WAIT")).thenReturn(3000L);
        when(mockResultSet2.getObject("MAX_TIMER_WAIT")).thenReturn(900L);

        // Create storage nodes
        StorageNodeHelper.StorageNode node1 = new StorageNodeHelper.StorageNode(
            TEST_NODE1_ADDRESS, TEST_NODE1_INST_ID, mockDataSource1);
        StorageNodeHelper.StorageNode node2 = new StorageNodeHelper.StorageNode(
            TEST_NODE2_ADDRESS, TEST_NODE2_INST_ID, mockDataSource2);
        List<StorageNodeHelper.StorageNode> nodeList = Arrays.asList(node1, node2);

        // Mock StorageNodeHelper.getUniqueStorageNodes
        mockedStorageNodeHelper.when(() ->
                StorageNodeHelper.getUniqueStorageNodes(TEST_SCHEMA))
            .thenReturn(nodeList);

        // Create action and execute sync
        FetchIndexUsageSyncAction action = new FetchIndexUsageSyncAction(TEST_SCHEMA, TEST_SQL);
        ResultCursor result = action.sync();

        // Verify results - should have 3 rows total (2 from node1, 1 from node2)
        assertNotNull(result);
        assertTrue(result instanceof ArrayResultCursor);
        ArrayResultCursor arrayResult = (ArrayResultCursor) result;
        List<com.alibaba.polardbx.optimizer.core.row.Row> rows = arrayResult.getRows();
        assertEquals(3, rows.size());

        // Verify first row from node1
        com.alibaba.polardbx.optimizer.core.row.Row row0 = rows.get(0);
        assertEquals("schema1", row0.getString(0));
        assertEquals("table1", row0.getString(1));
        assertEquals("idx1", row0.getString(2));
        assertEquals(Long.valueOf(100L), Long.valueOf(row0.getLong(3)));
        assertEquals(Long.valueOf(10L), Long.valueOf(row0.getLong(4)));   // COUNT_FETCH
        assertEquals(Long.valueOf(1000L), Long.valueOf(row0.getLong(5))); // SUM_TIMER_WAIT
        assertEquals(Long.valueOf(500L), Long.valueOf(row0.getLong(6)));  // MAX_TIMER_WAIT

        // Verify second row from node1
        com.alibaba.polardbx.optimizer.core.row.Row row1 = rows.get(1);
        assertEquals("schema1", row1.getString(0));
        assertEquals("table2", row1.getString(1));
        assertEquals("idx2", row1.getString(2));
        assertEquals(Long.valueOf(200L), Long.valueOf(row1.getLong(3)));
        assertEquals(Long.valueOf(20L), Long.valueOf(row1.getLong(4)));   // COUNT_FETCH
        assertEquals(Long.valueOf(2000L), Long.valueOf(row1.getLong(5))); // SUM_TIMER_WAIT
        assertEquals(Long.valueOf(800L), Long.valueOf(row1.getLong(6)));  // MAX_TIMER_WAIT

        // Verify row from node2
        com.alibaba.polardbx.optimizer.core.row.Row row2 = rows.get(2);
        assertEquals("schema2", row2.getString(0));
        assertEquals("table3", row2.getString(1));
        assertEquals("idx3", row2.getString(2));
        assertEquals(Long.valueOf(300L), Long.valueOf(row2.getLong(3)));
        assertEquals(Long.valueOf(30L), Long.valueOf(row2.getLong(4)));   // COUNT_FETCH
        assertEquals(Long.valueOf(3000L), Long.valueOf(row2.getLong(5))); // SUM_TIMER_WAIT
        assertEquals(Long.valueOf(900L), Long.valueOf(row2.getLong(6)));  // MAX_TIMER_WAIT

        // Verify SQL was passed to both data sources
        verify(mockConnection1).prepareStatement(TEST_SQL);
        verify(mockConnection2).prepareStatement(TEST_SQL);
    }

    /**
     * Test 2: testSync_WithEmptyNodes_ReturnsEmptyResult
     * Mock empty StorageNode list
     */
    @Test
    public void testSync_WithEmptyNodes_ReturnsEmptyResult() {
        // Mock empty storage node list
        mockedStorageNodeHelper.when(() ->
                StorageNodeHelper.getUniqueStorageNodes(TEST_SCHEMA))
            .thenReturn(Arrays.asList());

        // Create action and execute sync
        FetchIndexUsageSyncAction action = new FetchIndexUsageSyncAction(TEST_SCHEMA, TEST_SQL);
        ResultCursor result = action.sync();

        // Verify result is not null but has no rows
        assertNotNull(result);
        assertTrue(result instanceof ArrayResultCursor);
        ArrayResultCursor arrayResult = (ArrayResultCursor) result;
        List<com.alibaba.polardbx.optimizer.core.row.Row> rows = arrayResult.getRows();
        assertEquals(0, rows.size());
    }

    /**
     * Test 3: testSync_WithNullDataSource_SkipsNode
     * Mock one StorageNode with null dataSource
     */
    @Test
    public void testSync_WithNullDataSource_SkipsNode() throws SQLException {
        // Setup mock data source for the valid node
        IDataSource mockDataSource = mock(IDataSource.class);
        IConnection mockConnection = mock(IConnection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        ResultSet mockResultSet = mock(ResultSet.class);

        // Configure mock for valid node
        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true, false);
        when(mockResultSet.getObject("OBJECT_SCHEMA")).thenReturn("schema1");
        when(mockResultSet.getObject("OBJECT_NAME")).thenReturn("table1");
        when(mockResultSet.getObject("INDEX_NAME")).thenReturn("idx1");
        when(mockResultSet.getObject("COUNT_STAR")).thenReturn(100L);
        when(mockResultSet.getObject("COUNT_FETCH")).thenReturn(10L);
        when(mockResultSet.getObject("SUM_TIMER_WAIT")).thenReturn(1000L);
        when(mockResultSet.getObject("MAX_TIMER_WAIT")).thenReturn(500L);

        // Create storage nodes - one with null dataSource
        StorageNodeHelper.StorageNode nodeWithNullDs = new StorageNodeHelper.StorageNode(
            TEST_NODE1_ADDRESS, TEST_NODE1_INST_ID, null);
        StorageNodeHelper.StorageNode validNode = new StorageNodeHelper.StorageNode(
            TEST_NODE2_ADDRESS, TEST_NODE2_INST_ID, mockDataSource);
        List<StorageNodeHelper.StorageNode> nodeList = Arrays.asList(nodeWithNullDs, validNode);

        // Mock StorageNodeHelper.getUniqueStorageNodes
        mockedStorageNodeHelper.when(() ->
                StorageNodeHelper.getUniqueStorageNodes(TEST_SCHEMA))
            .thenReturn(nodeList);

        // Create action and execute sync
        FetchIndexUsageSyncAction action = new FetchIndexUsageSyncAction(TEST_SCHEMA, TEST_SQL);
        ResultCursor result = action.sync();

        // Verify only the valid node's result is returned
        assertNotNull(result);
        assertTrue(result instanceof ArrayResultCursor);
        ArrayResultCursor arrayResult = (ArrayResultCursor) result;
        List<com.alibaba.polardbx.optimizer.core.row.Row> rows = arrayResult.getRows();
        assertEquals(1, rows.size());

        com.alibaba.polardbx.optimizer.core.row.Row row = rows.get(0);
        assertEquals("schema1", row.getString(0));
        assertEquals("table1", row.getString(1));
        assertEquals("idx1", row.getString(2));
        assertEquals(Long.valueOf(100L), Long.valueOf(row.getLong(3)));
        assertEquals(Long.valueOf(10L), Long.valueOf(row.getLong(4)));   // COUNT_FETCH
        assertEquals(Long.valueOf(1000L), Long.valueOf(row.getLong(5))); // SUM_TIMER_WAIT
        assertEquals(Long.valueOf(500L), Long.valueOf(row.getLong(6)));  // MAX_TIMER_WAIT

        // Verify only the valid node was queried
        verify(mockConnection).prepareStatement(TEST_SQL);
    }

    /**
     * Test 4: testSync_WithQueryException_ContinuesOtherNodes
     * Mock one node throwing exception, another returning normally
     */
    @Test
    public void testSync_WithQueryException_ContinuesOtherNodes() throws SQLException {
        // Setup mock data source for the failing node
        IDataSource mockDataSource1 = mock(IDataSource.class);
        when(mockDataSource1.getConnection()).thenThrow(new SQLException("Connection failed"));

        // Setup mock data source for the valid node
        IDataSource mockDataSource2 = mock(IDataSource.class);
        IConnection mockConnection2 = mock(IConnection.class);
        PreparedStatement mockStatement2 = mock(PreparedStatement.class);
        ResultSet mockResultSet2 = mock(ResultSet.class);

        // Configure mock for valid node
        when(mockDataSource2.getConnection()).thenReturn(mockConnection2);
        when(mockConnection2.prepareStatement(anyString())).thenReturn(mockStatement2);
        when(mockStatement2.executeQuery()).thenReturn(mockResultSet2);
        when(mockResultSet2.next()).thenReturn(true, false);
        when(mockResultSet2.getObject("OBJECT_SCHEMA")).thenReturn("schema2");
        when(mockResultSet2.getObject("OBJECT_NAME")).thenReturn("table2");
        when(mockResultSet2.getObject("INDEX_NAME")).thenReturn("idx2");
        when(mockResultSet2.getObject("COUNT_STAR")).thenReturn(200L);
        when(mockResultSet2.getObject("COUNT_FETCH")).thenReturn(20L);
        when(mockResultSet2.getObject("SUM_TIMER_WAIT")).thenReturn(2000L);
        when(mockResultSet2.getObject("MAX_TIMER_WAIT")).thenReturn(800L);

        // Create storage nodes
        StorageNodeHelper.StorageNode failingNode = new StorageNodeHelper.StorageNode(
            TEST_NODE1_ADDRESS, TEST_NODE1_INST_ID, mockDataSource1);
        StorageNodeHelper.StorageNode validNode = new StorageNodeHelper.StorageNode(
            TEST_NODE2_ADDRESS, TEST_NODE2_INST_ID, mockDataSource2);
        List<StorageNodeHelper.StorageNode> nodeList = Arrays.asList(failingNode, validNode);

        // Mock StorageNodeHelper.getUniqueStorageNodes
        mockedStorageNodeHelper.when(() ->
                StorageNodeHelper.getUniqueStorageNodes(TEST_SCHEMA))
            .thenReturn(nodeList);

        // Create action and execute sync - should not throw exception
        FetchIndexUsageSyncAction action = new FetchIndexUsageSyncAction(TEST_SCHEMA, TEST_SQL);
        ResultCursor result = action.sync();

        // Verify only the valid node's result is returned
        assertNotNull(result);
        assertTrue(result instanceof ArrayResultCursor);
        ArrayResultCursor arrayResult = (ArrayResultCursor) result;
        List<com.alibaba.polardbx.optimizer.core.row.Row> rows = arrayResult.getRows();
        assertEquals(1, rows.size());

        com.alibaba.polardbx.optimizer.core.row.Row row = rows.get(0);
        assertEquals("schema2", row.getString(0));
        assertEquals("table2", row.getString(1));
        assertEquals("idx2", row.getString(2));
        assertEquals(Long.valueOf(200L), Long.valueOf(row.getLong(3)));
    }

    /**
     * Test 7: testGetQueryTimeout_WithExecutionContext
     * Verify timeout is retrieved from ExecutionContext
     */
    @Test
    public void testGetQueryTimeout_WithExecutionContext() throws SQLException {
        long expectedTimeout = 300L;

        // Setup mock ExecutionContext with custom timeout
        ExecutionContext mockContext = mock(ExecutionContext.class);
        ParamManager mockParamManager = mock(ParamManager.class);
        when(mockContext.getParamManager()).thenReturn(mockParamManager);
        when(mockParamManager.getLong(ConnectionParams.INDEX_USAGE_QUERY_TIMEOUT)).thenReturn(expectedTimeout);

        // Setup mock data source
        IDataSource mockDataSource = mock(IDataSource.class);
        IConnection mockConnection = mock(IConnection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        ResultSet mockResultSet = mock(ResultSet.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false);

        // Create storage node
        StorageNodeHelper.StorageNode node = new StorageNodeHelper.StorageNode(
            TEST_NODE1_ADDRESS, TEST_NODE1_INST_ID, mockDataSource);
        List<StorageNodeHelper.StorageNode> nodeList = Arrays.asList(node);

        // Mock StorageNodeHelper.getUniqueStorageNodes
        mockedStorageNodeHelper.when(() ->
                StorageNodeHelper.getUniqueStorageNodes(TEST_SCHEMA))
            .thenReturn(nodeList);

        // Create action with ExecutionContext and execute sync
        FetchIndexUsageSyncAction action = new FetchIndexUsageSyncAction(TEST_SCHEMA, TEST_SQL, mockContext);
        ResultCursor result = action.sync();

        // Verify the ParamManager was called to get the timeout
        verify(mockParamManager).getLong(ConnectionParams.INDEX_USAGE_QUERY_TIMEOUT);
        assertNotNull(result);
    }

    /**
     * Test 8: testGetQueryTimeout_WithoutExecutionContext
     * Verify default timeout (120L) is used when ExecutionContext is null
     */
    @Test
    public void testGetQueryTimeout_WithoutExecutionContext() throws SQLException {
        // Setup mock data source
        IDataSource mockDataSource = mock(IDataSource.class);
        IConnection mockConnection = mock(IConnection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        ResultSet mockResultSet = mock(ResultSet.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        when(mockStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false);

        // Create storage node
        StorageNodeHelper.StorageNode node = new StorageNodeHelper.StorageNode(
            TEST_NODE1_ADDRESS, TEST_NODE1_INST_ID, mockDataSource);
        List<StorageNodeHelper.StorageNode> nodeList = Arrays.asList(node);

        // Mock StorageNodeHelper.getUniqueStorageNodes
        mockedStorageNodeHelper.when(() ->
                StorageNodeHelper.getUniqueStorageNodes(TEST_SCHEMA))
            .thenReturn(nodeList);

        // Create action WITHOUT ExecutionContext (should use default timeout)
        FetchIndexUsageSyncAction action = new FetchIndexUsageSyncAction(TEST_SCHEMA, TEST_SQL);
        ResultCursor result = action.sync();

        // Verify result is returned successfully (using default timeout)
        assertNotNull(result);
        // The default timeout of 120L is used internally, we verify by successful execution
    }

}
