package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.utils.StorageNodeHelper;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class ClearIndexUsageSyncActionTest {

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

    @Test
    public void testSync_WithMultipleNodes_AllSuccess() throws SQLException {
        // Arrange
        String dbName = "testDb";
        ClearIndexUsageSyncAction action = new ClearIndexUsageSyncAction(dbName);

        List<StorageNodeHelper.StorageNode> storageNodes = createMockStorageNodes(2);
        mockedStorageNodeHelper.when(() -> StorageNodeHelper.getUniqueStorageNodes(dbName))
            .thenReturn(storageNodes);

        // Act
        ResultCursor result = action.sync();

        // Assert
        assertNotNull(result);
        assertTrue(result instanceof ArrayResultCursor);
        ArrayResultCursor arrayResult = (ArrayResultCursor) result;
        List<Row> rows = arrayResult.getRows();
        assertEquals(3, rows.size()); // 1 summary + 2 node results

        // First row should be Summary
        Row summaryRow = rows.get(0);
        assertEquals("Summary", summaryRow.getString(0));
        assertTrue(summaryRow.getString(1).contains("Cleared 2/2 DN nodes (Success: 2, Failed: 0)"));

        // Next rows should be individual node results
        Row node1Row = rows.get(1);
        assertEquals("node-0", node1Row.getString(0));
        assertEquals("SUCCESS", node1Row.getString(1));

        Row node2Row = rows.get(2);
        assertEquals("node-1", node2Row.getString(0));
        assertEquals("SUCCESS", node2Row.getString(1));
    }

    @Test
    public void testSync_WithEmptyNodes_ReturnsNoNodesFound() {
        // Arrange
        String dbName = "testDb";
        ClearIndexUsageSyncAction action = new ClearIndexUsageSyncAction(dbName);

        List<StorageNodeHelper.StorageNode> storageNodes = new ArrayList<>();
        mockedStorageNodeHelper.when(() -> StorageNodeHelper.getUniqueStorageNodes(dbName))
            .thenReturn(storageNodes);

        // Act
        ResultCursor result = action.sync();

        // Assert
        assertNotNull(result);
        assertTrue(result instanceof ArrayResultCursor);
        ArrayResultCursor arrayResult = (ArrayResultCursor) result;
        List<Row> rows = arrayResult.getRows();
        assertEquals(1, rows.size());

        Row row = rows.get(0);
        assertEquals("N/A", row.getString(0));
        assertEquals("No DN nodes found", row.getString(1));
    }

    @Test
    public void testSync_WithOneNodeFailing_ContinuesOtherNodes() throws SQLException {
        // Arrange
        String dbName = "testDb";
        ClearIndexUsageSyncAction action = new ClearIndexUsageSyncAction(dbName);

        List<StorageNodeHelper.StorageNode> storageNodes = createMockStorageNodes(3);
        mockedStorageNodeHelper.when(() -> StorageNodeHelper.getUniqueStorageNodes(dbName))
            .thenReturn(storageNodes);

        // Make the second node fail
        when(storageNodes.get(1).getDataSource().getConnection())
            .thenThrow(new SQLException("Connection failed"));

        // Act
        ResultCursor result = action.sync();

        // Assert
        assertNotNull(result);
        assertTrue(result instanceof ArrayResultCursor);
        ArrayResultCursor arrayResult = (ArrayResultCursor) result;
        List<Row> rows = arrayResult.getRows();
        assertEquals(4, rows.size()); // 1 summary + 3 node results

        Row summaryRow = rows.get(0);
        assertEquals("Summary", summaryRow.getString(0));
        assertTrue(summaryRow.getString(1).contains("Cleared 2/3 DN nodes (Success: 2, Failed: 1)"));

        Row node1Row = rows.get(1);
        assertEquals("SUCCESS", node1Row.getString(1));

        Row node2Row = rows.get(2);
        assertEquals("FAILED: Connection failed", node2Row.getString(1));

        Row node3Row = rows.get(3);
        assertEquals("SUCCESS", node3Row.getString(1));
    }

    @Test
    public void testSync_LongErrorMessage_Truncated() throws SQLException {
        // Arrange
        String dbName = "testDb";
        ClearIndexUsageSyncAction action = new ClearIndexUsageSyncAction(dbName);

        List<StorageNodeHelper.StorageNode> storageNodes = createMockStorageNodes(1);
        mockedStorageNodeHelper.when(() -> StorageNodeHelper.getUniqueStorageNodes(dbName))
            .thenReturn(storageNodes);

        // Create a long error message (more than 100 characters)
        String longErrorMessage =
            "This is a very long error message that exceeds one hundred characters and should be truncated when displayed in the result";
        when(storageNodes.get(0).getDataSource().getConnection())
            .thenThrow(new SQLException(longErrorMessage));

        // Act
        ResultCursor result = action.sync();

        // Assert
        assertNotNull(result);
        assertTrue(result instanceof ArrayResultCursor);
        ArrayResultCursor arrayResult = (ArrayResultCursor) result;
        List<Row> rows = arrayResult.getRows();
        assertEquals(2, rows.size()); // 1 summary + 1 node result

        Row nodeRow = rows.get(1);
        String status = nodeRow.getString(1);
        assertTrue(status.startsWith("FAILED: "));
        assertTrue(status.length() <= 100 + "FAILED: ".length() + 3); // 100 chars + prefix + "..."
        assertTrue(status.endsWith("..."));
    }

    @Test
    public void testSync_NullErrorMessage_HandledGracefully() throws SQLException {
        // Arrange
        String dbName = "testDb";
        ClearIndexUsageSyncAction action = new ClearIndexUsageSyncAction(dbName);

        List<StorageNodeHelper.StorageNode> storageNodes = createMockStorageNodes(1);
        mockedStorageNodeHelper.when(() -> StorageNodeHelper.getUniqueStorageNodes(dbName))
            .thenReturn(storageNodes);

        // Create a SQLException with null message
        SQLException nullMessageException = new SQLException((String) null);
        when(storageNodes.get(0).getDataSource().getConnection())
            .thenThrow(nullMessageException);

        // Act
        ResultCursor result = action.sync();

        // Assert
        assertNotNull(result);
        assertTrue(result instanceof ArrayResultCursor);
        ArrayResultCursor arrayResult = (ArrayResultCursor) result;
        List<Row> rows = arrayResult.getRows();
        assertEquals(2, rows.size()); // 1 summary + 1 node result

        Row nodeRow = rows.get(1);
        assertEquals("FAILED: null", nodeRow.getString(1));
    }

    @Test
    public void testSync_StorageNodeHelperThrows_PropagatesException() {
        // Arrange
        String dbName = "testDb";
        ClearIndexUsageSyncAction action = new ClearIndexUsageSyncAction(dbName);

        mockedStorageNodeHelper.when(() -> StorageNodeHelper.getUniqueStorageNodes(dbName))
            .thenThrow(new RuntimeException("StorageNodeHelper error"));

        // Act & Assert
        try {
            action.sync();
            fail("Expected TddlRuntimeException to be thrown");
        } catch (TddlRuntimeException e) {
            assertNotNull(e);
            assertTrue(e.getMessage().contains("Failed to clear index usage statistics"));
        }
    }

    @Test
    public void testSync_ResultCursorHasCorrectColumns() {
        // Arrange
        String dbName = "testDb";
        ClearIndexUsageSyncAction action = new ClearIndexUsageSyncAction(dbName);

        List<StorageNodeHelper.StorageNode> storageNodes = new ArrayList<>();
        mockedStorageNodeHelper.when(() -> StorageNodeHelper.getUniqueStorageNodes(dbName))
            .thenReturn(storageNodes);

        // Act
        ResultCursor result = action.sync();

        // Assert
        assertNotNull(result);
        assertEquals(2, result.getReturnColumns().size());
        assertEquals("DN_NODE", result.getReturnColumns().get(0).getName());
        assertEquals("STATUS", result.getReturnColumns().get(1).getName());
    }

    /**
     * Helper method to create mock storage nodes
     */
    private List<StorageNodeHelper.StorageNode> createMockStorageNodes(int count) throws SQLException {
        List<StorageNodeHelper.StorageNode> nodes = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            StorageNodeHelper.StorageNode mockNode = mock(StorageNodeHelper.StorageNode.class);
            IDataSource mockDataSource = mock(IDataSource.class);
            IConnection mockConnection = mock(IConnection.class);
            Statement mockStatement = mock(Statement.class);

            when(mockNode.getAddress()).thenReturn("node-" + i);
            when(mockNode.getDataSource()).thenReturn(mockDataSource);
            when(mockDataSource.getConnection()).thenReturn(mockConnection);
            when(mockConnection.createStatement()).thenReturn(mockStatement);

            nodes.add(mockNode);
        }

        return nodes;
    }
}