package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for StorageNodeHelper
 */
public class StorageNodeHelperTest {

    private MockedStatic<DbInfoManager> mockedDbInfoManager;
    private MockedStatic<ExecutorContext> mockedExecutorContext;
    private MockedStatic<MetaDbDataSource> mockedMetaDbDataSource;
    private MockedStatic<SystemDbHelper> mockedSystemDbHelper;

    @Before
    public void setUp() {
        mockedDbInfoManager = mockStatic(DbInfoManager.class);
        mockedExecutorContext = mockStatic(ExecutorContext.class);
        mockedMetaDbDataSource = mockStatic(MetaDbDataSource.class);
        mockedSystemDbHelper = mockStatic(SystemDbHelper.class);
    }

    @After
    public void tearDown() {
        if (mockedDbInfoManager != null) {
            mockedDbInfoManager.close();
        }
        if (mockedExecutorContext != null) {
            mockedExecutorContext.close();
        }
        if (mockedMetaDbDataSource != null) {
            mockedMetaDbDataSource.close();
        }
        if (mockedSystemDbHelper != null) {
            mockedSystemDbHelper.close();
        }
    }

    /**
     * Test 1: testStorageNode_GettersReturnCorrectValues
     * Test StorageNode constructor and getters
     */
    @Test
    public void testStorageNode_GettersReturnCorrectValues() {
        IDataSource mockDataSource = mock(IDataSource.class);
        String address = "192.168.1.1:3306";
        String storageInstId = "inst_001";

        StorageNodeHelper.StorageNode node = new StorageNodeHelper.StorageNode(address, storageInstId, mockDataSource);

        assertEquals(address, node.getAddress());
        assertEquals(storageInstId, node.getStorageInstId());
        assertEquals(mockDataSource, node.getDataSource());
    }

    /**
     * Test 2: testGetDataSourceAddress_ValidJdbcUrl
     * Mock connection.getMetaData().getURL() returns "jdbc:mysql://192.168.1.1:3306/testdb"
     * Verify returns "192.168.1.1:3306"
     */
    @Test
    public void testGetDataSourceAddress_ValidJdbcUrl() throws SQLException {
        IDataSource mockDataSource = mock(IDataSource.class);
        IConnection mockConnection = mock(IConnection.class);
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.getURL()).thenReturn("jdbc:mysql://192.168.1.1:3306/testdb");

        String result = StorageNodeHelper.getDataSourceAddress(mockDataSource);

        assertEquals("192.168.1.1:3306", result);
        verify(mockConnection).close();
    }

    /**
     * Test 4: testGetDataSourceAddress_NullUrl
     * getURL() returns null, verify returns null
     */
    @Test
    public void testGetDataSourceAddress_NullUrl() throws SQLException {
        IDataSource mockDataSource = mock(IDataSource.class);
        IConnection mockConnection = mock(IConnection.class);
        DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.getURL()).thenReturn(null);

        String result = StorageNodeHelper.getDataSourceAddress(mockDataSource);

        assertNull(result);
        verify(mockConnection).close();
    }

    /**
     * Test 5: testGetDataSourceAddress_ConnectionException
     * getConnection() throws exception, verify returns null
     */
    @Test
    public void testGetDataSourceAddress_ConnectionException() throws SQLException {
        IDataSource mockDataSource = mock(IDataSource.class);

        when(mockDataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

        String result = StorageNodeHelper.getDataSourceAddress(mockDataSource);

        assertNull(result);
    }

    /**
     * Test 6: testGetUniqueStorageNodes_EmptyDatabaseList
     * Mock DbInfoManager returns empty list
     */
    @Test
    public void testGetUniqueStorageNodes_EmptyDatabaseList() {
        DbInfoManager mockDbInfoManager = mock(DbInfoManager.class);
        mockedDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);
        when(mockDbInfoManager.getDbList()).thenReturn(new ArrayList<>());

        List<StorageNodeHelper.StorageNode> result = StorageNodeHelper.getUniqueStorageNodes("test_schema");

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    /**
     * Test 7: testGetUniqueStorageNodes_SkipsSystemDatabases
     * Mock list containing system databases, verify they are skipped
     */
    @Test
    public void testGetUniqueStorageNodes_SkipsSystemDatabases() {
        DbInfoManager mockDbInfoManager = mock(DbInfoManager.class);
        mockedDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

        List<String> allDatabases =
            Arrays.asList("information_schema", "mysql", "performance_schema", "sys", "user_db");
        when(mockDbInfoManager.getDbList()).thenReturn(allDatabases);

        mockedSystemDbHelper.when(() -> SystemDbHelper.isDBBuildIn(anyString())).thenAnswer(invocation -> {
            String db = invocation.getArgument(0);
            return db.equals("information_schema") || db.equals("mysql") ||
                db.equals("performance_schema") || db.equals("sys");
        });

        List<StorageNodeHelper.StorageNode> result = StorageNodeHelper.getUniqueStorageNodes("test_schema");

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    /**
     * Test 8: testGetUniqueStorageNodes_DeduplicatesByHostPort
     * Mock two groups pointing to same ip:port, verify only one StorageNode returned.
     * Uses mockConstruction to intercept GroupDetailInfoAccessor and StorageInfoAccessor
     * created inside the private getStorageInfoFromGroup method.
     */
    @Test
    public void testGetUniqueStorageNodes_DeduplicatesByHostPort() throws SQLException {
        DbInfoManager mockDbInfoManager = mock(DbInfoManager.class);
        mockedDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

        List<String> userDatabases = Arrays.asList("db1");
        when(mockDbInfoManager.getDbList()).thenReturn(userDatabases);
        mockedSystemDbHelper.when(() -> SystemDbHelper.isDBBuildIn(anyString())).thenReturn(false);

        ExecutorContext mockExecCtx = mock(ExecutorContext.class);
        mockedExecutorContext.when(() -> ExecutorContext.getContext("db1")).thenReturn(mockExecCtx);

        TopologyHandler mockTopologyHandler = mock(TopologyHandler.class);
        when(mockExecCtx.getTopologyHandler()).thenReturn(mockTopologyHandler);

        List<String> groupNames = Arrays.asList("group1", "group2");
        when(mockTopologyHandler.getGroupNames()).thenReturn(groupNames);

        IGroupExecutor mockGroupExecutor1 = mock(IGroupExecutor.class);
        IGroupExecutor mockGroupExecutor2 = mock(IGroupExecutor.class);
        IDataSource mockDataSource1 = mock(IDataSource.class);
        IDataSource mockDataSource2 = mock(IDataSource.class);

        when(mockTopologyHandler.get("group1")).thenReturn(mockGroupExecutor1);
        when(mockTopologyHandler.get("group2")).thenReturn(mockGroupExecutor2);
        when(mockGroupExecutor1.getDataSource()).thenReturn(mockDataSource1);
        when(mockGroupExecutor2.getDataSource()).thenReturn(mockDataSource2);

        // Mock MetaDbDataSource.getInstance() to return a mock that provides a connection
        MetaDbDataSource mockMetaDb = mock(MetaDbDataSource.class);
        Connection mockMetaConn = mock(Connection.class);
        mockedMetaDbDataSource.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDb);
        when(mockMetaDb.getConnection()).thenReturn(mockMetaConn);

        // Prepare GroupDetailInfoRecords
        GroupDetailInfoRecord groupRecord1 = new GroupDetailInfoRecord();
        groupRecord1.storageInstId = "inst_001";
        GroupDetailInfoRecord groupRecord2 = new GroupDetailInfoRecord();
        groupRecord2.storageInstId = "inst_002";

        // Prepare StorageInfoRecords — both point to same ip:port
        StorageInfoRecord storageInfo1 = new StorageInfoRecord();
        storageInfo1.storageInstId = "inst_001";
        storageInfo1.ip = "192.168.1.1";
        storageInfo1.port = 3306;
        StorageInfoRecord storageInfo2 = new StorageInfoRecord();
        storageInfo2.storageInstId = "inst_002";
        storageInfo2.ip = "192.168.1.1";
        storageInfo2.port = 3306;

        // Use mockConstruction to intercept new GroupDetailInfoAccessor() and new StorageInfoAccessor()
        try (MockedConstruction<GroupDetailInfoAccessor> mockedGroupAccessor =
            mockConstruction(GroupDetailInfoAccessor.class, (mock, ctx) -> {
                when(mock.getGroupDetailInfoByDbNameAndGroup("db1", "group1"))
                    .thenReturn(Arrays.asList(groupRecord1));
                when(mock.getGroupDetailInfoByDbNameAndGroup("db1", "group2"))
                    .thenReturn(Arrays.asList(groupRecord2));
            });
            MockedConstruction<StorageInfoAccessor> mockedStorageAccessor =
                mockConstruction(StorageInfoAccessor.class, (mock, ctx) -> {
                    when(mock.getStorageInfosByStorageInstId("inst_001"))
                        .thenReturn(Arrays.asList(storageInfo1));
                    when(mock.getStorageInfosByStorageInstId("inst_002"))
                        .thenReturn(Arrays.asList(storageInfo2));
                })) {

            List<StorageNodeHelper.StorageNode> result = StorageNodeHelper.getUniqueStorageNodes("test_schema");

            assertNotNull(result);
            // Both groups point to 192.168.1.1:3306, so deduplication should yield 1 node
            assertEquals(1, result.size());
            assertEquals("192.168.1.1:3306", result.get(0).getAddress());
        }
    }

    /**
     * Test 9: testGetUniqueStorageNodes_NullExecutorContext
     * Mock ExecutorContext.getContext() returns null, verify skip
     */
    @Test
    public void testGetUniqueStorageNodes_NullExecutorContext() {
        DbInfoManager mockDbInfoManager = mock(DbInfoManager.class);
        mockedDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

        List<String> userDatabases = Arrays.asList("db1");
        when(mockDbInfoManager.getDbList()).thenReturn(userDatabases);
        mockedSystemDbHelper.when(() -> SystemDbHelper.isDBBuildIn(anyString())).thenReturn(false);

        mockedExecutorContext.when(() -> ExecutorContext.getContext("db1")).thenReturn(null);

        List<StorageNodeHelper.StorageNode> result = StorageNodeHelper.getUniqueStorageNodes("test_schema");

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

}
