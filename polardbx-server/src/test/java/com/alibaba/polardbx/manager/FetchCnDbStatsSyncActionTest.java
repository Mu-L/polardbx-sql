package com.alibaba.polardbx.manager;

import com.alibaba.polardbx.CobarConfig;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.server.response.FetchCnDbStatsSyncAction;
import com.alibaba.polardbx.stats.MatrixStatistics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class FetchCnDbStatsSyncActionTest {

    private MockedStatic<CobarServer> cobarServerMockedStatic;
    private MockedStatic<TddlNode> tddlNodeMockedStatic;
    private CobarServer cobarServer;
    private CobarConfig cobarConfig;

    @Before
    public void setup() {
        // Mock CobarServer and its dependencie
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        cobarServerMockedStatic = Mockito.mockStatic(CobarServer.class);
        cobarServer = Mockito.mock(CobarServer.class);
        cobarServerMockedStatic.when(CobarServer::getInstance).thenReturn(cobarServer);

        cobarConfig = Mockito.mock(CobarConfig.class);
        Mockito.when(cobarServer.getConfig()).thenReturn(cobarConfig);

        // Mock ServerThreadPool
        ServerThreadPool serverThreadPool = Mockito.mock(ServerThreadPool.class);
        Mockito.when(cobarServer.getServerExecutor()).thenReturn(serverThreadPool);

        // Mock TddlNode
        tddlNodeMockedStatic = Mockito.mockStatic(TddlNode.class);
        tddlNodeMockedStatic.when(TddlNode::getHost).thenReturn("localhost");
        tddlNodeMockedStatic.when(TddlNode::getPort).thenReturn(3306);
    }

    @Test
    public void testSyncWithSingleSchema() {
        // Setup test schema
        Map<String, SchemaConfig> schemas = new HashMap<>();
        SchemaConfig schemaConfig = Mockito.mock(SchemaConfig.class);
        TDataSource dataSource = Mockito.mock(TDataSource.class);
        MatrixStatistics stats = new MatrixStatistics();

        // Initialize stats with test values
        stats.netIn = 1000L;
        stats.netOut = 2000L;
        stats.activeConnection = new AtomicLong(5L);
        stats.connectionCount = new AtomicLong(10L);
        stats.timeCost = 500L;
        stats.request = 100L;
        // Initialize other stats fields as needed for testing

        Mockito.when(schemaConfig.getDataSource()).thenReturn(dataSource);
        Mockito.when(schemaConfig.getName()).thenReturn("test_db");
        Mockito.when(dataSource.isInited()).thenReturn(true);
        Mockito.when(dataSource.getSchemaName()).thenReturn("test_db");
        Mockito.when(dataSource.getStatistics()).thenReturn(stats);

        // Mock ServerThreadPool behavior
        ServerThreadPool exec = cobarServer.getServerExecutor();
        Mockito.when(exec.getTaskCountBySchemaName("test_db")).thenReturn(3L);

        schemas.put("test_db", schemaConfig);
        Mockito.when(cobarConfig.getSchemas()).thenReturn(schemas);

        // Execute the action
        FetchCnDbStatsSyncAction action = new FetchCnDbStatsSyncAction();
        ResultCursor cursor = action.sync();

        // Verify results
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals("localhost:3306", row.getString(0));
        Assert.assertEquals("test_db", row.getString(1));
        Assert.assertEquals(1000L, (long) row.getLong(2));
        Assert.assertEquals(2000L, (long) row.getLong(3));
        Assert.assertEquals(5L, (long) row.getLong(4));
        Assert.assertEquals(10L, (long) row.getLong(5));
        // Verify other columns as needed

        // Verify no more rows
        Assert.assertNull(cursor.next());
    }

    @Test
    public void testSyncWithUninitializedDataSource() {
        // Setup test schema with uninitialized data source
        Map<String, SchemaConfig> schemas = new HashMap<>();
        SchemaConfig schemaConfig = Mockito.mock(SchemaConfig.class);
        TDataSource dataSource = Mockito.mock(TDataSource.class);

        Mockito.when(schemaConfig.getDataSource()).thenReturn(dataSource);
        Mockito.when(schemaConfig.getName()).thenReturn("uninit_db");
        Mockito.when(dataSource.isInited()).thenReturn(false);

        schemas.put("uninit_db", schemaConfig);
        Mockito.when(cobarConfig.getSchemas()).thenReturn(schemas);

        // Execute the action
        FetchCnDbStatsSyncAction action = new FetchCnDbStatsSyncAction();
        ResultCursor cursor = action.sync();

        // Verify no rows returned
        Assert.assertNull(cursor.next());
    }

    @Test
    public void testSyncWithCdcDb() {
        // Setup test schema with CDC database
        Map<String, SchemaConfig> schemas = new HashMap<>();
        SchemaConfig schemaConfig = Mockito.mock(SchemaConfig.class);
        TDataSource dataSource = Mockito.mock(TDataSource.class);

        Mockito.when(schemaConfig.getDataSource()).thenReturn(dataSource);
        Mockito.when(schemaConfig.getName()).thenReturn(SystemDbHelper.CDC_DB_NAME);
        Mockito.when(dataSource.isInited()).thenReturn(true);

        schemas.put(SystemDbHelper.CDC_DB_NAME, schemaConfig);
        Mockito.when(cobarConfig.getSchemas()).thenReturn(schemas);

        // Execute the action
        FetchCnDbStatsSyncAction action = new FetchCnDbStatsSyncAction();
        ResultCursor cursor = action.sync();

        // Verify no rows returned
        Assert.assertNull(cursor.next());
    }

    @After
    public void tearDown() {
        if (cobarServerMockedStatic != null) {
            cobarServerMockedStatic.close();
        }
        if (tddlNodeMockedStatic != null) {
            tddlNodeMockedStatic.close();
        }
    }
}