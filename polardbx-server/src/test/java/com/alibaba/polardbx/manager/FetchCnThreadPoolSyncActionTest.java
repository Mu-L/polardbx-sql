package com.alibaba.polardbx.manager;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;

import com.alibaba.polardbx.executor.mpp.deploy.Server;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.PriorityExecutorInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.server.response.FetchCnThreadPoolSyncAction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import com.alibaba.polardbx.executor.cursor.Cursor;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class for FetchCnThreadPoolSyncAction.
 */
public class FetchCnThreadPoolSyncActionTest {

    // MockedStatic fields for static method mocking
    private MockedStatic<CobarServer> cobarServerMockedStatic;
    private MockedStatic<TddlNode> tddlNodeMockedStatic;
    private MockedStatic<ServiceProvider> serviceProviderMockedStatic;

    // Mocked instances
    private CobarServer cobarServer;
    private ServiceProvider serviceProvider;
    private Server server;
    private TaskExecutor taskExecutor;
    private PriorityExecutorInfo lowPriorityInfo;
    private PriorityExecutorInfo highPriorityInfo;

    // Mocked thread pools
    private ServerThreadPool syncExecutor;
    private ServerThreadPool managerExecutor;
    private ServerThreadPool serverExecutor;
    private ServerThreadPool killExecutor;

    @Before
    public void setUp() {
        // Mock CobarServer.getInstance()
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        cobarServerMockedStatic = Mockito.mockStatic(CobarServer.class);
        cobarServer = Mockito.mock(CobarServer.class);
        cobarServerMockedStatic.when(CobarServer::getInstance).thenReturn(cobarServer);

        // Mock normal executors
        syncExecutor = Mockito.mock(ServerThreadPool.class);
        managerExecutor = Mockito.mock(ServerThreadPool.class);
        serverExecutor = Mockito.mock(ServerThreadPool.class);
        killExecutor = Mockito.mock(ServerThreadPool.class);

        Mockito.when(cobarServer.getSyncExecutor()).thenReturn(syncExecutor);
        Mockito.when(cobarServer.getManagerExecutor()).thenReturn(managerExecutor);
        Mockito.when(cobarServer.getServerExecutor()).thenReturn(serverExecutor);
        Mockito.when(cobarServer.getKillExecutor()).thenReturn(killExecutor);

        // Mock TddlNode static methods
        tddlNodeMockedStatic = Mockito.mockStatic(TddlNode.class);
        tddlNodeMockedStatic.when(TddlNode::getHost).thenReturn("localhost");
        tddlNodeMockedStatic.when(TddlNode::getPort).thenReturn(3306);

        // Mock ServiceProvider.getInstance()
        serviceProviderMockedStatic = Mockito.mockStatic(ServiceProvider.class);
        serviceProvider = Mockito.mock(ServiceProvider.class);
        serviceProviderMockedStatic.when(ServiceProvider::getInstance).thenReturn(serviceProvider);

        // Mock server
        server = Mockito.mock(Server.class);
        Mockito.when(serviceProvider.getServer()).thenReturn(server);

        // Mock TaskExecutor
        taskExecutor = Mockito.mock(TaskExecutor.class);
        Mockito.when(server.getTaskExecutor()).thenReturn(taskExecutor);

        // Mock PriorityExecutorInfo
        lowPriorityInfo = Mockito.mock(PriorityExecutorInfo.class);
        highPriorityInfo = Mockito.mock(PriorityExecutorInfo.class);

        Mockito.when(taskExecutor.getLowPriorityInfo()).thenReturn(lowPriorityInfo);
        Mockito.when(taskExecutor.getHighPriorityInfo()).thenReturn(highPriorityInfo);
    }

    /**
     * Test the sync action with both normal and priority executors active.
     */
    @Test
    public void testSyncWithNormalAndPriorityExecutors() {
        // Setup normal executors with active and queued tasks
        Mockito.when(syncExecutor.getPoolName()).thenReturn("syncExecutor");
        Mockito.when(syncExecutor.getPoolSize()).thenReturn(20);
        Mockito.when(syncExecutor.getActiveCount()).thenReturn(5);
        Mockito.when(syncExecutor.getQueuedCount()).thenReturn(10);
        Mockito.when(syncExecutor.getCompletedTaskCount()).thenReturn(1000L);
        Mockito.when(syncExecutor.getTaskCount()).thenReturn(1050L);

        Mockito.when(managerExecutor.getPoolName()).thenReturn("managerExecutor");
        Mockito.when(managerExecutor.getPoolSize()).thenReturn(15);
        Mockito.when(managerExecutor.getActiveCount()).thenReturn(3);
        Mockito.when(managerExecutor.getQueuedCount()).thenReturn(7);
        Mockito.when(managerExecutor.getCompletedTaskCount()).thenReturn(700L);
        Mockito.when(managerExecutor.getTaskCount()).thenReturn(720L);

        Mockito.when(serverExecutor.getPoolName()).thenReturn("serverExecutor");
        Mockito.when(serverExecutor.getPoolSize()).thenReturn(25);
        Mockito.when(serverExecutor.getActiveCount()).thenReturn(10);
        Mockito.when(serverExecutor.getQueuedCount()).thenReturn(15);
        Mockito.when(serverExecutor.getCompletedTaskCount()).thenReturn(1500L);
        Mockito.when(serverExecutor.getTaskCount()).thenReturn(1510L);

        Mockito.when(killExecutor.getPoolName()).thenReturn("killExecutor");
        Mockito.when(killExecutor.getPoolSize()).thenReturn(5);
        Mockito.when(killExecutor.getActiveCount()).thenReturn(1);
        Mockito.when(killExecutor.getQueuedCount()).thenReturn(2);
        Mockito.when(killExecutor.getCompletedTaskCount()).thenReturn(100L);
        Mockito.when(killExecutor.getTaskCount()).thenReturn(103L);

        // Setup priority executors
        Mockito.when(lowPriorityInfo.getName()).thenReturn("lowPriorityExecutor");
        Mockito.when(lowPriorityInfo.getPoolSize()).thenReturn(8);
        Mockito.when(lowPriorityInfo.getPendingSplitsSize()).thenReturn(2);
        Mockito.when(lowPriorityInfo.getActiveCount()).thenReturn(3);
        Mockito.when(lowPriorityInfo.getBlockedSplitSize()).thenReturn(1);
        Mockito.when(lowPriorityInfo.getCompletedTaskCount()).thenReturn(500L);
        Mockito.when(lowPriorityInfo.getTotalTask()).thenReturn(550L);

        Mockito.when(highPriorityInfo.getName()).thenReturn("highPriorityExecutor");
        Mockito.when(highPriorityInfo.getPoolSize()).thenReturn(10);
        Mockito.when(highPriorityInfo.getPendingSplitsSize()).thenReturn(1);
        Mockito.when(highPriorityInfo.getActiveCount()).thenReturn(4);
        Mockito.when(highPriorityInfo.getBlockedSplitSize()).thenReturn(0);
        Mockito.when(highPriorityInfo.getCompletedTaskCount()).thenReturn(600L);
        Mockito.when(highPriorityInfo.getTotalTask()).thenReturn(650L);

        // Execute the action
        FetchCnThreadPoolSyncAction action = new FetchCnThreadPoolSyncAction();
        ResultCursor cursor = action.sync();

        // Collect all rows
        List<Row> rows = new ArrayList<>();
        Row row;
        while ((row = cursor.next()) != null) {
            rows.add(row);
        }

        // Assert that there are 6 rows (4 normal + 2 priority)
        Assert.assertEquals(6, rows.size());

        // Validate normal executors
        for (int i = 0; i < 4; i++) {
            Row r = rows.get(i);
            Assert.assertEquals("localhost:3306", r.getString(0));
            String poolName = r.getString(1);
            switch (poolName) {
            case "syncExecutor":
                Assert.assertEquals(20L, (long) r.getLong(2));
                Assert.assertEquals(5L, (long) r.getLong(3));
                Assert.assertEquals(10L, (long) r.getLong(4));
                Assert.assertEquals(1000L, (long) r.getLong(5));
                Assert.assertEquals(1050L, (long) r.getLong(6));
                Assert.assertEquals("NORMAL", r.getString(7));
                break;
            case "managerExecutor":
                Assert.assertEquals(15L, (long) r.getLong(2));
                Assert.assertEquals(3L, (long) r.getLong(3));
                Assert.assertEquals(7L, (long) r.getLong(4));
                Assert.assertEquals(700L, (long) r.getLong(5));
                Assert.assertEquals(720L, (long) r.getLong(6));
                Assert.assertEquals("NORMAL", r.getString(7));
                break;
            case "serverExecutor":
                Assert.assertEquals(25L, (long) r.getLong(2));
                Assert.assertEquals(10L, (long) r.getLong(3));
                Assert.assertEquals(15L, (long) r.getLong(4));
                Assert.assertEquals(1500L, (long) r.getLong(5));
                Assert.assertEquals(1510L, (long) r.getLong(6));
                Assert.assertEquals("NORMAL", r.getString(7));
                break;
            case "killExecutor":
                Assert.assertEquals(5L, (long) r.getLong(2));
                Assert.assertEquals(1L, (long) r.getLong(3));
                Assert.assertEquals(2L, (long) r.getLong(4));
                Assert.assertEquals(100L, (long) r.getLong(5));
                Assert.assertEquals(103L, (long) r.getLong(6));
                Assert.assertEquals("NORMAL", r.getString(7));
                break;
            default:
                Assert.fail("Unexpected pool name: " + poolName);
            }
        }

        // Validate priority executors
        Row lowRow = rows.get(4);
        Assert.assertEquals("localhost:3306", lowRow.getString(0));
        Assert.assertEquals("lowPriorityExecutor", lowRow.getString(1));
        Assert.assertEquals(8L, (long) lowRow.getLong(2));
        Assert.assertEquals(5L, (long) lowRow.getLong(3)); // 2 + 3
        Assert.assertEquals(1L, (long) lowRow.getLong(4));
        Assert.assertEquals(500L, (long) lowRow.getLong(5));
        Assert.assertEquals(550L, (long) lowRow.getLong(6));
        Assert.assertEquals("LOW", lowRow.getString(7));

        Row highRow = rows.get(5);
        Assert.assertEquals("localhost:3306", highRow.getString(0));
        Assert.assertEquals("highPriorityExecutor", highRow.getString(1));
        Assert.assertEquals(10L, (long) highRow.getLong(2));
        Assert.assertEquals(5L, (long) highRow.getLong(3)); // 1 + 4
        Assert.assertEquals(0L, (long) highRow.getLong(4));
        Assert.assertEquals(600L, (long) highRow.getLong(5));
        Assert.assertEquals(650L, (long) highRow.getLong(6));
        Assert.assertEquals("HIGH", highRow.getString(7));
    }

    @After
    public void tearDown() {
        // Close all mocked static instances
        if (cobarServerMockedStatic != null) {
            cobarServerMockedStatic.close();
        }
        if (tddlNodeMockedStatic != null) {
            tddlNodeMockedStatic.close();
        }
        if (serviceProviderMockedStatic != null) {
            serviceProviderMockedStatic.close();
        }
    }
}
