package com.alibaba.polardbx.manager;

import com.alibaba.polardbx.CobarConfig;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.listener.ConfigManager;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.spill.SpillSpaceManager;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.pool.XClientPool;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.response.FetchCnStatusSyncAction;
import com.alibaba.polardbx.server.util.LogUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class FetchCnStatusSyncActionTest {

    private MockedStatic<CobarServer> cobarServerMockedStatic;
    private MockedStatic<TddlNode> tddlNodeMockedStatic;
    private MockedStatic<ManagementFactory> managementFactoryMockedStatic;
    private MockedStatic<ThreadCpuStatUtil> threadCpuStatUtilMockedStatic;
    private MockedStatic<SpillSpaceManager> spillSpaceManagerMockedStatic;
    private MockedStatic<LogUtils> logUtilsMockedStatic;

    private CobarServer cobarServer;
    private CobarConfig cobarConfig;
    private MemoryMXBean memoryMXBean;
    private MemoryUsage heapMemoryUsage;
    private MemoryUsage nonHeapMemoryUsage;

    @Before
    public void setup() {
        // Mock CobarServer
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        cobarServerMockedStatic = Mockito.mockStatic(CobarServer.class);
        cobarServer = Mockito.mock(CobarServer.class);
        cobarServerMockedStatic.when(CobarServer::getInstance).thenReturn(cobarServer);

        // Mock CobarConfig
        cobarConfig = Mockito.mock(CobarConfig.class);
        Mockito.when(cobarServer.getConfig()).thenReturn(cobarConfig);

        // Mock TddlNode
        tddlNodeMockedStatic = Mockito.mockStatic(TddlNode.class);
        tddlNodeMockedStatic.when(TddlNode::getHost).thenReturn("localhost");
        tddlNodeMockedStatic.when(TddlNode::getPort).thenReturn(3306);

        // Mock ManagementFactory and MemoryMXBean
        managementFactoryMockedStatic = Mockito.mockStatic(ManagementFactory.class);
        memoryMXBean = Mockito.mock(MemoryMXBean.class);
        managementFactoryMockedStatic.when(ManagementFactory::getMemoryMXBean).thenReturn(memoryMXBean);

        heapMemoryUsage = new MemoryUsage(0, 1000, 2000, 3000);
        nonHeapMemoryUsage = new MemoryUsage(0, 500, 1000, 1500);
        Mockito.when(memoryMXBean.getHeapMemoryUsage()).thenReturn(heapMemoryUsage);
        Mockito.when(memoryMXBean.getNonHeapMemoryUsage()).thenReturn(nonHeapMemoryUsage);

        // Mock ThreadCpuStatUtil
        threadCpuStatUtilMockedStatic = Mockito.mockStatic(ThreadCpuStatUtil.class);

        // Mock SpillSpaceManager
        spillSpaceManagerMockedStatic = Mockito.mockStatic(SpillSpaceManager.class);
        SpillSpaceManager spillSpaceManager = Mockito.mock(SpillSpaceManager.class);
        spillSpaceManagerMockedStatic.when(SpillSpaceManager::getInstance).thenReturn(spillSpaceManager);
        Mockito.when(spillSpaceManager.getTotalSpillSpace()).thenReturn(1024L);

        // Mock LogUtils
        logUtilsMockedStatic = Mockito.mockStatic(LogUtils.class);
        Mockito.when(LogUtils.getTotalLogSpace()).thenReturn(2048L);

        // Mock NIOProcessors and connections
        List<NIOProcessor> processors = new ArrayList<>();
        Mockito.when(cobarServer.getProcessors()).thenReturn(processors.toArray(new NIOProcessor[0]));
    }

    @Test
    public void testSyncWithNoConnections() {
        // Setup empty schemas
        Mockito.when(cobarConfig.getSchemas()).thenReturn(new HashMap<>());

        // Execute the action
        FetchCnStatusSyncAction action = new FetchCnStatusSyncAction();
        ResultCursor cursor = action.sync();

        // Verify results
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals("localhost:3306", row.getString(0));
        Assert.assertEquals(1000L, (long) row.getLong(2)); // Heap used
        Assert.assertEquals(3000L, (long) row.getLong(3)); // Heap max (free = max - used)
        Assert.assertEquals(500L, (long) row.getLong(4)); // Non-heap used
        Assert.assertEquals(1024L, (long) row.getLong(5)); // Spill usage
        Assert.assertEquals(2048L, (long) row.getLong(6)); // Log usage
        Assert.assertEquals(0L, (long) row.getLong(7)); // Active connections
        Assert.assertEquals(0L, (long) row.getLong(8)); // Client num
        Assert.assertEquals(0L, (long) row.getLong(9)); // Idle session
        Assert.assertEquals(0L, (long) row.getLong(10)); // Working session

        // Verify no more rows
        Assert.assertNull(cursor.next());
    }

    @Test
    public void testSyncWithActiveConnections() {
        // Setup empty schemas
        Mockito.when(cobarConfig.getSchemas()).thenReturn(new HashMap<>());

        // Setup active connections
        NIOProcessor processor = Mockito.mock(NIOProcessor.class);
        ConcurrentMap<Long, FrontendConnection> frontends = new ConcurrentHashMap<>();

        // Add 2 active connections
        ServerConnection activeConn1 = Mockito.mock(ServerConnection.class);
        Mockito.when(activeConn1.isStatementExecuting()).thenReturn(new AtomicBoolean(true));
        frontends.put(1L, activeConn1);

        ServerConnection activeConn2 = Mockito.mock(ServerConnection.class);
        Mockito.when(activeConn2.isStatementExecuting()).thenReturn(new AtomicBoolean(true));
        frontends.put(2L, activeConn2);

        // Add 1 inactive connection
        ServerConnection inactiveConn = Mockito.mock(ServerConnection.class);
        Mockito.when(inactiveConn.isStatementExecuting()).thenReturn(new AtomicBoolean(false));
        frontends.put(3L, inactiveConn);

        Mockito.when(processor.getFrontends()).thenReturn((ConcurrentMap<Long, FrontendConnection>) frontends);
        Mockito.when(cobarServer.getProcessors()).thenReturn(Collections.singletonList(processor).toArray(new NIOProcessor[0]));

        // Execute the action
        FetchCnStatusSyncAction action = new FetchCnStatusSyncAction();
        ResultCursor cursor = action.sync();

        // Verify active connections count
        Row row = cursor.next();
        Assert.assertEquals(2L, (long) row.getLong(7)); // Should count only active connections
    }

    @After
    public void tearDown() {
        if (cobarServerMockedStatic != null) {
            cobarServerMockedStatic.close();
        }
        if (tddlNodeMockedStatic != null) {
            tddlNodeMockedStatic.close();
        }
        if (managementFactoryMockedStatic != null) {
            managementFactoryMockedStatic.close();
        }
        if (threadCpuStatUtilMockedStatic != null) {
            threadCpuStatUtilMockedStatic.close();
        }
        if (spillSpaceManagerMockedStatic != null) {
            spillSpaceManagerMockedStatic.close();
        }
        if (logUtilsMockedStatic != null) {
            logUtilsMockedStatic.close();
        }
    }
}