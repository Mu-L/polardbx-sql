package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.LoadWeightAccessor;
import com.alibaba.polardbx.gms.metadb.table.LoadWeightRecord;
import com.alibaba.polardbx.gms.metadb.table.SubClusterAccessor;
import com.alibaba.polardbx.gms.metadb.table.SubClusterRecord;
import com.alibaba.polardbx.gms.node.AllNodes;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.MppScope;
import com.alibaba.polardbx.gms.topology.ServerInfoAccessor;
import com.alibaba.polardbx.gms.topology.ServerInfoRecord;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ShowMppTest {
    private ServiceProvider serviceProviderMock;
    private InternalNodeManager internalNodeManagerMock;
    private ServerConnection connection;
    private ByteBufferHolder buffer;

    @Before
    public void setUp() {
        connection = mock(ServerConnection.class);
        buffer = new ByteBufferHolder(ByteBuffer.allocate(8 * 1024));
        when(connection.allocate()).thenReturn(buffer);
        when(connection.checkWriteBuffer(any(), anyInt())).thenReturn(buffer);
        when(connection.getSchema()).thenReturn("test_schema");
        when(connection.getResultSetCharset()).thenReturn("utf8mb4");

        serviceProviderMock = mock(ServiceProvider.class, RETURNS_DEEP_STUBS);
        internalNodeManagerMock = mock(InternalNodeManager.class, RETURNS_DEEP_STUBS);


        // Mock AllNodes and its getAllWorkers method
        AllNodes allNodesMock = mock(AllNodes.class);
        MppScope scope = MppScope.CURRENT; // or whichever scope you need

        List<InternalNode> mockWorkers = new ArrayList<>();
        InternalNode node1 = mock(InternalNode.class);
        when(node1.isWorker()).thenReturn(true);
        mockWorkers.add(node1);

        when(allNodesMock.getAllWorkers(eq(scope))).thenReturn(mockWorkers);
        when(internalNodeManagerMock.getAllNodes()).thenReturn(allNodesMock);

        // Mock the ServiceProvider's getNodeManager method
        when(serviceProviderMock.getServer().getNodeManager()).thenReturn(internalNodeManagerMock);
    }

    @Test
    public void testExecuteAndGetRow() throws Exception {
        // Mock database records
        SubClusterRecord subClusterRecord = new SubClusterRecord();
        subClusterRecord.instId = "inst1";
        subClusterRecord.node = "127.0.0.1:3306";
        subClusterRecord.subCluster = "cluster1";

        LoadWeightRecord loadWeightRecord = new LoadWeightRecord();
        loadWeightRecord.instId = "inst1";
        loadWeightRecord.node = "127.0.0.1:3306";
        loadWeightRecord.loadWeight = "100";

        List<SubClusterRecord> subClusterRecords = Collections.singletonList(subClusterRecord);
        List<LoadWeightRecord> loadWeightRecords = Collections.singletonList(loadWeightRecord);

        // Mock MetaDbUtil.getConnection()
        Connection connectionMock = mock(Connection.class);
        Statement stmtMock = mock(Statement.class);
        when(connectionMock.createStatement()).thenReturn(stmtMock);
        ResultSet rs = mock(ResultSet.class);
        when(stmtMock.executeQuery(anyString())).thenReturn(rs);

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            MockedStatic<ServiceProvider> serviceProviderMockedStaticMock = mockStatic(ServiceProvider.class)) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(connectionMock);
            serviceProviderMockedStaticMock.when(() -> ServiceProvider.getInstance()).thenReturn(serviceProviderMock);

            SubClusterAccessor accessor = mock(SubClusterAccessor.class);
            when(accessor.query()).thenReturn(subClusterRecords);

            LoadWeightAccessor loadWeightAccessor = mock(LoadWeightAccessor.class);
            when(loadWeightAccessor.query()).thenReturn(loadWeightRecords);
            boolean result = ShowMpp.execute(connection);
            assertTrue(result);

        }
    }

    @Test
    public void testRole() {
        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = mockStatic(ConfigDataMode.class)) {
            configDataModeMockedStatic.when(() -> ConfigDataMode.isMasterMode()).thenReturn(true);
            configDataModeMockedStatic.when(() -> ConfigDataMode.isRowSlaveMode()).thenReturn(false);

            InternalNode node = mock(InternalNode.class);
            when(node.isLeader()).thenReturn(true);
            when(node.getInstId()).thenReturn("");
            when(node.getHostPort()).thenReturn("");
            RowDataPacket rowDataPacket = ShowMpp.getRow("utf8", node, MppScope.CURRENT, null, null);
            String role = new String(rowDataPacket.fieldValues.get(2));
            assertEquals("W", role);

            rowDataPacket = ShowMpp.getRow("utf8", node, MppScope.SLAVE, null, null);
            role = new String(rowDataPacket.fieldValues.get(2));
            assertEquals("R", role);

            rowDataPacket = ShowMpp.getRow("utf8", node, MppScope.COLUMNAR, null, null);
            role = new String(rowDataPacket.fieldValues.get(2));
            assertEquals("CR", role);
        }

    }

}
