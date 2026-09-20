package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.gms.metadb.table.LoadWeightAccessor;
import com.alibaba.polardbx.gms.metadb.table.LoadWeightRecord;
import com.alibaba.polardbx.gms.metadb.table.SubClusterAccessor;
import com.alibaba.polardbx.gms.metadb.table.SubClusterRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.Multimap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ServerInstSubManagerTest {

    private ServerInstSubManager manager;
    private MockedStatic<MetaDbUtil> metaDbUtilMock;
    private Connection connection;
    private SubClusterAccessor subClusterAccessor;

    @Before
    public void setUp() throws Exception {
        metaDbUtilMock = mockStatic(MetaDbUtil.class);
        connection = mock(Connection.class);
        subClusterAccessor = mock(SubClusterAccessor.class);

        metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(connection);

        manager = spy(ServerInstSubManager.getInstance());
    }

    @After
    public void tearDown() {
        if (metaDbUtilMock != null) {
            metaDbUtilMock.close();
        }
    }

    @Test
    public void testLoadNodeSubClusterAndGetIdToSubcluster() throws Exception {
        // 准备测试数据
        SubClusterRecord record1 = new SubClusterRecord();
        record1.instId = "inst1";
        record1.node = "node1";
        record1.subCluster = "subCluster1";

        SubClusterRecord record2 = new SubClusterRecord();
        record2.instId = "inst2";
        record2.node = "node2";
        record2.subCluster = "subCluster2";

        SubClusterRecord record3 = new SubClusterRecord();
        record3.instId = "inst1";
        record3.node = "node1";
        record3.subCluster = "subCluster3"; // 测试相同instId+node对应多个subCluster的情况

        doReturn(subClusterAccessor).when(manager).newSubClusterAccessor();
        when(subClusterAccessor.query()).thenReturn(Arrays.asList(record1, record2, record3));

        // 执行方法
        manager.loadNodeSubCluster();

        // 验证结果
        Multimap<String, String> result = manager.getIdToSubcluster();

        assertNotNull(result);
        assertEquals(3, result.size());

        // 验证inst1node1对应两个subCluster
        Collection<String> subClustersForInst1Node1 = result.get("inst1node1");
        assertEquals(2, subClustersForInst1Node1.size());
        assertTrue(subClustersForInst1Node1.contains("subCluster1"));
        assertTrue(subClustersForInst1Node1.contains("subCluster3"));

        // 验证inst2node2对应一个subCluster
        Collection<String> subClustersForInst2Node2 = result.get("inst2node2");
        assertEquals(1, subClustersForInst2Node2.size());
        assertTrue(subClustersForInst2Node2.contains("subCluster2"));

        // 验证不可变性
        try {
            result.put("inst3", "subCluster3");
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // 预期异常，因为返回的是不可变的Multimap
        }
    }

    @Test
    public void testLoadNodeLoadWeightAndGetIdToLoadWeight() throws Exception {
        // 准备测试数据
        LoadWeightRecord record1 = new LoadWeightRecord();
        record1.instId = "inst1";
        record1.node = "node1";
        record1.loadWeight = "10";

        LoadWeightRecord record2 = new LoadWeightRecord();
        record2.instId = "inst2";
        record2.node = "node2";
        record2.loadWeight = "20";

        // 准备 mock
        LoadWeightAccessor loadWeightAccessor = mock(LoadWeightAccessor.class);
        doReturn(loadWeightAccessor).when(manager).newLoadWeightAccessor();
        when(loadWeightAccessor.query()).thenReturn(Arrays.asList(record1, record2));

        // 执行方法
        manager.loadNodeLoadWeight();

        // 验证结果
        java.util.HashMap<String, String> result = manager.getIdToLoadWeight();

        assertNotNull(result);
        assertEquals(2, result.size());

        // 验证 inst1node1
        assertEquals("10", result.get("inst1node1"));

        // 验证 inst2node2
        assertEquals("20", result.get("inst2node2"));

        // 验证返回的是副本，不影响内部状态
        result.put("inst3node3", "30");
        assertNull(manager.getIdToLoadWeight().get("inst3node3"));
    }
}
