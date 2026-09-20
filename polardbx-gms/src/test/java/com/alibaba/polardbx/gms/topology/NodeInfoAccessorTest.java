package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.gms.node.NodeStatusManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

/**
 * Unit tests for NodeInfoAccessor.queryLatestMasterActiveNoColumnar
 */
@RunWith(MockitoJUnitRunner.class)
public class NodeInfoAccessorTest {

    @Mock
    private Connection mockConnection;

    private NodeInfoAccessor accessor;

    @Before
    public void setUp() {
        accessor = new NodeInfoAccessor();
        accessor.setConnection(mockConnection);
    }

    /**
     * 正常场景：返回包含单条 master 非列存节点记录
     */
    @Test
    public void testQueryLatestMasterActiveNoColumnar_returnsSingleRecord() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            NodeInfoRecord record = new NodeInfoRecord();
            record.instId = "test_inst";
            record.ip = "192.168.1.1";
            record.port = 3306;
            record.role = NodeStatusManager.ROLE_MASTER;

            List<NodeInfoRecord> expectedList = new ArrayList<>();
            expectedList.add(record);

            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(anyString(), anyMap(), eq(NodeInfoRecord.class), any(Connection.class)))
                .thenReturn(expectedList);

            List<NodeInfoRecord> result = accessor.queryLatestMasterActiveNoColumnar();

            Assert.assertNotNull(result);
            Assert.assertEquals(1, result.size());
            Assert.assertEquals("test_inst", result.get(0).instId);
            Assert.assertEquals("192.168.1.1", result.get(0).ip);
        }
    }

    /**
     * 正常场景：返回多条 master 非列存节点记录
     */
    @Test
    public void testQueryLatestMasterActiveNoColumnar_returnsMultipleRecords() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            NodeInfoRecord record1 = new NodeInfoRecord();
            record1.instId = "inst1";
            record1.ip = "10.0.0.1";
            record1.role = NodeStatusManager.ROLE_MASTER;

            // ROLE_MASTER | ROLE_LEADER，但不含 ROLE_COLUMNAR
            NodeInfoRecord record2 = new NodeInfoRecord();
            record2.instId = "inst2";
            record2.ip = "10.0.0.2";
            record2.role = NodeStatusManager.ROLE_MASTER | NodeStatusManager.ROLE_LEADER;

            List<NodeInfoRecord> expectedList = new ArrayList<>();
            expectedList.add(record1);
            expectedList.add(record2);

            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(anyString(), anyMap(), eq(NodeInfoRecord.class), any(Connection.class)))
                .thenReturn(expectedList);

            List<NodeInfoRecord> result = accessor.queryLatestMasterActiveNoColumnar();

            Assert.assertNotNull(result);
            Assert.assertEquals(2, result.size());
            Assert.assertEquals("inst1", result.get(0).instId);
            Assert.assertEquals("inst2", result.get(1).instId);
        }
    }

    /**
     * 边界场景：查询结果为空列表
     */
    @Test
    public void testQueryLatestMasterActiveNoColumnar_returnsEmptyList() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(anyString(), anyMap(), eq(NodeInfoRecord.class), any(Connection.class)))
                .thenReturn(new ArrayList<>());

            List<NodeInfoRecord> result = accessor.queryLatestMasterActiveNoColumnar();

            Assert.assertNotNull(result);
            Assert.assertTrue(result.isEmpty());
        }
    }

    /**
     * 异常场景：MetaDbUtil.query 抛出异常时，应包装成 TddlRuntimeException 抛出
     */
    @Test
    public void testQueryLatestMasterActiveNoColumnar_throwsTddlRuntimeException_onQueryFailure() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(anyString(), anyMap(), eq(NodeInfoRecord.class), any(Connection.class)))
                .thenThrow(new RuntimeException("DB connection failed"));

            try {
                accessor.queryLatestMasterActiveNoColumnar();
                Assert.fail("Expected TddlRuntimeException to be thrown");
            } catch (TddlRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("DB connection failed"));
            }
        }
    }

    /**
     * SQL 校验：确认生成的 SQL 包含 master 角色过滤条件和排除列存节点的条件
     */
    @Test
    public void testQueryLatestMasterActiveNoColumnar_sqlContainsMasterAndNoColumnarConditions() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(sqlCaptor.capture(), anyMap(), eq(NodeInfoRecord.class),
                        any(Connection.class)))
                .thenReturn(new ArrayList<>());

            accessor.queryLatestMasterActiveNoColumnar();

            String capturedSql = sqlCaptor.getValue();

            // 验证 SQL 包含 ROLE_MASTER 过滤条件：(role & 4) <> 0
            Assert.assertTrue("SQL 应包含 master role 过滤条件",
                capturedSql.contains("role & " + NodeStatusManager.ROLE_MASTER));

            // 验证 SQL 包含排除 ROLE_COLUMNAR 的条件：(role & 64) = 0
            Assert.assertTrue("SQL 应包含排除列存节点的过滤条件",
                capturedSql.contains("role & " + NodeStatusManager.ROLE_COLUMNAR));

            // 验证 SQL 查询的是 node_info 表
            Assert.assertTrue("SQL 应查询 node_info 表",
                capturedSql.contains("node_info"));

            // 验证 SQL 使用时间窗口过滤
            Assert.assertTrue("SQL 应包含时间窗口过滤条件",
                capturedSql.contains("gmt_modified"));
        }
    }

    /**
     * 连接传递校验：确认 accessor 使用正确的 Connection 执行查询
     */
    @Test
    public void testQueryLatestMasterActiveNoColumnar_usesSetConnection() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            ArgumentCaptor<Connection> connCaptor = ArgumentCaptor.forClass(Connection.class);

            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(anyString(), anyMap(), eq(NodeInfoRecord.class), connCaptor.capture()))
                .thenReturn(new ArrayList<>());

            accessor.queryLatestMasterActiveNoColumnar();

            Assert.assertSame("应使用通过 setConnection 设置的连接", mockConnection, connCaptor.getValue());
        }
    }
}
