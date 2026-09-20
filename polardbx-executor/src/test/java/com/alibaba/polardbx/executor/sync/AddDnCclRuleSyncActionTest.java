package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.node.CCLDetectDnActor;
import com.alibaba.polardbx.gms.node.CCLDetectManager;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class AddDnCclRuleSyncActionTest {

    @Test
    public void testAddDnCclRuleSyncActionSuccess() throws Exception {
        // 测试数据准备
        String targetStorageInstId = "test_storage_inst_001";
        String sqlType = "SELECT";
        long concurrency = 10L;
        String keywords = "test_keywords";
        String testHost = "test-host";
        int testPort = 8080;
        String generatedSql = "call dbms_ccl.add_ccl_rule('SELECT', '', '', 10, 'test_keywords')";
        Long ruleId = 12345L;

        // 创建被测试对象
        AddDnCclRuleSyncAction syncAction = new AddDnCclRuleSyncAction(
            targetStorageInstId, sqlType, concurrency, keywords);

        // Mock依赖对象
        StorageInstHaContext mockStorageContext = mock(StorageInstHaContext.class);
        when(mockStorageContext.getStorageInstId()).thenReturn(targetStorageInstId);

        LeaderStatusBridge mockLeaderBridge = mock(LeaderStatusBridge.class);
        when(mockLeaderBridge.hasLeadership()).thenReturn(true);

        CCLDetectDnActor mockCclActor = mock(CCLDetectDnActor.class);
        when(mockCclActor.getGenSql(any(Supplier.class), anyLong(), anyString())).thenReturn(generatedSql);
        when(mockCclActor.generateCcl(any(Supplier.class), anyString())).thenReturn(ruleId);

        Connection mockConnection = mock(Connection.class);

        // Mock静态方法
        try (MockedStatic<ConfigDataMode> configDataModeMock = mockStatic(ConfigDataMode.class);
            MockedStatic<LeaderStatusBridge> leaderBridgeMock = mockStatic(LeaderStatusBridge.class);
            MockedStatic<TddlNode> tddlNodeMock = mockStatic(TddlNode.class);
            MockedStatic<CCLDetectManager> cclManagerMock = mockStatic(CCLDetectManager.class);
            MockedStatic<DbTopologyManager> dbTopologyManagerMock = mockStatic(DbTopologyManager.class);
            MockedStatic<CCLDetectDnActor> cclActorMock = mockStatic(CCLDetectDnActor.class)) {

            // 配置mock行为
            configDataModeMock.when(ConfigDataMode::isMasterMode).thenReturn(true);
            leaderBridgeMock.when(LeaderStatusBridge::getInstance).thenReturn(mockLeaderBridge);

            tddlNodeMock.when(TddlNode::getHost).thenReturn(testHost);
            tddlNodeMock.when(TddlNode::getPort).thenReturn(testPort);

            // Mock迭代器返回包含目标存储实例的列表
            Iterator<StorageInstHaContext> mockIterator = Arrays.asList(mockStorageContext).iterator();
            cclManagerMock.when(CCLDetectManager::getDnMasterIterator).thenReturn(mockIterator);

            dbTopologyManagerMock.when(() -> DbTopologyManager.getConnectionForStorage(mockStorageContext))
                .thenReturn(mockConnection);
            // 执行测试
            Object result = syncAction.sync();

            // 验证结果
            assertNotNull("Result should not be null", result);
            Assert.assertTrue("Result should be ArrayResultCursor", result instanceof ArrayResultCursor);

            ArrayResultCursor cursor = (ArrayResultCursor) result;
            assertEquals("Table name should match", "ADD_DN_CCL_RULE", cursor.getTableName());

            // 验证列定义
            assertEquals("Should have 5 columns", 5, cursor.getCursorMeta().getColumns().size());
            assertEquals("First column should be COMPUTE_NODE", "COMPUTE_NODE",
                cursor.getCursorMeta().getColumns().get(0).getName());
            assertEquals("Second column should be STATUS", "STATUS",
                cursor.getCursorMeta().getColumns().get(1).getName());
            assertEquals("Third column should be STORAGE_INST_ID", "STORAGE_INST_ID",
                cursor.getCursorMeta().getColumns().get(2).getName());
            assertEquals("Fourth column should be GENERATED_SQL", "GENERATED_SQL",
                cursor.getCursorMeta().getColumns().get(3).getName());
            assertEquals("Fifth column should be MESSAGE", "MESSAGE",
                cursor.getCursorMeta().getColumns().get(4).getName());

            // 验证数据行
            Row row = cursor.next();
            assertNotNull("Row should not be null", row);
            assertEquals("Should have 5 columns in row", 5, row.getColNum());
            assertEquals("COMPUTE_NODE should match", testHost + ":" + testPort, row.getObject(0));
            assertEquals("STATUS should be SUCCESS", "SUCCESS", row.getObject(1));
            assertEquals("STORAGE_INST_ID should match", targetStorageInstId, row.getObject(2));
            assertEquals("GENERATED_SQL should match", generatedSql, row.getObject(3));
            // 测试setter方法
            String newStorageInstId = "new_storage_inst";
            String newSqlType = "INSERT";
            long newConcurrency = 20L;
            String newKeywords = "new_keywords";

            syncAction.setTargetStorageInstId(newStorageInstId);
            syncAction.setSqlType(newSqlType);
            syncAction.setConcurrency(newConcurrency);
            syncAction.setKeywords(newKeywords);

            assertEquals("New TargetStorageInstId should match", newStorageInstId,
                syncAction.getTargetStorageInstId());
            assertEquals("New SqlType should match", newSqlType, syncAction.getSqlType());
            assertEquals("New Concurrency should match", newConcurrency, syncAction.getConcurrency());
            assertEquals("New Keywords should match", newKeywords, syncAction.getKeywords());
        }
    }
}
