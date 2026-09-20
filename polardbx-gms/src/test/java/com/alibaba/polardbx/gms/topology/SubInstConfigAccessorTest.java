package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * SubInstConfigAccessor单元测试
 *
 * @author assistant
 */
public class SubInstConfigAccessorTest {

    private SubInstConfigAccessor accessor;
    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;

    @Before
    public void setUp() throws Exception {
        accessor = new SubInstConfigAccessor();
        connection = mock(Connection.class);
        preparedStatement = mock(PreparedStatement.class);
        resultSet = mock(ResultSet.class);
        accessor.setConnection(connection);
    }

    /**
     * 测试获取指定实例的所有子实例配置 - 成功场景
     */
    @Test
    public void testGetAllSubInstConfigsByInstId_Success() {
        String instId = "test-inst-001";
        List<SubInstConfigRecord> expectedRecords = createMockRecords(instId, "sub-inst-001", 2);

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(1), any(Map.class),
                eq(ParameterMethod.setString), eq(instId))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.query(anyString(), any(Map.class),
                eq(SubInstConfigRecord.class), eq(connection))).thenReturn(expectedRecords);

            List<SubInstConfigRecord> actualRecords = accessor.getAllSubInstConfigsByInstId(instId);

            Assert.assertNotNull(actualRecords);
            Assert.assertEquals(2, actualRecords.size());
            Assert.assertEquals(instId, actualRecords.get(0).instId);
        }
    }

    /**
     * 测试获取指定实例的所有子实例配置 - 异常场景
     */
    @Test
    public void testGetAllSubInstConfigsByInstId_Exception() {
        String instId = "test-inst-001";

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(1), any(Map.class),
                eq(ParameterMethod.setString), eq(instId))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.query(anyString(), any(Map.class),
                    eq(SubInstConfigRecord.class), eq(connection)))
                .thenThrow(new SQLException("Database connection failed"));

            try {
                accessor.getAllSubInstConfigsByInstId(instId);
                Assert.fail("Should throw TddlRuntimeException");
            } catch (TddlRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("query"));
            }
        }
    }

    /**
     * 测试获取指定实例和子实例的配置 - 成功场景
     */
    @Test
    public void testGetAllSubInstConfigsByInstIdAndSubInstId_Success() {
        String instId = "test-inst-001";
        String subInstId = "sub-inst-001";
        List<SubInstConfigRecord> expectedRecords = createMockRecords(instId, subInstId, 3);

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(1), any(Map.class),
                eq(ParameterMethod.setString), eq(instId))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(2), any(Map.class),
                eq(ParameterMethod.setString), eq(subInstId))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.query(anyString(), any(Map.class),
                eq(SubInstConfigRecord.class), eq(connection))).thenReturn(expectedRecords);

            List<SubInstConfigRecord> actualRecords =
                accessor.getAllSubInstConfigsByInstIdAndSubInstId(instId, subInstId);

            Assert.assertNotNull(actualRecords);
            Assert.assertEquals(3, actualRecords.size());
            Assert.assertEquals(instId, actualRecords.get(0).instId);
            Assert.assertEquals(subInstId, actualRecords.get(0).subInstId);

            metaDbUtilMock.when(() -> MetaDbUtil.query(anyString(), any(Map.class),
                    eq(SubInstConfigRecord.class), eq(connection)))
                .thenThrow(new SQLException("Update failed"));
            try {
                accessor.getAllSubInstConfigsByInstIdAndSubInstId(instId, subInstId);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
            }
        }
    }

    /**
     * 测试删除指定实例的所有子实例配置 - 成功场景
     */
    @Test
    public void testDeleteSubInstConfigsByInstId_Success() throws SQLException {
        String instId = "test-inst-001";

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(1), any(Map.class),
                eq(ParameterMethod.setString), eq(instId))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.delete(anyString(), any(Map.class), eq(connection)))
                .thenReturn(5);

            accessor.deleteSubInstConfigsByInstId(instId);

            metaDbUtilMock.verify(() -> MetaDbUtil.delete(anyString(), any(Map.class), eq(connection)), times(1));
        }
    }

    /**
     * 测试删除指定实例和子实例的配置 - 成功场景
     */
    @Test
    public void testDeleteSubInstConfigsByInstIdAndSubInstId_Success() throws SQLException {
        String instId = "test-inst-001";
        String subInstId = "sub-inst-001";

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(1), any(Map.class),
                eq(ParameterMethod.setString), eq(instId))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(2), any(Map.class),
                eq(ParameterMethod.setString), eq(subInstId))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.delete(anyString(), any(Map.class), eq(connection)))
                .thenReturn(3);

            accessor.deleteSubInstConfigsByInstIdAndSubInstId(instId, subInstId);

            metaDbUtilMock.verify(() -> MetaDbUtil.delete(anyString(), any(Map.class), eq(connection)), times(1));
        }
    }

    /**
     * 测试添加子实例配置 - 不触发通知
     */
    @Test
    public void testAddSubInstConfigs_NoNotify() throws Exception {
        String instId = "test-inst-001";
        String subInstId = "sub-inst-001";
        Properties props = new Properties();
        props.setProperty("param1", "value1");
        props.setProperty("param2", "value2");

        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        doNothing().when(preparedStatement).setString(Mockito.anyInt(), anyString());
        doNothing().when(preparedStatement).addBatch();
        when(preparedStatement.executeBatch()).thenReturn(new int[] {1, 1});

        accessor.addSubInstConfigs(instId, subInstId, props, false);

        verify(preparedStatement, times(2)).addBatch();
        verify(preparedStatement, times(1)).executeBatch();
    }

    /**
     * 测试添加子实例配置 - 触发通知
     */
    @Test
    public void testAddSubInstConfigs_WithNotify() throws Exception {
        String instId = "test-inst-001";
        String subInstId = "sub-inst-001";
        Properties props = new Properties();
        props.setProperty("param1", "value1");

        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        doNothing().when(preparedStatement).setString(Mockito.anyInt(), anyString());
        doNothing().when(preparedStatement).addBatch();
        when(preparedStatement.executeBatch()).thenReturn(new int[] {1});

        try (MockedStatic<MetaDbConfigManager> metaDbConfigManagerMock =
            Mockito.mockStatic(MetaDbConfigManager.class)) {
            MetaDbConfigManager configManager = mock(MetaDbConfigManager.class);
            metaDbConfigManagerMock.when(MetaDbConfigManager::getInstance).thenReturn(configManager);
            when(configManager.notify(anyString(), eq(connection))).thenReturn(1L);

            accessor.addSubInstConfigs(instId, subInstId, props, true);

            verify(configManager, times(1)).notify(anyString(), eq(connection));
        }
    }

    /**
     * 测试添加子实例配置 - 异常场景
     */
    @Test
    public void testAddSubInstConfigs_Exception() throws Exception {
        String instId = "test-inst-001";
        String subInstId = "sub-inst-001";
        Properties props = new Properties();
        props.setProperty("param1", "value1");

        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        doNothing().when(preparedStatement).setString(Mockito.anyInt(), anyString());
        doNothing().when(preparedStatement).addBatch();
        when(preparedStatement.executeBatch()).thenThrow(new SQLException("Insert failed"));

        try {
            accessor.addSubInstConfigs(instId, subInstId, props, false);
            Assert.fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("batch insert"));
        }
    }

    /**
     * 测试更新子实例配置值
     */
    @Test
    public void testUpdateSubInstConfigValue_Success() throws SQLException {
        String instId = "test-inst-001";
        String subInstId = "sub-inst-001";
        Properties props = new Properties();
        props.setProperty("param1", "new_value1");
        props.setProperty("param2", "new_value2");

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<MetaDbConfigManager> metaDbConfigManagerMock =
                Mockito.mockStatic(MetaDbConfigManager.class)) {

            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(1), any(Map.class),
                eq(ParameterMethod.setString), anyString())).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(2), any(Map.class),
                eq(ParameterMethod.setString), anyString())).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(3), any(Map.class),
                eq(ParameterMethod.setString), eq(instId))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(4), any(Map.class),
                eq(ParameterMethod.setString), eq(subInstId))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.update(anyString(), any(Map.class), eq(connection)))
                .thenReturn(1);

            MetaDbConfigManager configManager = mock(MetaDbConfigManager.class);
            metaDbConfigManagerMock.when(MetaDbConfigManager::getInstance).thenReturn(configManager);
            when(configManager.notify(anyString(), eq(connection))).thenReturn(1L);

            accessor.updateSubInstConfigValue(instId, subInstId, props);

            metaDbUtilMock.verify(() -> MetaDbUtil.update(anyString(), any(Map.class), eq(connection)),
                times(2));
            verify(configManager, times(1)).notify(anyString(), eq(connection));

            metaDbUtilMock.when(() -> MetaDbUtil.update(anyString(), any(Map.class), eq(connection)))
                .thenThrow(new SQLException("Update failed"));
            try {
                accessor.updateSubInstConfigValue(instId, subInstId, props);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
            }
        }
    }

    /**
     * 测试根据参数键查询配置
     */
    @Test
    public void testQueryByParamKey_Success() {
        String instId = "test-inst-001";
        String subInstId = "sub-inst-001";
        String paramKey = "test-param";
        List<SubInstConfigRecord> expectedRecords = createMockRecords(instId, subInstId, 1);
        expectedRecords.get(0).paramKey = paramKey;

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(1), any(Map.class),
                eq(ParameterMethod.setString), eq(instId))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(2), any(Map.class),
                eq(ParameterMethod.setString), eq(subInstId))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(3), any(Map.class),
                eq(ParameterMethod.setString), eq(paramKey))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.query(anyString(), any(Map.class),
                eq(SubInstConfigRecord.class), eq(connection))).thenReturn(expectedRecords);

            List<SubInstConfigRecord> actualRecords = accessor.queryByParamKey(instId, subInstId, paramKey);

            Assert.assertNotNull(actualRecords);
            Assert.assertEquals(1, actualRecords.size());
            Assert.assertEquals(paramKey, actualRecords.get(0).paramKey);

            metaDbUtilMock.when(() -> MetaDbUtil.query(anyString(), any(Map.class),
                eq(SubInstConfigRecord.class), eq(connection))).thenThrow(new SQLException("Query failed"));
            try {
                accessor.queryByParamKey(instId, subInstId, paramKey);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlNestableRuntimeException);
            }
        }
    }

    /**
     * 测试空配置属性
     */
    @Test
    public void testAddSubInstConfigs_EmptyProperties() throws Exception {
        String instId = "test-inst-001";
        String subInstId = "sub-inst-001";
        Properties emptyProps = new Properties();

        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeBatch()).thenReturn(new int[] {});

        accessor.addSubInstConfigs(instId, subInstId, emptyProps, false);

        verify(preparedStatement, times(0)).addBatch();
        verify(preparedStatement, times(1)).executeBatch();
    }

    /**
     * 测试null值处理
     */
    @Test
    public void testGetAllSubInstConfigsByInstId_EmptyResult() {
        String instId = "test-inst-001";
        List<SubInstConfigRecord> emptyRecords = new ArrayList<>();

        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(() -> MetaDbUtil.setParameter(eq(1), any(Map.class),
                eq(ParameterMethod.setString), eq(instId))).then(invocation -> null);
            metaDbUtilMock.when(() -> MetaDbUtil.query(anyString(), any(Map.class),
                eq(SubInstConfigRecord.class), eq(connection))).thenReturn(emptyRecords);

            List<SubInstConfigRecord> actualRecords = accessor.getAllSubInstConfigsByInstId(instId);

            Assert.assertNotNull(actualRecords);
            Assert.assertEquals(0, actualRecords.size());
        }
    }

    /**
     * 创建模拟的配置记录
     */
    private List<SubInstConfigRecord> createMockRecords(String instId, String subInstId, int count) {
        List<SubInstConfigRecord> records = new ArrayList<>();
        long currentTime = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            SubInstConfigRecord record = new SubInstConfigRecord();
            record.id = i + 1;
            record.gmtCreated = new Timestamp(currentTime);
            record.gmtModified = new Timestamp(currentTime);
            record.instId = instId;
            record.subInstId = subInstId;
            record.paramKey = "param_key_" + i;
            record.paramVal = "param_value_" + i;
            records.add(record);
        }

        return records;
    }
}
