package com.alibaba.polardbx;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.server.handler.ColumnarConfigHandler;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

public class CobarServerTest {
    @Test
    public void tryInitServerVariablesTest() throws SQLException {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic =
            Mockito.mockStatic(MetaDbDataSource.class);
            MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic =
                Mockito.mockStatic(MetaDbConfigManager.class);) {
            ColumnarConfigHandler.init();
            Connection mockConnection = mock(Connection.class, RETURNS_DEEP_STUBS);
            MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
            when(mockMetaDbDataSource.getConnection()).thenReturn(mockConnection);
            PreparedStatement prepareStatement = mock(PreparedStatement.class);
            when(mockConnection.prepareStatement(Mockito.anyString())).thenReturn(prepareStatement);
            doNothing().when(prepareStatement).setString(anyInt(), anyString());
            doNothing().when(prepareStatement).addBatch();
            when(prepareStatement.executeBatch()).thenReturn(new int[] {1, 1});
            MetaDbConfigManager configManager = mock(MetaDbConfigManager.class);
            when(configManager.notify(anyString(), any())).thenReturn(1L);
            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);
            metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(configManager);

            Statement statement = mock(Statement.class);
            when(mockConnection.createStatement()).thenReturn(statement);
            ResultSet resultSet = mock(ResultSet.class);
            when(statement.executeQuery(anyString())).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getInt(1)).thenReturn(1);

            CobarServer.tryInitServerVariables();

            ArgumentCaptor<String> propertyNames = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> propertyValues = ArgumentCaptor.forClass(String.class);
            Mockito.verify(prepareStatement, atLeastOnce()).setString(eq(2), propertyNames.capture());
            Mockito.verify(prepareStatement, atLeastOnce()).setString(eq(3), propertyValues.capture());
            int propertyIndex =
                propertyNames.getAllValues().indexOf(ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT);
            Assert.assertTrue("The new-instance switch must be initialized", propertyIndex >= 0);
            Assert.assertEquals("true", propertyValues.getAllValues().get(propertyIndex));

            int dynamicValuesIndex =
                propertyNames.getAllValues().indexOf(ConnectionProperties.ENABLE_DYNAMIC_VALUES_OPTIMIZATION);
            Assert.assertTrue("The dynamic values switch must be initialized for new instances",
                dynamicValuesIndex >= 0);
            Assert.assertEquals("true", propertyValues.getAllValues().get(dynamicValuesIndex));

            ArgumentCaptor<String> configKeyCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> configValueCaptor = ArgumentCaptor.forClass(String.class);
            Mockito.verify(prepareStatement, atLeastOnce()).setString(eq(2), configKeyCaptor.capture());
            Mockito.verify(prepareStatement, atLeastOnce()).setString(eq(3), configValueCaptor.capture());
            List<String> configKeys = configKeyCaptor.getAllValues();
            List<String> configValues = configValueCaptor.getAllValues();
            int switchConfigIndex = configKeys.indexOf(ConnectionProperties.ENABLE_SAME_DB_SWITCH_NOOP);
            Assert.assertTrue(switchConfigIndex >= 0);
            Assert.assertEquals(configKeys.size(), configValues.size());
            Assert.assertEquals("true", configValues.get(switchConfigIndex));

            when(prepareStatement.executeBatch()).thenThrow(new SQLException("test"));
            CobarServer.tryInitServerVariables();
        } finally {
            ConfigDataMode.setMode(mode);
        }
    }

    @Test
    public void testBlockCacheDisabledOnMasterNewInstance() throws SQLException {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
            try (MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic =
                Mockito.mockStatic(MetaDbDataSource.class);
                MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic =
                    Mockito.mockStatic(MetaDbConfigManager.class)) {
                ColumnarConfigHandler.init();
                Connection mockConnection = mock(Connection.class, RETURNS_DEEP_STUBS);
                MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
                when(mockMetaDbDataSource.getConnection()).thenReturn(mockConnection);
                PreparedStatement prepareStatement = mock(PreparedStatement.class);
                when(mockConnection.prepareStatement(Mockito.anyString())).thenReturn(prepareStatement);
                doNothing().when(prepareStatement).setString(anyInt(), anyString());
                doNothing().when(prepareStatement).addBatch();
                when(prepareStatement.executeBatch()).thenReturn(new int[] {1, 1});
                MetaDbConfigManager configManager = mock(MetaDbConfigManager.class);
                when(configManager.notify(anyString(), any())).thenReturn(1L);
                metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);
                metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(configManager);

                Statement statement = mock(Statement.class);
                when(mockConnection.createStatement()).thenReturn(statement);
                ResultSet resultSet = mock(ResultSet.class);
                when(statement.executeQuery(anyString())).thenReturn(resultSet);
                // Make isNewInstance() return true
                when(resultSet.next()).thenReturn(true);
                when(resultSet.getInt(1)).thenReturn(1);

                CobarServer.tryInitServerVariables();

                // Verify BLOCK_CACHE_MEMORY_SIZE_FACTOR="0" was set on master instance
                Mockito.verify(prepareStatement, atLeastOnce())
                    .setString(eq(2), eq(ConnectionProperties.BLOCK_CACHE_MEMORY_SIZE_FACTOR));
                Mockito.verify(prepareStatement, atLeastOnce()).setString(eq(3), eq("0"));
            }
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
            ConfigDataMode.setMode(mode);
        }
    }

    @Test
    public void testBlockCacheNotSetOnSlaveNewInstance() throws SQLException {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try {
            ConfigDataMode.setInstanceRole(InstanceRole.COLUMNAR_SLAVE);
            try (MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic =
                Mockito.mockStatic(MetaDbDataSource.class);
                MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic =
                    Mockito.mockStatic(MetaDbConfigManager.class)) {
                ColumnarConfigHandler.init();
                Connection mockConnection = mock(Connection.class, RETURNS_DEEP_STUBS);
                MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
                when(mockMetaDbDataSource.getConnection()).thenReturn(mockConnection);
                PreparedStatement prepareStatement = mock(PreparedStatement.class);
                when(mockConnection.prepareStatement(Mockito.anyString())).thenReturn(prepareStatement);
                doNothing().when(prepareStatement).setString(anyInt(), anyString());
                doNothing().when(prepareStatement).addBatch();
                when(prepareStatement.executeBatch()).thenReturn(new int[] {1, 1});
                MetaDbConfigManager configManager = mock(MetaDbConfigManager.class);
                when(configManager.notify(anyString(), any())).thenReturn(1L);
                metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);
                metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(configManager);

                Statement statement = mock(Statement.class);
                when(mockConnection.createStatement()).thenReturn(statement);
                ResultSet resultSet = mock(ResultSet.class);
                when(statement.executeQuery(anyString())).thenReturn(resultSet);
                // Make isNewInstance() return true
                when(resultSet.next()).thenReturn(true);
                when(resultSet.getInt(1)).thenReturn(1);

                CobarServer.tryInitServerVariables();

                // Verify BLOCK_CACHE_MEMORY_SIZE_FACTOR was NOT set on non-master instance
                Mockito.verify(prepareStatement, never())
                    .setString(eq(2), eq(ConnectionProperties.BLOCK_CACHE_MEMORY_SIZE_FACTOR));
            }
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
            ConfigDataMode.setMode(mode);
        }
    }

    @Test
    public void initColumnarSpecialConfigSetTest() throws Exception {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic =
            Mockito.mockStatic(MetaDbDataSource.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);
            when(mockMetaDbDataSource.getConnection()).thenReturn(null);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyList(),
                Mockito.any())).thenReturn(new int[] {1});

            CobarServer.initColumnarSpecialConfigSet();

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyList(),
                Mockito.any())).thenThrow(new RuntimeException("mock exception"));

            CobarServer.initColumnarSpecialConfigSet();
        }
        ConfigDataMode.setMode(mode);
    }
}
