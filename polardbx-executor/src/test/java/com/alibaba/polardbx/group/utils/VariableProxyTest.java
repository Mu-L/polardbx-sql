package com.alibaba.polardbx.group.utils;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.polardbx.atom.TAtomConnectionProxy;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.rpc.client.XClient;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class VariableProxyTest {

    private TGroupDataSource mockTGroupDataSource;
    private TGroupDirectConnection mockTGroupDirectConnection;
    private Connection mockConnection;

    @Before
    public void setUp() {
        mockTGroupDataSource = Mockito.mock(TGroupDataSource.class);
        mockTGroupDirectConnection = Mockito.mock(TGroupDirectConnection.class);
        mockConnection = Mockito.mock(Connection.class);
    }

    @Test
    public void testGetSessionVariablesForColumnarSlaveWithXProtocol() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<MetaDbUtil> staticMetaDBUtil = Mockito.mockStatic(MetaDbUtil.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.COLUMNAR_SLAVE);

            // Arrange for X protocol mode
            Connection mockMetaDbConnection = Mockito.mock(Connection.class);
            XConnection mockXConnection = Mockito.mock(XConnection.class);
            XClient mockXClient = Mockito.mock(XClient.class);
            XSession mockXSession = Mockito.mock(XSession.class);

            when(mockMetaDbConnection.isWrapperFor(XConnection.class)).thenReturn(true);
            when(mockMetaDbConnection.unwrap(XConnection.class)).thenReturn(mockXConnection);
            when(mockXConnection.getSession()).thenReturn(mockXSession);
            when(mockXSession.getClient()).thenReturn(mockXClient);

            ImmutableMap<String, Object> sessionVariables = ImmutableMap.of("max_connections", "100");
            when(mockXClient.getSessionVariablesL()).thenReturn(sessionVariables);

            // Mock DbInfoManager
            try (MockedStatic<DbInfoManager> staticDbInfoManager = Mockito.mockStatic(DbInfoManager.class)) {
                DbInfoManager mockDbInfoManager = Mockito.mock(DbInfoManager.class);
                staticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

                DbInfoRecord mockDbInfoRecord = Mockito.mock(DbInfoRecord.class);
                when(mockDbInfoManager.getDbInfo("testSchema")).thenReturn(mockDbInfoRecord);
                when(mockDbInfoManager.getDbChartSet("testSchema")).thenReturn("utf8mb4");
                when(mockDbInfoManager.getDbCollation("testSchema")).thenReturn("utf8mb4_general_ci");

                when(mockTGroupDataSource.getSchemaName()).thenReturn("testSchema");

                // Add the connection mock
                staticMetaDBUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(mockMetaDbConnection);

                // Act
                ImmutableMap<String, Object> result = proxy.getSessionVariables();

                // Assert
                Assert.assertTrue(result.containsKey("max_connections"));
                Assert.assertEquals("100", result.get("max_connections"));
                Assert.assertTrue(result.containsKey("character_set_database"));
                Assert.assertEquals("utf8mb4", result.get("character_set_database"));
                Assert.assertTrue(result.containsKey("collation_database"));
                Assert.assertEquals("utf8mb4_general_ci", result.get("collation_database"));
            }
        }
    }

    @Test
    public void testGetSessionVariablesForColumnarSlaveWithJDBC() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<MetaDbUtil> staticMetaDBUtil = Mockito.mockStatic(MetaDbUtil.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.COLUMNAR_SLAVE);

            // Arrange for JDBC mode
            Connection mockMetaDbConnection = Mockito.mock(Connection.class);
            Statement mockStatement = Mockito.mock(Statement.class);
            ResultSet mockResultSet = Mockito.mock(ResultSet.class);

            when(mockMetaDbConnection.createStatement()).thenReturn(mockStatement);
            when(mockStatement.executeQuery("SHOW SESSION VARIABLES")).thenReturn(mockResultSet);

            when(mockResultSet.next()).thenReturn(true, false);  // One record
            when(mockResultSet.getString("Variable_name")).thenReturn("max_connections");
            when(mockResultSet.getString("Value")).thenReturn("100");

            // Mock DbInfoManager
            try (MockedStatic<DbInfoManager> staticDbInfoManager = Mockito.mockStatic(DbInfoManager.class)) {
                DbInfoManager mockDbInfoManager = Mockito.mock(DbInfoManager.class);
                staticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

                when(mockDbInfoManager.getDbChartSet("testSchema")).thenReturn("utf8mb4");
                when(mockDbInfoManager.getDbCollation("testSchema")).thenReturn("utf8mb4_general_ci");

                when(mockTGroupDataSource.getSchemaName()).thenReturn("testSchema");

                // Add the connection mock
                staticMetaDBUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(mockMetaDbConnection);

                // Act
                ImmutableMap<String, Object> result = proxy.getSessionVariables();

                // Assert
                Assert.assertTrue(result.containsKey("max_connections"));
                Assert.assertEquals("100", result.get("max_connections"));
                Assert.assertTrue(result.containsKey("character_set_database"));
                Assert.assertEquals("utf8mb4", result.get("character_set_database"));
                Assert.assertTrue(result.containsKey("collation_database"));
                Assert.assertEquals("utf8mb4_general_ci", result.get("collation_database"));
            }
        }
    }

    @Test
    public void testGetSessionVariablesForMasterWithDruidConnection() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.MASTER);
            staticConfigDataMode.when(ConfigDataMode::needDNResource).thenReturn(true);

            // Arrange for Druid connection with TAtomConnectionProxy
            when(mockTGroupDataSource.getConnection()).thenReturn(mockTGroupDirectConnection);

            // Create a mock DruidPooledConnection and make mockConnection return it when cast
            DruidPooledConnection mockDruidPooledConnection = Mockito.mock(DruidPooledConnection.class);
            TAtomConnectionProxy mockTAtomConnectionProxy = Mockito.mock(TAtomConnectionProxy.class);

            // Simulate that mockTGroupDirectConnection.getConn() returns a DruidPooledConnection
            when(mockTGroupDirectConnection.getConn()).thenReturn(mockDruidPooledConnection);
            when(mockDruidPooledConnection.getConnection()).thenReturn(mockTAtomConnectionProxy);

            Map<String, Object> sessionVariables = new HashMap<>();
            sessionVariables.put("max_connections", "100");
            when(mockTAtomConnectionProxy.getSessionVariables()).thenReturn(sessionVariables);

            Map<String, Object> globalServerVariables = new HashMap<>();
            globalServerVariables.put("version", "8.0.26");
            when(mockTAtomConnectionProxy.getCurrentGlobalServerVariables()).thenReturn(globalServerVariables);

            // Mock DbInfoManager
            try (MockedStatic<DbInfoManager> staticDbInfoManager = Mockito.mockStatic(DbInfoManager.class)) {
                DbInfoManager mockDbInfoManager = Mockito.mock(DbInfoManager.class);
                staticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

                when(mockDbInfoManager.getDbChartSet("testSchema")).thenReturn("utf8mb4");
                when(mockDbInfoManager.getDbCollation("testSchema")).thenReturn("utf8mb4_general_ci");

                when(mockTGroupDataSource.getSchemaName()).thenReturn("testSchema");

                // Act
                ImmutableMap<String, Object> result = proxy.getSessionVariables();

                // Assert
                Assert.assertTrue(result.containsKey("max_connections"));
                Assert.assertEquals("100", result.get("max_connections"));
                Assert.assertTrue(result.containsKey("character_set_database"));
                Assert.assertEquals("utf8mb4", result.get("character_set_database"));
                Assert.assertTrue(result.containsKey("collation_database"));
                Assert.assertEquals("utf8mb4_general_ci", result.get("collation_database"));
            }
        }
    }

    @Test
    public void testGetSessionVariablesForMasterWithXConnection() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.MASTER);
            staticConfigDataMode.when(ConfigDataMode::needDNResource).thenReturn(true);

            // Arrange for XConnection
            when(mockTGroupDataSource.getConnection()).thenReturn(mockTGroupDirectConnection);
            when(mockTGroupDirectConnection.getConn()).thenReturn(mockConnection);

            XConnection mockXConnection = Mockito.mock(XConnection.class);
            XClient mockXClient = Mockito.mock(XClient.class);
            XSession mockXSession = Mockito.mock(XSession.class);

            when(mockConnection.isWrapperFor(XConnection.class)).thenReturn(true);
            when(mockConnection.unwrap(XConnection.class)).thenReturn(mockXConnection);
            when(mockXConnection.getSession()).thenReturn(mockXSession);
            when(mockXSession.getClient()).thenReturn(mockXClient);

            ImmutableMap<String, Object> sessionVariables = ImmutableMap.of("max_connections", "100");
            when(mockXClient.getSessionVariablesL()).thenReturn(sessionVariables);

            // Mock DbInfoManager
            try (MockedStatic<DbInfoManager> staticDbInfoManager = Mockito.mockStatic(DbInfoManager.class)) {
                DbInfoManager mockDbInfoManager = Mockito.mock(DbInfoManager.class);
                staticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

                when(mockDbInfoManager.getDbChartSet("testSchema")).thenReturn("utf8mb4");
                when(mockDbInfoManager.getDbCollation("testSchema")).thenReturn("utf8mb4_general_ci");

                when(mockTGroupDataSource.getSchemaName()).thenReturn("testSchema");

                // Act
                ImmutableMap<String, Object> result = proxy.getSessionVariables();

                // Assert
                Assert.assertTrue(result.containsKey("max_connections"));
                Assert.assertEquals("100", result.get("max_connections"));
                Assert.assertTrue(result.containsKey("character_set_database"));
                Assert.assertEquals("utf8mb4", result.get("character_set_database"));
                Assert.assertTrue(result.containsKey("collation_database"));
                Assert.assertEquals("utf8mb4_general_ci", result.get("collation_database"));
            }
        }
    }

    @Test
    public void testGetSessionVariablesWhenNoDNResource() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.MASTER);
            staticConfigDataMode.when(ConfigDataMode::needDNResource).thenReturn(false);

            // Act
            ImmutableMap<String, Object> result = proxy.getSessionVariables();

            // Assert
            Assert.assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testGetGlobalVariablesForMasterWithDruidConnection() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.MASTER);
            staticConfigDataMode.when(ConfigDataMode::needDNResource).thenReturn(true);

            // Arrange for Druid connection with TAtomConnectionProxy
            when(mockTGroupDataSource.getConnection()).thenReturn(mockTGroupDirectConnection);

            // Create a mock DruidPooledConnection and make mockConnection return it when cast
            DruidPooledConnection mockDruidPooledConnection = Mockito.mock(DruidPooledConnection.class);
            TAtomConnectionProxy mockTAtomConnectionProxy = Mockito.mock(TAtomConnectionProxy.class);

            // Simulate that mockTGroupDirectConnection.getConn() returns a DruidPooledConnection
            when(mockTGroupDirectConnection.getConn()).thenReturn(mockDruidPooledConnection);
            when(mockDruidPooledConnection.getConnection()).thenReturn(mockTAtomConnectionProxy);

            Map<String, Object> globalServerVariables = new HashMap<>();
            globalServerVariables.put("version", "8.0.26");
            globalServerVariables.put("max_connections", "1000");
            when(mockTAtomConnectionProxy.getCurrentGlobalServerVariables()).thenReturn(globalServerVariables);

            // Act
            ImmutableMap<String, Object> result = proxy.getGlobalVariables();

            // Assert
            Assert.assertTrue(result.containsKey("version"));
            Assert.assertEquals("8.0.26", result.get("version"));
            Assert.assertTrue(result.containsKey("max_connections"));
            Assert.assertEquals("1000", result.get("max_connections"));
        }
    }

    @Test
    public void testGetGlobalVariablesForMasterWithXConnection() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.MASTER);
            staticConfigDataMode.when(ConfigDataMode::needDNResource).thenReturn(true);

            // Arrange for XConnection
            when(mockTGroupDataSource.getConnection()).thenReturn(mockTGroupDirectConnection);
            when(mockTGroupDirectConnection.getConn()).thenReturn(mockConnection);

            XConnection mockXConnection = Mockito.mock(XConnection.class);
            XClient mockXClient = Mockito.mock(XClient.class);
            XSession mockXSession = Mockito.mock(XSession.class);

            when(mockConnection.isWrapperFor(XConnection.class)).thenReturn(true);
            when(mockConnection.unwrap(XConnection.class)).thenReturn(mockXConnection);
            when(mockXConnection.getSession()).thenReturn(mockXSession);
            when(mockXSession.getClient()).thenReturn(mockXClient);

            ImmutableMap<String, Object> globalVariables = ImmutableMap.of(
                "version", "8.0.26",
                "max_connections", "1000"
            );
            when(mockXClient.getGlobalVariablesL()).thenReturn(globalVariables);

            // Act
            ImmutableMap<String, Object> result = proxy.getGlobalVariables();

            // Assert
            Assert.assertTrue(result.containsKey("version"));
            Assert.assertEquals("8.0.26", result.get("version"));
            Assert.assertTrue(result.containsKey("max_connections"));
            Assert.assertEquals("1000", result.get("max_connections"));
        }
    }

    @Test
    public void testGetGlobalVariablesWhenNoDNResource() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.MASTER);
            staticConfigDataMode.when(ConfigDataMode::needDNResource).thenReturn(false);

            // Act
            ImmutableMap<String, Object> result = proxy.getGlobalVariables();

            // Assert
            Assert.assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testGetSessionVariablesForRowSlave() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.ROW_SLAVE);
            staticConfigDataMode.when(ConfigDataMode::needDNResource).thenReturn(true);

            // Arrange for XConnection
            when(mockTGroupDataSource.getConnection()).thenReturn(mockTGroupDirectConnection);
            when(mockTGroupDirectConnection.getConn()).thenReturn(mockConnection);

            XConnection mockXConnection = Mockito.mock(XConnection.class);
            XClient mockXClient = Mockito.mock(XClient.class);
            XSession mockXSession = Mockito.mock(XSession.class);

            when(mockConnection.isWrapperFor(XConnection.class)).thenReturn(true);
            when(mockConnection.unwrap(XConnection.class)).thenReturn(mockXConnection);
            when(mockXConnection.getSession()).thenReturn(mockXSession);
            when(mockXSession.getClient()).thenReturn(mockXClient);

            ImmutableMap<String, Object> sessionVariables = ImmutableMap.of("max_connections", "100");
            when(mockXClient.getSessionVariablesL()).thenReturn(sessionVariables);

            // Mock DbInfoManager
            try (MockedStatic<DbInfoManager> staticDbInfoManager = Mockito.mockStatic(DbInfoManager.class)) {
                DbInfoManager mockDbInfoManager = Mockito.mock(DbInfoManager.class);
                staticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

                when(mockDbInfoManager.getDbChartSet("testSchema")).thenReturn("utf8mb4");
                when(mockDbInfoManager.getDbCollation("testSchema")).thenReturn("utf8mb4_general_ci");

                when(mockTGroupDataSource.getSchemaName()).thenReturn("testSchema");

                // Act
                ImmutableMap<String, Object> result = proxy.getSessionVariables();

                // Assert
                Assert.assertTrue(result.containsKey("max_connections"));
                Assert.assertEquals("100", result.get("max_connections"));
                Assert.assertTrue(result.containsKey("character_set_database"));
                Assert.assertEquals("utf8mb4", result.get("character_set_database"));
                Assert.assertTrue(result.containsKey("collation_database"));
                Assert.assertEquals("utf8mb4_general_ci", result.get("collation_database"));
            }
        }
    }

    @Test
    public void testFetchDBVariablesWithEmptySchemaName() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<DbInfoManager> staticDbInfoManager = Mockito.mockStatic(DbInfoManager.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.COLUMNAR_SLAVE);

            DbInfoManager mockDbInfoManager = Mockito.mock(DbInfoManager.class);
            staticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

            when(mockTGroupDataSource.getSchemaName()).thenReturn("");

            Connection mockMetaDbConnection = Mockito.mock(Connection.class);
            XConnection mockXConnection = Mockito.mock(XConnection.class);
            XClient mockXClient = Mockito.mock(XClient.class);
            XSession mockXSession = Mockito.mock(XSession.class);

            when(mockMetaDbConnection.isWrapperFor(XConnection.class)).thenReturn(true);
            when(mockMetaDbConnection.unwrap(XConnection.class)).thenReturn(mockXConnection);
            when(mockXConnection.getSession()).thenReturn(mockXSession);
            when(mockXSession.getClient()).thenReturn(mockXClient);

            ImmutableMap<String, Object> sessionVariables = ImmutableMap.of();
            when(mockXClient.getSessionVariablesL()).thenReturn(sessionVariables);

            try (MockedStatic<MetaDbUtil> staticMetaDBUtil = Mockito.mockStatic(MetaDbUtil.class)) {
                staticMetaDBUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(mockMetaDbConnection);

                // Act
                ImmutableMap<String, Object> result = proxy.getSessionVariables();

                // Assert
                Assert.assertTrue(result.isEmpty());
            }
        }
    }

    @Test
    public void testResetDataSource() {
        // Arrange
        VariableProxy proxy = new VariableProxy(mockTGroupDataSource);
        TGroupDataSource newDataSource = Mockito.mock(TGroupDataSource.class);

        // Act
        proxy.resetDataSource(newDataSource);

        // Assert - We can't directly assert the internal state, but we can verify no exception is thrown
        // and that the method accepts the new data source
        Assert.assertNotNull(proxy);
    }

    @Test
    public void testGetSessionVariablesWithUnsupportedConnectionType() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.MASTER);
            staticConfigDataMode.when(ConfigDataMode::needDNResource).thenReturn(true);

            // Arrange for unsupported connection type
            when(mockTGroupDataSource.getConnection()).thenReturn(mockTGroupDirectConnection);
            when(mockTGroupDirectConnection.getConn()).thenReturn(mockConnection);

            // Make it not a DruidPooledConnection and not an XConnection
            when(mockConnection.isWrapperFor(XConnection.class)).thenReturn(false);
            // We cannot mock instanceof checks, so we'll rely on the fact that mockConnection is not
            // a DruidPooledConnection instance by default

            // Act - This should throw AssertionError wrapped in TddlNestableRuntimeException
            try {
                proxy.getSessionVariables();
                Assert.fail("Expected TddlNestableRuntimeException to be thrown");
            } catch (TddlNestableRuntimeException e) {
                Assert.assertTrue(e.getCause() instanceof AssertionError);
            }
        }
    }

    @Test
    public void testGetGlobalVariablesWithUnsupportedConnectionType() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.MASTER);
            staticConfigDataMode.when(ConfigDataMode::needDNResource).thenReturn(true);

            // Arrange for unsupported connection type
            when(mockTGroupDataSource.getConnection()).thenReturn(mockTGroupDirectConnection);
            when(mockTGroupDirectConnection.getConn()).thenReturn(mockConnection);

            // Make it not a DruidPooledConnection and not an XConnection
            when(mockConnection.isWrapperFor(XConnection.class)).thenReturn(false);
            // We cannot mock instanceof checks, so we'll rely on the fact that mockConnection is not
            // a DruidPooledConnection instance by default

            // Act - This should throw AssertionError wrapped in TddlNestableRuntimeException
            try {
                proxy.getGlobalVariables();
                Assert.fail("Expected TddlNestableRuntimeException to be thrown");
            } catch (TddlNestableRuntimeException e) {
                Assert.assertTrue(e.getCause() instanceof AssertionError);
            }
        }
    }

    @Test
    public void testGetSessionVariablesForFastMock() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.FAST_MOCK);

            // Act
            ImmutableMap<String, Object> result = proxy.getSessionVariables();

            // Assert
            Assert.assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testGetGlobalVariablesForRowSlave() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.ROW_SLAVE);
            staticConfigDataMode.when(ConfigDataMode::needDNResource).thenReturn(true);

            // Arrange for XConnection
            when(mockTGroupDataSource.getConnection()).thenReturn(mockTGroupDirectConnection);
            when(mockTGroupDirectConnection.getConn()).thenReturn(mockConnection);

            XConnection mockXConnection = Mockito.mock(XConnection.class);
            XClient mockXClient = Mockito.mock(XClient.class);
            XSession mockXSession = Mockito.mock(XSession.class);

            when(mockConnection.isWrapperFor(XConnection.class)).thenReturn(true);
            when(mockConnection.unwrap(XConnection.class)).thenReturn(mockXConnection);
            when(mockXConnection.getSession()).thenReturn(mockXSession);
            when(mockXSession.getClient()).thenReturn(mockXClient);

            ImmutableMap<String, Object> globalVariables = ImmutableMap.of(
                "version", "8.0.26",
                "max_connections", "1000"
            );
            when(mockXClient.getGlobalVariablesL()).thenReturn(globalVariables);

            // Act
            ImmutableMap<String, Object> result = proxy.getGlobalVariables();

            // Assert
            Assert.assertTrue(result.containsKey("version"));
            Assert.assertEquals("8.0.26", result.get("version"));
            Assert.assertTrue(result.containsKey("max_connections"));
            Assert.assertEquals("1000", result.get("max_connections"));
        }
    }

    @Test
    public void testGetGlobalVariablesForRowSlaveWithDruidConnection() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.ROW_SLAVE);
            staticConfigDataMode.when(ConfigDataMode::needDNResource).thenReturn(true);

            // Arrange for Druid connection with TAtomConnectionProxy
            when(mockTGroupDataSource.getConnection()).thenReturn(mockTGroupDirectConnection);

            // Create a mock DruidPooledConnection and make mockConnection return it when cast
            DruidPooledConnection mockDruidPooledConnection = Mockito.mock(DruidPooledConnection.class);
            TAtomConnectionProxy mockTAtomConnectionProxy = Mockito.mock(TAtomConnectionProxy.class);

            // Simulate that mockTGroupDirectConnection.getConn() returns a DruidPooledConnection
            when(mockTGroupDirectConnection.getConn()).thenReturn(mockDruidPooledConnection);
            when(mockDruidPooledConnection.getConnection()).thenReturn(mockTAtomConnectionProxy);

            Map<String, Object> globalServerVariables = new HashMap<>();
            globalServerVariables.put("version", "8.0.26");
            globalServerVariables.put("max_connections", "1000");
            when(mockTAtomConnectionProxy.getCurrentGlobalServerVariables()).thenReturn(globalServerVariables);

            // Act
            ImmutableMap<String, Object> result = proxy.getGlobalVariables();

            // Assert
            Assert.assertTrue(result.containsKey("version"));
            Assert.assertEquals("8.0.26", result.get("version"));
            Assert.assertTrue(result.containsKey("max_connections"));
            Assert.assertEquals("1000", result.get("max_connections"));
        }
    }

    @Test
    public void testFetchDBVariablesWithNullSchemaName() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<DbInfoManager> staticDbInfoManager = Mockito.mockStatic(DbInfoManager.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.COLUMNAR_SLAVE);

            DbInfoManager mockDbInfoManager = Mockito.mock(DbInfoManager.class);
            staticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

            when(mockTGroupDataSource.getSchemaName()).thenReturn(null);

            Connection mockMetaDbConnection = Mockito.mock(Connection.class);
            XConnection mockXConnection = Mockito.mock(XConnection.class);
            XClient mockXClient = Mockito.mock(XClient.class);
            XSession mockXSession = Mockito.mock(XSession.class);

            when(mockMetaDbConnection.isWrapperFor(XConnection.class)).thenReturn(true);
            when(mockMetaDbConnection.unwrap(XConnection.class)).thenReturn(mockXConnection);
            when(mockXConnection.getSession()).thenReturn(mockXSession);
            when(mockXSession.getClient()).thenReturn(mockXClient);

            ImmutableMap<String, Object> sessionVariables = ImmutableMap.of();
            when(mockXClient.getSessionVariablesL()).thenReturn(sessionVariables);

            try (MockedStatic<MetaDbUtil> staticMetaDBUtil = Mockito.mockStatic(MetaDbUtil.class)) {
                staticMetaDBUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(mockMetaDbConnection);

                // Act
                ImmutableMap<String, Object> result = proxy.getSessionVariables();

                // Assert
                Assert.assertTrue(result.isEmpty());
            }
        }
    }

    @Test
    public void testFetchDBVariablesWithDbInfoManagerReturnsNull() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<DbInfoManager> staticDbInfoManager = Mockito.mockStatic(DbInfoManager.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.COLUMNAR_SLAVE);

            DbInfoManager mockDbInfoManager = Mockito.mock(DbInfoManager.class);
            staticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

            when(mockDbInfoManager.getDbChartSet("testSchema")).thenReturn(null);
            when(mockDbInfoManager.getDbCollation("testSchema")).thenReturn(null);

            when(mockTGroupDataSource.getSchemaName()).thenReturn("testSchema");

            Connection mockMetaDbConnection = Mockito.mock(Connection.class);
            XConnection mockXConnection = Mockito.mock(XConnection.class);
            XClient mockXClient = Mockito.mock(XClient.class);
            XSession mockXSession = Mockito.mock(XSession.class);

            when(mockMetaDbConnection.isWrapperFor(XConnection.class)).thenReturn(true);
            when(mockMetaDbConnection.unwrap(XConnection.class)).thenReturn(mockXConnection);
            when(mockXConnection.getSession()).thenReturn(mockXSession);
            when(mockXSession.getClient()).thenReturn(mockXClient);

            ImmutableMap<String, Object> sessionVariables = ImmutableMap.of("max_connections", "100");
            when(mockXClient.getSessionVariablesL()).thenReturn(sessionVariables);

            try (MockedStatic<MetaDbUtil> staticMetaDBUtil = Mockito.mockStatic(MetaDbUtil.class)) {
                staticMetaDBUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(mockMetaDbConnection);

                // Act
                ImmutableMap<String, Object> result = proxy.getSessionVariables();

                // Assert
                Assert.assertTrue(result.containsKey("max_connections"));
                Assert.assertEquals("100", result.get("max_connections"));
                // Should not contain database character set and collation since they are null
                Assert.assertFalse(result.containsKey("character_set_database"));
                Assert.assertFalse(result.containsKey("collation_database"));
            }
        }
    }

    @Test
    public void testFetchDBVariablesWithEmptyValues() throws Throwable {
        try (MockedStatic<ConfigDataMode> staticConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<DbInfoManager> staticDbInfoManager = Mockito.mockStatic(DbInfoManager.class)) {

            VariableProxy proxy = new VariableProxy(mockTGroupDataSource);

            staticConfigDataMode.when(() -> ConfigDataMode.getInstanceRole()).thenReturn(InstanceRole.COLUMNAR_SLAVE);

            DbInfoManager mockDbInfoManager = Mockito.mock(DbInfoManager.class);
            staticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(mockDbInfoManager);

            when(mockDbInfoManager.getDbChartSet("testSchema")).thenReturn("");
            when(mockDbInfoManager.getDbCollation("testSchema")).thenReturn("");

            when(mockTGroupDataSource.getSchemaName()).thenReturn("testSchema");

            Connection mockMetaDbConnection = Mockito.mock(Connection.class);
            XConnection mockXConnection = Mockito.mock(XConnection.class);
            XClient mockXClient = Mockito.mock(XClient.class);
            XSession mockXSession = Mockito.mock(XSession.class);

            when(mockMetaDbConnection.isWrapperFor(XConnection.class)).thenReturn(true);
            when(mockMetaDbConnection.unwrap(XConnection.class)).thenReturn(mockXConnection);
            when(mockXConnection.getSession()).thenReturn(mockXSession);
            when(mockXSession.getClient()).thenReturn(mockXClient);

            ImmutableMap<String, Object> sessionVariables = ImmutableMap.of("max_connections", "100");
            when(mockXClient.getSessionVariablesL()).thenReturn(sessionVariables);

            try (MockedStatic<MetaDbUtil> staticMetaDBUtil = Mockito.mockStatic(MetaDbUtil.class)) {
                staticMetaDBUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(mockMetaDbConnection);

                // Act
                ImmutableMap<String, Object> result = proxy.getSessionVariables();

                // Assert
                Assert.assertTrue(result.containsKey("max_connections"));
                Assert.assertEquals("100", result.get("max_connections"));
                // Should not contain database character set and collation since they are empty
                Assert.assertFalse(result.containsKey("character_set_database"));
                Assert.assertFalse(result.containsKey("collation_database"));
            }
        }
    }
}