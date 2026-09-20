package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalNameValidator;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExternalCatalogValidateTaskTest {

    @Test
    public void testIncrementErrorCountBindsCurrentTimeForExpiredWindowReset() throws Exception {
        ExternalCatalogValidateTask task = new ExternalCatalogValidateTask("cat", "mock", new byte[1], null);
        Connection conn = mock(Connection.class);
        PreparedStatement upsert = mock(PreparedStatement.class);
        PreparedStatement select = mock(PreparedStatement.class);
        List<String> sqls = new ArrayList<>();
        when(conn.prepareStatement(anyString())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            sqls.add(sql);
            if (sql.contains("ON DUPLICATE KEY UPDATE")) {
                return upsert;
            }
            return select;
        });

        Method method = ExternalCatalogValidateTask.class.getDeclaredMethod(
            "incrementErrorCount", Connection.class, String.class);
        method.setAccessible(true);
        method.invoke(task, conn, "EXT_CATALOG:U@H");

        Assert.assertTrue(sqls.get(0).contains("CASE"));
        Assert.assertTrue(sqls.get(0).contains("expire_date < ? THEN 1"));
        verify(upsert).setString(1, "EXT_CATALOG:U@H");
        verify(upsert).setInt(2, 5);
        verify(upsert).setTimestamp(eq(3), any(Timestamp.class));
        verify(upsert).setTimestamp(eq(4), any(Timestamp.class));
        verify(upsert).executeUpdate();
    }

    @Test
    public void testExecuteImplSuccessMockConnector() throws Exception {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPs = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);
        when(mockPs.executeQuery()).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(false);

        ConnectorRegistry mockRegistry = mock(ConnectorRegistry.class);
        ConnectorDescriptor mockDescriptor = mock(ConnectorDescriptor.class);
        ConnectorMetadata mockMetadata = mock(ConnectorMetadata.class);

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedStatic<ExternalNameValidator> envMock = mockStatic(ExternalNameValidator.class);
            MockedStatic<ExternalCredentialEncryptor> eceMock = mockStatic(ExternalCredentialEncryptor.class);
            MockedStatic<ExternalCatalogConstants> eccMock = mockStatic(ExternalCatalogConstants.class);
            MockedStatic<ConnectorRegistry> crMock = mockStatic(ConnectorRegistry.class);
            MockedConstruction<ExternalCatalogInfoAccessor> caMock = mockConstruction(
                ExternalCatalogInfoAccessor.class,
                (m, ctx) -> when(m.selectByName(anyString())).thenReturn(null))) {

            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);
            eceMock.when(() -> ExternalCredentialEncryptor.decryptToMap(any(byte[].class)))
                .thenReturn(Collections.emptyMap());
            eccMock.when(() -> ExternalCatalogConstants.isMockConnector(anyString())).thenReturn(true);
            crMock.when(ConnectorRegistry::getInstance).thenReturn(mockRegistry);
            when(mockRegistry.get(anyString())).thenReturn(mockDescriptor);
            when(mockDescriptor.createMetadata(any(), any())).thenReturn(mockMetadata);
            when(mockMetadata.listDatabases()).thenReturn(Collections.emptyList());

            ExternalCatalogValidateTask task =
                new ExternalCatalogValidateTask("cat1", "mock", new byte[0], null);
            task.executeImpl(mockEc);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExecuteImplRateLimitExceeded() throws Exception {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPs = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);
        when(mockPs.executeQuery()).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(true);
        when(mockRs.getInt("error_count")).thenReturn(5);
        when(mockRs.getTimestamp("expire_date"))
            .thenReturn(new Timestamp(System.currentTimeMillis() + 60000));

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class)) {
            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            ExternalCatalogValidateTask task =
                new ExternalCatalogValidateTask("cat1", "mock", new byte[0], null);
            task.executeImpl(mockEc);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExecuteImplCatalogAlreadyExists() throws Exception {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPs = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);
        when(mockPs.executeQuery()).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(false);

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedStatic<ExternalNameValidator> envMock = mockStatic(ExternalNameValidator.class);
            MockedConstruction<ExternalCatalogInfoAccessor> caMock = mockConstruction(
                ExternalCatalogInfoAccessor.class,
                (m, ctx) -> when(m.selectByName(anyString())).thenReturn(
                    mock(com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoRecord.class)))) {
            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            ExternalCatalogValidateTask task =
                new ExternalCatalogValidateTask("cat1", "mock", new byte[0], null);
            task.executeImpl(mockEc);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExecuteImplConnectivityFailure() throws Exception {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPs = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);
        when(mockPs.executeQuery()).thenReturn(mockRs);
        when(mockPs.executeUpdate()).thenReturn(1);
        when(mockRs.next()).thenReturn(false, true);
        when(mockRs.getInt("error_count")).thenReturn(5);

        ConnectorRegistry mockRegistry = mock(ConnectorRegistry.class);
        ConnectorDescriptor mockDescriptor = mock(ConnectorDescriptor.class);
        ConnectorMetadata mockMetadata = mock(ConnectorMetadata.class);

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedStatic<ExternalNameValidator> envMock = mockStatic(ExternalNameValidator.class);
            MockedStatic<ExternalCredentialEncryptor> eceMock = mockStatic(ExternalCredentialEncryptor.class);
            MockedStatic<ExternalCatalogConstants> eccMock = mockStatic(ExternalCatalogConstants.class);
            MockedStatic<ConnectorRegistry> crMock = mockStatic(ConnectorRegistry.class);
            MockedConstruction<ExternalCatalogInfoAccessor> caMock = mockConstruction(
                ExternalCatalogInfoAccessor.class,
                (m, ctx) -> when(m.selectByName(anyString())).thenReturn(null))) {

            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);
            eceMock.when(() -> ExternalCredentialEncryptor.decryptToMap(any(byte[].class)))
                .thenReturn(Collections.emptyMap());
            eccMock.when(() -> ExternalCatalogConstants.isMockConnector(anyString())).thenReturn(true);
            crMock.when(ConnectorRegistry::getInstance).thenReturn(mockRegistry);
            when(mockRegistry.get(anyString())).thenReturn(mockDescriptor);
            when(mockDescriptor.createMetadata(any(), any())).thenReturn(mockMetadata);
            when(mockMetadata.listDatabases()).thenThrow(new RuntimeException("connection refused"));

            ExternalCatalogValidateTask task =
                new ExternalCatalogValidateTask("cat1", "mock", new byte[0], null);
            task.executeImpl(mockEc);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExecuteImplSecretNotFound() throws Exception {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPs = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);
        when(mockPs.executeQuery()).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(false);

        try (MockedStatic<MetaDbUtil> metaDbMock = mockStatic(MetaDbUtil.class);
            MockedStatic<ExternalNameValidator> envMock = mockStatic(ExternalNameValidator.class);
            MockedStatic<ExternalCredentialEncryptor> eceMock = mockStatic(ExternalCredentialEncryptor.class);
            MockedStatic<ExternalCatalogConstants> eccMock = mockStatic(ExternalCatalogConstants.class);
            MockedConstruction<ExternalCatalogInfoAccessor> caMock = mockConstruction(
                ExternalCatalogInfoAccessor.class,
                (m, ctx) -> when(m.selectByName(anyString())).thenReturn(null));
            MockedConstruction<ExternalSecretAccessor> saMock = mockConstruction(
                ExternalSecretAccessor.class,
                (m, ctx) -> when(m.selectByName(anyString())).thenReturn(null))) {

            metaDbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);
            eceMock.when(() -> ExternalCredentialEncryptor.decryptToMap(any(byte[].class)))
                .thenReturn(Collections.emptyMap());
            eccMock.when(() -> ExternalCatalogConstants.isMockConnector(anyString())).thenReturn(false);

            ExternalCatalogValidateTask task =
                new ExternalCatalogValidateTask("cat1", "oss", new byte[0], "secret1");
            task.executeImpl(mockEc);
        }
    }

    @Test
    public void testGetLimitKeyWithPrivilegeContext() throws Exception {
        ExecutionContext mockEc = mock(ExecutionContext.class);
        PrivilegeContext mockPc = mock(PrivilegeContext.class);
        when(mockEc.getPrivilegeContext()).thenReturn(mockPc);
        when(mockPc.getUser()).thenReturn("testuser");
        when(mockPc.getHost()).thenReturn("testhost");

        ExternalCatalogValidateTask task =
            new ExternalCatalogValidateTask("cat", "mock", new byte[0], null);
        Method method = ExternalCatalogValidateTask.class.getDeclaredMethod(
            "getLimitKey", ExecutionContext.class);
        method.setAccessible(true);
        String result = (String) method.invoke(task, mockEc);
        Assert.assertTrue(result.contains("TESTUSER"));
        Assert.assertTrue(result.contains("testhost"));
    }

    @Test
    public void testGetLimitKeyNullContext() throws Exception {
        ExternalCatalogValidateTask task =
            new ExternalCatalogValidateTask("cat", "mock", new byte[0], null);
        Method method = ExternalCatalogValidateTask.class.getDeclaredMethod(
            "getLimitKey", ExecutionContext.class);
        method.setAccessible(true);
        String result = (String) method.invoke(task, (ExecutionContext) null);
        Assert.assertTrue(result.contains("UNKNOWN"));
        Assert.assertTrue(result.contains("unknown"));
    }

    @Test
    public void testClearErrorCount() throws Exception {
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPs = mock(PreparedStatement.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);

        ExternalCatalogValidateTask task =
            new ExternalCatalogValidateTask("cat", "mock", new byte[0], null);
        Method method = ExternalCatalogValidateTask.class.getDeclaredMethod(
            "clearErrorCount", Connection.class, String.class);
        method.setAccessible(true);
        method.invoke(task, mockConn, "EXT_CATALOG:U@H");

        verify(mockPs).setString(1, "EXT_CATALOG:U@H");
        verify(mockPs).executeUpdate();
    }

    @Test
    public void testCheckRateLimitNoRecord() throws Exception {
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPs = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);
        when(mockPs.executeQuery()).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(false);

        ExternalCatalogValidateTask task =
            new ExternalCatalogValidateTask("cat", "mock", new byte[0], null);
        Method method = ExternalCatalogValidateTask.class.getDeclaredMethod(
            "checkRateLimit", Connection.class, String.class);
        method.setAccessible(true);
        method.invoke(task, mockConn, "EXT_CATALOG:U@H");

        verify(mockRs).next();
    }

    @Test
    public void testCheckRateLimitExceeded() throws Exception {
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPs = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);
        when(mockPs.executeQuery()).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(true);
        when(mockRs.getInt("error_count")).thenReturn(5);
        when(mockRs.getTimestamp("expire_date"))
            .thenReturn(new Timestamp(System.currentTimeMillis() + 60000));

        ExternalCatalogValidateTask task =
            new ExternalCatalogValidateTask("cat", "mock", new byte[0], null);
        Method method = ExternalCatalogValidateTask.class.getDeclaredMethod(
            "checkRateLimit", Connection.class, String.class);
        method.setAccessible(true);
        try {
            method.invoke(task, mockConn, "EXT_CATALOG:U@H");
            Assert.fail("Expected TddlRuntimeException");
        } catch (InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof TddlRuntimeException);
        }
    }

    @Test
    public void testGetters() {
        byte[] props = new byte[] {1, 2};
        ExternalCatalogValidateTask task =
            new ExternalCatalogValidateTask("cat1", "oss", props, "secret1");
        Assert.assertEquals("cat1", task.getCatalogName());
        Assert.assertEquals("oss", task.getConnector());
        Assert.assertArrayEquals(props, task.getEncryptedProperties());
        Assert.assertEquals("secret1", task.getSecretName());
    }
}
