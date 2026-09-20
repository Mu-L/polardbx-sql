package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StorageInfoManagerTest {
    @Mock
    private IGroupExecutor groupExecutor;
    @Mock
    private Group group;
    @Mock
    private IDataSource dataSource;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        InstanceVersion.setSupportsVectorIndexes(false);
        // Initialize mocks here if needed
    }

    @After
    public void tearDown() {
        InstanceVersion.setSupportsVectorIndexes(false);
    }

    @Test
    public void testDoInitWithDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        TopologyHandler topologyHandler = new TopologyHandler("test", "test", "test", new ExecutorContext());
        StorageInfoManager storageInfoManager = new StorageInfoManager(topologyHandler);
        when(groupExecutor.getDataSource()).thenReturn(dataSource);
        when(group.getType()).thenReturn(GroupType.MYSQL_JDBC);

        storageInfoManager.doInit();

        // modify isInited through reflection
        Class<?> parentClass = storageInfoManager.getClass().getSuperclass();
        Field field = parentClass.getDeclaredField("isInited");
        if (field.isAccessible() == false) {
            field.setAccessible(true);
        }
        field.set(storageInfoManager, true);

        // Verify the expected value here
        Assert.assertTrue(storageInfoManager.supportXA());
        Assert.assertTrue(storageInfoManager.supportTso());
        Assert.assertTrue(!storageInfoManager.supportPurgeTso());
        Assert.assertTrue(!storageInfoManager.isLessMy56Version());
        Assert.assertTrue(storageInfoManager.isMysql80());
        Assert.assertTrue(!storageInfoManager.supportTsoHeartbeat());
        Assert.assertTrue(storageInfoManager.supportCtsTransaction());
        Assert.assertTrue(storageInfoManager.supportAsyncCommit57());
        Assert.assertTrue(storageInfoManager.supportLizard1PCTransaction());
        Assert.assertTrue(storageInfoManager.supportDeadlockDetection());
        Assert.assertTrue(storageInfoManager.supportMdlDeadlockDetection());
        Assert.assertTrue(storageInfoManager.supportsBloomFilter());
        Assert.assertTrue(storageInfoManager.supportSharedReadView());
        Assert.assertTrue(storageInfoManager.supportOpenSSL());
        Assert.assertTrue(storageInfoManager.supportsHyperLogLog());
        Assert.assertTrue(storageInfoManager.supportsXxHash());
        Assert.assertTrue(storageInfoManager.supportsReturning());
        Assert.assertTrue(storageInfoManager.supportsBackfillReturning());
        Assert.assertTrue(storageInfoManager.supportsAlterType());
        Assert.assertTrue(!storageInfoManager.isReadOnly());
        Assert.assertTrue(storageInfoManager.isLowerCaseTableNames());
        Assert.assertTrue(storageInfoManager.supportFastChecker());
        Assert.assertTrue(storageInfoManager.supportChangeSet());
        Assert.assertTrue(storageInfoManager.supportXOptForAutoSp());
        Assert.assertTrue(storageInfoManager.supportXRpc());
        Assert.assertTrue(storageInfoManager.isSupportMarkDistributed());
        Assert.assertTrue(storageInfoManager.supportXOptForPhysicalBackfill());
    }

    @Test
    public void testDoInit() {
        StorageInfoManager storageInfoManager = initStorageInfoManager(
            Collections.singletonList(newStorageInfo(true)));

        assertTrue(storageInfoManager.isSupportSyncPoint());
        assertTrue(storageInfoManager.getMergedStorageInfo().isSupportsVectorIndexes());
        assertTrue(InstanceVersion.supportsVectorIndexes());
    }

    @Test
    public void testVectorIndexesRequireSupportFromAllStorageNodes() {
        StorageInfoManager storageInfoManager = initStorageInfoManager(
            java.util.Arrays.asList(newStorageInfo(true), newStorageInfo(false)));

        assertFalse(storageInfoManager.getMergedStorageInfo().isSupportsVectorIndexes());
        assertFalse(InstanceVersion.supportsVectorIndexes());
    }

    @Test
    public void checkSupportSyncPointTest() throws SQLException {
        IDataSource dataSource = mock(IDataSource.class);
        IConnection connection = mock(IConnection.class);
        Statement statement = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        assertTrue(StorageInfoManager.checkSupportSyncPoint(dataSource));

        when(rs.next()).thenReturn(false);
        assertFalse(StorageInfoManager.checkSupportSyncPoint(dataSource));

        when(statement.executeQuery(any())).thenThrow(new SQLException("test"));
        try {
            StorageInfoManager.checkSupportSyncPoint(dataSource);
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Failed to check sync point support: test"));
        }
    }

    @Test
    public void checkSupportFlashbackAreaTest() throws SQLException {
        IDataSource dataSource = mock(IDataSource.class);
        IConnection connection = mock(IConnection.class);
        Statement statement = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        assertTrue(StorageInfoManager.checkSupportFlashbackArea(dataSource));

        when(rs.next()).thenReturn(false);
        assertFalse(StorageInfoManager.checkSupportFlashbackArea(dataSource));

        when(statement.executeQuery(any())).thenThrow(new SQLException("test"));
        assertFalse(StorageInfoManager.checkSupportFlashbackArea(dataSource));
    }

    @Test
    public void checkSupportGetCidxTest() throws SQLException {
        IDataSource dataSource = mock(IDataSource.class);
        IConnection connection = mock(IConnection.class);
        Statement statement = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery("call dbms_consensus.get_cidx()")).thenReturn(rs);

        // Case 1: rs.next() returns true -> supported
        when(rs.next()).thenReturn(true);
        assertTrue(StorageInfoManager.checkSupportGetCidx(dataSource));

        // Case 2: rs.next() returns false -> not supported
        when(rs.next()).thenReturn(false);
        assertFalse(StorageInfoManager.checkSupportGetCidx(dataSource));

        // Case 3: procedure does not exist -> not supported
        when(statement.executeQuery("call dbms_consensus.get_cidx()")).thenThrow(
            new SQLException("PROCEDURE dbms_consensus.get_cidx does not exist"));
        assertFalse(StorageInfoManager.checkSupportGetCidx(dataSource));

        // Case 4: other SQLException -> not supported
        Mockito.doThrow(new SQLException("Connection refused"))
            .when(statement).executeQuery("call dbms_consensus.get_cidx()");
        assertFalse(StorageInfoManager.checkSupportGetCidx(dataSource));
    }

    @Test
    public void checkSupportsVectorIndexesTest() throws SQLException {
        IDataSource dataSource = mock(IDataSource.class);
        IConnection connection = mock(IConnection.class);
        Statement statement = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(rs);

        when(rs.next()).thenReturn(true);
        assertTrue(StorageInfoManager.checkSupportsVectorIndexes(dataSource));

        when(rs.next()).thenReturn(false);
        assertFalse(StorageInfoManager.checkSupportsVectorIndexes(dataSource));

        when(statement.executeQuery(anyString())).thenThrow(new SQLException("VECTOR_INDEXES unavailable"));
        assertFalse(StorageInfoManager.checkSupportsVectorIndexes(dataSource));
        verify(statement, Mockito.times(3)).executeQuery(
            "SELECT 1 FROM information_schema.tables "
                + "WHERE table_schema = 'information_schema' AND table_name = 'VECTOR_INDEXES' LIMIT 1");
    }

    @Test
    public void testStorageInfoCreateDetectsVectorIndexSupport() throws SQLException {
        IDataSource dataSource = mock(IDataSource.class);
        IConnection connection = mock(IConnection.class);
        Statement statement = mock(Statement.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(dataSource.getConnection(MasterSlave.MASTER_ONLY)).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenAnswer(
            invocation -> mockResultSet((String) invocation.getArgument(0)));

        StorageInfoManager.StorageInfo storageInfo = StorageInfoManager.StorageInfo.create(dataSource);

        assertTrue(storageInfo.supportsVectorIndexes);
    }

    private static ResultSet mockResultSet(String sql) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString(1)).thenReturn("8.0.32");
        when(rs.getString(2)).thenReturn("ON");
        when(rs.getInt(1)).thenReturn(1);
        return rs;
    }

    private StorageInfoManager initStorageInfoManager(List<StorageInfoManager.StorageInfo> storageInfoList) {
        List<Group> groups = new ArrayList<>();
        for (int i = 0; i < storageInfoList.size(); i++) {
            Group storageGroup = mock(Group.class);
            when(storageGroup.getType()).thenReturn(Group.GroupType.MYSQL_JDBC);
            when(storageGroup.getName()).thenReturn("test_group_" + i);
            groups.add(storageGroup);
        }

        IGroupExecutor storageGroupExecutor = mock(IGroupExecutor.class);
        when(storageGroupExecutor.getDataSource()).thenReturn(mock(TGroupDataSource.class));
        Matrix matrix = mock(Matrix.class);
        TopologyHandler topologyHandler = mock(TopologyHandler.class);
        when(topologyHandler.getMatrix()).thenReturn(matrix);
        when(matrix.getGroups()).thenReturn(groups);
        when(topologyHandler.get(any())).thenReturn(storageGroupExecutor);

        AtomicInteger storageIndex = new AtomicInteger();
        try (MockedStatic<StorageInfoManager.StorageInfo> storageInfoMock =
            Mockito.mockStatic(StorageInfoManager.StorageInfo.class);
            MockedStatic<DbGroupInfoManager> dbGroupInfoManagerMock =
                Mockito.mockStatic(DbGroupInfoManager.class)) {
            storageInfoMock.when(() -> StorageInfoManager.StorageInfo.create(any()))
                .thenAnswer(invocation -> storageInfoList.get(storageIndex.getAndIncrement()));
            dbGroupInfoManagerMock.when(() -> DbGroupInfoManager.isNormalGroup(any())).thenReturn(true);

            StorageInfoManager storageInfoManager = new StorageInfoManager(topologyHandler);
            storageInfoManager.init();
            return storageInfoManager;
        }
    }

    private StorageInfoManager.StorageInfo newStorageInfo(boolean supportsVectorIndexes) {
        return new StorageInfoManager.StorageInfo(
            "5.7",
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            1,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            true,
            true,
            true,
            true,
            true,
            false,
            true,
            supportsVectorIndexes
        );
    }
}
