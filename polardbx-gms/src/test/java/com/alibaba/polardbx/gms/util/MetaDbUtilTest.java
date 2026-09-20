package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbInfoAccessor;
import com.alibaba.polardbx.gms.topology.InstConfigAccessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static com.alibaba.polardbx.gms.topology.DbInfoAccessor.QUERY_EXISTS_USER_DB;
import static com.alibaba.polardbx.gms.topology.InstConfigAccessor.QUERY_OLDEST_RECORD;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MetaDbUtilTest {

    private MockedStatic<MetaDbUtil> mockMetaDbUtil;

    @Before
    public void setUp() throws Exception {
        mockMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
        mockMetaDbUtil.when(MetaDbUtil::getGmsPolardbVersion).thenCallRealMethod();
        mockMetaDbUtil.when(MetaDbUtil::isNewInstance).thenCallRealMethod();
    }

    @After
    public void cleanUp() {
        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }
    }

    @Test
    public void testGmsPolardbVersion() throws SQLException {

        final String mockReleaseDate = "20240412";
        final String mockEngineVersion = "5.4.19";

        Connection mockConnection = mock(Connection.class);
        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenAnswer(i -> mockConnection);
        Statement statement = mock(Statement.class);
        when(mockConnection.createStatement()).thenReturn(statement);
        ResultSet resultSet = mock(ResultSet.class);
        when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString(1)).thenReturn(mockEngineVersion);
        when(resultSet.getString(2)).thenReturn(mockReleaseDate);

        String gmsPolardbVersion = null;
        try {
            gmsPolardbVersion = MetaDbUtil.getGmsPolardbVersion();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotNull(gmsPolardbVersion);
        Assert.assertEquals(String.format("%s-%s", mockEngineVersion, mockReleaseDate), gmsPolardbVersion);
    }

    @Test
    public void testGmsPolardbVersionWithException() throws SQLException {

        final String mockExceptionMessage = "Mock SQLException";

        Connection mockConnection = mock(Connection.class);
        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenAnswer(i -> mockConnection);
        Statement statement = mock(Statement.class);
        when(mockConnection.createStatement()).thenReturn(statement);
        ResultSet resultSet = mock(ResultSet.class);
        when(statement.executeQuery(Mockito.anyString())).thenThrow(new SQLException(mockExceptionMessage));

        String gmsPolardbVersion = null;
        try {
            gmsPolardbVersion = MetaDbUtil.getGmsPolardbVersion();
            Assert.fail("Expect failed with exception");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(mockExceptionMessage));
        }
        Assert.assertNull(gmsPolardbVersion);
    }

    /**
     * 测试用例: 当 oldest record 是在一天内创建时，返回 true。
     */
    @Test
    public void testIsNewInstanceWhenOldestRecordCreatedWithinOneDayReturnsTrue() throws SQLException {
        MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
        Connection mockMetaDbConn = mock(Connection.class);
        Statement mockStatement = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(mockMetaDbDataSource.getConnection()).thenReturn(mockMetaDbConn);
        when(mockMetaDbConn.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(eq(QUERY_OLDEST_RECORD))).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(1);

        try (MockedStatic<MetaDbDataSource> mockStaticMetaDbDataSource = Mockito.mockStatic(MetaDbDataSource.class)) {
            mockStaticMetaDbDataSource.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);

            boolean result = MetaDbUtil.isNewInstance();

            assertTrue(result);
            verify(mockStatement, times(0)).executeQuery(QUERY_EXISTS_USER_DB);
        }
    }

    /**
     * 测试用例: 当 oldest record 不是在一天内创建且不存在 user database 时，返回 true。
     */
    @Test
    public void testIsNewInstanceWhenOldestRecordNotCreatedWithinOneDayAndNoUserDbReturnsTrue() throws SQLException {
        MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
        Connection mockMetaDbConn = mock(Connection.class);
        Statement mockStatement = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        ResultSet rs1 = mock(ResultSet.class);

        when(mockMetaDbDataSource.getConnection()).thenReturn(mockMetaDbConn);
        when(mockMetaDbConn.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(eq(QUERY_OLDEST_RECORD))).thenReturn(rs);
        when(mockStatement.executeQuery(eq(QUERY_EXISTS_USER_DB))).thenReturn(rs1);
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(0);
        when(rs1.next()).thenReturn(true);
        when(rs1.getInt(1)).thenReturn(0);
        try (MockedStatic<MetaDbDataSource> mockStaticMetaDbDataSource = Mockito.mockStatic(MetaDbDataSource.class)) {
            mockStaticMetaDbDataSource.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);

            boolean result = MetaDbUtil.isNewInstance();

            assertTrue(result);
            verify(mockStatement, times(1)).executeQuery(QUERY_OLDEST_RECORD);
            verify(mockStatement, times(1)).executeQuery(QUERY_EXISTS_USER_DB);
        }
    }

    /**
     * 测试用例: 当 oldest record 不是在一天内创建且存在 user database 时，返回 false。
     */
    @Test
    public void testIsNewInstanceWhenOldestRecordNotCreatedWithinOneDayAndHasUserDbReturnsFalse() throws SQLException {
        MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
        Connection mockMetaDbConn = mock(Connection.class);
        Statement mockStatement = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        ResultSet rs1 = mock(ResultSet.class);

        when(mockMetaDbDataSource.getConnection()).thenReturn(mockMetaDbConn);
        when(mockMetaDbConn.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(eq(QUERY_OLDEST_RECORD))).thenReturn(rs);
        when(mockStatement.executeQuery(eq(QUERY_EXISTS_USER_DB))).thenReturn(rs1);
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(0);
        when(rs1.next()).thenReturn(true);
        when(rs1.getInt(1)).thenReturn(2);

        try (MockedStatic<MetaDbDataSource> mockStaticMetaDbDataSource = Mockito.mockStatic(MetaDbDataSource.class)) {
            mockStaticMetaDbDataSource.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);

            boolean result = MetaDbUtil.isNewInstance();

            assertFalse(result);
            verify(mockStatement, times(1)).executeQuery(QUERY_OLDEST_RECORD);
            verify(mockStatement, times(1)).executeQuery(QUERY_EXISTS_USER_DB);
        }
    }
}
