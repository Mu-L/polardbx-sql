package com.alibaba.polardbx.cdc;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static org.mockito.Mockito.when;

public class CdcStorageUtilTest {

    @Test
    public void testIsCdcStreamStatusColumnExists() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);

        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        boolean result = CdcStorageUtil.isCdcStreamStatusColumnExists(connection);
        Assert.assertFalse(result);

        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("Field")).thenReturn("status");
        result = CdcStorageUtil.isCdcStreamStatusColumnExists(connection);
        Assert.assertTrue(result);
    }

    @Test
    public void testGetStreamSetByGroup_1() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet resultSet1 = Mockito.mock(ResultSet.class);
        ResultSet resultSet2 = Mockito.mock(ResultSet.class);

        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery("SHOW COLUMNS FROM binlog_x_stream")).thenReturn(resultSet1);
        when(resultSet1.next()).thenReturn(true).thenReturn(false);
        when(resultSet1.getString("Field")).thenReturn("status");

        when(statement.executeQuery(
            "select stream_name,status from `binlog_x_stream` where group_name = 'test_group'")).thenReturn(resultSet2);
        when(resultSet2.next()).thenReturn(true).thenReturn(false);
        when(resultSet2.getInt(2)).thenReturn(0);
        when(resultSet2.getString(1)).thenReturn("stream1");

        Set<String> result = CdcStorageUtil.getStreamSetByGroup(connection, "test_group");
        Assert.assertEquals(Sets.newHashSet("stream1"), result);
    }

    @Test
    public void testGetStreamSetByGroup_2() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet resultSet1 = Mockito.mock(ResultSet.class);
        ResultSet resultSet2 = Mockito.mock(ResultSet.class);

        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery("SHOW COLUMNS FROM binlog_x_stream")).thenReturn(resultSet1);
        when(resultSet1.next()).thenReturn(true).thenReturn(false);
        when(resultSet1.getString("Field")).thenReturn("status");

        when(statement.executeQuery(
            "select stream_name,status from `binlog_x_stream` where group_name = 'test_group'")).thenReturn(resultSet2);
        when(resultSet2.next()).thenReturn(true).thenReturn(false);
        when(resultSet2.getInt(2)).thenReturn(1);
        when(resultSet2.getString(1)).thenReturn("stream1");

        Set<String> result = CdcStorageUtil.getStreamSetByGroup(connection, "test_group");
        Assert.assertEquals(Sets.newHashSet(), result);
    }

    @Test
    public void testGetStreamSetByGroup_3() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet resultSet1 = Mockito.mock(ResultSet.class);
        ResultSet resultSet2 = Mockito.mock(ResultSet.class);

        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery("SHOW COLUMNS FROM binlog_x_stream")).thenReturn(resultSet1);
        when(resultSet1.next()).thenReturn(false);

        when(statement.executeQuery(
            "select stream_name from `binlog_x_stream` where group_name = 'test_group'")).thenReturn(resultSet2);
        when(resultSet2.next()).thenReturn(true).thenReturn(false);
        when(resultSet2.getString(1)).thenReturn("stream1");

        Set<String> result = CdcStorageUtil.getStreamSetByGroup(connection, "test_group");
        Assert.assertEquals(Sets.newHashSet("stream1"), result);
    }
}
