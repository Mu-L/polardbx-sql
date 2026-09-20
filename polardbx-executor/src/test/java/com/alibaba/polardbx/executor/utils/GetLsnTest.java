package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.rpc.pool.XConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for ExecUtils.getLsn(), specifically the get_cidx() subtract-1 logic.
 */
public class GetLsnTest {

    @After
    public void tearDown() {
        StorageInfoManager.setSupportGetCidx(false);
    }

    /**
     * When supportGetCidx is true, getLsn should return (rawValue - 1).
     */
    @Test
    public void testGetLsnWithSupportGetCidx() throws SQLException {
        long rawLsn = 100L;
        StorageInfoManager.setSupportGetCidx(true);

        long result = ExecUtils.getLsn(mockDataSource(rawLsn), 0, "");
        Assert.assertEquals(rawLsn - 1, result);
    }

    /**
     * When supportGetCidx is false, getLsn should return the raw value directly.
     */
    @Test
    public void testGetLsnWithoutSupportGetCidx() throws SQLException {
        long rawLsn = 100L;
        StorageInfoManager.setSupportGetCidx(false);

        long result = ExecUtils.getLsn(mockDataSource(rawLsn), 0, "");
        Assert.assertEquals(rawLsn, result);
    }

    /**
     * Edge case: when get_cidx() returns 1, subtracting 1 should give 0.
     */
    @Test
    public void testGetLsnWithCidxReturnsOne() throws SQLException {
        long rawLsn = 1L;
        StorageInfoManager.setSupportGetCidx(true);

        long result = ExecUtils.getLsn(mockDataSource(rawLsn), 0, "");
        Assert.assertEquals(0L, result);
    }

    /**
     * When result set is empty, should throw SQLException.
     */
    @Test(expected = SQLException.class)
    public void testGetLsnEmptyResultSet() throws SQLException {
        StorageInfoManager.setSupportGetCidx(false);

        IDataSource dataSource = mock(IDataSource.class);
        IConnection connection = mock(IConnection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(dataSource.getConnection(MasterSlave.MASTER_ONLY)).thenReturn(connection);
        when(connection.isWrapperFor(XConnection.class)).thenReturn(false);
        when(connection.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        ExecUtils.getLsn(dataSource, 0, "");
    }

    private IDataSource mockDataSource(long lsnValue) throws SQLException {
        IDataSource dataSource = mock(IDataSource.class);
        IConnection connection = mock(IConnection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(dataSource.getConnection(MasterSlave.MASTER_ONLY)).thenReturn(connection);
        when(connection.isWrapperFor(XConnection.class)).thenReturn(false);
        when(connection.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString(1)).thenReturn(String.valueOf(lsnValue));

        return dataSource;
    }
}
