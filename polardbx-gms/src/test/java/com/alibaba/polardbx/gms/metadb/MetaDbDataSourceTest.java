package com.alibaba.polardbx.gms.metadb;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.rpc.client.XClient;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.pool.XConnection;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Unit tests for MetaDbDataSource class
 */
public class MetaDbDataSourceTest {

    private XDataSource mockDataSource;
    private XConnection mockConnection;
    private Statement mockStatement;
    private ResultSet mockResultSet;
    private XSession mockXSession;
    private XClient mockXClient;

    @Before
    public void setUp() throws SQLException {
        mockDataSource = Mockito.mock(XDataSource.class);
        mockConnection = Mockito.mock(XConnection.class);
        mockStatement = Mockito.mock(Statement.class);
        mockResultSet = Mockito.mock(ResultSet.class);
        mockXSession = Mockito.mock(XSession.class);
        mockXClient = Mockito.mock(XClient.class);

        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false);
        when(mockConnection.unwrap(XConnection.class)).thenReturn(mockConnection);
        when(mockConnection.getSession()).thenReturn(mockXSession);
        when(mockXSession.getClient()).thenReturn(mockXClient);
        when(mockXClient.getBaseVersion()).thenReturn(XClient.DnBaseVersion.DN_X_CLUSTER);
    }

    @Test(expected = TddlNestableRuntimeException.class)
    public void testInitXDataSourceByJdbcProps() throws Exception {
        final MetaDbDataSource metaDbDataSource = new MetaDbDataSource("", "",
            "connProperties=useServerPrepStmts=true&useUnicode=true&characterEncoding=utf-8&useSSL=false", "", "");

        // Create mocks for dependencies
        XDataSource mockXDataSource = Mockito.mock(XDataSource.class);
        // Create a real instance of MetaDbConnConf for the method call
        MetaDbConnConf metaDbDruidConf = new MetaDbConnConf();
        metaDbDruidConf.setBlockingTimeout(15000);

        // Properties string to test with
        String prop = "characterEncoding=utf8&connectTimeout=8000&socketTimeout=25000";

        // We need to mock the internal dependencies for initTsoServicesX and checkReadableMetaDB
        when(mockXDataSource.getConnection()).thenThrow(new SQLException("already exists"));

        metaDbDataSource.initXDataSourceByJdbcProps(mockDataSource, prop, metaDbDruidConf);
        metaDbDataSource.initXDataSourceByJdbcProps(mockXDataSource, prop, metaDbDruidConf);
    }
}
