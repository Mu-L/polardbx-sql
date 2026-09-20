package com.alibaba.polardbx.gms.metadb.schema;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SchemaChangeManagerTest {

    @Test
    public void testEnsureInnodbLargePrefix_setsOnWhenOff() throws Exception {
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_large_prefix'")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString("Value")).thenReturn("OFF");

        SchemaChangeManager.ensureInnodbLargePrefix(conn);

        verify(stmt).executeUpdate("SET GLOBAL innodb_large_prefix = ON");
    }

    @Test
    public void testEnsureInnodbLargePrefix_skipsWhenAlreadyOn() throws Exception {
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_large_prefix'")).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString("Value")).thenReturn("ON");

        SchemaChangeManager.ensureInnodbLargePrefix(conn);

        verify(stmt, never()).executeUpdate(anyString());
    }

    @Test
    public void testEnsureInnodbLargePrefix_handlesVariableNotExist() throws Exception {
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_large_prefix'")).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        SchemaChangeManager.ensureInnodbLargePrefix(conn);

        verify(stmt, never()).executeUpdate(anyString());
    }

    @Test
    public void testEnsureInnodbLargePrefix_handlesExceptionGracefully() throws Exception {
        Connection conn = mock(Connection.class);
        when(conn.createStatement()).thenThrow(new RuntimeException("connection error"));

        SchemaChangeManager.ensureInnodbLargePrefix(conn);
    }
}
