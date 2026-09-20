package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.handler.pl.inner.JdbcUrlAddProcedure;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class JdbcUrlAddProcedureTest {

    @Test
    public void testValidJdbcUrl() {
        // Mock ServerConnection
        ServerConnection mockConnection = Mockito.mock(ServerConnection.class);
        Mockito.when(mockConnection.getId()).thenReturn(1L);
        Mockito.when(mockConnection.getUser()).thenReturn("testUser");
        Mockito.when(mockConnection.getHost()).thenReturn("localhost");
        Mockito.when(mockConnection.getPort()).thenReturn(3306);
        Mockito.when(mockConnection.getSchema()).thenReturn("testDb");
        Mockito.when(mockConnection.getLastActiveTime()).thenReturn(System.nanoTime() - 5000000000L); // 5 seconds ago

        // Parse SQL statement
        SQLCallStatement statement =
            (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.jdbc_url_add('jdbc:mysql://localhost:3306/testDb')",
                SQLParserFeature.IgnoreNameQuotes).get(0);

        // Create cursor and procedure
        ArrayResultCursor cursor = new ArrayResultCursor("jdbc_url_add");
        JdbcUrlAddProcedure procedure = new JdbcUrlAddProcedure();

        // Execute the procedure
        procedure.execute(mockConnection, statement, cursor);

        // Verify the result
        Assert.assertEquals(1, cursor.getRows().size());
        Row row =  cursor.getRows().get(0);
        Assert.assertEquals(1L, row.getObject(0)); // ID
        Assert.assertEquals("testUser", row.getObject(1)); // USER
        Assert.assertEquals("localhost:3306", row.getObject(2)); // HOST
        Assert.assertEquals("testDb", row.getObject(3)); // DB
        Assert.assertEquals("jdbc:mysql://localhost:3306/testDb", row.getObject(5)); // JDBC_CONNECTION
    }
}
