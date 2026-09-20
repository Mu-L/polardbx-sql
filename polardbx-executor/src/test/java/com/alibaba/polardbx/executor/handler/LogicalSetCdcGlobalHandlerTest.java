package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.logical.ITStatement;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

import static org.mockito.Mockito.*;

public class LogicalSetCdcGlobalHandlerTest {
    private ITConnection connection;
    private ITStatement statement;
    private LogicalSetCdcGlobalHandler handler;

    @Before
    @SneakyThrows
    public void setUp() {
        connection = mock(ITConnection.class);
        statement = mock(ITStatement.class);
        when(connection.createStatement()).thenReturn(statement);
        handler = new LogicalSetCdcGlobalHandler(null);
    }

    @Test
    public void testWriteHistory() throws SQLException {
        String key = "testKey";
        String value = "testValue";

        handler.writeHistory(key, value, connection);
        Assert.assertFalse(handler.isNeedToWriteHistory(key));

        verify(connection).setTrxPolicy(ITransactionPolicy.TSO, false);
        verify(statement).executeQuery(anyString());
    }
}
