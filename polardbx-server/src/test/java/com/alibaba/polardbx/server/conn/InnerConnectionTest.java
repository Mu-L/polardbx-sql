package com.alibaba.polardbx.server.conn;

import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InnerConnectionTest {
    @Test
    public void testInitAgentSessionVariables() {
        TConnection connection = mock(TConnection.class);
        Map<String, Object> extraServerVariables = new HashMap<>();
        when(connection.getExtraServerVariables()).thenReturn(extraServerVariables);
        InnerConnection target = new InnerConnection(connection);
        Map<String, Object> sessionVariables = new HashMap<>();
        sessionVariables.put("polardbx_server_id", 181818L);
        sessionVariables.put("sql_mode", "STRICT_TRANS_TABLES");
        sessionVariables.put("names", "utf8mb4");

        target.initConnBySessionVariables(sessionVariables);

        assertEquals(181818L, extraServerVariables.get("polardbx_server_id"));
        verify(connection).setSqlMode("STRICT_TRANS_TABLES");
        verify(connection).setEncoding("utf8mb4");
    }

    @Test
    public void testSetExtraServerVariables() {
        TConnection connection = mock(TConnection.class);
        InnerConnection mockConnection = new InnerConnection(connection);
        Map<String, Object> map = new HashMap<>();
        when(connection.getExtraServerVariables()).thenReturn(map);
        String key = "testKey";
        Object value = "testValue";
        mockConnection.setExtraServerVariables(key, value);

        Map<String, Object> extraServerVariables = mockConnection.getTConnection().getExtraServerVariables();
        assertEquals(value, extraServerVariables.get(key));

        Object newValue = "newValue";
        mockConnection.setExtraServerVariables(key, newValue);
        assertEquals(newValue, extraServerVariables.get(key));

        final boolean[] first = {true};
        when(connection.getExtraServerVariables()).thenAnswer(i -> {
            if (first[0]) {
                first[0] = false;
                return null;
            } else {
                return map;
            }
        });
        doNothing().when(connection).setExtraServerVariables(anyMap());
        newValue = "nnn";
        mockConnection.setExtraServerVariables(key, newValue);
        assertEquals(newValue, extraServerVariables.get(key));

        ITransaction trx = mock(ITransaction.class);
        when(connection.getTrx()).thenReturn(trx);
        Assert.assertEquals(trx, mockConnection.getTransaction());
    }

    @Test
    public void commitTsoTest() throws Exception {
        TConnection connection = mock(TConnection.class);
        when(connection.getCommitTso()).thenReturn(123L);
        doNothing().when(connection).commit();
        InnerConnection mockConnection = new InnerConnection(connection);
        mockConnection.setRecordCommitTso(true);
        mockConnection.commit();

        assertEquals(123L, mockConnection.getCommitTso());
    }
}
