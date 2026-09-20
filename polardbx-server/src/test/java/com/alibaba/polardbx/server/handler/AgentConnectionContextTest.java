/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.executor.ai.AgentSession;
import com.alibaba.polardbx.gms.privilege.ActiveRoles;
import com.alibaba.polardbx.server.ServerConnection;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class AgentConnectionContextTest {

    @Test
    public void testCaptureCopiesAllowedSessionState() {
        ServerConnection mockConnection = Mockito.mock(ServerConnection.class);
        Map<String, Object> extraVariables = new HashMap<>();
        extraVariables.put(ICdcManager.POLARDBX_SERVER_ID, 181818L);
        extraVariables.put("time_zone", "+08:00");
        extraVariables.put("sockettimeout", 1000L);
        Set<Long> roles = new HashSet<>(Arrays.asList(1L, 2L));
        ActiveRoles activeRoles = new ActiveRoles(ActiveRoles.ActiveRoleSpec.ROLES, roles);

        Mockito.when(mockConnection.getExtraServerVariables()).thenReturn(extraVariables);
        Mockito.when(mockConnection.getSqlMode()).thenReturn("STRICT_TRANS_TABLES");
        Mockito.when(mockConnection.getConnectionCharset()).thenReturn("utf8mb4");
        Mockito.when(mockConnection.getActiveRoles()).thenReturn(activeRoles);
        Mockito.when(mockConnection.getTxIsolation()).thenReturn(Connection.TRANSACTION_READ_COMMITTED);
        Mockito.when(mockConnection.isTxReadOnly()).thenReturn(true);

        AgentConnectionContext target = AgentConnectionContext.capture(mockConnection);

        Assert.assertEquals(181818L,
            target.getSessionVariables().get(ICdcManager.POLARDBX_SERVER_ID));
        Assert.assertEquals("+08:00", target.getSessionVariables().get("time_zone"));
        Assert.assertEquals("STRICT_TRANS_TABLES", target.getSessionVariables().get("sql_mode"));
        Assert.assertEquals("utf8mb4", target.getSessionVariables().get("names"));
        Assert.assertFalse(target.getSessionVariables().containsKey("sockettimeout"));
        Assert.assertEquals(Connection.TRANSACTION_READ_COMMITTED, target.getTransactionIsolation());
        Assert.assertTrue(target.isReadOnly());
        Assert.assertEquals(ActiveRoles.ActiveRoleSpec.ROLES, target.getActiveRoles().getActiveRoleSpec());
        Assert.assertEquals(new HashSet<>(Arrays.asList(1L, 2L)), target.getActiveRoles().getRoles());

        extraVariables.put(ICdcManager.POLARDBX_SERVER_ID, 1L);
        roles.add(3L);
        Assert.assertEquals(181818L,
            target.getSessionVariables().get(ICdcManager.POLARDBX_SERVER_ID));
        Assert.assertFalse(target.getActiveRoles().getRoles().contains(3L));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCapturedSessionVariablesAreImmutable() {
        ServerConnection mockConnection = Mockito.mock(ServerConnection.class);
        Mockito.when(mockConnection.getExtraServerVariables()).thenReturn(new HashMap<>());

        AgentConnectionContext target = AgentConnectionContext.capture(mockConnection);

        target.getSessionVariables().put("sql_mode", "ANSI");
    }

    @Test
    public void testCaptureUsesSafeDefaults() {
        ServerConnection mockConnection = Mockito.mock(ServerConnection.class);
        Mockito.when(mockConnection.getExtraServerVariables()).thenReturn(new HashMap<>());
        Mockito.when(mockConnection.getActiveRoles()).thenReturn(null);

        AgentConnectionContext target = AgentConnectionContext.capture(mockConnection);

        Assert.assertTrue(target.getSessionVariables().isEmpty());
        Assert.assertEquals(ActiveRoles.ActiveRoleSpec.DEFAULT, target.getActiveRoles().getActiveRoleSpec());
        Assert.assertTrue(target.getActiveRoles().getRoles().isEmpty());
        Assert.assertFalse(target.isReadOnly());
    }

    @Test
    public void testReadOnlyContextRejectsExecuteSql() throws Exception {
        ServerConnection mockConnection = Mockito.mock(ServerConnection.class);
        Mockito.when(mockConnection.getExtraServerVariables()).thenReturn(new HashMap<>());
        Mockito.when(mockConnection.isReadOnly()).thenReturn(true);
        AgentConnectionContext connectionContext = AgentConnectionContext.capture(mockConnection);
        Method method = AgentService.class.getDeclaredMethod("dispatchToolCall",
            String.class, String.class, AgentSession.class, String.class, String.class, String.class,
            AgentConnectionContext.class, int[].class, int[].class, Consumer.class, Consumer.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "execute_sql",
            "{\"sql\":\"INSERT INTO t VALUES (1)\",\"reason\":\"test\"}",
            new AgentSession(10), "test_db", "test_user", "%", connectionContext,
            new int[1], new int[1], null, null);

        Assert.assertEquals("Error: current session is read-only; execute_sql is not allowed.", result);
    }
}
