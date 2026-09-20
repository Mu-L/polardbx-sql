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
import com.alibaba.polardbx.gms.privilege.ActiveRoles;
import com.alibaba.polardbx.server.ServerConnection;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Immutable snapshot of the outer session state that is safe to propagate to
 * connections created by the NL2SQL agent.
 */
final class AgentConnectionContext {

    private final Map<String, Object> sessionVariables;
    private final ActiveRoles activeRoles;
    private final int transactionIsolation;
    private final boolean readOnly;

    private AgentConnectionContext(Map<String, Object> sessionVariables, ActiveRoles activeRoles,
                                   int transactionIsolation, boolean readOnly) {
        this.sessionVariables = Collections.unmodifiableMap(new HashMap<>(sessionVariables));
        this.activeRoles = copyActiveRoles(activeRoles);
        this.transactionIsolation = transactionIsolation;
        this.readOnly = readOnly;
    }

    static AgentConnectionContext capture(ServerConnection connection) {
        Map<String, Object> sessionVariables = new HashMap<>();
        Map<String, Object> extraServerVariables = connection.getExtraServerVariables();
        copyIfPresent(extraServerVariables, sessionVariables, ICdcManager.POLARDBX_SERVER_ID);
        copyIfPresent(extraServerVariables, sessionVariables, "time_zone");

        if (connection.getSqlMode() != null) {
            sessionVariables.put("sql_mode", connection.getSqlMode());
        }
        if (connection.getConnectionCharset() != null) {
            sessionVariables.put("names", connection.getConnectionCharset());
        }

        return new AgentConnectionContext(sessionVariables, connection.getActiveRoles(),
            connection.getTxIsolation(), connection.isReadOnly() || connection.isTxReadOnly());
    }

    private static void copyIfPresent(Map<String, Object> source, Map<String, Object> target, String key) {
        Object value = source.get(key);
        if (value != null) {
            target.put(key, value);
        }
    }

    private static ActiveRoles copyActiveRoles(ActiveRoles activeRoles) {
        if (activeRoles == null) {
            return ActiveRoles.defaultValue();
        }
        return new ActiveRoles(activeRoles.getActiveRoleSpec(), new HashSet<>(activeRoles.getRoles()));
    }

    Map<String, Object> getSessionVariables() {
        return sessionVariables;
    }

    ActiveRoles getActiveRoles() {
        return activeRoles;
    }

    int getTransactionIsolation() {
        return transactionIsolation;
    }

    boolean isReadOnly() {
        return readOnly;
    }
}
