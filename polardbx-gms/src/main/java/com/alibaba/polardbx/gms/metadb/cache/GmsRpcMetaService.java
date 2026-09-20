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

package com.alibaba.polardbx.gms.metadb.cache;

import com.alibaba.polardbx.cache.external.impl.rpc.meta.PeerInfo;
import com.alibaba.polardbx.cache.external.impl.rpc.meta.PeerRole;
import com.alibaba.polardbx.cache.external.impl.rpc.meta.RpcMetaService;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import lombok.Getter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * GMS-based implementation of RpcMetaService.
 * Uses MetaDB cache_user and cache_peer tables for authentication and peer discovery.
 */
public class GmsRpcMetaService implements RpcMetaService {
    private static final Logger LOGGER = LoggerFactory.getLogger(GmsRpcMetaService.class);

    private final Connection connection;
    @Getter
    private final CacheUserAccessor userAccessor;
    @Getter
    private final CachePeerAccessor peerAccessor;

    public GmsRpcMetaService(Connection connection) {
        this.connection = connection;
        this.userAccessor = new CacheUserAccessor();
        this.userAccessor.setConnection(connection);
        this.peerAccessor = new CachePeerAccessor();
        this.peerAccessor.setConnection(connection);
    }

    // --- MetaServiceBase transaction methods ---

    @Override
    public void beginRepeatableRead() throws SQLException {
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        connection.setAutoCommit(false);
    }

    @Override
    public void beginReadCommitted() throws SQLException {
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        connection.setAutoCommit(false);
    }

    @Override
    public void beginReadUncommitted() throws SQLException {
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        connection.setAutoCommit(false);
    }

    @Override
    public void commit() throws SQLException, IOException {
        connection.commit();
    }

    @Override
    public void rollback() throws SQLException, IOException {
        connection.rollback();
    }

    // --- RpcMetaService methods ---

    @Override
    @Nullable
    public byte[] getPassword(byte[] user) {
        final String userName = new String(user, StandardCharsets.UTF_8);
        final CacheUserRecord record = userAccessor.getByUserName(userName);
        if (record == null) {
            return null;
        }
        // Decrypt the password stored in DB (encrypted by PasswdUtil) before returning.
        final String decrypted = PasswdUtil.decrypt(record.password);
        return decrypted.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getUser() {
        // Return the first user that has admin privilege, or the first user if none has admin.
        final List<CacheUserRecord> users = userAccessor.getAllUsers();
        if (users.isEmpty()) {
            return new byte[0];
        }
        for (CacheUserRecord u : users) {
            if (u.adminPriv == 1) {
                return u.userName.getBytes(StandardCharsets.UTF_8);
            }
        }
        return users.get(0).userName.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Collection<PeerInfo> getPeers() {
        final long nowUTC = System.currentTimeMillis();
        final List<CachePeerRecord> records = peerAccessor.getActivePeers(nowUTC);
        final List<PeerInfo> peers = new ArrayList<>(records.size());
        for (CachePeerRecord record : records) {
            final InetSocketAddress address = parseAddress(record.host);
            if (address != null) {
                // Convert role string to PeerRole enum
                PeerRole role = parsePeerRole(record.role);
                // Use leader field from database
                peers.add(new PeerInfo(record.peerName, address, record.nodeHash, role, record.leader));
            }
        }
        return peers;
    }

    /**
     * Parse role string to PeerRole enum.
     * Defaults to OTHERS if role is null or unrecognized.
     */
    private static PeerRole parsePeerRole(String roleStr) {
        if (roleStr == null || roleStr.isEmpty()) {
            return PeerRole.OTHERS;
        }
        try {
            return PeerRole.valueOf(roleStr);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Unknown peer role: " + roleStr + ", defaulting to OTHERS");
            return PeerRole.OTHERS;
        }
    }

    // --- AutoCloseable ---

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    /**
     * Parse "host:port" string to InetSocketAddress.
     */
    @Nullable
    private static InetSocketAddress parseAddress(String hostPort) {
        if (hostPort == null || hostPort.isEmpty()) {
            return null;
        }
        final int lastColon = hostPort.lastIndexOf(':');
        if (lastColon < 0 || lastColon == hostPort.length() - 1) {
            return null;
        }
        try {
            final String host = hostPort.substring(0, lastColon);
            final int port = Integer.parseInt(hostPort.substring(lastColon + 1));
            return new InetSocketAddress(host, port);
        } catch (NumberFormatException e) {
            LOGGER.warn("Failed to parse peer address: " + hostPort, e);
            return null;
        }
    }
}
