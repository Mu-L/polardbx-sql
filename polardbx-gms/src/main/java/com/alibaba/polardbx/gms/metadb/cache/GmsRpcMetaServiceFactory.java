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

import com.alibaba.polardbx.cache.GeneralCache;
import com.alibaba.polardbx.cache.external.impl.rpc.meta.PeerInfo;
import com.alibaba.polardbx.cache.external.impl.rpc.meta.PeerRole;
import com.alibaba.polardbx.cache.external.impl.rpc.meta.RpcMetaService;
import com.alibaba.polardbx.cache.external.impl.rpc.meta.RpcMetaServiceFactory;
import com.alibaba.polardbx.cache.statistics.CacheStatisticsCollector;
import com.alibaba.polardbx.cache.utils.XxHash64;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;

/**
 * GMS-based implementation of RpcMetaServiceFactory.
 * Creates GmsRpcMetaService instances backed by MetaDB connections.
 */
public class GmsRpcMetaServiceFactory implements RpcMetaServiceFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(GmsRpcMetaServiceFactory.class);

    private final CacheStatisticsCollector collector;
    private volatile GeneralCache generalCache;
    private volatile PeerInfo myself;

    public GmsRpcMetaServiceFactory(CacheStatisticsCollector collector) {
        this.collector = collector;
    }

    public GmsRpcMetaServiceFactory(CacheStatisticsCollector collector, GeneralCache generalCache) {
        this.collector = collector;
        this.generalCache = generalCache;
    }

    /**
     * Initialize myself PeerInfo with port and tag
     */
    public void initMyself(int port, String peerTag, PeerRole peerRole) {
        try {
            String ip = InetAddress.getLocalHost().getHostAddress();
            InetSocketAddress address = new InetSocketAddress(ip, port);
            String peerName = peerTag + "@" + ip + ":" + port;

            // Calculate nodeHash using xxhash64
            byte[] bytes = peerName.getBytes(StandardCharsets.UTF_8);
            final XxHash64 hasher = new XxHash64();
            hasher.update(bytes, 0, bytes.length);
            long nodeHash = hasher.hash();

            // Create PeerInfo with role=OTHERS and leader=false
            this.myself = new PeerInfo(peerName, address, nodeHash, peerRole, false);
            LOGGER.info("Initialized myself: peerName=" + peerName + ", address=" + address + ", nodeHash=" + nodeHash);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize myself PeerInfo", e);
        }
    }

    @Override
    public RpcMetaService getRpcMetaService() {
        final Connection connection = MetaDbDataSource.getInstance().getConnection();
        return new GmsRpcMetaService(connection);
    }

    @Override
    public @Nullable GeneralCache getGeneralCache() {
        return generalCache;
    }

    @Override
    public PeerInfo getMyself() {
        if (myself == null) {
            throw new IllegalStateException("Myself PeerInfo not initialized. Call initMyself() first.");
        }
        return myself;
    }

    @Override
    public CacheStatisticsCollector getCacheStatisticsCollector() {
        return collector;
    }

    /**
     * Update the PeerRole of myself PeerInfo.
     * Since PeerInfo fields are final, this creates a new PeerInfo with the updated role.
     */
    public void updateRole(PeerRole newRole) {
        PeerInfo current = this.myself;
        if (current == null) {
            throw new IllegalStateException("Myself PeerInfo not initialized. Call initMyself() first.");
        }
        this.myself = new PeerInfo(current.name, current.address, current.nodeHash, newRole, current.leader);
        LOGGER.info("Updated myself role from " + current.role + " to " + newRole);
    }

    public void setGeneralCache(@Nullable GeneralCache generalCache) {
        this.generalCache = generalCache;
    }
}
