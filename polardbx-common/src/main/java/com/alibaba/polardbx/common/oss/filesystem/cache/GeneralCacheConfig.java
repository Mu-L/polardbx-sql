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

package com.alibaba.polardbx.common.oss.filesystem.cache;

import java.util.Properties;

/**
 * Unified configuration class for the GeneralCache system.
 * Consolidates all cache-related parameters previously scattered in SystemConfig/ServerLoader.
 * <p>
 * Built once at startup from server.properties via {@link #fromProperties(Properties)}, then immutable.
 * Runtime behavior (enable/disable cache) is controlled by DynamicConfig, not this class.
 * <p>
 * Lifecycle: ServerLoader calls fromProperties() -> CacheInitializer.initialize(config, ...) consumes -> never modified.
 */
public class GeneralCacheConfig {

    // --- RPC configuration ---
    private final int cacheRpcPort;

    // --- Memory configuration ---
    private final long cacheOffheapMemory;
    private final long dynamicArenaMemory;

    // --- Thread configuration ---
    private final int cacheThreads;
    private final int queryThreads;

    // --- Local cache configuration ---
    private final long cacheLocalDiskSize;

    // --- Lease configuration ---
    private final long cacheLease;

    // --- Node identity ---
    private final String peerTag;
    private final String peerRole;

    // --- OSS rate limit ---
    private final long ossRateLimit;

    // --- Spill root path (optional, null means use FileConfig default) ---
    private final String spillRootPath;

    private GeneralCacheConfig(Builder builder) {
        this.cacheRpcPort = builder.cacheRpcPort;
        this.cacheOffheapMemory = builder.cacheOffheapMemory;
        this.dynamicArenaMemory = builder.dynamicArenaMemory;
        this.cacheThreads = builder.cacheThreads;
        this.queryThreads = builder.queryThreads;
        this.cacheLocalDiskSize = builder.cacheLocalDiskSize;
        this.cacheLease = builder.cacheLease;
        this.peerTag = builder.peerTag;
        this.peerRole = builder.peerRole;
        this.ossRateLimit = builder.ossRateLimit;
        this.spillRootPath = builder.spillRootPath;
    }

    // --- Getters (immutable, read-only) ---

    /**
     * RPC server port. -1 means client-only mode (no RPC server started).
     */
    public int getCacheRpcPort() {
        return cacheRpcPort;
    }

    /**
     * Total off-heap arena memory in bytes (default 1GB).
     */
    public long getCacheOffheapMemory() {
        return cacheOffheapMemory;
    }

    /**
     * Memory reserved for dynamic allocation in bytes (default 512MB).
     */
    public long getDynamicArenaMemory() {
        return dynamicArenaMemory;
    }

    /**
     * Local/remote/push write thread count (default 16).
     */
    public int getCacheThreads() {
        return cacheThreads;
    }

    /**
     * Query thread count (default 64).
     */
    public int getQueryThreads() {
        return queryThreads;
    }

    /**
     * Local SSD cache size in bytes (default 200GB).
     */
    public long getCacheLocalDiskSize() {
        return cacheLocalDiskSize;
    }

    /**
     * Cache peer lease time in milliseconds (default 10000).
     */
    public long getCacheLease() {
        return cacheLease;
    }

    /**
     * Custom tag for peer identification (default "TAG_CN").
     */
    public String getPeerTag() {
        return peerTag;
    }

    /**
     * Custom role for peer identification (default "CACHE_READER").
     */
    public String getPeerRole() {
        return peerRole;
    }

    /**
     * OSS read rate limit in bytes/sec (default 1GB/s).
     */
    public long getOssRateLimit() {
        return ossRateLimit;
    }

    /**
     * External spill root path for cache directory.
     * null means use CN default (FileConfig.getInstance().getRootPath()).
     * Columnar nodes set this to their own spill directory.
     */
    public String getSpillRootPath() {
        return spillRootPath;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a GeneralCacheConfig from server.properties.
     * Handles all cache-related property parsing, defaults, and special logic
     * (e.g., cacheRpcPort fallback to adminPort - 1).
     * <p>
     * This is the primary entry point for configuration loading, replacing
     * the previous scattered logic in ServerLoader.configSystem() + SystemConfig fields.
     *
     * @param props server.properties content
     * @return validated, immutable GeneralCacheConfig
     * @throws IllegalArgumentException if validation fails
     */
    public static GeneralCacheConfig fromProperties(Properties props) {
        Builder b = builder();

        // cacheRpcPort: explicit value, or fallback to adminPort - 1
        String cacheRpcPortStr = props.getProperty("cacheRpcPort");
        if (cacheRpcPortStr != null && !cacheRpcPortStr.isEmpty()) {
            b.cacheRpcPort(Integer.parseInt(cacheRpcPortStr));
        } else {
            String adminPort = props.getProperty("adminPort");
            if (adminPort != null && !adminPort.isEmpty()) {
                b.cacheRpcPort(Integer.parseInt(adminPort) - 1);
            }
        }

        String cacheOffheapMemory = props.getProperty("cacheOffheapMemory");
        if (cacheOffheapMemory != null && !cacheOffheapMemory.isEmpty()) {
            b.cacheOffheapMemory(Long.parseLong(cacheOffheapMemory));
        }
        String dynamicArenaMemory = props.getProperty("dynamicArenaMemory");
        if (dynamicArenaMemory != null && !dynamicArenaMemory.isEmpty()) {
            b.dynamicArenaMemory(Long.parseLong(dynamicArenaMemory));
        }
        String cacheThreads = props.getProperty("cacheThreads");
        if (cacheThreads != null && !cacheThreads.isEmpty()) {
            b.cacheThreads(Integer.parseInt(cacheThreads));
        }
        String queryThreads = props.getProperty("queryThreads");
        if (queryThreads != null && !queryThreads.isEmpty()) {
            b.queryThreads(Integer.parseInt(queryThreads));
        }
        String cacheLocalDiskSize = props.getProperty("cacheLocalDiskSize");
        if (cacheLocalDiskSize != null && !cacheLocalDiskSize.isEmpty()) {
            b.cacheLocalDiskSize(Long.parseLong(cacheLocalDiskSize));
        }
        String cacheLease = props.getProperty("cacheLease");
        if (cacheLease != null && !cacheLease.isEmpty()) {
            b.cacheLease(Long.parseLong(cacheLease));
        }
        String peerTag = props.getProperty("peerTag");
        if (peerTag != null && !peerTag.isEmpty()) {
            b.peerTag(peerTag);
        }
        String peerRole = props.getProperty("peerRole");
        if (peerRole != null && !peerRole.isEmpty()) {
            b.peerRole(peerRole);
        }
        String ossRateLimit = props.getProperty("ossRateLimit");
        if (ossRateLimit != null && !ossRateLimit.isEmpty()) {
            b.ossRateLimit(Long.parseLong(ossRateLimit));
        }
        String spillRootPath = props.getProperty("spillRootPath");
        if (spillRootPath != null && !spillRootPath.isEmpty()) {
            b.spillRootPath(spillRootPath);
        }

        return b.build();
    }

    @Override
    public String toString() {
        return "GeneralCacheConfig{"
            + "cacheRpcPort=" + cacheRpcPort
            + ", cacheOffheapMemory=" + cacheOffheapMemory
            + ", dynamicArenaMemory=" + dynamicArenaMemory
            + ", cacheThreads=" + cacheThreads
            + ", queryThreads=" + queryThreads
            + ", cacheLocalDiskSize=" + cacheLocalDiskSize
            + ", cacheLease=" + cacheLease
            + ", peerTag='" + peerTag + '\''
            + ", peerRole='" + peerRole + '\''
            + ", ossRateLimit=" + ossRateLimit
            + ", spillRootPath='" + spillRootPath + '\''
            + '}';
    }

    /**
     * Builder for GeneralCacheConfig. Validates parameters on build().
     */
    public static class Builder {
        private int cacheRpcPort = -1;
        private long cacheOffheapMemory = (long) 1024 * 1024 * 1024;
        private long dynamicArenaMemory = 512L * 1024 * 1024;
        private int cacheThreads = 16;
        private int queryThreads = 64;
        private long cacheLocalDiskSize = 214748364800L; // 200GB
        private long cacheLease = 10000L;
        private String peerTag = "TAG_CN";
        private String peerRole = "CACHE_READER";
        private long ossRateLimit = 1024L * 1024 * 1024; // 1GB/s
        private String spillRootPath = null; // null = use FileConfig default

        public Builder cacheRpcPort(int cacheRpcPort) {
            this.cacheRpcPort = cacheRpcPort;
            return this;
        }

        public Builder cacheOffheapMemory(long cacheOffheapMemory) {
            this.cacheOffheapMemory = cacheOffheapMemory;
            return this;
        }

        public Builder dynamicArenaMemory(long dynamicArenaMemory) {
            this.dynamicArenaMemory = dynamicArenaMemory;
            return this;
        }

        public Builder cacheThreads(int cacheThreads) {
            this.cacheThreads = cacheThreads;
            return this;
        }

        public Builder queryThreads(int queryThreads) {
            this.queryThreads = queryThreads;
            return this;
        }

        public Builder cacheLocalDiskSize(long cacheLocalDiskSize) {
            this.cacheLocalDiskSize = cacheLocalDiskSize;
            return this;
        }

        public Builder cacheLease(long cacheLease) {
            this.cacheLease = cacheLease;
            return this;
        }

        public Builder peerTag(String peerTag) {
            this.peerTag = peerTag;
            return this;
        }

        public Builder peerRole(String peerRole) {
            this.peerRole = peerRole;
            return this;
        }

        public Builder ossRateLimit(long ossRateLimit) {
            this.ossRateLimit = ossRateLimit;
            return this;
        }

        public Builder spillRootPath(String spillRootPath) {
            this.spillRootPath = spillRootPath;
            return this;
        }

        /**
         * Build and validate the configuration.
         *
         * @throws IllegalArgumentException if any parameter is invalid
         */
        public GeneralCacheConfig build() {
            // Validate parameters
            if (cacheOffheapMemory <= 0) {
                throw new IllegalArgumentException("cacheOffheapMemory must be > 0, got: " + cacheOffheapMemory);
            }
            if (cacheThreads <= 0) {
                throw new IllegalArgumentException("cacheThreads must be > 0, got: " + cacheThreads);
            }
            if (queryThreads <= 0) {
                throw new IllegalArgumentException("queryThreads must be > 0, got: " + queryThreads);
            }
            if (cacheLocalDiskSize <= 0) {
                throw new IllegalArgumentException("cacheLocalDiskSize must be > 0, got: " + cacheLocalDiskSize);
            }
            if (cacheLease <= 0) {
                throw new IllegalArgumentException("cacheLease must be > 0, got: " + cacheLease);
            }
            if (ossRateLimit <= 0) {
                throw new IllegalArgumentException("ossRateLimit must be > 0, got: " + ossRateLimit);
            }
            if (peerTag == null || peerTag.isEmpty()) {
                throw new IllegalArgumentException("peerTag must not be null or empty");
            }
            if (peerRole == null || peerRole.isEmpty()) {
                throw new IllegalArgumentException("peerRole must not be null or empty");
            }
            if (cacheRpcPort != -1 && cacheRpcPort <= 0) {
                throw new IllegalArgumentException(
                    "cacheRpcPort must be > 0 (server mode) or -1 (client-only mode), got: " + cacheRpcPort);
            }
            return new GeneralCacheConfig(this);
        }
    }
}
