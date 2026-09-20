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

package com.alibaba.polardbx.common.oss.filesystem;

import com.alibaba.polardbx.cache.external.impl.OssRemoteStorageService;
import com.aliyun.oss.OSS;
import com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.UnaryOperator;

/**
 * RemoteStorageService with dynamic path mapping and rate limiting.
 * <p>
 * Delegates path resolution to parent via pathMapper, only adds rate limiting on top.
 * <p>
 * Design background:
 * - Cache internally stores pure file names (e.g. "abc.orc"), consistent with files system table
 * - Reading from OSS requires prepending a directory prefix (e.g. "instId/abc.orc")
 * - The prefix can change dynamically (columnarDirectory controlled by DynamicConfig)
 * - pathMapper handles the mapping from pure file name to full OSS key
 * <p>
 * This class is only invoked on cache miss (memory/SSD both missed).
 */
public class PrefixRoutingRemoteStorageService extends OssRemoteStorageService {

    private volatile RateLimiter rateLimiter;

    /**
     * @param oss OSS client instance
     * @param bucketName OSS bucket name
     * @param encAlg server-side encryption algorithm
     * @param rateLimit initial rate limit (bytes/sec)
     * @param pathMapper maps pure cache file names to full OSS keys
     */
    public PrefixRoutingRemoteStorageService(
        OSS oss, String bucketName, String encAlg,
        long rateLimit, UnaryOperator<String> pathMapper) {
        super(oss, bucketName, pathMapper, encAlg);
        this.rateLimiter = RateLimiter.create(rateLimit);
    }

    /**
     * Read data from OSS with rate limiting.
     * Path mapping is handled by parent via pathMapper.
     */
    @Override
    public InputStream read(String cacheFileName, long offset, long length) throws IOException {
        acquireRateLimit(length);
        return super.read(cacheFileName, offset, length);
    }

    /**
     * Rate limiting: acquire tokens by byte count.
     * Handles length > Integer.MAX_VALUE by acquiring in batches.
     */
    private void acquireRateLimit(long length) {
        long remaining = length;
        while (remaining > 0) {
            int permit = (int) Math.min(remaining, Integer.MAX_VALUE);
            rateLimiter.acquire(permit);
            remaining -= permit;
        }
    }

    /**
     * Dynamically update rate limit. Old RateLimiter is replaced atomically.
     *
     * @param newRateLimit new rate limit in bytes/sec
     */
    public void updateRateLimit(long newRateLimit) {
        this.rateLimiter = RateLimiter.create(newRateLimit);
    }
}
