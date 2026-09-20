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

package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.orc.FileStatusManager;
import com.alibaba.polardbx.common.orc.PreheatMetaManager;
import com.alibaba.polardbx.common.oss.filesystem.FetchPolicy;
import com.alibaba.polardbx.common.oss.filesystem.FileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

import static com.alibaba.polardbx.common.oss.filesystem.Constants.ACCESS_KEY_ID;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.ACCESS_KEY_SECRET;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.ENDPOINT_KEY;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.GENERAL_CACHE_WORKING_DIR;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.OSS_FETCH_POLICY;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.PRIVATE_CLOUD_KEY;

public class OSSInstanceInitializer {
    /**
     * oss://[accessKeyId:accessKeySecret@]bucket[.endpoint]/object/path
     */
    public static final String URI_FORMAT = "oss://%s/%s";

    public String accessKeyIdValue;
    public String accessKeySecretValue;
    public String bucketUri;
    public String endpointValue;
    public CachePolicy cachePolicy;
    public boolean privateCloud;

    public OSSInstanceInitializer() {
    }

    public static OSSInstanceInitializer newBuilder() {
        return new OSSInstanceInitializer();
    }

    public OSSInstanceInitializer accessKeyIdValue(String accessKeyIdValue) {
        this.accessKeyIdValue = accessKeyIdValue;
        return this;
    }

    public OSSInstanceInitializer accessKeySecretValue(String accessKeySecretValue) {
        this.accessKeySecretValue = accessKeySecretValue;
        return this;
    }

    public OSSInstanceInitializer bucketName(String bucketUri) {
        this.bucketUri = bucketUri;
        return this;
    }

    public OSSInstanceInitializer endpointValue(String endpointValue) {
        this.endpointValue = endpointValue;
        return this;
    }

    public OSSInstanceInitializer cachePolicy(CachePolicy cachePolicy) {
        this.cachePolicy = cachePolicy;
        return this;
    }

    public OSSInstanceInitializer privateCloud(boolean privateCloud) {
        this.privateCloud = privateCloud;
        return this;
    }

    public FileSystem initialize() {
        OSSFileSystem ossFileSystem = null;
        try {
            URI ossFileUri = URI.create(this.bucketUri);
            ossFileSystem = createOSSFileSystem(ossFileUri);
            return new DynamicCacheFileSystem(ossFileSystem);
        } catch (Throwable t) {
            if (ossFileSystem != null) {
                try {
                    ossFileSystem.close();
                } catch (Throwable t1) {
                    // ignore
                }
            }
            throw GeneralUtil.nestedException("Fail to create OSS file system!", t);
        }
    }

    private synchronized OSSFileSystem createOSSFileSystem(URI ossFileUri) throws
        IOException {
        FileSystemRateLimiter rateLimiter = FileSystemUtils.newRateLimiter();
        FileStatusManager fileStatusManager = PreheatMetaManager.getInstance();
        // oss file system
        // oss://[accessKeyId:accessKeySecret@]bucket[.endpoint]/object/path
        OSSFileSystem OSS_FILE_SYSTEM = new OSSFileSystem(fileStatusManager, rateLimiter);
        Configuration fsConf = new Configuration();
        fsConf.set(ACCESS_KEY_ID, this.accessKeyIdValue);
        fsConf.set(ACCESS_KEY_SECRET, this.accessKeySecretValue);
        fsConf.set(ENDPOINT_KEY, this.endpointValue);
        fsConf.set(OSS_FETCH_POLICY, FetchPolicy.REQUESTED.name());
        fsConf.setBoolean(PRIVATE_CLOUD_KEY, this.privateCloud);
        fsConf.set(GENERAL_CACHE_WORKING_DIR, FileSystemUtils.getColumnarDirectory());
        OSS_FILE_SYSTEM.initialize(ossFileUri, fsConf);
        return OSS_FILE_SYSTEM;

    }

}
