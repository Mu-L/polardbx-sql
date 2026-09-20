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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.oss.filesystem.cache;

import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

import static java.util.Objects.requireNonNull;

public final class FileMergeCachingFileSystem
    extends CachingFileSystem {
    private final CacheManager cacheManager;
    private final boolean cacheValidationEnabled;

    public FileMergeCachingFileSystem(
        URI uri,
        Configuration configuration,
        CacheManager cacheManager,
        FileSystem dataTier,
        boolean cacheValidationEnabled,
        boolean enableCache) {
        super(dataTier, uri);
        requireNonNull(configuration, "configuration is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.cacheValidationEnabled = cacheValidationEnabled;

        setConf(configuration);
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
        return new FileMergeCachingInputStream(
            dataTier.open(path),
            cacheManager,
            path,
            cacheManager.getMaxCacheQuota(),
            cacheValidationEnabled);
    }

    /**
     * Open an FSDataInputStream at the indicated Path with specified buffer size and range.
     * This method is optimized for OSSFileSystem to avoid getting file length from fileStatus.
     *
     * @param path the file to open
     * @param position the position to start reading from
     * @param length the number of bytes to read
     * @return FSDataInputStream
     */
    public FSDataInputStream open(Path path, long position, long length) throws IOException {
        // Check if dataTier is OSSFileSystem and call the optimized open method
        FileSystem innerDataTier = getDataTier();
        Preconditions.checkArgument(innerDataTier instanceof OSSFileSystem, "dataTier is not OSSFileSystem");
        OSSFileSystem ossFileSystem = (OSSFileSystem) innerDataTier;
        // Try to call the optimized open method with position and length
        FSDataInputStream inputStream = new FileMergeCachingInputStream(
            ossFileSystem.uncheckedOpen(path, position + length),
            cacheManager,
            path,
            cacheManager.getMaxCacheQuota(),
            cacheValidationEnabled);
        inputStream.seek(position);

        return inputStream;
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public void close() throws IOException {
        cacheManager.close();
        super.close();
    }
}