/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package com.alibaba.polardbx.gms.engine;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Per-statement wrapper around {@link DynamicCacheFileSystem} that carries a
 * GeneralCache on/off override decided from HINT/session at the caller side.
 * All open() calls are routed to
 * {@link DynamicCacheFileSystem#open(Path, int, Boolean)} so the 3-arg overload
 * becomes the single entry that actually branches on the effective switch value.
 * <p>
 * Non-open methods (getFileStatus, listStatus, getUri, ...) are transparently
 * delegated by {@link FilterFileSystem}, so ORC library calls like
 * {@code ReaderImpl.extractFileTail} and {@code RecordReaderUtils.DefaultDataReader.open}
 * see this wrapper seamlessly.
 */
public class OssGeneralCacheOverrideFileSystem extends FilterFileSystem {

    private final DynamicCacheFileSystem delegate;
    private final boolean generalCacheEnabled;

    public OssGeneralCacheOverrideFileSystem(DynamicCacheFileSystem fs, boolean generalCacheEnabled) {
        super(fs);
        this.delegate = fs;
        this.generalCacheEnabled = generalCacheEnabled;
    }

    /**
     * Expose the per-statement override so callers that bypass the usual
     * FileSystem open() path (e.g. FileSystemUtils.readFile, MultiVersionDelPartitionInfo)
     * can still honor the HINT.
     */
    public boolean isGeneralCacheEnabled() {
        return generalCacheEnabled;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        return delegate.open(path, bufferSize, generalCacheEnabled);
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
        return open(path, getConf().getInt("io.file.buffer.size", 4096));
    }
}
