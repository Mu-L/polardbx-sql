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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link OssGeneralCacheOverrideFileSystem}: per-statement HINT wrapper
 * must route all open() calls through the 3-arg {@link DynamicCacheFileSystem#open}
 * carrying the override decided by the caller.
 */
public class OssGeneralCacheOverrideFileSystemTest {

    private DynamicCacheFileSystem delegate;
    private FSDataInputStream mockStream;

    @Before
    public void setUp() throws IOException {
        delegate = mock(DynamicCacheFileSystem.class);
        mockStream = mock(FSDataInputStream.class);
        Configuration conf = new Configuration();
        when(delegate.getConf()).thenReturn(conf);
    }

    @Test
    public void testIsGeneralCacheEnabledTrue() {
        OssGeneralCacheOverrideFileSystem fs = new OssGeneralCacheOverrideFileSystem(delegate, true);
        Assert.assertTrue(fs.isGeneralCacheEnabled());
    }

    @Test
    public void testIsGeneralCacheEnabledFalse() {
        OssGeneralCacheOverrideFileSystem fs = new OssGeneralCacheOverrideFileSystem(delegate, false);
        Assert.assertFalse(fs.isGeneralCacheEnabled());
    }

    @Test
    public void testOpenWithBufferSizeForwardsTrueOverride() throws IOException {
        when(delegate.open(any(Path.class), anyInt(), any(Boolean.class))).thenReturn(mockStream);
        OssGeneralCacheOverrideFileSystem fs = new OssGeneralCacheOverrideFileSystem(delegate, true);

        Path p = new Path("/a.orc");
        FSDataInputStream result = fs.open(p, 8192);

        Assert.assertSame(mockStream, result);
        // Ensure the override is forwarded exactly as configured.
        verify(delegate).open(eq(p), eq(8192), eq(Boolean.TRUE));
    }

    @Test
    public void testOpenWithBufferSizeForwardsFalseOverride() throws IOException {
        when(delegate.open(any(Path.class), anyInt(), any(Boolean.class))).thenReturn(mockStream);
        OssGeneralCacheOverrideFileSystem fs = new OssGeneralCacheOverrideFileSystem(delegate, false);

        Path p = new Path("/b.orc");
        fs.open(p, 4096);

        verify(delegate).open(eq(p), eq(4096), eq(Boolean.FALSE));
    }

    @Test
    public void testSingleArgOpenUsesConfiguredBufferSize() throws IOException {
        Configuration conf = new Configuration();
        conf.setInt("io.file.buffer.size", 16384);
        when(delegate.getConf()).thenReturn(conf);
        when(delegate.open(any(Path.class), anyInt(), any(Boolean.class))).thenReturn(mockStream);

        OssGeneralCacheOverrideFileSystem fs = new OssGeneralCacheOverrideFileSystem(delegate, true);
        fs.setConf(conf);

        Path p = new Path("/c.orc");
        fs.open(p);

        // Single-arg open() should read "io.file.buffer.size" from its own conf.
        verify(delegate).open(eq(p), eq(16384), eq(Boolean.TRUE));
    }

    @Test
    public void testSingleArgOpenUsesDefaultBufferSizeWhenMissing() throws IOException {
        Configuration conf = new Configuration();
        // Do NOT set io.file.buffer.size -> default 4096 should be used.
        when(delegate.getConf()).thenReturn(conf);
        when(delegate.open(any(Path.class), anyInt(), any(Boolean.class))).thenReturn(mockStream);

        OssGeneralCacheOverrideFileSystem fs = new OssGeneralCacheOverrideFileSystem(delegate, false);
        fs.setConf(conf);

        Path p = new Path("/d.orc");
        fs.open(p);

        verify(delegate).open(eq(p), eq(4096), eq(Boolean.FALSE));
    }
}
