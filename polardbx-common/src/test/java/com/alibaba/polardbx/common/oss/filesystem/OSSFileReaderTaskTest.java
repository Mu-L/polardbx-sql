/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.alibaba.polardbx.common.oss.filesystem;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link OSSFileReaderTask}: verify the allowBypass flag is
 * forwarded into the 4-arg {@link OSSFileSystemStore#retrieve} call.
 */
public class OSSFileReaderTaskTest {

    /**
     * Build a ReadBuffer for byteStart..byteEnd (inclusive).
     * ReadBuffer allocates (end - start + 1) bytes internally.
     */
    private static ReadBuffer newBuffer() {
        return new ReadBuffer(0, 3); // 4 bytes
    }

    @Test
    public void testRunForwardsAllowBypassTrueToStore() {
        OSSFileSystemStore store = mock(OSSFileSystemStore.class);
        ReadBuffer buf = newBuffer();

        when(store.retrieve(anyString(), anyLong(), anyLong(), anyBoolean()))
            .thenReturn(new ByteArrayInputStream(new byte[] {1, 2, 3, 4}));

        OSSFileReaderTask task = new OSSFileReaderTask("k", store, buf, true);
        task.run();

        ArgumentCaptor<Boolean> bypassCap = ArgumentCaptor.forClass(Boolean.class);
        verify(store).retrieve(anyString(), anyLong(), anyLong(), bypassCap.capture());
        Assert.assertTrue("allowBypass=true must be forwarded", bypassCap.getValue());
        Assert.assertEquals(ReadBuffer.STATUS.SUCCESS, buf.getStatus());
    }

    @Test
    public void testRunForwardsAllowBypassFalseToStore() {
        OSSFileSystemStore store = mock(OSSFileSystemStore.class);
        ReadBuffer buf = newBuffer();

        when(store.retrieve(anyString(), anyLong(), anyLong(), anyBoolean()))
            .thenReturn(new ByteArrayInputStream(new byte[] {1, 2, 3, 4}));

        OSSFileReaderTask task = new OSSFileReaderTask("k", store, buf, false);
        task.run();

        ArgumentCaptor<Boolean> bypassCap = ArgumentCaptor.forClass(Boolean.class);
        verify(store).retrieve(anyString(), anyLong(), anyLong(), bypassCap.capture());
        Assert.assertFalse("allowBypass=false must be forwarded", bypassCap.getValue());
    }

    /**
     * Legacy 3-arg constructor must default allowBypass=false, ensuring no code
     * path silently gains bypass privileges after the upgrade.
     */
    @Test
    public void testLegacyConstructorDefaultsToAllowBypassFalse() {
        OSSFileSystemStore store = mock(OSSFileSystemStore.class);
        ReadBuffer buf = newBuffer();

        when(store.retrieve(anyString(), anyLong(), anyLong(), anyBoolean()))
            .thenReturn(new ByteArrayInputStream(new byte[] {1, 2, 3, 4}));

        OSSFileReaderTask task = new OSSFileReaderTask("k", store, buf);
        task.run();

        ArgumentCaptor<Boolean> bypassCap = ArgumentCaptor.forClass(Boolean.class);
        verify(store).retrieve(anyString(), anyLong(), anyLong(), bypassCap.capture());
        Assert.assertFalse("legacy constructor must default to allowBypass=false", bypassCap.getValue());
    }
}
