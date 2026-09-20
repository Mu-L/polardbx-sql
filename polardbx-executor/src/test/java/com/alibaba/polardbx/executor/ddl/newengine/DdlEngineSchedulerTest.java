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

package com.alibaba.polardbx.executor.ddl.newengine;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.google.common.collect.Sets;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DdlEngineSchedulerTest {

    private DdlJobManager ddlJobManager;

    @Before
    public void setUp() {
        ddlJobManager = Mockito.mock(DdlJobManager.class);
    }

    @SneakyThrows
    @Test
    public void testCompareAndExecute_Timeout() {
        try {
            DdlEngineScheduler ddlEngineScheduler = DdlEngineScheduler.getInstance();

            DdlEngineScheduler.DdlJobScheduler mockScheduler = mock(DdlEngineScheduler.DdlJobScheduler.class);
            when(mockScheduler.isIdle()).thenReturn(false);

            // 使用反射设置私有字段
            java.lang.reflect.Field ddlJobSchedulerField = DdlEngineScheduler.class.getDeclaredField("ddlJobScheduler");
            ddlJobSchedulerField.setAccessible(true);
            ddlJobSchedulerField.set(ddlEngineScheduler, mockScheduler);

            ddlEngineScheduler.compareAndExecute(0, 0, () -> null);
            Assert.fail("should trigger timeout exception, but not.");
        } catch (TimeoutException ignored) {
        }
    }

    @SneakyThrows
    @Test
    public void testCompareAndExecute_With_PausedDdl() throws TimeoutException {
        // 准备测试数据
        DdlEngineRecord record = new DdlEngineRecord();
        record.jobId = 1L;
        record.state = DdlState.TERMINATED.toString();
        List<DdlEngineRecord> records = Collections.singletonList(record);

        try (MockedConstruction<DdlJobManager> mockedConstruction =
            Mockito.mockConstruction(DdlJobManager.class,
                (mock, context) -> {
                    when(mock.fetchRecords(DdlState.ALL_STATES)).thenReturn(records);
                    when(mock.getMaxId()).thenReturn(0L);
                })) {

            // 获取实例并调用测试方法
            DdlEngineScheduler ddlEngineScheduler = DdlEngineScheduler.getInstance();

            DdlEngineScheduler.DdlJobScheduler mockScheduler = mock(DdlEngineScheduler.DdlJobScheduler.class);
            when(mockScheduler.isIdle()).thenReturn(true);

            // 使用反射设置私有字段
            java.lang.reflect.Field ddlJobSchedulerField = DdlEngineScheduler.class.getDeclaredField("ddlJobScheduler");
            ddlJobSchedulerField.setAccessible(true);
            ddlJobSchedulerField.set(ddlEngineScheduler, mockScheduler);

            boolean result = ddlEngineScheduler.compareAndExecute(0, 0, mock(Supplier.class));
            // 验证结果
            assertFalse(result);
            assertEquals(0L, ddlEngineScheduler.getPerformVersion());
        }
    }

    @Test
    public void testExistsPausedDdl_WithPausedJobs() {
        // 准备测试数据
        DdlEngineRecord record = new DdlEngineRecord();
        record.jobId = 1L;
        record.state = DdlState.ALL_STATES.toString();
        List<DdlEngineRecord> records = Collections.singletonList(record);

        // 模拟DdlJobManager的行为
        when(ddlJobManager.fetchRecords(DdlState.ALL_STATES)).thenReturn(records);

        // 获取实例并调用测试方法
        DdlEngineScheduler ddlEngineScheduler = DdlEngineScheduler.getInstance();
        boolean result = ddlEngineScheduler.existsNoneCompleteDdl(ddlJobManager);

        // 验证结果
        assertTrue(result);
        verify(ddlJobManager).fetchRecords(DdlState.ALL_STATES);
    }

    @Test
    public void testExistsPausedDdl_WithoutPausedJobs() {
        // 模拟没有暂停的作业
        when(ddlJobManager.fetchRecords(DdlState.ALL_STATES)).thenReturn(Collections.emptyList());

        // 获取实例并调用测试方法
        DdlEngineScheduler ddlEngineScheduler = DdlEngineScheduler.getInstance();
        boolean result = ddlEngineScheduler.existsNoneCompleteDdl(ddlJobManager);

        // 验证结果
        assertFalse(result);
        verify(ddlJobManager).fetchRecords(DdlState.ALL_STATES);
    }

    @Test
    public void testCompareAndExecute_VersionMismatch() throws TimeoutException {
        // 准备测试数据
        long expectVersion = 3L;
        Supplier<?> supplier = Mockito.mock(Supplier.class);

        // 获取实例
        DdlEngineScheduler ddlEngineScheduler = DdlEngineScheduler.getInstance();
        long currentVersion = ddlEngineScheduler.getPerformVersion();

        // 确保版本不匹配
        while (currentVersion == expectVersion) {
            expectVersion++;
        }

        // 调用测试方法
        boolean result = ddlEngineScheduler.compareAndExecute(expectVersion, 0, supplier);

        // 验证结果
        assertFalse(result);
        // 验证supplier没有被调用
        verify(supplier, never()).get();
    }

    @SneakyThrows
    @Test
    public void testProcessInitialTimeoutFollowsDynamicConfig() {
        try (MockedConstruction<DdlJobManager> mockedConstruction =
            Mockito.mockConstruction(DdlJobManager.class,
                (mock, context) -> when(mock.fetchRecords(anySet(), anyInt()))
                    .thenReturn(Collections.emptyList()))) {

            DdlEngineScheduler ddlEngineScheduler = DdlEngineScheduler.getInstance();

            // 反射构造私有内部类 DdlJobDispatcher 并获取 processInitial 方法
            Class<?> dispatcherClass =
                Class.forName("com.alibaba.polardbx.executor.ddl.newengine.DdlEngineScheduler$DdlJobDispatcher");
            java.lang.reflect.Constructor<?> constructor =
                dispatcherClass.getDeclaredConstructor(DdlEngineScheduler.class);
            constructor.setAccessible(true);
            Object dispatcher = constructor.newInstance(ddlEngineScheduler);

            java.lang.reflect.Method processInitial = dispatcherClass.getDeclaredMethod("processInitial");
            processInitial.setAccessible(true);

            DdlJobManager mockDdlJobManager = mockedConstruction.constructed().get(0);

            try {
                // 超时时间应为 acquireResource 超时分钟数 + 15min 缓冲
                DynamicConfig.getInstance()
                    .loadValue(null, ConnectionProperties.DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES, "30");
                processInitial.invoke(dispatcher);
                verify(mockDdlJobManager).fetchRecords(Sets.newHashSet(DdlState.INITIAL), 45);

                // 默认 60min 时保持原有 75min 行为
                DynamicConfig.getInstance()
                    .loadValue(null, ConnectionProperties.DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES, "60");
                processInitial.invoke(dispatcher);
                verify(mockDdlJobManager).fetchRecords(Sets.newHashSet(DdlState.INITIAL), 75);
            } finally {
                // 恢复默认值
                DynamicConfig.getInstance()
                    .loadValue(null, ConnectionProperties.DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES, "60");
            }
        }
    }
}