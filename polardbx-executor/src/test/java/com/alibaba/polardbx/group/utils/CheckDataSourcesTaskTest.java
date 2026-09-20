package com.alibaba.polardbx.group.utils;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingRuleClassifier;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingRuleManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CheckDataSourcesTask
 */
public class CheckDataSourcesTaskTest {

    @Test
    public void testSetInMemoryFollowReadAndWaitWithMasterModeAndNoFollowers() throws InterruptedException {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = mockStatic(ConfigDataMode.class);
            MockedStatic<StorageHaManager> mockedStorageHaManager = mockStatic(StorageHaManager.class);
            MockedStatic<DynamicConfig> mockedDynamicConfig = mockStatic(DynamicConfig.class)) {

            // Setup mocks
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);

            StorageHaManager mockStorageHaManager = mock(StorageHaManager.class);
            mockedStorageHaManager.when(StorageHaManager::getInstance).thenReturn(mockStorageHaManager);

            Map<String, StorageInstHaContext> emptyStorageMap = new HashMap<>();
            when(mockStorageHaManager.getStorageHaCtxCache()).thenReturn(emptyStorageMap);

            DynamicConfig mockDynamicConfig = mock(DynamicConfig.class);
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            when(mockDynamicConfig.enableFollowReadForPolarDBX()).thenReturn(false);

            // This should throw RuntimeException due to no available followers in master mode
            try {
                CheckDataSourcesTask.setInMemoryFollowReadAndWait(5000L, true);
                assert false : "Should have thrown RuntimeException";
            } catch (RuntimeException e) {
                assert e.getMessage().contains("No available followers!");
            }
        }
    }

    @Test
    public void testSetInMemoryFollowReadAndWaitWithNonMasterMode() throws InterruptedException {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = mockStatic(ConfigDataMode.class);
            MockedStatic<StorageHaManager> mockedStorageHaManager = mockStatic(StorageHaManager.class);
            MockedStatic<DynamicConfig> mockedDynamicConfig = mockStatic(DynamicConfig.class)) {

            // Setup mocks
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(false);

            StorageHaManager mockStorageHaManager = mock(StorageHaManager.class);
            mockedStorageHaManager.when(StorageHaManager::getInstance).thenReturn(mockStorageHaManager);

            Map<String, StorageInstHaContext> emptyStorageMap = new HashMap<>();
            when(mockStorageHaManager.getStorageHaCtxCache()).thenReturn(emptyStorageMap);

            DynamicConfig mockDynamicConfig = mock(DynamicConfig.class);
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            when(mockDynamicConfig.enableFollowReadForPolarDBX()).thenReturn(false);

            // This should not throw exception in non-master mode
            CheckDataSourcesTask.setInMemoryFollowReadAndWait(5000L, true);
        }
    }

    @Test
    public void testSetInMemoryFollowReadAndWaitDisableFollowerRead() throws InterruptedException {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = mockStatic(ConfigDataMode.class);
            MockedStatic<DynamicConfig> mockedDynamicConfig = mockStatic(DynamicConfig.class)) {

            // Setup mocks
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);

            DynamicConfig mockDynamicConfig = mock(DynamicConfig.class);
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            when(mockDynamicConfig.enableFollowReadForPolarDBX()).thenReturn(false);

            // This should succeed when disabling follower read (newFollowerRead = false)
            CheckDataSourcesTask.setInMemoryFollowReadAndWait(5000L, false);
        }
    }

    @Test
    public void testRunMethod() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = mockStatic(DynamicConfig.class);
            MockedStatic<StorageHaManager> mockedStorageHaManager = mockStatic(StorageHaManager.class);
            MockedStatic<RoutingRuleManager> mockedRoutingRuleManager = mockStatic(RoutingRuleManager.class)) {
            DynamicConfig mockDynamicConfig = mock(DynamicConfig.class);
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            when(mockDynamicConfig.enableFollowReadForPolarDBX()).thenReturn(true);

            StorageHaManager mockStorageHaManager = mock(StorageHaManager.class);
            mockedStorageHaManager.when(StorageHaManager::getInstance).thenReturn(mockStorageHaManager);

            RoutingRuleManager mockRoutingRuleManager = mock(RoutingRuleManager.class);
            mockedRoutingRuleManager.when(RoutingRuleManager::getInstance).thenReturn(mockRoutingRuleManager);
            RoutingRuleClassifier mockRoutingRuleClassifier = mock(RoutingRuleClassifier.class);
            when(mockRoutingRuleManager.getClassifier()).thenReturn(mock(RoutingRuleClassifier.class));
            when(mockRoutingRuleClassifier.isHasFollowerRead()).thenReturn(false);

            Map<String, StorageInstHaContext> emptyStorageMap = new HashMap<>();
            when(mockStorageHaManager.getStorageHaCtxCache()).thenReturn(emptyStorageMap);

            CheckDataSourcesTask task = new CheckDataSourcesTask();
            task.run(); // Should not throw exception
        }
    }

    @Test
    public void testRunMethodExp() {
        try (MockedStatic<StorageHaManager> mockedStorageHaManager = mockStatic(StorageHaManager.class);
            MockedStatic<CheckDataSourcesTask> mockedCheckTask = mockStatic(CheckDataSourcesTask.class);
            MockedStatic<OptimizerUtils> mockedOptimizerUtils = mockStatic(OptimizerUtils.class);
            MockedStatic<ConfigDataMode> mockedConfigDataMode = mockStatic(ConfigDataMode.class)) {
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(false);
            mockedOptimizerUtils.when(OptimizerUtils::enableFollowRead).thenReturn(true);
            StorageHaManager mockStorageHaManager = mock(StorageHaManager.class);
            mockedStorageHaManager.when(StorageHaManager::getInstance).thenReturn(mockStorageHaManager);
            mockedCheckTask.when(CheckDataSourcesTask::existAvailableFollower).thenReturn(true);

            CheckDataSourcesTask task = mock(CheckDataSourcesTask.class);
            doCallRealMethod().when(task).run();
            task.run();

            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);
            mockedCheckTask.when(() -> CheckDataSourcesTask.isMatchFollowerReadSetting(true)).thenReturn(true);
            task.run();
            verify(mockStorageHaManager, times(1)).setNeedRefreshFollowSources(false);
            verify(mockStorageHaManager, times(0)).setNeedRefreshFollowSources(true);

            mockedCheckTask.when(() -> CheckDataSourcesTask.isMatchFollowerReadSetting(true)).thenReturn(false);
            task.run();
            verify(mockStorageHaManager, times(1)).setNeedRefreshFollowSources(true);
        }
    }
}