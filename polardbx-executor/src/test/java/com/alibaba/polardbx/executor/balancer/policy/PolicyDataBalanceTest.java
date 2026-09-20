package com.alibaba.polardbx.executor.balancer.policy;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.GroupStats;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.locality.StoragePoolInfo;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for PolicyDataBalance.
 * <p>
 * Covers:
 * - isStoragePoolTriggered() method body
 * - getStoragePoolDnList() method body
 * - applyToShardingDb() call sites for isStoragePoolTriggered / getStoragePoolDnList
 */
public class PolicyDataBalanceTest {

    // ========== Mock subclasses ==========

    static class MockedPolicyDataBalance extends PolicyDataBalance {

        @Override
        protected boolean isStorageReady(String storageInst) {
            return true;
        }

        @Override
        protected boolean isStoragePoolTriggered() {
            return false;
        }

        @Override
        protected List<String> getStoragePoolDnList() {
            return Collections.emptyList();
        }
    }

    static class StoragePoolTriggeredPolicyDataBalance extends PolicyDataBalance {

        @Override
        protected boolean isStorageReady(String storageInst) {
            return true;
        }

        @Override
        protected boolean isStoragePoolTriggered() {
            return true;
        }

        @Override
        protected List<String> getStoragePoolDnList() {
            return Arrays.asList("dn1", "dn2");
        }
    }

    // ========== Helpers ==========

    private List<GroupDetailInfoExRecord> groupLists(String dn, int startId, int endId) {
        return IntStream.range(startId, endId)
            .mapToObj(id -> new GroupDetailInfoExRecord("g" + id, dn))
            .collect(Collectors.toList());
    }

    private Map<String, Pair<Long, Long>> buildDataSizeMap(String dn, int startId, int endId, long dataSize) {
        return IntStream.range(startId, endId)
            .mapToObj(id -> Pair.of("g" + id, dataSize))
            .collect(Collectors.toMap(e -> e.getKey(), e -> Pair.of(0L, e.getValue())));
    }

    // ========== Direct method coverage tests ==========

    @Test
    public void testIsStoragePoolTriggeredReturnsDefaultWhenNoManager() {
        PolicyDataBalance policy = new PolicyDataBalance();
        // When StoragePoolManager is not initialized, should return false (not throw)
        Assert.assertFalse(policy.isStoragePoolTriggered());
    }

    @Test
    public void testGetStoragePoolDnListReturnsEmptyWhenNoManager() {
        PolicyDataBalance policy = new PolicyDataBalance();
        // When StoragePoolManager is not initialized, should return empty list
        List<String> result = policy.getStoragePoolDnList();
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testIsStoragePoolTriggeredWithMockedManager() {
        try (MockedStatic<StoragePoolManager> mocked = Mockito.mockStatic(StoragePoolManager.class)) {
            StoragePoolManager mockManager = mock(StoragePoolManager.class);
            mocked.when(StoragePoolManager::getInstance).thenReturn(mockManager);
            when(mockManager.isTriggered()).thenReturn(false);

            PolicyDataBalance policy = new PolicyDataBalance();
            Assert.assertFalse(policy.isStoragePoolTriggered());
        }
    }

    @Test
    public void testIsStoragePoolTriggeredReturnsTrueWithMockedManager() {
        try (MockedStatic<StoragePoolManager> mocked = Mockito.mockStatic(StoragePoolManager.class)) {
            StoragePoolManager mockManager = mock(StoragePoolManager.class);
            mocked.when(StoragePoolManager::getInstance).thenReturn(mockManager);
            when(mockManager.isTriggered()).thenReturn(true);

            PolicyDataBalance policy = new PolicyDataBalance();
            Assert.assertTrue(policy.isStoragePoolTriggered());
        }
    }

    @Test
    public void testGetStoragePoolDnListWithMockedManager() {
        try (MockedStatic<StoragePoolManager> mocked = Mockito.mockStatic(StoragePoolManager.class)) {
            StoragePoolManager mockManager = mock(StoragePoolManager.class);
            mocked.when(StoragePoolManager::getInstance).thenReturn(mockManager);

            StoragePoolInfo mockInfo = mock(StoragePoolInfo.class);
            when(mockManager.getStoragePoolInfo(Mockito.anyString())).thenReturn(mockInfo);
            when(mockInfo.getDnLists()).thenReturn(Arrays.asList("dn1", "dn2"));

            PolicyDataBalance policy = new PolicyDataBalance();
            List<String> result = policy.getStoragePoolDnList();
            Assert.assertNotNull(result);
            Assert.assertEquals(2, result.size());
            Assert.assertEquals("dn1", result.get(0));
            Assert.assertEquals("dn2", result.get(1));
        }
    }

    // ========== applyToShardingDb tests covering isStoragePoolTriggered/getStoragePoolDnList call sites ==========

    @Test
    public void testApplyToShardingDbStoragePoolNotTriggered() {
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        MockedPolicyDataBalance policy = new MockedPolicyDataBalance();

        List<GroupStats.GroupsOfStorage> storageList = Arrays.asList(
            new GroupStats.GroupsOfStorage("dn1",
                groupLists("dn1", 0, 1),
                buildDataSizeMap("dn1", 0, 1, 100)),
            new GroupStats.GroupsOfStorage("dn2",
                groupLists("dn2", 4, 9),
                buildDataSizeMap("dn2", 4, 9, 100)));
        BalanceStats stats = BalanceStats.createForSharding("test_db", storageList);

        List<BalanceAction> actions = policy.applyToShardingDb(null, BalanceOptions.withDefault(), stats, "test_db");
        // In MOCK mode, isNewPartitionDb returns false, so DRDS path is taken
        // With isStoragePoolTriggered=false, groups are not filtered
        Assert.assertNotNull(actions);
    }

    @Test
    public void testApplyToShardingDbStoragePoolTriggered() {
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        StoragePoolTriggeredPolicyDataBalance policy = new StoragePoolTriggeredPolicyDataBalance();

        List<GroupStats.GroupsOfStorage> storageList = Arrays.asList(
            new GroupStats.GroupsOfStorage("dn1",
                groupLists("dn1", 0, 1),
                buildDataSizeMap("dn1", 0, 1, 100)),
            new GroupStats.GroupsOfStorage("dn2",
                groupLists("dn2", 4, 9),
                buildDataSizeMap("dn2", 4, 9, 100)));
        BalanceStats stats = BalanceStats.createForSharding("test_db", storageList);

        List<BalanceAction> actions = policy.applyToShardingDb(null, BalanceOptions.withDefault(), stats, "test_db");
        // With isStoragePoolTriggered=true, groups are filtered by getStoragePoolDnList
        Assert.assertNotNull(actions);
    }

    @Test
    public void testApplyToShardingDbEmptyGroups() {
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        MockedPolicyDataBalance policy = new MockedPolicyDataBalance();

        BalanceStats stats = BalanceStats.createForSharding("test_db", Collections.emptyList());
        List<BalanceAction> actions = policy.applyToShardingDb(null, BalanceOptions.withDefault(), stats, "test_db");
        Assert.assertNotNull(actions);
    }
}
