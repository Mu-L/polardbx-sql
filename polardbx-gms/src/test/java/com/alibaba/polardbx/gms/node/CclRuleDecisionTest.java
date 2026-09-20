package com.alibaba.polardbx.gms.node;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.alibaba.polardbx.gms.node.CCLDetectUtils.DubiousItem;

@RunWith(MockitoJUnitRunner.class)
public class CclRuleDecisionTest {
    @Mock
    private CCLDetectConfig mockCclDetectConfig;

    @Mock
    private CCLDetectDnActor mockCclDetectDnActor;

    @Mock
    private StorageInstHaContext mockStorageInstHaContext;

    @Mock
    private Connection mockConnection;

    @Test
    public void testDecideCclRuleAction() {
        CCLDetectManager manager = CCLDetectManager.getInstance();

        // Mock config
        Mockito.when(mockCclDetectConfig.isDryRun()).thenReturn(true).thenReturn(false);

        // Scenario 1: Dry run mode - should return default values
        CCLDetectManager.CclRuleDecision dryRunDecision =
            manager.decideCclRuleAction(mockCclDetectConfig, "storage1;template1", 5L);

        Assert.assertEquals("Dry run should not delete", -1L, dryRunDecision.shouldDelId);
        Assert.assertFalse("Dry run should not delete", dryRunDecision.shouldDelete);
        Assert.assertTrue("Dry run should generate", dryRunDecision.shouldGen);
        Assert.assertEquals("Dry run should keep original concurrency", 5L, dryRunDecision.concurrency);

        // Scenario 2: Non-dry run, new key (not in globalGenKeyDict) - should generate new rule
        String newKey = "storage1;template2";
        CCLDetectManager.CclRuleDecision newKeyDecision =
            manager.decideCclRuleAction(mockCclDetectConfig, newKey, 5L);

        Assert.assertEquals("New key should not delete", -1L, newKeyDecision.shouldDelId);
        Assert.assertFalse("New key should not delete", newKeyDecision.shouldDelete);
        Assert.assertTrue("New key should generate", newKeyDecision.shouldGen);
        Assert.assertEquals("New key should keep original concurrency", 5L, newKeyDecision.concurrency);

        // Scenario 3: Non-dry run, existing key with higher previous concurrency - should delete old and generate new
        String existingKeyHigher = "storage1;template3";
        manager.getGlobalGenKeyDict().put(existingKeyHigher, Pair.of(10L, 123L)); // prevCon=10 > concurrency=5

        CCLDetectManager.CclRuleDecision deleteDecision =
            manager.decideCclRuleAction(mockCclDetectConfig, existingKeyHigher, 5L);

        Assert.assertEquals("Should delete when prev concurrency is higher", 123L, deleteDecision.shouldDelId);
        Assert.assertTrue("Should delete when prev concurrency is higher", deleteDecision.shouldDelete);
        Assert.assertTrue("Should generate new rule when prev concurrency is higher", deleteDecision.shouldGen);
        Assert.assertEquals("Should keep new concurrency when prev concurrency is higher", 5L,
            deleteDecision.concurrency);

        // Scenario 4: Non-dry run, existing key with lower or equal previous concurrency - should only kill, not generate
        String existingKeyLower = "storage1;template4";
        manager.getGlobalGenKeyDict().put(existingKeyLower, Pair.of(3L, 456L)); // prevCon=3 < concurrency=5

        CCLDetectManager.CclRuleDecision onlyKillDecision =
            manager.decideCclRuleAction(mockCclDetectConfig, existingKeyLower, 5L);

        Assert.assertEquals("Should not delete when prev concurrency is lower", -1L, onlyKillDecision.shouldDelId);
        Assert.assertFalse("Should not delete when prev concurrency is lower", onlyKillDecision.shouldDelete);
        Assert.assertFalse("Should not generate when prev concurrency is lower", onlyKillDecision.shouldGen);
        Assert.assertEquals("Should use prev concurrency when prev concurrency is lower", 3L,
            onlyKillDecision.concurrency);

        // Scenario 5: Edge case - equal concurrency (should only kill, not generate)
        String existingKeyEqual = "storage1;template5";
        manager.getGlobalGenKeyDict().put(existingKeyEqual, Pair.of(5L, 789L)); // prevCon=5 == concurrency=5

        CCLDetectManager.CclRuleDecision equalDecision =
            manager.decideCclRuleAction(mockCclDetectConfig, existingKeyEqual, 5L);

        Assert.assertEquals("Should not delete when concurrency is equal", -1L, equalDecision.shouldDelId);
        Assert.assertFalse("Should not delete when concurrency is equal", equalDecision.shouldDelete);
        Assert.assertFalse("Should not generate when concurrency is equal", equalDecision.shouldGen);
        Assert.assertEquals("Should use prev concurrency when concurrency is equal", 5L, equalDecision.concurrency);
    }

    /**
     * Regression test for AONE 84718272: ms/s unit mismatch in
     * CCLDetectManager.generateCclRulesAndKill's slow/max threshold filter.
     * <p>
     * On new DN (milli_processlist), DubiousItem time is in milliseconds, while
     * slowThreshold/maxThreshold are configured in seconds (3 / 300). Verifies the
     * filter converts thresholds to milliseconds before comparing, so a genuine slow
     * query (avg 38000ms, inside real [3s,300s]) is captured and a normal fast query
     * (avg 50ms, outside the window) is not.
     */
    @Test
    public void testGenerateCclRulesAndKillMsUnitMismatch() {
        try (MockedStatic<CCLDetectUtils> mockedCclDetectUtils = Mockito.mockStatic(CCLDetectUtils.class);
            MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class)) {

            Mockito.when(mockCclDetectConfig.isDryRun()).thenReturn(false);
            Mockito.when(mockCclDetectConfig.getSlowThreshold()).thenReturn(3);
            Mockito.when(mockCclDetectConfig.getKillMinConcurrency()).thenReturn(1);
            Mockito.when(mockCclDetectConfig.getMaxThreshold()).thenReturn(300);
            Mockito.when(mockCclDetectConfig.getKillBatch()).thenReturn(10);
            Mockito.when(mockCclDetectConfig.getDnRuleExpireTime()).thenReturn(300);

            Mockito.when(mockStorageInstHaContext.toBriefString()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.getStorageInstId()).thenReturn("storage1");

            List<DubiousItem> slowInfos = new ArrayList<>();
            slowInfos.add(new DubiousItem(1L, "/*DRDS /127.0.0.1/test1/0//slow_query/ */SELECT count(*) FROM t",
                38000L));

            List<DubiousItem> fastInfos = new ArrayList<>();
            fastInfos.add(new DubiousItem(2L, "/*DRDS /127.0.0.1/test2/0//fast_query/ */SELECT 1", 50L));

            Map<String, Pair<Long, Integer>> interceptTime = new HashMap<>();
            interceptTime.put("slow_query", Pair.of(608000L, 16));
            interceptTime.put("fast_query", Pair.of(800L, 16));

            Map<String, List<DubiousItem>> interceptInfos = new HashMap<>();
            interceptInfos.put("slow_query", slowInfos);
            interceptInfos.put("fast_query", fastInfos);

            List<DubiousItem> allInfos = new ArrayList<>();
            allInfos.addAll(slowInfos);
            allInfos.addAll(fastInfos);

            mockedCclDetectUtils.when(() -> CCLDetectUtils.hasOutTrx(
                    Mockito.eq(allInfos), Mockito.any(Map.class), Mockito.any(Map.class), Mockito.anyBoolean()))
                .thenAnswer(invocation -> {
                    Map<String, Pair<Long, Integer>> timeMap = invocation.getArgument(1);
                    Map<String, List<DubiousItem>> infoMap = invocation.getArgument(2);
                    timeMap.putAll(interceptTime);
                    infoMap.putAll(interceptInfos);
                    return true;
                });

            CCLDetectManager manager = CCLDetectManager.getInstance();
            try {
                java.lang.reflect.Field field = CCLDetectManager.class.getDeclaredField("cclDetectDnActor");
                field.setAccessible(true);
                field.set(manager, mockCclDetectDnActor);
            } catch (Exception e) {
                Assert.fail("Failed to inject mock: " + e.getMessage());
            }

            Mockito.when(mockCclDetectDnActor.isTimeInMillis(Mockito.any(Supplier.class))).thenReturn(true);

            Mockito.when(mockCclDetectDnActor.getCclConcurrency(Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(5L);
            Mockito.when(
                    mockCclDetectDnActor.getGenSql(Mockito.any(Supplier.class), Mockito.anyLong(), Mockito.anyString()))
                .thenReturn("call dbms_ccl.add_ccl_rule(...)");

            try {
                Mockito.when(mockCclDetectDnActor.killAndGenerateCcl(
                        Mockito.any(Supplier.class), Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyString()))
                    .thenReturn(123L);
            } catch (SQLException e) {
                Assert.fail("Mock setup failed: " + e.getMessage());
            }

            Supplier<Connection> connectionSupplier = () -> mockConnection;

            manager.generateCclRulesAndKill(connectionSupplier, allInfos, mockStorageInstHaContext, 0,
                mockCclDetectConfig);

            Mockito.verify(mockCclDetectDnActor).getGenSql(
                Mockito.any(Supplier.class), Mockito.anyLong(), Mockito.eq("slow_query"));

            Mockito.verify(mockCclDetectDnActor, Mockito.never()).getGenSql(
                Mockito.any(Supplier.class), Mockito.anyLong(), Mockito.eq("fast_query"));

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }
}
