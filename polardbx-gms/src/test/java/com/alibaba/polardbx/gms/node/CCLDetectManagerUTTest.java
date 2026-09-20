package com.alibaba.polardbx.gms.node;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.ccl.DnCclRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.alibaba.polardbx.gms.node.CCLDetectUtils.DubiousItem;

/**
 * Unit test for CCLDetectManager
 *
 * @author liugaoji
 */
@RunWith(MockitoJUnitRunner.class)
public class CCLDetectManagerUTTest {

    @InjectMocks
    private CCLDetectManager target;

    @Mock
    private StorageHaManager mockStorageHaManager;

    @Mock
    private LeaderStatusBridge mockLeaderStatusBridge;

    @Mock
    private CCLDetectDnActor mockCclDetectDnActor;

    @Mock
    private StorageInstHaContext mockStorageInstHaContext;

    @Mock
    private Connection mockConnection;

    @Mock
    private DynamicConfig mockDynamicConfig;

    @Mock
    private CCLDetectConfig mockCclDetectConfig;

    @Before
    public void setUp() {
        // Reset singleton instance for testing - avoid calling getInstance() which triggers initialization
        try {
            java.lang.reflect.Field instanceField = CCLDetectManager.class.getDeclaredField("instance");
            instanceField.setAccessible(true);
            CCLDetectManager instance = (CCLDetectManager) instanceField.get(null);
            if (instance != null && instance.isInited()) {
                instance.clearGlobalGenKey();
            }
        } catch (Exception e) {
            // Ignore reflection errors in test setup
        }
    }

    @Test
    public void testGetInstanceSingleton() {
        CCLDetectManager instance1 = CCLDetectManager.getInstance();
        CCLDetectManager instance2 = CCLDetectManager.getInstance();

        Assert.assertNotNull("Instance should not be null", instance1);
        Assert.assertSame("Should return same singleton instance", instance1, instance2);
        Assert.assertTrue("Instance should be initialized", instance1.isInited());
    }

    @Test
    public void testClearGlobalGenKey() {
        // Setup test data
        CCLDetectManager manager = CCLDetectManager.getInstance();

        // Add some test data to global key dict (using reflection to access private field)
        try {
            java.lang.reflect.Field field = CCLDetectManager.class.getDeclaredField("globalGenKeyDict");
            field.setAccessible(true);
            ConcurrentHashMap<String, Pair<Long, Long>> globalGenKeyDict =
                (ConcurrentHashMap<String, Pair<Long, Long>>) field.get(manager);
            globalGenKeyDict.put("test_key", Pair.of(10L, 100L));

            Assert.assertFalse("Global key dict should not be empty before clear", globalGenKeyDict.isEmpty());

            // Test clear
            manager.clearGlobalGenKey();

            Assert.assertTrue("Global key dict should be empty after clear", globalGenKeyDict.isEmpty());
        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testInitGlobalGenKeySuccess() {
        try (MockedStatic<StorageHaManager> mockedStorageHaManager = Mockito.mockStatic(StorageHaManager.class);
            MockedStatic<CCLDetectDnActor> mockedCclDetectDnActor = Mockito.mockStatic(CCLDetectDnActor.class);
            MockedStatic<DbTopologyManager> mockedDbTopologyManager = Mockito.mockStatic(DbTopologyManager.class)) {

            // Mock StorageHaManager
            mockedStorageHaManager.when(StorageHaManager::getInstance).thenReturn(mockStorageHaManager);
            Map<String, StorageInstHaContext> storageStatusMap = new HashMap<>();
            storageStatusMap.put("storage1", mockStorageInstHaContext);
            Mockito.when(mockStorageHaManager.getStorageHaCtxCache()).thenReturn(storageStatusMap);

            // Mock StorageInstHaContext - Fix: Add isDNMaster() mock
            Mockito.when(mockStorageInstHaContext.getInstId()).thenReturn("inst1");
            Mockito.when(mockStorageInstHaContext.getStorageInstId()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.isDNMaster()).thenReturn(true);

            // Mock DbTopologyManager
            mockedDbTopologyManager.when(() -> DbTopologyManager.getConnectionForStorage(mockStorageInstHaContext))
                .thenReturn(mockConnection);

            // Mock CCLDetectDnActor.getDnCclRules
            List<DnCclRecord> records = new ArrayList<>();
            DnCclRecord record = new DnCclRecord();
            record.keywords = "CCL_TEST_KEY";
            record.concurrencyCount = "5";
            record.id = "123";
            records.add(record);

            mockedCclDetectDnActor.when(() -> CCLDetectDnActor.getDnCclRules(
                    Mockito.any(Supplier.class), Mockito.eq("inst1"), Mockito.eq("storage1")))
                .thenReturn(records);

            // Test initGlobalGenKey
            CCLDetectManager manager = CCLDetectManager.getInstance();
            manager.initGlobalGenKey();

            // Verify the global key dict is populated
            try {
                java.lang.reflect.Field field = CCLDetectManager.class.getDeclaredField("globalGenKeyDict");
                field.setAccessible(true);
                ConcurrentHashMap<String, Pair<Long, Long>> globalGenKeyDict =
                    (ConcurrentHashMap<String, Pair<Long, Long>>) field.get(manager);

                Assert.assertFalse("Global key dict should not be empty after init", globalGenKeyDict.isEmpty());
                Assert.assertTrue("Should contain expected key",
                    globalGenKeyDict.containsKey("storage1;CCL_TEST_KEY"));

                Pair<Long, Long> value = globalGenKeyDict.get("storage1;CCL_TEST_KEY");
                Assert.assertEquals("Should have correct concurrency", Long.valueOf(5L), value.getKey());
                Assert.assertEquals("Should have correct id", Long.valueOf(123L), value.getValue());
            } catch (Exception e) {
                Assert.fail("Should not throw exception: " + e.getMessage());
            }

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testInitGlobalGenKeyWithException() {
        try (MockedStatic<StorageHaManager> mockedStorageHaManager = Mockito.mockStatic(StorageHaManager.class)) {

            // Mock StorageHaManager to throw exception
            mockedStorageHaManager.when(StorageHaManager::getInstance)
                .thenThrow(new RuntimeException("Test exception"));

            // Test initGlobalGenKey with exception
            CCLDetectManager manager = CCLDetectManager.getInstance();

            // Should not throw exception, just log error
            manager.initGlobalGenKey();

            // Verify the global key dict remains empty
            try {
                java.lang.reflect.Field field = CCLDetectManager.class.getDeclaredField("globalGenKeyDict");
                field.setAccessible(true);
                ConcurrentHashMap<String, Pair<Long, Long>> globalGenKeyDict =
                    (ConcurrentHashMap<String, Pair<Long, Long>>) field.get(manager);

                Assert.assertTrue("Global key dict should be empty after exception", globalGenKeyDict.isEmpty());
            } catch (Exception e) {
                Assert.fail("Should not throw exception: " + e.getMessage());
            }

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testCCLDetectTaskRunWithoutLeadership() {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<LeaderStatusBridge> mockedLeaderStatusBridge = Mockito.mockStatic(LeaderStatusBridge.class)) {

            // Mock ConfigDataMode
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);

            // Mock LeaderStatusBridge
            mockedLeaderStatusBridge.when(LeaderStatusBridge::getInstance).thenReturn(mockLeaderStatusBridge);
            Mockito.when(mockLeaderStatusBridge.hasLeadership()).thenReturn(false);

            CCLDetectManager manager = CCLDetectManager.getInstance();
            CCLDetectManager.CCLDetectTask task = manager.new CCLDetectTask(mockCclDetectConfig);

            // Test run without leadership - should return early
            task.run();

            // Verify no storage operations were performed
            // (This is mainly testing that the method completes without exception)

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testCCLDetectTaskRunWithLeadership() {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<LeaderStatusBridge> mockedLeaderStatusBridge = Mockito.mockStatic(LeaderStatusBridge.class);
            MockedStatic<StorageHaManager> mockedStorageHaManager = Mockito.mockStatic(StorageHaManager.class);
            MockedStatic<DbTopologyManager> mockedDbTopologyManager = Mockito.mockStatic(DbTopologyManager.class);
            MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class)) {

            // Mock Config - Fix: Add required config mocks

            // Mock ConfigDataMode
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);

            // Mock LeaderStatusBridge
            mockedLeaderStatusBridge.when(LeaderStatusBridge::getInstance).thenReturn(mockLeaderStatusBridge);
            Mockito.when(mockLeaderStatusBridge.hasLeadership()).thenReturn(true);

            // Mock StorageHaManager
            mockedStorageHaManager.when(StorageHaManager::getInstance).thenReturn(mockStorageHaManager);
            Map<String, StorageInstHaContext> storageStatusMap = new HashMap<>();
            storageStatusMap.put("storage1", mockStorageInstHaContext);
            Mockito.when(mockStorageHaManager.getStorageHaCtxCache()).thenReturn(storageStatusMap);

            // Mock StorageInstHaContext - Fix: Add isDNMaster() mock
            Mockito.when(mockStorageInstHaContext.getInstId()).thenReturn("inst1");
            Mockito.when(mockStorageInstHaContext.getStorageInstId()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.isMetaDb()).thenReturn(false);
            Mockito.when(mockStorageInstHaContext.isDNMaster()).thenReturn(true);

            // Mock DbTopologyManager
            mockedDbTopologyManager.when(() -> DbTopologyManager.getConnectionForStorage(mockStorageInstHaContext))
                .thenReturn(mockConnection);

            CCLDetectManager manager = CCLDetectManager.getInstance();

            // Fix: Directly set the mock cclDetectDnActor since it's public
            manager.cclDetectDnActor = mockCclDetectDnActor;

            // Mock active session count (below threshold)
            Mockito.when(mockCclDetectDnActor.getActiveSessionNum(
                    Mockito.any(Supplier.class), Mockito.eq("inst1"), Mockito.eq("storage1")))
                .thenReturn(50L);

            CCLDetectManager.CCLDetectTask task = manager.new CCLDetectTask(mockCclDetectConfig);

            // Test run with leadership but below threshold
            task.run();

            // Verify getActiveSessionNum was called
            Mockito.verify(mockCclDetectDnActor).getActiveSessionNum(
                Mockito.any(Supplier.class), Mockito.eq("inst1"), Mockito.eq("storage1"));

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testCCLDetectTaskRunWithHighActiveSession() {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<LeaderStatusBridge> mockedLeaderStatusBridge = Mockito.mockStatic(LeaderStatusBridge.class);
            MockedStatic<StorageHaManager> mockedStorageHaManager = Mockito.mockStatic(StorageHaManager.class);
            MockedStatic<DbTopologyManager> mockedDbTopologyManager = Mockito.mockStatic(DbTopologyManager.class);
            MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class)) {

            // Mock ConfigDataMode
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);

            // Mock LeaderStatusBridge
            mockedLeaderStatusBridge.when(LeaderStatusBridge::getInstance).thenReturn(mockLeaderStatusBridge);
            Mockito.when(mockLeaderStatusBridge.hasLeadership()).thenReturn(true);

            // Mock StorageHaManager
            mockedStorageHaManager.when(StorageHaManager::getInstance).thenReturn(mockStorageHaManager);
            Map<String, StorageInstHaContext> storageStatusMap = new HashMap<>();
            storageStatusMap.put("storage1", mockStorageInstHaContext);
            Mockito.when(mockStorageHaManager.getStorageHaCtxCache()).thenReturn(storageStatusMap);

            // Mock StorageInstHaContext - Fix: Add isDNMaster() mock
            Mockito.when(mockStorageInstHaContext.getInstId()).thenReturn("inst1");
            Mockito.when(mockStorageInstHaContext.getStorageInstId()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.isMetaDb()).thenReturn(false);
            Mockito.when(mockStorageInstHaContext.toBriefString()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.isDNMaster()).thenReturn(true);

            // Mock DbTopologyManager
            mockedDbTopologyManager.when(() -> DbTopologyManager.getConnectionForStorage(mockStorageInstHaContext))
                .thenReturn(mockConnection);

            CCLDetectManager manager = CCLDetectManager.getInstance();

            // Fix: Directly set the mock cclDetectDnActor since it's public
            manager.cclDetectDnActor = mockCclDetectDnActor;

            // Mock active session count (above threshold) - Allow multiple calls
            Mockito.when(mockCclDetectDnActor.getActiveSessionNum(
                    Mockito.any(Supplier.class), Mockito.eq("inst1"), Mockito.eq("storage1")))
                .thenReturn(150L);

            // Mock DDL checker (pass)
            Mockito.when(mockCclDetectDnActor.hasDdlChecker(
                    Mockito.any(Supplier.class), Mockito.eq("storage1")))
                .thenReturn(true);

            CCLDetectManager.CCLDetectTask task = manager.new CCLDetectTask(mockCclDetectConfig);

            // Test run with high active session count
            task.run();

            // Wait a bit for async operations to complete
            Thread.sleep(100);

            // Verify methods were called - Allow multiple calls since the method is called in both
            // the main detection loop and the dubious instance handler
            Mockito.verify(mockCclDetectDnActor, Mockito.atLeastOnce()).getActiveSessionNum(
                Mockito.any(Supplier.class), Mockito.eq("inst1"), Mockito.eq("storage1"));
            Mockito.verify(mockCclDetectDnActor).hasDdlChecker(
                Mockito.any(Supplier.class), Mockito.eq("storage1"));

            // Verify EventLogger was called
            mockedEventLogger.verify(() -> EventLogger.log(
                Mockito.eq(EventType.CCL_DETECT), Mockito.anyString()));

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testCCLDetectTaskRunWithException() {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<LeaderStatusBridge> mockedLeaderStatusBridge = Mockito.mockStatic(LeaderStatusBridge.class);
            MockedStatic<StorageHaManager> mockedStorageHaManager = Mockito.mockStatic(StorageHaManager.class)) {

            // Mock ConfigDataMode
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);

            // Mock LeaderStatusBridge
            mockedLeaderStatusBridge.when(LeaderStatusBridge::getInstance).thenReturn(mockLeaderStatusBridge);
            Mockito.when(mockLeaderStatusBridge.hasLeadership()).thenReturn(true);

            // Mock StorageHaManager to throw exception
            mockedStorageHaManager.when(StorageHaManager::getInstance)
                .thenThrow(new RuntimeException("Test exception"));

            CCLDetectManager manager = CCLDetectManager.getInstance();
            CCLDetectManager.CCLDetectTask task = manager.new CCLDetectTask(mockCclDetectConfig);

            // Test run with exception - should not throw, just log error
            task.run();

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGenerateCclRulesAndKillWithDryRun() {
        try (MockedStatic<CCLDetectUtils> mockedCclDetectUtils = Mockito.mockStatic(CCLDetectUtils.class);
            MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class)) {

            // Mock Config
            Mockito.when(mockCclDetectConfig.isDryRun()).thenReturn(true);
            Mockito.when(mockCclDetectConfig.getSlowThreshold()).thenReturn(1000);
            Mockito.when(mockCclDetectConfig.getKillMinConcurrency()).thenReturn(1);
            Mockito.when(mockCclDetectConfig.getMaxThreshold()).thenReturn(10000);
            Mockito.when(mockCclDetectConfig.getKillBatch()).thenReturn(10);

            // Mock StorageInstHaContext
            Mockito.when(mockStorageInstHaContext.toBriefString()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.getStorageInstId()).thenReturn("storage1");

            // Mock CCLDetectUtils.hasOutTrx
            List<DubiousItem> infos = new ArrayList<>();
            infos.add(new DubiousItem(1L, "SELECT * FROM test", 2000L));

            Map<String, Pair<Long, Integer>> interceptTime = new HashMap<>();
            interceptTime.put("template1;user1", Pair.of(3600000L, 3));

            Map<String, List<DubiousItem>> interceptInfos = new HashMap<>();
            interceptInfos.put("template1;user1", infos);

            mockedCclDetectUtils.when(() -> CCLDetectUtils.hasOutTrx(
                    Mockito.eq(infos), Mockito.any(Map.class), Mockito.any(Map.class), Mockito.anyBoolean()))
                .thenAnswer(invocation -> {
                    Map<String, Pair<Long, Integer>> timeMap = invocation.getArgument(1);
                    Map<String, List<DubiousItem>> infoMap = invocation.getArgument(2);
                    timeMap.putAll(interceptTime);
                    infoMap.putAll(interceptInfos);
                    return true;
                });

            // Mock CCLDetectDnActor
            try {
                java.lang.reflect.Field field = CCLDetectManager.class.getDeclaredField("cclDetectDnActor");
                field.setAccessible(true);
                field.set(CCLDetectManager.getInstance(), mockCclDetectDnActor);
            } catch (Exception e) {
                Assert.fail("Failed to inject mock: " + e.getMessage());
            }

            // interceptTime values are in millisecond scale, simulating the new DN (milli_processlist)
            Mockito.when(mockCclDetectDnActor.isTimeInMillis(Mockito.any(Supplier.class))).thenReturn(true);

            CCLDetectManager manager = CCLDetectManager.getInstance();

            Supplier<Connection> connectionSupplier = () -> mockConnection;
            int result = manager.generateCclRulesAndKill(connectionSupplier, infos, mockStorageInstHaContext, 0,
                mockCclDetectConfig);

            Assert.assertTrue("Should return positive result for dry run", result >= 0);

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGenerateCclRulesAndKillWithNoOutTrxSql() {
        try (MockedStatic<CCLDetectUtils> mockedCclDetectUtils = Mockito.mockStatic(CCLDetectUtils.class);
            MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class)) {

            // Mock Config
            Mockito.when(mockCclDetectConfig.getSlowThreshold()).thenReturn(1000);

            // Mock CCLDetectUtils.hasOutTrx to return false
            List<DubiousItem> infos = new ArrayList<>();
            infos.add(new DubiousItem(1L, "SELECT * FROM test", 500L)); // Below threshold

            mockedCclDetectUtils.when(() -> CCLDetectUtils.hasOutTrx(
                    Mockito.eq(infos), Mockito.any(Map.class), Mockito.any(Map.class), Mockito.anyBoolean()))
                .thenReturn(false);

            CCLDetectManager manager = CCLDetectManager.getInstance();

            Supplier<Connection> connectionSupplier = () -> mockConnection;
            int result = manager.generateCclRulesAndKill(connectionSupplier, infos, mockStorageInstHaContext, 0,
                mockCclDetectConfig);

            Assert.assertEquals("Should return 0 when no out-trx SQL", 0, result);

            // Verify EventLogger was called with appropriate message
            mockedEventLogger.verify(() -> EventLogger.log(
                Mockito.eq(EventType.CCL_DETECT), Mockito.contains("No OutTrxSQL")));

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testCCLDetectTaskDisabled() {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            // Mock ConfigDataMode
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);

            CCLDetectManager manager = CCLDetectManager.getInstance();
            CCLDetectManager.CCLDetectTask task = manager.new CCLDetectTask(mockCclDetectConfig);

            // Test run with CCL detect disabled - should return early
            task.run();

            // Method should complete without any operations

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testCCLDetectTaskNotMasterMode() {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)) {

            // Mock ConfigDataMode - not master mode
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(false);

            CCLDetectManager manager = CCLDetectManager.getInstance();
            CCLDetectManager.CCLDetectTask task = manager.new CCLDetectTask(mockCclDetectConfig);

            // Test run when not in master mode - should return early
            task.run();

            // Method should complete without any operations

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    /**
     * Test handleDubiousInstWithLoop method with normal recursion scenario
     */
    @Test
    public void testHandleDubiousInstWithLoopNormalRecursion() throws InterruptedException {
        try (MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class)) {

            // Mock Config
            Mockito.when(mockCclDetectConfig.getDnDelayInterval()).thenReturn(1L); // Short delay for testing
            Mockito.when(mockCclDetectConfig.getConnectionLimit()).thenReturn(100);

            // Mock StorageInstHaContext
            Mockito.when(mockStorageInstHaContext.toBriefString()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.getStorageInstId()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.getInstId()).thenReturn("inst1");

            CCLDetectManager manager = CCLDetectManager.getInstance();

            // Directly set the mock cclDetectDnActor since it's now public
            manager.cclDetectDnActor = mockCclDetectDnActor;

            // Mock active session count (above threshold) - Allow null instId
            Mockito.when(mockCclDetectDnActor.getActiveSessionNum(
                    Mockito.any(Supplier.class), Mockito.nullable(String.class), Mockito.anyString()))
                .thenReturn(150L);

            // Mock getActiveSession
            List<DubiousItem> mockInfos = new ArrayList<>();
            mockInfos.add(new DubiousItem(1L, "/*DRDS /127.0.0.1/test123/0//template456/ */SELECT * FROM test", 2000L));
            Mockito.when(mockCclDetectDnActor.getActiveSession(
                    Mockito.any(Supplier.class), Mockito.anyString()))
                .thenReturn(mockInfos);

            CCLDetectManager.CCLDetectTask task = manager.new CCLDetectTask(mockCclDetectConfig);

            // Add storage instance to dubious set
            try {
                java.lang.reflect.Field dubiousInstsField = CCLDetectManager.class.getDeclaredField("dubiousInsts");
                dubiousInstsField.setAccessible(true);
                @SuppressWarnings("unchecked")
                java.util.Set<StorageInstHaContext> dubiousInsts =
                    (java.util.Set<StorageInstHaContext>) dubiousInstsField.get(manager);
                dubiousInsts.add(mockStorageInstHaContext);
            } catch (Exception e) {
                Assert.fail("Failed to access dubious instances: " + e.getMessage());
            }

            // Test handleDubiousInstWithLoop
            task.handleDubiousInstWithLoop(mockStorageInstHaContext, 0);

            // Wait for async execution
            Thread.sleep(2000);

            // Verify methods were called - Allow null instId
            Mockito.verify(mockCclDetectDnActor, Mockito.atLeastOnce()).getActiveSessionNum(
                Mockito.any(Supplier.class), Mockito.nullable(String.class), Mockito.anyString());

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    /**
     * Test handleDubiousInstWithLoop method when session count recovers
     */
    @Test
    public void testHandleDubiousInstWithLoopSessionRecovered() throws InterruptedException {
        try (MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class);
            MockedStatic<DbTopologyManager> mockedDbTopologyManager = Mockito.mockStatic(DbTopologyManager.class)) {

            // Mock Config
            Mockito.when(mockCclDetectConfig.getDnDelayInterval()).thenReturn(1L); // Short delay for testing
            Mockito.when(mockCclDetectConfig.getConnectionLimit()).thenReturn(100);

            // Mock StorageInstHaContext
            Mockito.when(mockStorageInstHaContext.toBriefString()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.getStorageInstId()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.getInstId()).thenReturn("inst1");

            // Mock DbTopologyManager
            mockedDbTopologyManager.when(() -> DbTopologyManager.getConnectionForStorage(mockStorageInstHaContext))
                .thenReturn(mockConnection);

            // Mock EventLogger to avoid actual logging
            mockedEventLogger.when(() -> EventLogger.log(Mockito.any(EventType.class), Mockito.anyString()))
                .then(invocation -> {
                    // Just capture the call, don't do anything
                    return null;
                });

            CCLDetectManager manager = CCLDetectManager.getInstance();

            // Directly set the mock cclDetectDnActor since it's now public and non-final
            manager.cclDetectDnActor = mockCclDetectDnActor;

            // Mock active session count (below threshold - recovered) - Allow null instId
            Mockito.when(mockCclDetectDnActor.getActiveSessionNum(
                    Mockito.any(Supplier.class), Mockito.nullable(String.class), Mockito.anyString()))
                .thenReturn(50L);

            CCLDetectManager.CCLDetectTask task = manager.new CCLDetectTask(mockCclDetectConfig);

            // Add storage instance to dubious set
            try {
                java.lang.reflect.Field dubiousInstsField = CCLDetectManager.class.getDeclaredField("dubiousInsts");
                dubiousInstsField.setAccessible(true);
                @SuppressWarnings("unchecked")
                java.util.Set<StorageInstHaContext> dubiousInsts =
                    (java.util.Set<StorageInstHaContext>) dubiousInstsField.get(manager);
                dubiousInsts.add(mockStorageInstHaContext);
            } catch (Exception e) {
                Assert.fail("Failed to access dubious instances: " + e.getMessage());
            }

            // Test handleDubiousInstWithLoop
            task.handleDubiousInstWithLoop(mockStorageInstHaContext, 0);

            // Wait for async execution
            Thread.sleep(3000);

            // Verify the instance was removed from dubious set
            try {
                java.lang.reflect.Field dubiousInstsField = CCLDetectManager.class.getDeclaredField("dubiousInsts");
                dubiousInstsField.setAccessible(true);
                @SuppressWarnings("unchecked")
                java.util.Set<StorageInstHaContext> dubiousInsts =
                    (java.util.Set<StorageInstHaContext>) dubiousInstsField.get(manager);
                Assert.assertFalse("Instance should be removed from dubious set when recovered",
                    dubiousInsts.contains(mockStorageInstHaContext));
            } catch (Exception e) {
                Assert.fail("Failed to access dubious instances: " + e.getMessage());
            }
        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    /**
     * Test generateCclRulesAndKill method with normal execution
     */
    @Test
    public void testGenerateCclRulesAndKillNormalExecution() {
        try (MockedStatic<CCLDetectUtils> mockedCclDetectUtils = Mockito.mockStatic(CCLDetectUtils.class);
            MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class)) {

            // Mock Config
            Mockito.when(mockCclDetectConfig.isDryRun()).thenReturn(false);
            Mockito.when(mockCclDetectConfig.getSlowThreshold()).thenReturn(1000);
            Mockito.when(mockCclDetectConfig.getKillMinConcurrency()).thenReturn(1);
            Mockito.when(mockCclDetectConfig.getMaxThreshold()).thenReturn(10000);
            Mockito.when(mockCclDetectConfig.getKillBatch()).thenReturn(10);
            Mockito.when(mockCclDetectConfig.getDnRuleExpireTime()).thenReturn(300);

            // Mock StorageInstHaContext
            Mockito.when(mockStorageInstHaContext.toBriefString()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.getStorageInstId()).thenReturn("storage1");

            // Mock CCLDetectUtils.hasOutTrx
            List<DubiousItem> infos = new ArrayList<>();
            infos.add(new DubiousItem(1L, "/*DRDS /127.0.0.1/test123/0//template456/ */SELECT * FROM test", 2000L));

            Map<String, Pair<Long, Integer>> interceptTime = new HashMap<>();
            interceptTime.put("template456", Pair.of(3600000L, 3));

            Map<String, List<DubiousItem>> interceptInfos = new HashMap<>();
            interceptInfos.put("template456", infos);

            mockedCclDetectUtils.when(() -> CCLDetectUtils.hasOutTrx(
                    Mockito.eq(infos), Mockito.any(Map.class), Mockito.any(Map.class), Mockito.anyBoolean()))
                .thenAnswer(invocation -> {
                    Map<String, Pair<Long, Integer>> timeMap = invocation.getArgument(1);
                    Map<String, List<DubiousItem>> infoMap = invocation.getArgument(2);
                    timeMap.putAll(interceptTime);
                    infoMap.putAll(interceptInfos);
                    return true;
                });

            // Mock CCLDetectDnActor
            try {
                java.lang.reflect.Field field = CCLDetectManager.class.getDeclaredField("cclDetectDnActor");
                field.setAccessible(true);
                field.set(CCLDetectManager.getInstance(), mockCclDetectDnActor);
            } catch (Exception e) {
                Assert.fail("Failed to inject mock: " + e.getMessage());
            }

            // interceptTime values are in millisecond scale, simulating the new DN (milli_processlist)
            Mockito.when(mockCclDetectDnActor.isTimeInMillis(Mockito.any(Supplier.class))).thenReturn(true);

            // Mock CCLDetectDnActor methods
            Mockito.when(mockCclDetectDnActor.getCclConcurrency(Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(5L);
            Mockito.when(
                    mockCclDetectDnActor.getGenSql(Mockito.any(Supplier.class), Mockito.anyLong(), Mockito.anyString()))
                .thenReturn("call dbms_ccl.add_ccl_rule('SELECT', '', '', 5, 'template456')");

            try {
                Mockito.when(mockCclDetectDnActor.killAndGenerateCcl(
                        Mockito.any(Supplier.class), Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyString()))
                    .thenReturn(123L);
            } catch (SQLException e) {
                Assert.fail("Mock setup failed: " + e.getMessage());
            }

            CCLDetectManager manager = CCLDetectManager.getInstance();

            Supplier<Connection> connectionSupplier = () -> mockConnection;
            int result = manager.generateCclRulesAndKill(connectionSupplier, infos, mockStorageInstHaContext, 0,
                mockCclDetectConfig);

            Assert.assertTrue("Should return positive result for normal execution", result > 0);

            // Verify CCLDetectDnActor methods were called
            Mockito.verify(mockCclDetectDnActor).getCclConcurrency(Mockito.anyInt(), Mockito.anyInt());
            Mockito.verify(mockCclDetectDnActor)
                .getGenSql(Mockito.any(Supplier.class), Mockito.anyLong(), Mockito.anyString());

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    /**
     * Test generateCclRulesAndKill method with exception handling
     */
    @Test
    public void testGenerateCclRulesAndKillWithException() {
        try (MockedStatic<CCLDetectUtils> mockedCclDetectUtils = Mockito.mockStatic(CCLDetectUtils.class)) {

            // Mock CCLDetectUtils.hasOutTrx to throw exception
            List<DubiousItem> infos = new ArrayList<>();
            infos.add(new DubiousItem(1L, "SELECT * FROM test", 2000L));

            mockedCclDetectUtils.when(() -> CCLDetectUtils.hasOutTrx(
                    Mockito.eq(infos), Mockito.any(Map.class), Mockito.any(Map.class), Mockito.anyBoolean()))
                .thenThrow(new RuntimeException("Test exception"));

            CCLDetectManager manager = CCLDetectManager.getInstance();

            Supplier<Connection> connectionSupplier = () -> mockConnection;

            // Should not throw exception, should handle gracefully
            try {
                int result = manager.generateCclRulesAndKill(connectionSupplier, infos, mockStorageInstHaContext, 0,
                    mockCclDetectConfig);
                // Method should handle exception and return 0 or appropriate value
                Assert.assertTrue("Should handle exception gracefully", result >= 0);
            } catch (Exception e) {
                // If exception is thrown, it should be handled properly
                Assert.assertTrue("Exception should be handled gracefully",
                    e instanceof RuntimeException && e.getMessage().contains("Test exception"));
            }

        } catch (Exception e) {
            // Test setup exception
            Assert.fail("Test setup should not throw exception: " + e.getMessage());
        }
    }

    /**
     * Test handleDubiousInstWithLoop method with exception handling
     */
    @Test
    public void testHandleDubiousInstWithLoopWithException() throws InterruptedException {

        // Mock Config
        Mockito.when(mockCclDetectConfig.getDnDelayInterval()).thenReturn(1L); // Short delay for testing

        // Mock StorageInstHaContext
        Mockito.when(mockStorageInstHaContext.toBriefString()).thenReturn("storage1");
        Mockito.when(mockStorageInstHaContext.getStorageInstId()).thenReturn("storage1");

        // Mock CCLDetectDnActor to throw exception
        try {
            java.lang.reflect.Field field = CCLDetectManager.class.getDeclaredField("cclDetectDnActor");
            field.setAccessible(true);
            field.set(CCLDetectManager.getInstance(), mockCclDetectDnActor);
        } catch (Exception e) {
            Assert.fail("Failed to inject mock: " + e.getMessage());
        }

        CCLDetectManager manager = CCLDetectManager.getInstance();
        CCLDetectManager.CCLDetectTask task = manager.new CCLDetectTask(mockCclDetectConfig);

        // Test handleDubiousInstWithLoop - should not throw exception
        task.handleDubiousInstWithLoop(mockStorageInstHaContext, 0);

        // Wait for async execution
        Thread.sleep(2000);

        // Method should complete without throwing exception (exception should be caught and logged)
    }

    /**
     * Test DnCclRecord fill method with complete data
     */
    @Test
    public void testDnCclRecordFill() throws SQLException {
        // Mock ResultSet with all required fields
        ResultSet mockResultSet = Mockito.mock(ResultSet.class);

        // Setup mock data for all fields
        Mockito.when(mockResultSet.getString("id")).thenReturn("123");
        Mockito.when(mockResultSet.getString("type")).thenReturn("SELECT");
        Mockito.when(mockResultSet.getString("schema")).thenReturn("test_schema");
        Mockito.when(mockResultSet.getString("table")).thenReturn("test_table");
        Mockito.when(mockResultSet.getString("state")).thenReturn("ACTIVE");
        Mockito.when(mockResultSet.getString("order")).thenReturn("1");
        Mockito.when(mockResultSet.getString("concurrency_count")).thenReturn("5");
        Mockito.when(mockResultSet.getString("matched")).thenReturn("10");
        Mockito.when(mockResultSet.getString("running")).thenReturn("3");
        Mockito.when(mockResultSet.getString("waitting")).thenReturn("2"); // Note: typo in original code
        Mockito.when(mockResultSet.getString("keywords")).thenReturn("test_keywords");

        // Create DnCclRecord and test fill method
        DnCclRecord record = new DnCclRecord();
        DnCclRecord result = record.fill(mockResultSet);

        // Verify all fields are correctly filled
        Assert.assertSame("Should return same instance", record, result);
        Assert.assertEquals("Should set id correctly", "123", record.id);
        Assert.assertEquals("Should set type correctly", "SELECT", record.type);
        Assert.assertEquals("Should set schema correctly", "test_schema", record.schema);
        Assert.assertEquals("Should set table correctly", "test_table", record.table);
        Assert.assertEquals("Should set state correctly", "ACTIVE", record.state);
        Assert.assertEquals("Should set order correctly", "1", record.order);
        Assert.assertEquals("Should set concurrency count correctly", "5", record.concurrencyCount);
        Assert.assertEquals("Should set matched correctly", "10", record.matched);
        Assert.assertEquals("Should set running correctly", "3", record.running);
        Assert.assertEquals("Should set waiting correctly", "2", record.waiting);
        Assert.assertEquals("Should set keywords correctly", "test_keywords", record.keywords);

        // Verify all expected methods were called
        Mockito.verify(mockResultSet).getString("id");
        Mockito.verify(mockResultSet).getString("type");
        Mockito.verify(mockResultSet, Mockito.times(2)).getString("schema"); // Called twice in original code
        Mockito.verify(mockResultSet).getString("table");
        Mockito.verify(mockResultSet).getString("state");
        Mockito.verify(mockResultSet).getString("order");
        Mockito.verify(mockResultSet).getString("concurrency_count");
        Mockito.verify(mockResultSet).getString("matched");
        Mockito.verify(mockResultSet).getString("running");
        Mockito.verify(mockResultSet).getString("waitting"); // Note: typo in original code
        Mockito.verify(mockResultSet).getString("keywords");
    }

    /**
     * Test CCLDetectDnActor deleteDnCCLWithKill method
     */
    @Test
    public void testDeleteDnCCLWithKill() throws SQLException {
        // Create test data
        Pair<String, Long> cclRuleKey = Pair.of("test_template_key", 456L);

        // Mock connection and statement
        Connection mockConn = Mockito.mock(Connection.class);
        Statement mockStmt = Mockito.mock(Statement.class);
        ResultSet mockWaitingListResult = Mockito.mock(ResultSet.class);
        ResultSet mockVersionCheckResult = Mockito.mock(ResultSet.class);

        Supplier<Connection> connectionSupplier = () -> mockConn;

        try {
            Mockito.when(mockConn.createStatement()).thenReturn(mockStmt);

            // Mock version check for new version
            Mockito.when(mockStmt.getResultSet())
                .thenReturn(mockVersionCheckResult)  // First call for version check
                .thenReturn(mockWaitingListResult);  // Second call for waiting list

            // Mock version check to return new version
            Mockito.when(mockVersionCheckResult.next()).thenReturn(true);

            // Mock waiting list query results
            Mockito.when(mockWaitingListResult.next()).thenReturn(true).thenReturn(true).thenReturn(false);
            Mockito.when(mockWaitingListResult.getString("INFO"))
                .thenReturn("/*DRDS /127.0.0.1/test123/0//test_template_key/ */SELECT * FROM test1")
                .thenReturn("/*DRDS /127.0.0.1/test456/0//other_key/ */SELECT * FROM test2");
            Mockito.when(mockWaitingListResult.getLong(1)).thenReturn(101L).thenReturn(102L);

            // Create CCLDetectDnActor instance and test deleteDnCCLWithKill
            CCLDetectDnActor actor = new CCLDetectDnActor();
            actor.deleteDnCCLWithKill(connectionSupplier, cclRuleKey);
            // Verify kill statement was executed for matching session
            Mockito.verify(mockStmt).execute("kill 101");

            // Verify kill statement was NOT executed for non-matching session
            Mockito.verify(mockStmt, Mockito.never()).execute("kill 102");

            // Verify statement was closed (called multiple times: once in version check, once in deleteDnCCLWithKill)
            Mockito.verify(mockStmt, Mockito.atLeast(1)).close();

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    /**
     * Test CCLDetectDnActor deleteDnCCLWithKill method with old version DN
     */
    @Test
    public void testDeleteDnCCLWithKillOldVersion() throws SQLException {
        // Create test data
        Pair<String, Long> cclRuleKey = Pair.of("test_template_key", 456L);

        // Mock connection and statement
        Connection mockConn = Mockito.mock(Connection.class);
        Statement mockStmt = Mockito.mock(Statement.class);
        ResultSet mockWaitingListResult = Mockito.mock(ResultSet.class);
        ResultSet mockVersionCheckResult = Mockito.mock(ResultSet.class);
        ResultSet mockShowRuleResult = Mockito.mock(ResultSet.class);

        Supplier<Connection> connectionSupplier = () -> mockConn;

        try {
            Mockito.when(mockConn.createStatement()).thenReturn(mockStmt);

            // Mock version check for old version (no result returned)
            Mockito.when(mockStmt.getResultSet())
                .thenReturn(mockVersionCheckResult)  // First call for version check
                .thenReturn(mockWaitingListResult)   // Second call for waiting list
                .thenReturn(mockShowRuleResult);     // Third call for show CCL rules

            // Mock version check to return old version (no next() result)
            Mockito.when(mockVersionCheckResult.next()).thenReturn(false);

            // Mock waiting list query results
            Mockito.when(mockWaitingListResult.next()).thenReturn(true).thenReturn(true).thenReturn(false);
            Mockito.when(mockWaitingListResult.getString("INFO"))
                .thenReturn("/*DRDS /127.0.0.1/test123/0//test_template_key/ */SELECT * FROM test1")
                .thenReturn("/*DRDS /127.0.0.1/test456/0//other_key/ */SELECT * FROM test2");
            Mockito.when(mockWaitingListResult.getLong(1)).thenReturn(101L).thenReturn(102L);

            // Mock show CCL rules result for old version
            // Return 2 matching rules with same keywords but different concurrency
            Mockito.when(mockShowRuleResult.next()).thenReturn(true).thenReturn(true).thenReturn(false);
            Mockito.when(mockShowRuleResult.getString("KEYWORDS"))
                .thenReturn("test_template_key")
                .thenReturn("test_template_key");  // Both rules have same keywords
            Mockito.when(mockShowRuleResult.getLong("ID")).thenReturn(201L).thenReturn(202L);
            Mockito.when(mockShowRuleResult.getLong("CONCURRENCY_COUNT")).thenReturn(5L).thenReturn(3L);

            // Create CCLDetectDnActor instance and test deleteDnCCLWithKill
            CCLDetectDnActor actor = new CCLDetectDnActor();
            actor.deleteDnCCLWithKill(connectionSupplier, cclRuleKey);

            // Verify waiting list query was executed
            Mockito.verify(mockStmt).execute(
                "select ID, INFO from information_schema.processlist where INFO is not NULL and command <> 'Sleep' and command <> 'Binlog Dump' and state = 'Concurrency control waiting'");

            // Verify kill statement was executed for matching session
            Mockito.verify(mockStmt).execute("kill 101");

            // Verify kill statement was NOT executed for non-matching session
            Mockito.verify(mockStmt, Mockito.never()).execute("kill 102");

            // Verify SHOW_DN_CCL_RULE was executed for old version
            Mockito.verify(mockStmt).execute("call dbms_ccl.show_ccl_rule()");

            // Verify CCL rule deletion for old version (should delete rules with higher concurrency)
            // Since we have 2 rules with concurrency 5 and 3, and we sort by concurrency,
            // only the rule with concurrency 5 (ID 201) should be deleted (keeping the minimal one)
            Mockito.verify(mockStmt).execute("call dbms_ccl.del_ccl_rule(201)");

            // Verify the rule with minimal concurrency (ID 202) was NOT deleted
            Mockito.verify(mockStmt, Mockito.never()).execute("call dbms_ccl.del_ccl_rule(202)");

            // Verify ResultSet was closed
            Mockito.verify(mockShowRuleResult).close();

            // Verify statement was closed (called multiple times: once in version check, once in deleteDnCCLWithKill)
            Mockito.verify(mockStmt, Mockito.atLeast(1)).close();

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    /**
     * Test generateCclRulesAndKill method with globalGenKeyDict containing existing key
     * This covers the branch where globalGenKeyDict.containsKey(globalKey) is true
     * and prevCon > concurrency (should trigger delete)
     */
    @Test
    public void testGenerateCclRulesAndKillWithExistingGlobalKey() {
        try (MockedStatic<CCLDetectUtils> mockedCclDetectUtils = Mockito.mockStatic(CCLDetectUtils.class);
            MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class)) {

            // Mock Config - NOT dry run to enter the target branch
            Mockito.when(mockCclDetectConfig.isDryRun()).thenReturn(false);
            Mockito.when(mockCclDetectConfig.getSlowThreshold()).thenReturn(1000);
            Mockito.when(mockCclDetectConfig.getKillMinConcurrency()).thenReturn(1);
            Mockito.when(mockCclDetectConfig.getMaxThreshold()).thenReturn(10000);
            Mockito.when(mockCclDetectConfig.getKillBatch()).thenReturn(10);
            Mockito.when(mockCclDetectConfig.getDnRuleExpireTime()).thenReturn(300);

            // Mock StorageInstHaContext
            Mockito.when(mockStorageInstHaContext.toBriefString()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.getStorageInstId()).thenReturn("storage1");

            // Mock CCLDetectUtils.hasOutTrx
            List<DubiousItem> infos = new ArrayList<>();
            infos.add(new DubiousItem(1L, "/*DRDS /127.0.0.1/test123/0//template456/ */SELECT * FROM test", 2000L));

            Map<String, Pair<Long, Integer>> interceptTime = new HashMap<>();
            interceptTime.put("template456", Pair.of(3600000L, 3));

            Map<String, List<DubiousItem>> interceptInfos = new HashMap<>();
            interceptInfos.put("template456", infos);

            mockedCclDetectUtils.when(() -> CCLDetectUtils.hasOutTrx(
                    Mockito.eq(infos), Mockito.any(Map.class), Mockito.any(Map.class), Mockito.anyBoolean()))
                .thenAnswer(invocation -> {
                    Map<String, Pair<Long, Integer>> timeMap = invocation.getArgument(1);
                    Map<String, List<DubiousItem>> infoMap = invocation.getArgument(2);
                    timeMap.putAll(interceptTime);
                    infoMap.putAll(interceptInfos);
                    return true;
                });

            CCLDetectManager manager = CCLDetectManager.getInstance();

            // Inject mock CCLDetectDnActor
            try {
                java.lang.reflect.Field field = CCLDetectManager.class.getDeclaredField("cclDetectDnActor");
                field.setAccessible(true);
                field.set(manager, mockCclDetectDnActor);
            } catch (Exception e) {
                Assert.fail("Failed to inject mock: " + e.getMessage());
            }

            // interceptTime values are in millisecond scale, simulating the new DN (milli_processlist)
            Mockito.when(mockCclDetectDnActor.isTimeInMillis(Mockito.any(Supplier.class))).thenReturn(true);

            // Pre-populate globalGenKeyDict with existing key (higher concurrency - should trigger delete)
            try {
                java.lang.reflect.Field globalGenKeyDictField =
                    CCLDetectManager.class.getDeclaredField("globalGenKeyDict");
                globalGenKeyDictField.setAccessible(true);
                @SuppressWarnings("unchecked")
                ConcurrentHashMap<String, Pair<Long, Long>> globalGenKeyDict =
                    (ConcurrentHashMap<String, Pair<Long, Long>>) globalGenKeyDictField.get(manager);

                // The key format is "storageId;templateKey"
                String globalKey = "storage1;template456";
                // Previous concurrency is 8, current will be calculated as lower, so shouldDelete should be true
                globalGenKeyDict.put(globalKey, Pair.of(8L, 999L)); // concurrency=8, ruleId=999

            } catch (Exception e) {
                Assert.fail("Failed to setup globalGenKeyDict: " + e.getMessage());
            }

            // Mock CCLDetectDnActor methods
            Mockito.when(mockCclDetectDnActor.getCclConcurrency(Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(5L); // Lower than the existing 8L, should trigger delete
            Mockito.when(
                    mockCclDetectDnActor.getGenSql(Mockito.any(Supplier.class), Mockito.anyLong(), Mockito.anyString()))
                .thenReturn("call dbms_ccl.add_ccl_rule('SELECT', '', '', 5, 'template456')");

            try {
                Mockito.when(mockCclDetectDnActor.killAndGenerateCcl(
                        Mockito.any(Supplier.class), Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyString()))
                    .thenReturn(123L);
            } catch (SQLException e) {
                Assert.fail("Mock setup failed: " + e.getMessage());
            }

            // Mock deleteDnCCLWithKill method
            Mockito.doNothing().when(mockCclDetectDnActor).deleteDnCCLWithKill(
                Mockito.any(Supplier.class), Mockito.any(Pair.class));

            Supplier<Connection> connectionSupplier = () -> mockConnection;
            int result = manager.generateCclRulesAndKill(connectionSupplier, infos, mockStorageInstHaContext, 0,
                mockCclDetectConfig);

            Assert.assertTrue("Should return positive result when deleting existing rule", result > 0);

            // Verify that deleteDnCCLWithKill was called due to higher existing concurrency
            Mockito.verify(mockCclDetectDnActor).deleteDnCCLWithKill(
                Mockito.any(Supplier.class), Mockito.eq(Pair.of("template456", 999L)));

            // Verify CCLDetectDnActor methods were called
            Mockito.verify(mockCclDetectDnActor).getCclConcurrency(Mockito.anyInt(), Mockito.anyInt());

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }

    /**
     * Reproduces AONE 84718272: ms/s unit mismatch in generateCclRulesAndKill's
     * slow/max threshold filter (CCLDetectManager.java:299-306).
     * <p>
     * On new DN (milli_processlist), DubiousItem time is accumulated in milliseconds,
     * but slowThreshold/maxThreshold are configured in seconds (3 / 300). The filter
     * directly compares them, effectively shrinking the intended [3s, 300s] window to
     * [3ms, 300ms]. As a result:
     * - a genuine slow query averaging 38000ms (well inside [3s, 300s]) is wrongly
     * excluded from CCL rule generation (real slow SQL not throttled);
     * - a normal query averaging 50ms (well outside [3s, 300s]) is wrongly included
     * (normal traffic mistakenly throttled).
     */
    @Test
    public void testGenerateCclRulesAndKillMsUnitMismatch() {
        try (MockedStatic<CCLDetectUtils> mockedCclDetectUtils = Mockito.mockStatic(CCLDetectUtils.class);
            MockedStatic<EventLogger> mockedEventLogger = Mockito.mockStatic(EventLogger.class)) {

            // Config uses seconds semantics: slowThreshold=3s, maxThreshold=300s
            Mockito.when(mockCclDetectConfig.isDryRun()).thenReturn(false);
            Mockito.when(mockCclDetectConfig.getSlowThreshold()).thenReturn(3);
            Mockito.when(mockCclDetectConfig.getKillMinConcurrency()).thenReturn(1);
            Mockito.when(mockCclDetectConfig.getMaxThreshold()).thenReturn(300);
            Mockito.when(mockCclDetectConfig.getKillBatch()).thenReturn(10);
            Mockito.when(mockCclDetectConfig.getDnRuleExpireTime()).thenReturn(300);

            Mockito.when(mockStorageInstHaContext.toBriefString()).thenReturn("storage1");
            Mockito.when(mockStorageInstHaContext.getStorageInstId()).thenReturn("storage1");

            // Genuine slow SQL: avg 38000ms (totalTime 608000ms / concurrency 16), inside real [3s,300s] window
            List<DubiousItem> slowInfos = new ArrayList<>();
            slowInfos.add(new DubiousItem(1L, "/*DRDS /127.0.0.1/test1/0//slow_query/ */SELECT count(*) FROM t",
                38000L));

            // Normal fast SQL: avg 50ms (totalTime 800ms / concurrency 16), outside real [3s,300s] window
            List<DubiousItem> fastInfos = new ArrayList<>();
            fastInfos.add(new DubiousItem(2L, "/*DRDS /127.0.0.1/test2/0//fast_query/ */SELECT 1",
                50L));

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

            // Inject mock CCLDetectDnActor
            try {
                java.lang.reflect.Field field = CCLDetectManager.class.getDeclaredField("cclDetectDnActor");
                field.setAccessible(true);
                field.set(CCLDetectManager.getInstance(), mockCclDetectDnActor);
            } catch (Exception e) {
                Assert.fail("Failed to inject mock: " + e.getMessage());
            }

            // Simulate new DN (milli_processlist): time is reported in milliseconds
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

            CCLDetectManager manager = CCLDetectManager.getInstance();
            Supplier<Connection> connectionSupplier = () -> mockConnection;

            manager.generateCclRulesAndKill(connectionSupplier, allInfos, mockStorageInstHaContext, 0,
                mockCclDetectConfig);

            // The genuine slow SQL (avg 38000ms, inside real [3s,300s] window) must be
            // captured and a CCL rule generated for it.
            Mockito.verify(mockCclDetectDnActor).getGenSql(
                Mockito.any(Supplier.class), Mockito.anyLong(), Mockito.eq("slow_query"));

            // The normal fast SQL (avg 50ms, outside real [3s,300s] window) must NOT be
            // mistakenly captured as slow.
            Mockito.verify(mockCclDetectDnActor, Mockito.never()).getGenSql(
                Mockito.any(Supplier.class), Mockito.anyLong(), Mockito.eq("fast_query"));

        } catch (Exception e) {
            Assert.fail("Should not throw exception: " + e.getMessage());
        }
    }
}