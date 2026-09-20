package com.alibaba.polardbx.transaction.async;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.mock.MockStatus;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.TrxLookupSet;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.transaction.mock.MockDatasource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicDouble;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.executor.handler.LogicalShowLocalDeadlocksHandler.SHOW_ENGINE_INNODB_STATUS;
import static com.alibaba.polardbx.transaction.async.DeadlockDetectionTask.SQL_QUERY_DEADLOCKS;
import static com.alibaba.polardbx.transaction.async.DeadlockDetectionTask.SQL_QUERY_HOTSPOT_LOCK;
import static com.alibaba.polardbx.transaction.async.DeadlockDetectionTask.SQL_QUERY_HOTSPOT_LOCK_80;
import static com.alibaba.polardbx.transaction.async.DeadlockDetectionTask.SQL_QUERY_LOCK_WAITS_80;
import static com.alibaba.polardbx.transaction.async.DeadlockDetectionTask.SQL_QUERY_TRX_80;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeadlockStressTest {

    private MockDatasource mockDataSource;
    private DeadlockDetectionTask task;
    private MockStatus status;
    private MockedStatic<DeadlockDetectionTask> mockedStaticDeadlockDetectionTask;
    private MockedStatic<ExecUtils> mockedStaticExecUtils;
    private MockedStatic<MetaDbUtil> mockedStaticMetaDbUtil;
    private MockedStatic<InstConfUtil> mockedStaticInstConfUtil;
    private MockDatasource metaDbDatasource;
    private AtomicInteger skip;
    private AtomicInteger currentSkip;
    private AtomicDouble estimateMeanInnodbTrxRT;
    private AtomicDouble estimateMeanLockWaitsRT;
    private Map<String, String> lastLocalDeadlocks;
    private final long normalRt = 50;

    @Before
    public void setUp() throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException, SQLException {
        status = new MockStatus();
        mockDataSource = new MockDatasource();
        task = new DeadlockDetectionTask(ImmutableList.of("mock_schema"));
        mockedStaticDeadlockDetectionTask = Mockito.mockStatic(DeadlockDetectionTask.class);
        mockedStaticDeadlockDetectionTask.when(DeadlockDetectionTask::recoverMeanRtFromMetaDb)
            .thenCallRealMethod();
        mockedStaticDeadlockDetectionTask.when(() -> DeadlockDetectionTask.maybeTooManyDataLockWaits(any()))
            .thenCallRealMethod();
        mockedStaticDeadlockDetectionTask.when(() -> DeadlockDetectionTask.shouldDegradeInnodbTrx(anyDouble()))
            .thenCallRealMethod();
        mockedStaticDeadlockDetectionTask.when(() -> DeadlockDetectionTask.shouldDegradeLockWaits(anyDouble()))
            .thenCallRealMethod();
        mockedStaticDeadlockDetectionTask.when(() -> DeadlockDetectionTask.checkDegrade(anyLong(), any()))
            .thenCallRealMethod();
        mockedStaticDeadlockDetectionTask.when(DeadlockDetectionTask::degrade)
            .thenCallRealMethod();
        mockedStaticDeadlockDetectionTask.when(DeadlockDetectionTask::recover)
            .thenCallRealMethod();
        mockedStaticDeadlockDetectionTask.when(() -> DeadlockDetectionTask.updateInnodbTrxMeanRt(anyDouble()))
            .thenCallRealMethod();
        mockedStaticDeadlockDetectionTask.when(() -> DeadlockDetectionTask.updateLockWaitsMeanRt(anyDouble()))
            .thenCallRealMethod();
        mockedStaticDeadlockDetectionTask.when(() -> DeadlockDetectionTask.scanLocalDeadlocks(any()))
            .thenCallRealMethod();
        mockedStaticDeadlockDetectionTask.when(DeadlockDetectionTask::recordMeanRt)
            .thenCallRealMethod();
        mockedStaticMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class, Mockito.CALLS_REAL_METHODS);
        metaDbDatasource = new MockDatasource();
        metaDbDatasource.configureSqlResult(
            "SELECT value FROM `db_status` WHERE key = ?",
            new String[] {"value"},
            new String[] {
                JSON.toJSONString(ImmutableMap.of(
                    "INNODB_TRX_MEAN_RT", Double.toString(50),
                    "LOCK_WAITS_MEAN_RT", Double.toString(50)
                ))}
        );
        mockedStaticMetaDbUtil.when(MetaDbUtil::getConnection)
            .thenAnswer(invocation -> metaDbDatasource.getConnection());
        mockedStaticExecUtils = Mockito.mockStatic(ExecUtils.class, Mockito.CALLS_REAL_METHODS);
        mockedStaticInstConfUtil = Mockito.mockStatic(InstConfUtil.class);
        mockedStaticInstConfUtil.when(() -> InstConfUtil.getBool(any())).thenReturn(true);

        // get private member variables to check
        Class<?> clazz = Class.forName("com.alibaba.polardbx.transaction.async.DeadlockDetectionTask");
        Field skipField = clazz.getDeclaredField("skip");
        skipField.setAccessible(true);
        skip = (AtomicInteger) skipField.get(null); // null 因为是静态字段
        clazz = Class.forName("com.alibaba.polardbx.transaction.async.DeadlockDetectionTask");
        skipField = clazz.getDeclaredField("currentSkip");
        skipField.setAccessible(true);
        currentSkip = (AtomicInteger) skipField.get(null); // null 因为是静态字段
        clazz = Class.forName("com.alibaba.polardbx.transaction.async.DeadlockDetectionTask");
        skipField = clazz.getDeclaredField("estimateMeanInnodbTrxRT");
        skipField.setAccessible(true);
        estimateMeanInnodbTrxRT = (AtomicDouble) skipField.get(null); // null 因为是静态字段
        clazz = Class.forName("com.alibaba.polardbx.transaction.async.DeadlockDetectionTask");
        skipField = clazz.getDeclaredField("estimateMeanLockWaitsRT");
        skipField.setAccessible(true);
        estimateMeanLockWaitsRT = (AtomicDouble) skipField.get(null); // null 因为是静态字段
        clazz = Class.forName("com.alibaba.polardbx.transaction.async.DeadlockDetectionTask");
        skipField = clazz.getDeclaredField("lastLocalDeadlocks");
        skipField.setAccessible(true);
        lastLocalDeadlocks = (Map<String, String>) skipField.get(null); // null 因为是静态字段

        skip.set(1);
        currentSkip.set(0);
    }

    @After
    public void tearDown() {
        if (mockDataSource != null) {
            mockDataSource.clearSqlResults();
        }
        if (status != null) {
            status.close();
        }
        if (mockedStaticDeadlockDetectionTask != null) {
            mockedStaticDeadlockDetectionTask.close();
        }
        if (mockedStaticMetaDbUtil != null) {
            mockedStaticMetaDbUtil.close();
        }
        if (mockedStaticExecUtils != null) {
            mockedStaticExecUtils.close();
        }
        if (mockedStaticInstConfUtil != null) {
            mockedStaticInstConfUtil.close();
        }
    }

    /**
     * 测试性能监控功能 - 锁等待过多时应该降级
     */
    @Test
    public void testMaybeTooManyDataLockWaitsDegraded() throws SQLException {
        // 设置较低的阈值
        DynamicConfig.getInstance()
            .loadValue(null, ConnectionProperties.DEADLOCK_DETECTION_DATA_LOCK_WAITS_THRESHOLD, "1000");

        // Mock 大量锁等待的查询结果
        String[] columnNames = {"cnt", "lock_id"};
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] {50L, "lock_1"});  // 50 * 49 / 2 = 1225
        rows.add(new Object[] {30L, "lock_2"});  // 30 * 29 / 2 = 435
        mockDataSource.configureSqlResult(SQL_QUERY_HOTSPOT_LOCK, columnNames, rows);

        Connection mockConnection = mockDataSource.getConnection();
        mockedStaticDeadlockDetectionTask.when(
                () -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(any()))
            .thenReturn(mockConnection);

        TGroupDataSource mockGroupDataSource = mock(TGroupDataSource.class);
        when(mockGroupDataSource.getMasterSourceAddress()).thenReturn("127.0.0.1:3306");

        // 应该返回 true，表示需要跳过检测
        assertTrue("锁等待过多时应该跳过死锁检测",
            task.maybeTooManyDataLockWaits(mockGroupDataSource));
        assertEquals("actual skip: " + skip.get(), 2, skip.get());
        assertEquals("actual currentSkip: " + currentSkip.get(), 2, currentSkip.get());
    }

    /**
     * 测试响应时间过长导致的降级
     */
    @Test
    public void testResponseTimeDegradation57() {
        DynamicConfig.getInstance()
            .loadValue(null, ConnectionProperties.DEADLOCK_DETECTION_DATA_LOCK_WAITS_THRESHOLD, "10000");

        // mock trx lookup set
        TrxLookupSet lookupSet = new TrxLookupSet();
        mockedStaticDeadlockDetectionTask.when(
                DeadlockDetectionTask::fetchTransInfo)
            .thenReturn(lookupSet);

        // mock dn datasource
        Map<String, List<TGroupDataSource>> instId2GroupList = new HashMap<>();
        TGroupDataSource groupDataSource = Mockito.mock(TGroupDataSource.class);
        instId2GroupList.put("127.0.0.1:3306", ImmutableList.of(groupDataSource));
        mockedStaticExecUtils.when(
                () -> ExecUtils.getInstId2GroupList(any(Collection.class)))
            .thenReturn(instId2GroupList);

        MockDatasource mockDataSource = new MockDatasource();
        // select innodb trx rt too long, skip 2 rounds
        {
            simulateDelay57(mockDataSource, 200, normalRt);
            mockedStaticDeadlockDetectionTask.when(
                    () -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(any()))
                .thenAnswer(invocation -> mockDataSource.getConnection());
            task.run();
            assertEquals("actual skip: " + skip.get(), 2, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 2, currentSkip.get());
            task.run();
            assertEquals("actual skip: " + skip.get(), 2, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 1, currentSkip.get());
            task.run();
            assertEquals("actual skip: " + skip.get(), 2, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
        }

        // before recover, select lock waits rt too long, skip 4 rounds
        {
            simulateDelay57(mockDataSource, normalRt, 200);
            mockedStaticDeadlockDetectionTask.when(
                    () -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(any()))
                .thenAnswer(invocation -> mockDataSource.getConnection());
            task.run();
            assertEquals("actual skip: " + skip.get(), 4, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 4, currentSkip.get());
            task.run();
            assertEquals("actual skip: " + skip.get(), 4, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 3, currentSkip.get());
            task.run();
            assertEquals("actual skip: " + skip.get(), 4, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 2, currentSkip.get());
            task.run();
            assertEquals("actual skip: " + skip.get(), 4, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 1, currentSkip.get());
            task.run();
            assertEquals("actual skip: " + skip.get(), 4, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
        }

        // recover
        {
            simulateDelay57(mockDataSource, normalRt, normalRt);
            mockedStaticDeadlockDetectionTask.when(
                    () -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(any()))
                .thenAnswer(invocation -> mockDataSource.getConnection());
            task.run();
            assertEquals("actual skip: " + skip.get(), 3, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
            task.run();
            assertEquals("actual skip: " + skip.get(), 2, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
            task.run();
            assertEquals("actual skip: " + skip.get(), 1, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
            task.run();
            assertEquals("actual skip: " + skip.get(), 1, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
            task.run();
            assertEquals("actual skip: " + skip.get(), 1, skip.get());
            assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
        }

        // check rt calculation
        simulateDelay57(mockDataSource, normalRt, normalRt);
        mockedStaticDeadlockDetectionTask.when(
                () -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(any()))
            .thenAnswer(invocation -> mockDataSource.getConnection());
        for (int i = 0; i < 300; i++) {
            task.run();
        }
        // should close to 5ms
        System.out.println("actual estimateMeanInnodbTrxRT: " + estimateMeanInnodbTrxRT);
        System.out.println("actual estimateMeanLockWaitsRT: " + estimateMeanLockWaitsRT);
        assertTrue("actual estimateMeanInnodbTrxRT: " + estimateMeanInnodbTrxRT,
            Math.abs(estimateMeanInnodbTrxRT.get() - normalRt) < 5);
        assertTrue("actual estimateMeanLockWaitsRT: " + estimateMeanLockWaitsRT,
            Math.abs(estimateMeanLockWaitsRT.get() - normalRt) < 5);
    }

    @Test
    public void testResponseTimeDegradation80() throws SQLException {
        boolean isMysql80 = InstanceVersion.isMYSQL80();
        InstanceVersion.setMYSQL80(true);
        try {
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.DEADLOCK_DETECTION_DATA_LOCK_WAITS_THRESHOLD, "10000");

            // mock trx lookup set
            TrxLookupSet lookupSet = new TrxLookupSet();
            mockedStaticDeadlockDetectionTask.when(
                    DeadlockDetectionTask::fetchTransInfo)
                .thenReturn(lookupSet);

            // mock dn datasource
            Map<String, List<TGroupDataSource>> instId2GroupList = new HashMap<>();
            TGroupDataSource groupDataSource = Mockito.mock(TGroupDataSource.class);
            instId2GroupList.put("127.0.0.1:3306", ImmutableList.of(groupDataSource));
            mockedStaticExecUtils.when(
                    () -> ExecUtils.getInstId2GroupList(any(Collection.class)))
                .thenReturn(instId2GroupList);

            MockDatasource mockDataSource = new MockDatasource();
            // select innodb trx rt too long, skip 2 rounds
            {
                simulateDelay80(mockDataSource, 200, normalRt);
                mockedStaticDeadlockDetectionTask.when(
                        () -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(any()))
                    .thenAnswer(invocation -> mockDataSource.getConnection());
                task.run();
                assertEquals("actual skip: " + skip.get(), 2, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 2, currentSkip.get());
                task.run();
                assertEquals("actual skip: " + skip.get(), 2, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 1, currentSkip.get());
                task.run();
                assertEquals("actual skip: " + skip.get(), 2, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
            }

            // before recover, select lock waits rt too long, skip 4 rounds
            {
                simulateDelay80(mockDataSource, normalRt, 200);
                mockedStaticDeadlockDetectionTask.when(
                        () -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(any()))
                    .thenAnswer(invocation -> mockDataSource.getConnection());
                task.run();
                assertEquals("actual skip: " + skip.get(), 4, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 4, currentSkip.get());
                task.run();
                assertEquals("actual skip: " + skip.get(), 4, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 3, currentSkip.get());
                task.run();
                assertEquals("actual skip: " + skip.get(), 4, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 2, currentSkip.get());
                task.run();
                assertEquals("actual skip: " + skip.get(), 4, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 1, currentSkip.get());
                task.run();
                assertEquals("actual skip: " + skip.get(), 4, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
            }

            // recover
            {
                simulateDelay80(mockDataSource, normalRt, normalRt);
                mockedStaticDeadlockDetectionTask.when(
                        () -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(any()))
                    .thenAnswer(invocation -> mockDataSource.getConnection());
                task.run();
                assertEquals("actual skip: " + skip.get(), 3, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
                task.run();
                assertEquals("actual skip: " + skip.get(), 2, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
                task.run();
                assertEquals("actual skip: " + skip.get(), 1, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
                task.run();
                assertEquals("actual skip: " + skip.get(), 1, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
                task.run();
                assertEquals("actual skip: " + skip.get(), 1, skip.get());
                assertEquals("actual currentSkip: " + currentSkip.get(), 0, currentSkip.get());
            }

            // check rt calculation
            simulateDelay80(mockDataSource, normalRt, normalRt);
            mockedStaticDeadlockDetectionTask.when(
                    () -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(any()))
                .thenAnswer(invocation -> mockDataSource.getConnection());
            for (int i = 0; i < 100; i++) {
                task.run();
            }
            // should close to 5ms
            System.out.println("actual estimateMeanInnodbTrxRT: " + estimateMeanInnodbTrxRT);
            System.out.println("actual estimateMeanLockWaitsRT: " + estimateMeanLockWaitsRT);
            assertTrue("actual estimateMeanInnodbTrxRT: " + estimateMeanInnodbTrxRT,
                Math.abs(estimateMeanInnodbTrxRT.get() - normalRt) < 5);
            assertTrue("actual estimateMeanLockWaitsRT: " + estimateMeanLockWaitsRT,
                Math.abs(estimateMeanLockWaitsRT.get() - normalRt) < 5);
        } finally {
            InstanceVersion.setMYSQL80(isMysql80);
        }
    }

    @Test
    public void testLocalDeadlockScan() {
        // mock dn datasource
        Map<String, List<TGroupDataSource>> instId2GroupList = new HashMap<>();
        TGroupDataSource groupDataSource = Mockito.mock(TGroupDataSource.class);
        Mockito.when(groupDataSource.getMasterDNId()).thenReturn("127.0.0.1:3306");
        instId2GroupList.put("127.0.0.1:3306", ImmutableList.of(groupDataSource));
        mockedStaticExecUtils.when(
                () -> ExecUtils.getInstId2GroupList(any(Collection.class)))
            .thenReturn(instId2GroupList);

        MockDatasource mockDataSource = new MockDatasource();
        simulateDelay57(mockDataSource, normalRt, normalRt);
        simulateLocalDeadlock(mockDataSource);
        mockedStaticDeadlockDetectionTask.when(
                () -> DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(any()))
            .thenAnswer(invocation -> mockDataSource.getConnection());

        for (int i = 0; i < DynamicConfig.getInstance().getLocalDeadlockScanInterval(); i++) {
            task.run();
        }

        System.out.println(lastLocalDeadlocks);

        assertFalse(lastLocalDeadlocks.isEmpty());
    }

    private void simulateDelay57(MockDatasource mockDataSource, long innodbTrxDelay, long lockWaitsDelay) {
        String[] innodbTrxColumnNames = {"cnt", "lock_id"};
        List<Object[]> innodbTrxRows = new ArrayList<>();
        innodbTrxRows.add(new Object[] {5L, "lock_1"});

        mockDataSource.clearSqlResults();
        mockDataSource.clearSqlDelays();
        mockDataSource.configureSqlResult(SQL_QUERY_HOTSPOT_LOCK, innodbTrxColumnNames, innodbTrxRows);
        mockDataSource.configureSqlDelay(SQL_QUERY_HOTSPOT_LOCK, innodbTrxDelay);
        mockDataSource.configureSqlResult(SQL_QUERY_DEADLOCKS, new String[] {}, new ArrayList<>());
        mockDataSource.configureSqlDelay(SQL_QUERY_DEADLOCKS, lockWaitsDelay);
    }

    private void simulateDelay80(MockDatasource mockDataSource, long innodbTrxDelay, long lockWaitsDelay) {
        String[] innodbTrxColumnNames = {"cnt", "lock_id"};
        List<Object[]> innodbTrxRows = new ArrayList<>();
        innodbTrxRows.add(new Object[] {5L, "lock_1"});

        String[] innodbTrxColumnNames2 = {
            "trx_id", "conn_id", "state", "physical_sql", "operation_state", "tables_in_use", "tables_locked",
            "lock_structs", "heap_size", "row_locks"};
        List<Object[]> innodbTrxRows2 = new ArrayList<>();
        innodbTrxRows2.add(new Object[] {
            1, 100, "LOCK WAIT", "waiting_physical_sql", "operation_state", 6, 7, 8, 9, 10
        });
        innodbTrxRows2.add(new Object[] {
            2, 200, "NULL", "blocking_physical_sql", "operation_state", 6, 7, 8, 9, 10
        });

        mockDataSource.clearSqlResults();
        mockDataSource.clearSqlDelays();
        mockDataSource.configureSqlResult(SQL_QUERY_HOTSPOT_LOCK_80, innodbTrxColumnNames, innodbTrxRows);
        mockDataSource.configureSqlDelay(SQL_QUERY_HOTSPOT_LOCK_80, innodbTrxDelay);
        long maxFetchRows = DynamicConfig.getInstance().getDeadlockDetection80FetchTrxRows();
        mockDataSource.configureSqlResult(SQL_QUERY_TRX_80 + " LIMIT " + maxFetchRows,
            innodbTrxColumnNames2, innodbTrxRows2);
        mockDataSource.configureSqlResult(SQL_QUERY_LOCK_WAITS_80, new String[] {}, new ArrayList<>());
        mockDataSource.configureSqlDelay(SQL_QUERY_LOCK_WAITS_80, lockWaitsDelay);
    }

    private void simulateLocalDeadlock(MockDatasource mockDataSource) {
        String status = "=====================================\n"
            + "2025-11-18 17:08:27 139884793935616 INNODB MONITOR OUTPUT\n"
            + "=====================================\n"
            + "Per second averages calculated from the last 3 seconds\n"
            + "-----------------\n"
            + "BACKGROUND THREAD\n"
            + "-----------------\n"
            + "srv_master_thread loops: 1062036 srv_active, 0 srv_shutdown, 0 srv_idle\n"
            + "srv_master_thread log flush and writes: 0\n"
            + "----------\n"
            + "SEMAPHORES\n"
            + "----------\n"
            + "OS WAIT ARRAY INFO: reservation count 62588\n"
            + "OS WAIT ARRAY INFO: signal count 68727\n"
            + "RW-shared spins 0, rounds 0, OS waits 0\n"
            + "RW-excl spins 0, rounds 0, OS waits 0\n"
            + "RW-sx spins 0, rounds 0, OS waits 0\n"
            + "Spin rounds per wait: 0.00 RW-shared, 0.00 RW-excl, 0.00 RW-sx\n"
            + "------------------------\n"
            + "LATEST DETECTED DEADLOCK\n"
            + "------------------------\n"
            + "2025-11-18 17:07:30 139827188070144\n"
            + "*** (1) TRANSACTION:\n"
            + "TRANSACTION 44064285, XID X'647264732d316162666665393263323830663030304062623236613963383163636433386265',X'5f5f4344435f5f5f3030303030335f47524f5550',1, ACTIVE 5 sec starting index read\n"
            + "mysql tables in use 1, locked 1\n"
            + "LOCK WAIT 3 lock struct(s), heap size 1128, 2 row lock(s)\n"
            + "MySQL thread id 26831986, OS thread handle 139826353403648, query id 690661928 26.49.111.103 rds_polardb_x statistics\n"
            + "/*DRDS /127.0.0.1/1abffec2a800f000-2/0// */SELECT `tb1`.`id`, `tb1`.`a`\n"
            + "FROM `tb1_TjYE` AS `tb1`\n"
            + "WHERE (`tb1`.`id` = 0) FOR UPDATE\n"
            + "\n"
            + "*** (1) HOLDS THE LOCK(S):\n"
            + "RECORD LOCKS space id 2162 page no 4 n bits 72 index PRIMARY of table `wuzhe_p00005`.`tb1_tjye` trx id 44064285 XID X'647264732d316162666665393263323830663030304062623236613963383163636433386265',X'5f5f4344435f5f5f3030303030335f47524f5550',1 lock_mode X locks rec but not gap\n"
            + "Record lock, heap no 3 PHYSICAL RECORD: n_fields 7; compact format; info bits 0\n"
            + " 0: len 4; hex 80000001; asc     ;;\n"
            + " 1: len 6; hex 000002a05cfe; asc     \\ ;;\n"
            + " 2: len 7; hex 85000000a80135; asc       5;;\n"
            + " 3: len 8; hex 0000000002b099ed; asc         ;;\n"
            + " 4: len 8; hex 8002000079cf04a2; asc     y   ;;\n"
            + " 5: len 8; hex 66a58de65e800040; asc f   ^  @;;\n"
            + " 6: len 4; hex 80000001; asc     ;;\n"
            + "\n"
            + "\n"
            + "*** (1) WAITING FOR THIS LOCK TO BE GRANTED:\n"
            + "RECORD LOCKS space id 2162 page no 4 n bits 72 index PRIMARY of table `wuzhe_p00005`.`tb1_tjye` trx id 44064285 XID X'647264732d316162666665393263323830663030304062623236613963383163636433386265',X'5f5f4344435f5f5f3030303030335f47524f5550',1 lock_mode X locks rec but not gap waiting\n"
            + "Record lock, heap no 2 PHYSICAL RECORD: n_fields 7; compact format; info bits 0\n"
            + " 0: len 4; hex 80000000; asc     ;;\n"
            + " 1: len 6; hex 000002a05cfe; asc     \\ ;;\n"
            + " 2: len 7; hex 85000000a80128; asc       (;;\n"
            + " 3: len 8; hex 0000000002b099ed; asc         ;;\n"
            + " 4: len 8; hex 8002000079cf04a2; asc     y   ;;\n"
            + " 5: len 8; hex 66a58de65e800040; asc f   ^  @;;\n"
            + " 6: len 4; hex 80000000; asc     ;;\n"
            + "\n"
            + "\n"
            + "*** (2) TRANSACTION:\n"
            + "TRANSACTION 44064145, XID X'647264732d316162666665623232613831313030304062623236613963383163636433386265',X'5f5f4344435f5f5f3030303030335f47524f5550',1, ACTIVE 19 sec starting index read\n"
            + "mysql tables in use 1, locked 1\n"
            + "LOCK WAIT 3 lock struct(s), heap size 1128, 2 row lock(s)\n"
            + "MySQL thread id 26839567, OS thread handle 139826374366976, query id 690663079 26.73.29.111 rds_polardb_x statistics\n"
            + "/*DRDS /127.0.0.1/1abffeb25d011000-2/0//CCL;00b67a21;UNDETERMINED_COLUMN/ */SELECT `tb1`.`id`, `tb1`.`a`\n"
            + "FROM `tb1_TjYE` AS `tb1`\n"
            + "WHERE (`tb1`.`id` = 1) FOR UPDATE\n"
            + "\n"
            + "*** (2) HOLDS THE LOCK(S):\n"
            + "RECORD LOCKS space id 2162 page no 4 n bits 72 index PRIMARY of table `wuzhe_p00005`.`tb1_tjye` trx id 44064145 XID X'647264732d316162666665623232613831313030304062623236613963383163636433386265',X'5f5f4344435f5f5f3030303030335f47524f5550',1 lock_mode X locks rec but not gap\n"
            + "Record lock, heap no 2 PHYSICAL RECORD: n_fields 7; compact format; info bits 0\n"
            + " 0: len 4; hex 80000000; asc     ;;\n"
            + " 1: len 6; hex 000002a05cfe; asc     \\ ;;\n"
            + " 2: len 7; hex 85000000a80128; asc       (;;\n"
            + " 3: len 8; hex 0000000002b099ed; asc         ;;\n"
            + " 4: len 8; hex 8002000079cf04a2; asc     y   ;;\n"
            + " 5: len 8; hex 66a58de65e800040; asc f   ^  @;;\n"
            + " 6: len 4; hex 80000000; asc     ;;\n"
            + "\n"
            + "\n"
            + "*** (2) WAITING FOR THIS LOCK TO BE GRANTED:\n"
            + "RECORD LOCKS space id 2162 page no 4 n bits 72 index PRIMARY of table `wuzhe_p00005`.`tb1_tjye` trx id 44064145 XID X'647264732d316162666665623232613831313030304062623236613963383163636433386265',X'5f5f4344435f5f5f3030303030335f47524f5550',1 lock_mode X locks rec but not gap waiting\n"
            + "Record lock, heap no 3 PHYSICAL RECORD: n_fields 7; compact format; info bits 0\n"
            + " 0: len 4; hex 80000001; asc     ;;\n"
            + " 1: len 6; hex 000002a05cfe; asc     \\ ;;\n"
            + " 2: len 7; hex 85000000a80135; asc       5;;\n"
            + " 3: len 8; hex 0000000002b099ed; asc         ;;\n"
            + " 4: len 8; hex 8002000079cf04a2; asc     y   ;;\n"
            + " 5: len 8; hex 66a58de65e800040; asc f   ^  @;;\n"
            + " 6: len 4; hex 80000001; asc     ;;\n"
            + "\n"
            + "*** WE ROLL BACK TRANSACTION (2)\n"
            + "------------\n"
            + "TRANSACTIONS\n"
            + "------------";
        mockDataSource.configureSqlResult(SHOW_ENGINE_INNODB_STATUS,
            new String[] {"Type", "Name", "Status"},
            ImmutableList.of(new String[] {"InnoDB", "", status}));
    }
}