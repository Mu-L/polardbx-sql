package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.recyclebin.PhyRecycleBinInfoAccessor;
import com.alibaba.polardbx.gms.recyclebin.PhyRecycleBinInfoRecord;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for PurgeRecycleBinScheduledJob
 *
 * @author luoyanxin.pt
 */
public class PurgeRecycleBinScheduledJobTest {

    private static final long SCHEDULE_ID = 100L;
    private static final long FIRE_TIME = 200L;

    private static MockedStatic<ScheduledJobsManager> mockedScheduledJobsManager;
    private static MockedStatic<ScheduleJobStarter> mockedScheduleJobStarter;
    private static MockedStatic<InstConfUtil> mockedInstConfUtil;
    private static MockedStatic<ConfigDataMode> mockedConfigDataMode;

    @BeforeClass
    public static void setUp() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        Mockito.clearAllCaches();

        mockedInstConfUtil = Mockito.mockStatic(InstConfUtil.class);
        mockedInstConfUtil.when(() -> InstConfUtil.getInt(any())).thenReturn(5);
        mockedInstConfUtil.when(() -> InstConfUtil.getLong(any())).thenReturn(5L);

        mockedScheduleJobStarter = Mockito.mockStatic(ScheduleJobStarter.class);
        mockedScheduleJobStarter.when(ScheduleJobStarter::launchAll).thenAnswer((Answer<Void>) i -> null);

        mockedScheduledJobsManager = Mockito.mockStatic(ScheduledJobsManager.class);

        mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
        mockedConfigDataMode.when(ConfigDataMode::isFastMock).thenReturn(true);
    }

    @AfterClass
    public static void tearDown() {
        mockedScheduledJobsManager.close();
        mockedScheduleJobStarter.close();
        mockedConfigDataMode.close();
        mockedInstConfUtil.close();
        MetaDbInstConfigManager.setConfigFromMetaDb(true);
        Mockito.clearAllCaches();
    }

    private PurgeRecycleBinScheduledJob createJob() {
        ExecutableScheduledJob esj = new ExecutableScheduledJob();
        esj.setScheduleId(SCHEDULE_ID);
        esj.setFireTime(FIRE_TIME);
        return new PurgeRecycleBinScheduledJob(esj);
    }

    private PhyRecycleBinInfoRecord buildRecord(String storageInstId, String dbName, String tbName) {
        PhyRecycleBinInfoRecord r = new PhyRecycleBinInfoRecord();
        r.setStorageInstId(storageInstId);
        r.setCurDbName(dbName);
        r.setCurTbName(tbName);
        r.setOriginDbName(dbName);
        r.setOriginTbName(tbName);
        return r;
    }

    // ==================== getScheduleId ====================

    @Test
    public void testGetScheduleId() {
        assertEquals(SCHEDULE_ID, createJob().getScheduleId());
    }

    // ==================== execute: CAS failure ====================

    @Test
    public void testExecute_casStateFails_returnsFalse() {
        // default returns false for unset mock
        assertFalse(createJob().execute());
    }

    // ==================== execute: not in maintenance window ====================

    @Test
    public void testExecute_notInMaintenanceWindow_skips() {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        // isInPurgePhyRecyclebinMaintenanceTimeWindow default or explicitly false
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(false);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("SKIP_CLEAN_PHY_RECYCLE_BIN_TASK")))
            .thenReturn(true);

        assertTrue(createJob().execute());
    }

    // ==================== execute: success with normal records ====================

    @Test
    public void testExecute_successWithRecords() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 1")))
            .thenReturn(true);

        PhyRecycleBinInfoRecord record = buildRecord("s1", "d1", "t1");
        Connection mockMetaConn = mock(Connection.class);
        Connection mockStorageConn = mock(Connection.class);
        Statement mockStmt = mock(Statement.class);
        when(mockStorageConn.createStatement()).thenReturn(mockStmt);

        StorageHaManager mockSHM = mock(StorageHaManager.class);
        when(mockSHM.getStorageHaSwitchParams("s1")).thenReturn(new HaSwitchParams());

        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<StorageHaManager> mSHM = Mockito.mockStatic(StorageHaManager.class);
            MockedStatic<DbTopologyManager> mDTM = Mockito.mockStatic(DbTopologyManager.class);
            MockedConstruction<PhyRecycleBinInfoAccessor> mc = Mockito.mockConstruction(
                PhyRecycleBinInfoAccessor.class, (mock, ctx) ->
                    when(mock.getUnDropRecordByMinute(anyLong())).thenReturn(Collections.singletonList(record))
            )) {
            mMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockMetaConn);
            mSHM.when(StorageHaManager::getInstance).thenReturn(mockSHM);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s1")).thenReturn(mockStorageConn);

            assertTrue(createJob().execute());

            verify(mockStmt).executeUpdate("drop table if exists d1.t1");
            assertEquals(2, mc.constructed().size());
            verify(mc.constructed().get(1)).updateByStorageAndTb("s1", "t1", PhyRecycleBinInfoRecord.STATUS_DROP);
            verify(mc.constructed().get(1)).deleteFinishRecordByStorageAndTb("s1", "t1");
        }
    }

    // ==================== execute: MetaDb conn fails -> FAILED ====================

    @Test
    public void testExecute_metaDbConnectionFails_marksFailed() {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);

        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class)) {
            // MetaDbUtil.getConnection throws TddlRuntimeException (unchecked)
            mMetaDb.when(MetaDbUtil::getConnection).thenThrow(new RuntimeException("metadb connection failed"));

            assertFalse(createJob().execute());

            mockedScheduledJobsManager.verify(() ->
                ScheduledJobsManager.updateState(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(FAILED), anyString(), anyString()));
        }
    }

    // ==================== haSwitchParams null -> skip and mark finished ====================

    @Test
    public void testCleanUp_haSwitchParamsNull_skipAndMarkFinished() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 1")))
            .thenReturn(true);

        PhyRecycleBinInfoRecord record = buildRecord("s_null", "db_null", "tb_null");
        Connection mockMetaConn = mock(Connection.class);
        StorageHaManager mockSHM = mock(StorageHaManager.class);
        when(mockSHM.getStorageHaSwitchParams("s_null")).thenReturn(null);

        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<StorageHaManager> mSHM = Mockito.mockStatic(StorageHaManager.class);
            MockedConstruction<PhyRecycleBinInfoAccessor> mc = Mockito.mockConstruction(
                PhyRecycleBinInfoAccessor.class, (mock, ctx) ->
                    when(mock.getUnDropRecordByMinute(anyLong())).thenReturn(Collections.singletonList(record))
            )) {
            mMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockMetaConn);
            mSHM.when(StorageHaManager::getInstance).thenReturn(mockSHM);

            assertTrue(createJob().execute());

            assertEquals(2, mc.constructed().size());
            verify(mc.constructed().get(1)).updateByStorageAndTb("s_null", "tb_null",
                PhyRecycleBinInfoRecord.STATUS_DROP);
            verify(mc.constructed().get(1)).deleteFinishRecordByStorageAndTb("s_null", "tb_null");
        }
    }

    // ==================== getStorageHaSwitchParams throws -> skip ====================

    @Test
    public void testCleanUp_getHaSwitchParamsThrows_skipAndContinue() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 2")))
            .thenReturn(true);

        PhyRecycleBinInfoRecord r1 = buildRecord("s_err", "db_err", "tb_err");
        PhyRecycleBinInfoRecord r2 = buildRecord("s_ok", "db_ok", "tb_ok");
        List<PhyRecycleBinInfoRecord> records = new ArrayList<>();
        records.add(r1);
        records.add(r2);

        Connection mockMetaConn = mock(Connection.class);
        Connection okConn = mock(Connection.class);
        Statement okStmt = mock(Statement.class);
        when(okConn.createStatement()).thenReturn(okStmt);

        StorageHaManager mockSHM = mock(StorageHaManager.class);
        when(mockSHM.getStorageHaSwitchParams("s_err")).thenThrow(new RuntimeException("storage not found"));
        when(mockSHM.getStorageHaSwitchParams("s_ok")).thenReturn(new HaSwitchParams());

        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<StorageHaManager> mSHM = Mockito.mockStatic(StorageHaManager.class);
            MockedStatic<DbTopologyManager> mDTM = Mockito.mockStatic(DbTopologyManager.class);
            MockedConstruction<PhyRecycleBinInfoAccessor> mc = Mockito.mockConstruction(
                PhyRecycleBinInfoAccessor.class, (mock, ctx) ->
                    when(mock.getUnDropRecordByMinute(anyLong())).thenReturn(records)
            )) {
            mMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockMetaConn);
            mSHM.when(StorageHaManager::getInstance).thenReturn(mockSHM);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_ok")).thenReturn(okConn);

            assertTrue(createJob().execute());

            verify(okStmt).executeUpdate("drop table if exists db_ok.tb_ok");
            PhyRecycleBinInfoAccessor upd = mc.constructed().get(1);
            verify(upd).updateByStorageAndTb("s_err", "tb_err", PhyRecycleBinInfoRecord.STATUS_DROP);
            verify(upd).updateByStorageAndTb("s_ok", "tb_ok", PhyRecycleBinInfoRecord.STATUS_DROP);
        }
    }

    // ==================== drop table fails -> continues next record ====================

    @Test
    public void testCleanUp_dropTableFails_continuesProcessing() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 1")))
            .thenReturn(true);

        PhyRecycleBinInfoRecord rFail = buildRecord("s_fail", "db_f", "tb_f");
        PhyRecycleBinInfoRecord rOk = buildRecord("s_ok2", "db_o2", "tb_o2");
        List<PhyRecycleBinInfoRecord> records = new ArrayList<>();
        records.add(rFail);
        records.add(rOk);

        Connection mockMetaConn = mock(Connection.class);
        Connection failConn = mock(Connection.class);
        Statement failStmt = mock(Statement.class);
        when(failConn.createStatement()).thenReturn(failStmt);
        when(failStmt.executeUpdate(anyString())).thenThrow(new SQLException("drop failed"));
        Connection okConn = mock(Connection.class);
        Statement okStmt = mock(Statement.class);
        when(okConn.createStatement()).thenReturn(okStmt);

        StorageHaManager mockSHM = mock(StorageHaManager.class);
        when(mockSHM.getStorageHaSwitchParams(anyString())).thenReturn(new HaSwitchParams());

        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<StorageHaManager> mSHM = Mockito.mockStatic(StorageHaManager.class);
            MockedStatic<DbTopologyManager> mDTM = Mockito.mockStatic(DbTopologyManager.class);
            MockedConstruction<PhyRecycleBinInfoAccessor> mc = Mockito.mockConstruction(
                PhyRecycleBinInfoAccessor.class, (mock, ctx) ->
                    when(mock.getUnDropRecordByMinute(anyLong())).thenReturn(records)
            )) {
            mMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockMetaConn);
            mSHM.when(StorageHaManager::getInstance).thenReturn(mockSHM);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_fail")).thenReturn(failConn);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_ok2")).thenReturn(okConn);

            assertTrue(createJob().execute());

            verify(okStmt).executeUpdate("drop table if exists db_o2.tb_o2");
            PhyRecycleBinInfoAccessor upd = mc.constructed().get(1);
            verify(upd).updateByStorageAndTb("s_ok2", "tb_o2", PhyRecycleBinInfoRecord.STATUS_DROP);
            verify(upd, never()).updateByStorageAndTb(eq("s_fail"), eq("tb_f"), any(Integer.class));
        }
    }

    // ==================== meta update fails -> continues other records ====================

    @Test
    public void testCleanUp_metaUpdateFails_continuesProcessing() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 2")))
            .thenReturn(true);

        PhyRecycleBinInfoRecord r1 = buildRecord("s_m1", "db_m1", "tb_m1");
        PhyRecycleBinInfoRecord r2 = buildRecord("s_m2", "db_m2", "tb_m2");
        List<PhyRecycleBinInfoRecord> records = new ArrayList<>();
        records.add(r1);
        records.add(r2);

        Connection mockMetaConn = mock(Connection.class);
        Connection conn1 = mock(Connection.class);
        Statement stmt1 = mock(Statement.class);
        when(conn1.createStatement()).thenReturn(stmt1);
        Connection conn2 = mock(Connection.class);
        Statement stmt2 = mock(Statement.class);
        when(conn2.createStatement()).thenReturn(stmt2);

        StorageHaManager mockSHM = mock(StorageHaManager.class);
        when(mockSHM.getStorageHaSwitchParams(anyString())).thenReturn(new HaSwitchParams());

        final int[] constructCount = {0};
        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<StorageHaManager> mSHM = Mockito.mockStatic(StorageHaManager.class);
            MockedStatic<DbTopologyManager> mDTM = Mockito.mockStatic(DbTopologyManager.class);
            MockedConstruction<PhyRecycleBinInfoAccessor> mc = Mockito.mockConstruction(
                PhyRecycleBinInfoAccessor.class, (mock, ctx) -> {
                    constructCount[0]++;
                    if (constructCount[0] == 1) {
                        when(mock.getUnDropRecordByMinute(anyLong())).thenReturn(records);
                    } else {
                        Mockito.doThrow(new RuntimeException("update meta failed")).doNothing()
                            .when(mock).updateByStorageAndTb(anyString(), anyString(), any(Integer.class));
                    }
                })) {
            mMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockMetaConn);
            mSHM.when(StorageHaManager::getInstance).thenReturn(mockSHM);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_m1")).thenReturn(conn1);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_m2")).thenReturn(conn2);

            assertTrue(createJob().execute());

            verify(stmt1).executeUpdate("drop table if exists db_m1.tb_m1");
            verify(stmt2).executeUpdate("drop table if exists db_m2.tb_m2");
            verify(mc.constructed().get(1), times(2))
                .updateByStorageAndTb(anyString(), anyString(), any(Integer.class));
        }
    }

    // ==================== MetaDb conn fails on update phase -> handled ====================

    @Test
    public void testCleanUp_metaDbConnFailsOnUpdate_handledGracefully() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 1")))
            .thenReturn(true);

        PhyRecycleBinInfoRecord record = buildRecord("s_x", "db_x", "tb_x");
        Connection mockMetaConn = mock(Connection.class);
        Connection storageConn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        when(storageConn.createStatement()).thenReturn(stmt);

        StorageHaManager mockSHM = mock(StorageHaManager.class);
        when(mockSHM.getStorageHaSwitchParams("s_x")).thenReturn(new HaSwitchParams());

        final int[] metaDbCallCount = {0};
        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<StorageHaManager> mSHM = Mockito.mockStatic(StorageHaManager.class);
            MockedStatic<DbTopologyManager> mDTM = Mockito.mockStatic(DbTopologyManager.class);
            MockedConstruction<PhyRecycleBinInfoAccessor> mc = Mockito.mockConstruction(
                PhyRecycleBinInfoAccessor.class, (mock, ctx) ->
                    when(mock.getUnDropRecordByMinute(anyLong())).thenReturn(Collections.singletonList(record))
            )) {
            mMetaDb.when(MetaDbUtil::getConnection).thenAnswer(inv -> {
                metaDbCallCount[0]++;
                if (metaDbCallCount[0] == 1) {
                    return mockMetaConn;
                }
                // Return a connection whose close() throws SQLException,
                // which is caught by catch(SQLException e) in production code
                Connection badConn = mock(Connection.class);
                Mockito.doThrow(new SQLException("close fail")).when(badConn).close();
                return badConn;
            });
            mSHM.when(StorageHaManager::getInstance).thenReturn(mockSHM);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_x")).thenReturn(storageConn);

            assertTrue(createJob().execute());
        }
    }

    // ==================== empty records ====================

    @Test
    public void testCleanUp_emptyRecords_success() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 0")))
            .thenReturn(true);

        Connection mockMetaConn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class);
            MockedConstruction<PhyRecycleBinInfoAccessor> mc = Mockito.mockConstruction(
                PhyRecycleBinInfoAccessor.class, (mock, ctx) ->
                    when(mock.getUnDropRecordByMinute(anyLong())).thenReturn(Collections.emptyList())
            )) {
            mMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockMetaConn);

            assertTrue(createJob().execute());

            assertEquals(2, mc.constructed().size());
            verify(mc.constructed().get(1), never())
                .updateByStorageAndTb(anyString(), anyString(), any(Integer.class));
        }
    }

    // ==================== mixed scenario ====================

    @Test
    public void testCleanUp_mixedScenario() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 2")))
            .thenReturn(true);

        PhyRecycleBinInfoRecord rNull = buildRecord("s_null", "db_n", "tb_n");
        PhyRecycleBinInfoRecord rOk = buildRecord("s_ok", "db_o", "tb_o");
        PhyRecycleBinInfoRecord rFail = buildRecord("s_fail", "db_f", "tb_f");
        List<PhyRecycleBinInfoRecord> records = new ArrayList<>();
        records.add(rNull);
        records.add(rOk);
        records.add(rFail);

        Connection mockMetaConn = mock(Connection.class);
        Connection okConn = mock(Connection.class);
        Statement okStmt = mock(Statement.class);
        when(okConn.createStatement()).thenReturn(okStmt);
        Connection failConn = mock(Connection.class);
        Statement failStmt = mock(Statement.class);
        when(failConn.createStatement()).thenReturn(failStmt);
        when(failStmt.executeUpdate(anyString())).thenThrow(new SQLException("drop error"));

        StorageHaManager mockSHM = mock(StorageHaManager.class);
        when(mockSHM.getStorageHaSwitchParams("s_null")).thenReturn(null);
        when(mockSHM.getStorageHaSwitchParams("s_ok")).thenReturn(new HaSwitchParams());
        when(mockSHM.getStorageHaSwitchParams("s_fail")).thenReturn(new HaSwitchParams());

        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<StorageHaManager> mSHM = Mockito.mockStatic(StorageHaManager.class);
            MockedStatic<DbTopologyManager> mDTM = Mockito.mockStatic(DbTopologyManager.class);
            MockedConstruction<PhyRecycleBinInfoAccessor> mc = Mockito.mockConstruction(
                PhyRecycleBinInfoAccessor.class, (mock, ctx) ->
                    when(mock.getUnDropRecordByMinute(anyLong())).thenReturn(records)
            )) {
            mMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockMetaConn);
            mSHM.when(StorageHaManager::getInstance).thenReturn(mockSHM);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_ok")).thenReturn(okConn);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_fail")).thenReturn(failConn);

            assertTrue(createJob().execute());

            PhyRecycleBinInfoAccessor upd = mc.constructed().get(1);
            verify(upd).updateByStorageAndTb("s_null", "tb_n", PhyRecycleBinInfoRecord.STATUS_DROP);
            verify(upd).updateByStorageAndTb("s_ok", "tb_o", PhyRecycleBinInfoRecord.STATUS_DROP);
            verify(upd, never()).updateByStorageAndTb(eq("s_fail"), eq("tb_f"), any(Integer.class));
        }
    }

    // ==================== getConnectionForStorage throws -> continues ====================

    @Test
    public void testCleanUp_getConnectionForStorageThrows() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 1")))
            .thenReturn(true);

        PhyRecycleBinInfoRecord r1 = buildRecord("s_cerr", "db_c", "tb_c");
        PhyRecycleBinInfoRecord r2 = buildRecord("s_cok", "db_c2", "tb_c2");
        List<PhyRecycleBinInfoRecord> records = new ArrayList<>();
        records.add(r1);
        records.add(r2);

        Connection mockMetaConn = mock(Connection.class);
        Connection okConn = mock(Connection.class);
        Statement okStmt = mock(Statement.class);
        when(okConn.createStatement()).thenReturn(okStmt);

        StorageHaManager mockSHM = mock(StorageHaManager.class);
        when(mockSHM.getStorageHaSwitchParams(anyString())).thenReturn(new HaSwitchParams());

        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<StorageHaManager> mSHM = Mockito.mockStatic(StorageHaManager.class);
            MockedStatic<DbTopologyManager> mDTM = Mockito.mockStatic(DbTopologyManager.class);
            MockedConstruction<PhyRecycleBinInfoAccessor> mc = Mockito.mockConstruction(
                PhyRecycleBinInfoAccessor.class, (mock, ctx) ->
                    when(mock.getUnDropRecordByMinute(anyLong())).thenReturn(records)
            )) {
            mMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockMetaConn);
            mSHM.when(StorageHaManager::getInstance).thenReturn(mockSHM);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_cerr"))
                .thenThrow(new RuntimeException("connection failed"));
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_cok")).thenReturn(okConn);

            assertTrue(createJob().execute());

            verify(okStmt).executeUpdate("drop table if exists db_c2.tb_c2");
            PhyRecycleBinInfoAccessor upd = mc.constructed().get(1);
            verify(upd).updateByStorageAndTb("s_cok", "tb_c2", PhyRecycleBinInfoRecord.STATUS_DROP);
            verify(upd, never()).updateByStorageAndTb(eq("s_cerr"), eq("tb_c"), any(Integer.class));
        }
    }

    // ==================== all haSwitchParams null ====================

    @Test
    public void testCleanUp_allHaSwitchParamsNull() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 3")))
            .thenReturn(true);

        PhyRecycleBinInfoRecord r1 = buildRecord("s1", "d1", "t1");
        PhyRecycleBinInfoRecord r2 = buildRecord("s2", "d2", "t2");
        PhyRecycleBinInfoRecord r3 = buildRecord("s3", "d3", "t3");
        List<PhyRecycleBinInfoRecord> records = new ArrayList<>();
        records.add(r1);
        records.add(r2);
        records.add(r3);

        Connection mockMetaConn = mock(Connection.class);
        StorageHaManager mockSHM = mock(StorageHaManager.class);
        when(mockSHM.getStorageHaSwitchParams(anyString())).thenReturn(null);

        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<StorageHaManager> mSHM = Mockito.mockStatic(StorageHaManager.class);
            MockedConstruction<PhyRecycleBinInfoAccessor> mc = Mockito.mockConstruction(
                PhyRecycleBinInfoAccessor.class, (mock, ctx) ->
                    when(mock.getUnDropRecordByMinute(anyLong())).thenReturn(records)
            )) {
            mMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockMetaConn);
            mSHM.when(StorageHaManager::getInstance).thenReturn(mockSHM);

            assertTrue(createJob().execute());

            PhyRecycleBinInfoAccessor upd = mc.constructed().get(1);
            verify(upd).updateByStorageAndTb("s1", "t1", PhyRecycleBinInfoRecord.STATUS_DROP);
            verify(upd).updateByStorageAndTb("s2", "t2", PhyRecycleBinInfoRecord.STATUS_DROP);
            verify(upd).updateByStorageAndTb("s3", "t3", PhyRecycleBinInfoRecord.STATUS_DROP);
            verify(upd).deleteFinishRecordByStorageAndTb("s1", "t1");
            verify(upd).deleteFinishRecordByStorageAndTb("s2", "t2");
            verify(upd).deleteFinishRecordByStorageAndTb("s3", "t3");
        }
    }

    // ==================== MetaDbUtil.getConnection() returns null on query phase ====================

    @Test
    public void testCleanUp_metaDbConnNullOnQuery_returnsZero() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 0")))
            .thenReturn(true);

        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class)) {
            mMetaDb.when(MetaDbUtil::getConnection).thenReturn(null);
            assertTrue(createJob().execute());
        }
    }

    // ==================== getConnectionForStorage returns null -> skip ====================

    @Test
    public void testCleanUp_getConnectionForStorageReturnsNull_skips() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 1")))
            .thenReturn(true);

        PhyRecycleBinInfoRecord rNullConn = buildRecord("s_nc", "db_nc", "tb_nc");
        PhyRecycleBinInfoRecord rOk = buildRecord("s_ok3", "db_ok3", "tb_ok3");
        List<PhyRecycleBinInfoRecord> records = new ArrayList<>();
        records.add(rNullConn);
        records.add(rOk);

        Connection mockMetaConn = mock(Connection.class);
        Connection okConn = mock(Connection.class);
        Statement okStmt = mock(Statement.class);
        when(okConn.createStatement()).thenReturn(okStmt);

        StorageHaManager mockSHM = mock(StorageHaManager.class);
        when(mockSHM.getStorageHaSwitchParams(anyString())).thenReturn(new HaSwitchParams());

        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<StorageHaManager> mSHM = Mockito.mockStatic(StorageHaManager.class);
            MockedStatic<DbTopologyManager> mDTM = Mockito.mockStatic(DbTopologyManager.class);
            MockedConstruction<PhyRecycleBinInfoAccessor> mc = Mockito.mockConstruction(
                PhyRecycleBinInfoAccessor.class, (mock, ctx) ->
                    when(mock.getUnDropRecordByMinute(anyLong())).thenReturn(records)
            )) {
            mMetaDb.when(MetaDbUtil::getConnection).thenReturn(mockMetaConn);
            mSHM.when(StorageHaManager::getInstance).thenReturn(mockSHM);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_nc")).thenReturn(null);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_ok3")).thenReturn(okConn);

            assertTrue(createJob().execute());

            verify(okStmt).executeUpdate("drop table if exists db_ok3.tb_ok3");
            PhyRecycleBinInfoAccessor upd = mc.constructed().get(1);
            verify(upd).updateByStorageAndTb("s_ok3", "tb_ok3", PhyRecycleBinInfoRecord.STATUS_DROP);
            verify(upd, never()).updateByStorageAndTb(eq("s_nc"), eq("tb_nc"), any(Integer.class));
        }
    }

    // ==================== MetaDbUtil.getConnection() returns null on update phase ====================

    @Test
    public void testCleanUp_metaDbConnNullOnUpdate_returnsEarly() throws Exception {
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(QUEUED), eq(RUNNING), anyLong()))
            .thenReturn(true);
        mockedInstConfUtil.when(InstConfUtil::isInPurgePhyRecyclebinMaintenanceTimeWindow).thenReturn(true);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(
                    eq(SCHEDULE_ID), eq(FIRE_TIME), eq(RUNNING), eq(SUCCESS),
                    anyLong(), eq("clean up finished records: 1")))
            .thenReturn(true);

        PhyRecycleBinInfoRecord record = buildRecord("s_u2", "db_u2", "tb_u2");
        Connection mockMetaConn = mock(Connection.class);
        Connection storageConn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        when(storageConn.createStatement()).thenReturn(stmt);

        StorageHaManager mockSHM = mock(StorageHaManager.class);
        when(mockSHM.getStorageHaSwitchParams("s_u2")).thenReturn(new HaSwitchParams());

        final int[] metaDbCallCount = {0};
        try (MockedStatic<MetaDbUtil> mMetaDb = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<StorageHaManager> mSHM = Mockito.mockStatic(StorageHaManager.class);
            MockedStatic<DbTopologyManager> mDTM = Mockito.mockStatic(DbTopologyManager.class);
            MockedConstruction<PhyRecycleBinInfoAccessor> mc = Mockito.mockConstruction(
                PhyRecycleBinInfoAccessor.class, (mock, ctx) ->
                    when(mock.getUnDropRecordByMinute(anyLong())).thenReturn(Collections.singletonList(record))
            )) {
            mMetaDb.when(MetaDbUtil::getConnection).thenAnswer(inv -> {
                metaDbCallCount[0]++;
                if (metaDbCallCount[0] == 1) {
                    return mockMetaConn;
                }
                return null;
            });
            mSHM.when(StorageHaManager::getInstance).thenReturn(mockSHM);
            mDTM.when(() -> DbTopologyManager.getConnectionForStorage("s_u2")).thenReturn(storageConn);

            assertTrue(createJob().execute());

            verify(stmt).executeUpdate("drop table if exists db_u2.tb_u2");
            // Only 1 accessor constructed (query phase); update phase returns early due to null connection
            assertEquals(1, mc.constructed().size());
        }
    }

    // ==================== getInstConfigAsLong ====================

    @SuppressWarnings("unchecked")
    private Map<String, String> getPropertiesInfoMap() throws Exception {
        MetaDbInstConfigManager mgr = MetaDbInstConfigManager.getInstance();
        Class<?> clazz = mgr.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField("propertiesInfoMap");
                field.setAccessible(true);
                Map<String, String> map = (Map<String, String>) field.get(mgr);
                if (map == null) {
                    map = new java.util.concurrent.ConcurrentHashMap<>();
                    field.set(mgr, map);
                }
                return map;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new RuntimeException("propertiesInfoMap field not found");
    }

    @Test
    public void testGetInstConfigAsLong_nullValue_returnsDefault() {
        assertEquals(42L, createJob().getInstConfigAsLong("test_key_null", 42L));
    }

    @Test
    public void testGetInstConfigAsLong_emptyString_returnsDefault() throws Exception {
        Map<String, String> map = getPropertiesInfoMap();
        map.put("test_key_empty", "");
        try {
            assertEquals(42L, createJob().getInstConfigAsLong("test_key_empty", 42L));
        } finally {
            map.remove("test_key_empty");
        }
    }

    @Test
    public void testGetInstConfigAsLong_validValue_returnsParsed() throws Exception {
        Map<String, String> map = getPropertiesInfoMap();
        map.put("test_key_valid", "100");
        try {
            assertEquals(100L, createJob().getInstConfigAsLong("test_key_valid", 42L));
        } finally {
            map.remove("test_key_valid");
        }
    }

    @Test
    public void testGetInstConfigAsLong_invalidValue_returnsDefault() throws Exception {
        Map<String, String> map = getPropertiesInfoMap();
        map.put("test_key_invalid", "not_a_number");
        try {
            assertEquals(42L, createJob().getInstConfigAsLong("test_key_invalid", 42L));
        } finally {
            map.remove("test_key_invalid");
        }
    }
}
