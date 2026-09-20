package com.alibaba.polardbx.executor.ddl.newengine.meta;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.gms.metadb.misc.PersistentReadWriteLock;
import com.alibaba.polardbx.gms.metadb.misc.ReadWriteLockAcquireResult;
import com.alibaba.polardbx.gms.metadb.misc.ReadWriteLockDeadlockDetector;
import com.alibaba.polardbx.gms.metadb.misc.ReadWriteLockRecord;
import com.alibaba.polardbx.gms.metadb.misc.ReadWriteLockWaitingRecord;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashSet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;

public class DdlEngineResourceManagerTest {

    private static final String EXCLUSIVE = "EXCLUSIVE";

    @Test
    public void testAcquireResourceTimeoutUsesDynamicConfig() {
        try (MockedStatic<PersistentReadWriteLock> mockedLockStatic =
            Mockito.mockStatic(PersistentReadWriteLock.class)) {
            PersistentReadWriteLock mockLockManager = Mockito.mock(PersistentReadWriteLock.class);
            mockedLockStatic.when(PersistentReadWriteLock::create).thenReturn(mockLockManager);
            Mockito.when(mockLockManager.tryReadWriteLockBatch(anyString(), anyString(), anySet(), anySet(), any()))
                .thenReturn(false);
            Mockito.when(mockLockManager.queryBlocker(anySet())).thenReturn(new HashSet<>());

            DdlEngineResourceManager target = new DdlEngineResourceManager();
            // This test exercises the legacy batch retry/timeout path; the FIFO waiting-queue
            // path (default-on) calls a different, unmocked method and must be disabled here.
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE, "false");
            // set timeout to 0 minute, so acquireResource should time out immediately
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES, "0");
            try {
                target.acquireResource("test_schema", 1L, Sets.newHashSet("r1"), Sets.newHashSet("r2"));
                Assert.fail("expect GET DDL LOCK TIMEOUT exception");
            } catch (TddlNestableRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("GET DDL LOCK TIMEOUT"));
            } finally {
                // restore default
                DynamicConfig.getInstance()
                    .loadValue(null, ConnectionProperties.DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES, "60");
                DynamicConfig.getInstance()
                    .loadValue(null, ConnectionProperties.ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE, "true");
            }
        }
    }

    @Test
    public void testAcquireResourceSucceedsBeforeTimeout() {
        try (MockedStatic<PersistentReadWriteLock> mockedLockStatic =
            Mockito.mockStatic(PersistentReadWriteLock.class)) {
            PersistentReadWriteLock mockLockManager = Mockito.mock(PersistentReadWriteLock.class);
            mockedLockStatic.when(PersistentReadWriteLock::create).thenReturn(mockLockManager);
            // fail once, then succeed on retry within the default 60min timeout
            Mockito.when(mockLockManager.tryReadWriteLockBatch(anyString(), anyString(), anySet(), anySet(), any()))
                .thenReturn(false)
                .thenReturn(true);
            Mockito.when(mockLockManager.queryBlocker(anySet())).thenReturn(new HashSet<>());

            DdlEngineResourceManager target = new DdlEngineResourceManager();
            // This test exercises the legacy batch retry path; the FIFO waiting-queue path
            // (default-on) calls a different, unmocked method and must be disabled here.
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE, "false");
            try {
                target.acquireResource("test_schema", 1L, Sets.newHashSet("r1"), Sets.newHashSet("r2"));

                Mockito.verify(mockLockManager, Mockito.times(2))
                    .tryReadWriteLockBatch(anyString(), anyString(), anySet(), anySet(), any());
            } finally {
                DynamicConfig.getInstance()
                    .loadValue(null, ConnectionProperties.ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE, "true");
            }
        }
    }

    @Test
    public void testCheckResourcesBelongToRecognizesOwnPrefixedOwner() {
        try (MockedStatic<PersistentReadWriteLock> mockedLockStatic =
            Mockito.mockStatic(PersistentReadWriteLock.class)) {
            PersistentReadWriteLock mockLockManager = Mockito.mock(PersistentReadWriteLock.class);
            mockedLockStatic.when(PersistentReadWriteLock::create).thenReturn(mockLockManager);

            long jobId = 12345L;
            // The job legitimately holds its own fixed resource lock, so queryBlocker reports
            // the job's own owner string ("DDL_" + jobId), not the bare jobId.
            Mockito.when(mockLockManager.queryBlocker(anySet()))
                .thenReturn(Sets.newHashSet(PersistentReadWriteLock.OWNER_PREFIX + jobId));

            DdlEngineResourceManager target = new DdlEngineResourceManager();
            Assert.assertTrue("job holding its own fixed resource must be recognized as 'belongs to me'",
                target.checkResourcesBelongTo(jobId, Sets.newHashSet("r1"), Sets.newHashSet("r2")));
        }
    }

    @Test
    public void testCheckResourcesBelongToRejectsOtherOwner() {
        try (MockedStatic<PersistentReadWriteLock> mockedLockStatic =
            Mockito.mockStatic(PersistentReadWriteLock.class)) {
            PersistentReadWriteLock mockLockManager = Mockito.mock(PersistentReadWriteLock.class);
            mockedLockStatic.when(PersistentReadWriteLock::create).thenReturn(mockLockManager);

            long jobId = 12345L;
            long otherJobId = 67890L;
            Mockito.when(mockLockManager.queryBlocker(anySet()))
                .thenReturn(Sets.newHashSet(PersistentReadWriteLock.OWNER_PREFIX + otherJobId));

            DdlEngineResourceManager target = new DdlEngineResourceManager();
            Assert.assertFalse("a resource held by a different owner must not be reported as 'belongs to me'",
                target.checkResourcesBelongTo(jobId, Sets.newHashSet("r1"), Sets.newHashSet("r2")));
        }
    }

    @Test
    public void testDeadlockDetectionIntervalUsesDynamicConfig() {
        DynamicConfig.getInstance()
            .loadValue(null, ConnectionProperties.DDL_RW_LOCK_DEADLOCK_DETECTION_INTERVAL, "3");
        try {
            Assert.assertEquals(3, DynamicConfig.getInstance().getDdlRwLockDeadlockDetectionInterval());
        } finally {
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.DDL_RW_LOCK_DEADLOCK_DETECTION_INTERVAL, "10");
        }
    }

    @Test
    public void testDeadlockDetectorIgnoresOrdinaryQueue() {
        String older = PersistentReadWriteLock.OWNER_PREFIX + 1L;
        String newer = PersistentReadWriteLock.OWNER_PREFIX + 2L;

        Assert.assertFalse(ReadWriteLockDeadlockDetector.shouldAbort(newer,
            Sets.newHashSet(granted(older, "r1", EXCLUSIVE)),
            Sets.newHashSet(waiting(newer, "r1", EXCLUSIVE, 1L))));
    }

    @Test
    public void testDeadlockDetectorAbortsMaxJobIdInTwoOwnerCycle() {
        String older = PersistentReadWriteLock.OWNER_PREFIX + 1L;
        String newer = PersistentReadWriteLock.OWNER_PREFIX + 2L;

        Assert.assertTrue(ReadWriteLockDeadlockDetector.shouldAbort(newer,
            Sets.newHashSet(
                granted(older, "r1", EXCLUSIVE),
                granted(newer, "r2", EXCLUSIVE)),
            Sets.newHashSet(
                waiting(newer, "r1", EXCLUSIVE, 1L),
                waiting(older, "r2", EXCLUSIVE, 1L))));
        Assert.assertFalse(ReadWriteLockDeadlockDetector.shouldAbort(older,
            Sets.newHashSet(
                granted(older, "r1", EXCLUSIVE),
                granted(newer, "r2", EXCLUSIVE)),
            Sets.newHashSet(
                waiting(newer, "r1", EXCLUSIVE, 1L),
                waiting(older, "r2", EXCLUSIVE, 1L))));
    }

    @Test
    public void testDeadlockDetectorFindsThreeOwnerCycle() {
        String d1 = PersistentReadWriteLock.OWNER_PREFIX + 1L;
        String d2 = PersistentReadWriteLock.OWNER_PREFIX + 2L;
        String d3 = PersistentReadWriteLock.OWNER_PREFIX + 3L;

        Assert.assertTrue(ReadWriteLockDeadlockDetector.shouldAbort(d3,
            Sets.newHashSet(
                granted(d1, "r1", EXCLUSIVE),
                granted(d2, "r2", EXCLUSIVE),
                granted(d3, "r3", EXCLUSIVE)),
            Sets.newHashSet(
                waiting(d2, "r1", EXCLUSIVE, 1L),
                waiting(d3, "r2", EXCLUSIVE, 1L),
                waiting(d1, "r3", EXCLUSIVE, 1L))));
    }

    @Test
    public void testDeadlockDetectorSearchesMultipleCyclesBeforeDeciding() {
        String d1 = PersistentReadWriteLock.OWNER_PREFIX + 1L;
        String d2 = PersistentReadWriteLock.OWNER_PREFIX + 2L;
        String d3 = PersistentReadWriteLock.OWNER_PREFIX + 3L;
        String d4 = PersistentReadWriteLock.OWNER_PREFIX + 4L;
        String d5 = PersistentReadWriteLock.OWNER_PREFIX + 5L;

        Assert.assertTrue(ReadWriteLockDeadlockDetector.shouldAbort(d3,
            Sets.newHashSet(
                granted(d1, "r1", EXCLUSIVE),
                granted(d2, "r2", EXCLUSIVE),
                granted(d3, "r3", EXCLUSIVE),
                granted(d4, "r4", EXCLUSIVE),
                granted(d5, "r5", EXCLUSIVE)),
            Sets.newHashSet(
                waiting(d2, "r1", EXCLUSIVE, 1L),
                waiting(d3, "r2", EXCLUSIVE, 1L),
                waiting(d1, "r3", EXCLUSIVE, 1L),
                waiting(d4, "r3", EXCLUSIVE, 2L),
                waiting(d5, "r4", EXCLUSIVE, 1L),
                waiting(d3, "r5", EXCLUSIVE, 1L))));
    }

    @Test
    public void testDeadlockDetectorUsesEarlierWaitingWriteAsReadBlocker() {
        String d1 = PersistentReadWriteLock.OWNER_PREFIX + 1L;
        String d2 = PersistentReadWriteLock.OWNER_PREFIX + 2L;

        Assert.assertFalse(ReadWriteLockDeadlockDetector.shouldAbort(d2,
            Sets.newHashSet(),
            Sets.newHashSet(
                waiting(d1, "r1", EXCLUSIVE, 1L),
                waiting(d2, "r1", d2, 2L))));
    }

    @Test
    public void testAcquireResourceCleansNewlyGrantedAndWaitingResourcesOnDeadlockVictim() {
        try (MockedStatic<PersistentReadWriteLock> mockedLockStatic =
            Mockito.mockStatic(PersistentReadWriteLock.class)) {
            PersistentReadWriteLock mockLockManager = Mockito.mock(PersistentReadWriteLock.class);
            mockedLockStatic.when(PersistentReadWriteLock::create).thenReturn(mockLockManager);

            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.DDL_RW_LOCK_DEADLOCK_DETECTION_INTERVAL, "1");
            try {
                Mockito.when(mockLockManager.tryReadWriteLockOneResourceWithWaitingQueue(
                        Mockito.eq("test_schema"), Mockito.eq("DDL_1"), Mockito.eq("r1"), Mockito.eq(true), any(),
                        Mockito.anyBoolean()))
                    .thenReturn(ReadWriteLockAcquireResult.NEWLY_GRANTED);
                Mockito.when(mockLockManager.tryReadWriteLockOneResourceWithWaitingQueue(
                        Mockito.eq("test_schema"), Mockito.eq("DDL_1"), Mockito.eq("r2"), Mockito.eq(true), any(),
                        Mockito.anyBoolean()))
                    .thenReturn(ReadWriteLockAcquireResult.WAITING);
                Mockito.when(mockLockManager.queryBlocker(anySet())).thenReturn(Sets.newHashSet());
                Mockito.when(mockLockManager.shouldAbortForDeadlock("DDL_1")).thenReturn(true);

                DdlEngineResourceManager target = new DdlEngineResourceManager();
                try {
                    target.acquireResource("test_schema", 1L, ignored -> false,
                        Sets.newHashSet(), Sets.newHashSet("r1", "r2"), -1L, conn -> true);
                    Assert.fail("expect deadlock victim exception");
                } catch (TddlNestableRuntimeException e) {
                    Assert.assertTrue(e.getMessage().contains("selected as victim"));
                }

                Mockito.verify(mockLockManager).unlockReadWriteByOwner("DDL_1",
                    Sets.newHashSet("r1"), Sets.newHashSet("r2"));
            } finally {
                DynamicConfig.getInstance()
                    .loadValue(null, ConnectionProperties.DDL_RW_LOCK_DEADLOCK_DETECTION_INTERVAL, "10");
            }
        }
    }

    @Test
    public void testAcquireResourceDoesNotCleanAlreadyHeldResourceOnDeadlockVictim() {
        try (MockedStatic<PersistentReadWriteLock> mockedLockStatic =
            Mockito.mockStatic(PersistentReadWriteLock.class)) {
            PersistentReadWriteLock mockLockManager = Mockito.mock(PersistentReadWriteLock.class);
            mockedLockStatic.when(PersistentReadWriteLock::create).thenReturn(mockLockManager);

            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.DDL_RW_LOCK_DEADLOCK_DETECTION_INTERVAL, "1");
            try {
                Mockito.when(mockLockManager.tryReadWriteLockOneResourceWithWaitingQueue(
                        Mockito.eq("test_schema"), Mockito.eq("DDL_1"), Mockito.eq("r1"), Mockito.eq(true), any(),
                        Mockito.anyBoolean()))
                    .thenReturn(ReadWriteLockAcquireResult.ALREADY_HELD);
                Mockito.when(mockLockManager.tryReadWriteLockOneResourceWithWaitingQueue(
                        Mockito.eq("test_schema"), Mockito.eq("DDL_1"), Mockito.eq("r2"), Mockito.eq(true), any(),
                        Mockito.anyBoolean()))
                    .thenReturn(ReadWriteLockAcquireResult.WAITING);
                Mockito.when(mockLockManager.queryBlocker(anySet())).thenReturn(Sets.newHashSet());
                Mockito.when(mockLockManager.shouldAbortForDeadlock("DDL_1")).thenReturn(true);

                DdlEngineResourceManager target = new DdlEngineResourceManager();
                try {
                    target.acquireResource("test_schema", 1L, ignored -> false,
                        Sets.newHashSet(), Sets.newHashSet("r1", "r2"), -1L, conn -> true);
                    Assert.fail("expect deadlock victim exception");
                } catch (TddlNestableRuntimeException e) {
                    Assert.assertTrue(e.getMessage().contains("selected as victim"));
                }

                Mockito.verify(mockLockManager).unlockReadWriteByOwner("DDL_1",
                    Sets.newHashSet(), Sets.newHashSet("r2"));
            } finally {
                DynamicConfig.getInstance()
                    .loadValue(null, ConnectionProperties.DDL_RW_LOCK_DEADLOCK_DETECTION_INTERVAL, "10");
            }
        }
    }

    @Test
    public void testAcquireResourceDoesNotCleanupAfterSuccessfulQueueUpdate() {
        try (MockedStatic<PersistentReadWriteLock> mockedLockStatic =
            Mockito.mockStatic(PersistentReadWriteLock.class)) {
            PersistentReadWriteLock mockLockManager = Mockito.mock(PersistentReadWriteLock.class);
            mockedLockStatic.when(PersistentReadWriteLock::create).thenReturn(mockLockManager);

            Mockito.when(mockLockManager.tryReadWriteLockOneResourceWithWaitingQueue(
                    Mockito.eq("test_schema"), Mockito.eq("DDL_1"), Mockito.eq("r1"), Mockito.eq(true), any(),
                    Mockito.anyBoolean()))
                .thenReturn(ReadWriteLockAcquireResult.NEWLY_GRANTED);
            Mockito.when(mockLockManager.tryReadWriteLockOneResourceWithWaitingQueue(
                    Mockito.eq("test_schema"), Mockito.eq("DDL_1"), Mockito.eq("r2"), Mockito.eq(true), any(),
                    Mockito.anyBoolean()))
                .thenReturn(ReadWriteLockAcquireResult.NEWLY_GRANTED);

            DdlEngineResourceManager target = new DdlEngineResourceManager();
            target.acquireResource("test_schema", 1L, ignored -> false,
                Sets.newHashSet(), Sets.newHashSet("r1", "r2"), -1L, conn -> true);

            Mockito.verify(mockLockManager, Mockito.never())
                .unlockReadWriteByOwner(anyString(), anySet(), anySet());
        }
    }

    private static ReadWriteLockRecord granted(String owner, String resource, String type) {
        ReadWriteLockRecord record = new ReadWriteLockRecord();
        record.schemaName = "test_schema";
        record.owner = owner;
        record.resource = resource;
        record.type = type;
        return record;
    }

    private static ReadWriteLockWaitingRecord waiting(String owner, String resource, String type, long queueSeq) {
        ReadWriteLockWaitingRecord record = new ReadWriteLockWaitingRecord();
        record.schemaName = "test_schema";
        record.owner = owner;
        record.resource = resource;
        record.type = type;
        record.queueSeq = queueSeq;
        return record;
    }

    @Test
    public void testFirstCallbackInvokedOnWaitingForPhaseOne() {
        try (MockedStatic<PersistentReadWriteLock> mockedLockStatic =
            Mockito.mockStatic(PersistentReadWriteLock.class)) {
            PersistentReadWriteLock mockLockManager = Mockito.mock(PersistentReadWriteLock.class);
            mockedLockStatic.when(PersistentReadWriteLock::create).thenReturn(mockLockManager);

            // first resource waits once then granted; second resource granted directly
            Mockito.when(mockLockManager.tryReadWriteLockOneResourceWithWaitingQueue(
                    Mockito.eq("test_schema"), Mockito.eq("DDL_1"), Mockito.eq("r1"), Mockito.eq(true), any(),
                    Mockito.eq(true)))
                .thenReturn(ReadWriteLockAcquireResult.WAITING)
                .thenReturn(ReadWriteLockAcquireResult.NEWLY_GRANTED);
            Mockito.when(mockLockManager.tryReadWriteLockOneResourceWithWaitingQueue(
                    Mockito.eq("test_schema"), Mockito.eq("DDL_1"), Mockito.eq("r2"), Mockito.eq(false), any(),
                    Mockito.eq(false)))
                .thenReturn(ReadWriteLockAcquireResult.NEWLY_GRANTED);
            Mockito.when(mockLockManager.queryBlocker(anySet())).thenReturn(Sets.newHashSet());

            DdlEngineResourceManager target = new DdlEngineResourceManager();
            target.acquireResourceWithFirstGrantedCallback("test_schema", 1L, ignored -> false,
                Sets.newHashSet("r2"), Sets.newHashSet("r1"), -1L, conn -> true);

            // the first resource carries invokeCallbackOnWaiting=true on every retry,
            // so the INITIAL record is persisted even while the first attempt is WAITING
            Mockito.verify(mockLockManager, Mockito.times(2)).tryReadWriteLockOneResourceWithWaitingQueue(
                Mockito.eq("test_schema"), Mockito.eq("DDL_1"), Mockito.eq("r1"), Mockito.eq(true), any(),
                Mockito.eq(true));
            Mockito.verify(mockLockManager, Mockito.times(1)).tryReadWriteLockOneResourceWithWaitingQueue(
                Mockito.eq("test_schema"), Mockito.eq("DDL_1"), Mockito.eq("r2"), Mockito.eq(false), any(),
                Mockito.eq(false));
        }
    }

    @Test
    public void testFirstCallbackNotInvokedOnWaitingForPhaseTwo() {
        try (MockedStatic<PersistentReadWriteLock> mockedLockStatic =
            Mockito.mockStatic(PersistentReadWriteLock.class)) {
            PersistentReadWriteLock mockLockManager = Mockito.mock(PersistentReadWriteLock.class);
            mockedLockStatic.when(PersistentReadWriteLock::create).thenReturn(mockLockManager);

            Mockito.when(mockLockManager.tryReadWriteLockOneResourceWithWaitingQueue(
                    Mockito.eq("test_schema"), Mockito.eq("DDL_1"), Mockito.eq("r1"), Mockito.eq(true), any(),
                    Mockito.eq(false)))
                .thenReturn(ReadWriteLockAcquireResult.WAITING)
                .thenReturn(ReadWriteLockAcquireResult.NEWLY_GRANTED);
            Mockito.when(mockLockManager.queryBlocker(anySet())).thenReturn(Sets.newHashSet());

            DdlEngineResourceManager target = new DdlEngineResourceManager();
            target.acquireResourceWithCallbacks("test_schema", 1L, ignored -> false,
                Sets.newHashSet(), Sets.newHashSet("r1"), -1L, conn -> true, conn -> true);

            // phase 2 keeps invokeCallbackOnWaiting=false so storeDdlRecord never runs before grant
            Mockito.verify(mockLockManager, Mockito.times(2)).tryReadWriteLockOneResourceWithWaitingQueue(
                Mockito.eq("test_schema"), Mockito.eq("DDL_1"), Mockito.eq("r1"), Mockito.eq(true), any(),
                Mockito.eq(false));
        }
    }
}
