package com.alibaba.polardbx.gms.cache;

import com.alibaba.polardbx.cache.external.impl.rpc.meta.PeerInfo;
import com.alibaba.polardbx.cache.external.impl.rpc.meta.PeerRole;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.cache.CachePeerAccessor;
import com.alibaba.polardbx.gms.metadb.cache.GmsRpcMetaServiceFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.sql.Connection;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CachePeerRefreshTask: lifecycle, refresh logic, leader election.
 */
public class CachePeerRefreshTaskTest {

    private GmsRpcMetaServiceFactory mockFactory;
    private PeerInfo mockPeerInfo;
    private MockedStatic<MetaDbDataSource> mockedMetaDb;
    private MockedStatic<CacheInitializer> mockedCacheInit;
    private Connection mockConnection;

    @Before
    public void setUp() throws Exception {
        mockFactory = mock(GmsRpcMetaServiceFactory.class);
        mockPeerInfo = new PeerInfo("test-peer", new InetSocketAddress("127.0.0.1", 9999),
            12345L, PeerRole.CACHE_READER, false);
        when(mockFactory.getMyself()).thenReturn(mockPeerInfo);

        mockConnection = mock(Connection.class);
        MetaDbDataSource mockDs = mock(MetaDbDataSource.class);
        when(mockDs.getConnection()).thenReturn(mockConnection);

        mockedMetaDb = Mockito.mockStatic(MetaDbDataSource.class);
        mockedMetaDb.when(MetaDbDataSource::getInstance).thenReturn(mockDs);

        mockedCacheInit = Mockito.mockStatic(CacheInitializer.class);
        CacheInitializer mockInit = mock(CacheInitializer.class);
        when(mockInit.collectStatusJson()).thenReturn("{\"status\":\"ok\"}");
        mockedCacheInit.when(CacheInitializer::getInstance).thenReturn(mockInit);
    }

    @After
    public void tearDown() {
        mockedMetaDb.close();
        mockedCacheInit.close();
    }

    @Test
    public void testStartAndStop() {
        CachePeerRefreshTask task = new CachePeerRefreshTask(5000, 10000, mockFactory);
        task.start();
        // Start again should be no-op
        task.start();
        task.stop();
    }

    @Test
    public void testIsLeaderDefaultFalse() {
        CachePeerRefreshTask task = new CachePeerRefreshTask(5000, 10000, mockFactory);
        Assert.assertFalse(task.isLeader());
    }

    @Test
    public void testRefreshAndElectBecomesLeader() throws Exception {
        CachePeerRefreshTask task = new CachePeerRefreshTask(5000, 10000, mockFactory);

        try (MockedConstruction<CachePeerAccessor> mocked =
            Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) -> {
                when(mock.elect(anyString(), anyLong(), anyLong())).thenReturn(true);
            })) {

            invokeRefreshAndElect(task);

            Assert.assertTrue(task.isLeader());
            // refreshAndElect now runs 3 short transactions: refresh, cleanup, elect.
            // Each phase constructs its own accessor instance.
            Assert.assertEquals(3, mocked.constructed().size());
            verify(mocked.constructed().get(0))
                .refresh(anyString(), anyString(), anyLong(), anyString(), anyLong(), anyLong(), any());
            verify(mocked.constructed().get(1)).cleanup(anyLong());
            verify(mocked.constructed().get(2)).elect(anyString(), anyLong(), anyLong());
            verify(mockConnection, Mockito.times(3)).commit();
        }
    }

    @Test
    public void testRefreshAndElectNotLeader() throws Exception {
        CachePeerRefreshTask task = new CachePeerRefreshTask(5000, 10000, mockFactory);

        try (MockedConstruction<CachePeerAccessor> mocked =
            Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) -> {
                when(mock.elect(anyString(), anyLong(), anyLong())).thenReturn(false);
            })) {

            invokeRefreshAndElect(task);

            Assert.assertFalse(task.isLeader());
            Assert.assertEquals(3, mocked.constructed().size());
        }
    }

    @Test
    public void testRefreshAndElectHandlesException() throws Exception {
        // Connection throws exception on commit
        Mockito.doThrow(new RuntimeException("DB error")).when(mockConnection).setAutoCommit(false);

        CachePeerRefreshTask task = new CachePeerRefreshTask(5000, 10000, mockFactory);

        // Should not throw
        invokeRefreshAndElect(task);
        Assert.assertFalse(task.isLeader());
    }

    @Test
    public void testLeaderStatusTransition() throws Exception {
        CachePeerRefreshTask task = new CachePeerRefreshTask(5000, 10000, mockFactory);

        // First: become leader
        try (MockedConstruction<CachePeerAccessor> mocked =
            Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) -> {
                when(mock.elect(anyString(), anyLong(), anyLong())).thenReturn(true);
            })) {
            invokeRefreshAndElect(task);
            Assert.assertTrue(task.isLeader());
        }

        // Second: lose leadership
        try (MockedConstruction<CachePeerAccessor> mocked =
            Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) -> {
                when(mock.elect(anyString(), anyLong(), anyLong())).thenReturn(false);
            })) {
            invokeRefreshAndElect(task);
            Assert.assertFalse(task.isLeader());
        }
    }

    // ===================== runInTxn: deadlock retry / short-circuit =====================

    @Test
    public void testRefreshDeadlockRetriesAndSucceeds() throws Exception {
        CachePeerRefreshTask task = new CachePeerRefreshTask(5000, 10000, mockFactory);

        try (MockedConstruction<CachePeerAccessor> mocked =
            Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) -> {
                // 1st refresh attempt deadlocks; 2nd attempt succeeds; then cleanup + elect.
                if (ctx.getCount() == 1) {
                    doThrow(new RuntimeException("ER_LOCK_DEADLOCK: Deadlock found"))
                        .when(mock).refresh(anyString(), anyString(), anyLong(), anyString(),
                            anyLong(), anyLong(), any());
                }
                when(mock.elect(anyString(), anyLong(), anyLong())).thenReturn(true);
            })) {

            invokeRefreshAndElect(task);

            // 4 accessors: 2 refresh attempts (1 failed + 1 succeeded) + cleanup + elect.
            Assert.assertEquals(4, mocked.constructed().size());
            Assert.assertTrue(task.isLeader());
            verify(mocked.constructed().get(1))
                .refresh(anyString(), anyString(), anyLong(), anyString(), anyLong(), anyLong(), any());
            verify(mocked.constructed().get(2)).cleanup(anyLong());
            verify(mocked.constructed().get(3)).elect(anyString(), anyLong(), anyLong());
            // 3 phases committed (refresh succeeded on 2nd try + cleanup + elect = 3 commits).
            verify(mockConnection, Mockito.times(3)).commit();
        }
    }

    @Test
    public void testRefreshDeadlockExhaustedSkipsRemainingPhases() throws Exception {
        CachePeerRefreshTask task = new CachePeerRefreshTask(5000, 10000, mockFactory);

        try (MockedConstruction<CachePeerAccessor> mocked =
            Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) -> {
                doThrow(new RuntimeException("deadlock detected"))
                    .when(mock).refresh(anyString(), anyString(), anyLong(), anyString(),
                        anyLong(), anyLong(), any());
            })) {

            invokeRefreshAndElect(task);

            // Only the 2 refresh attempts; cleanup and elect must be skipped.
            Assert.assertEquals(2, mocked.constructed().size());
            Assert.assertFalse(task.isLeader());
            verify(mockConnection, Mockito.never()).commit();
        }
    }

    @Test
    public void testRefreshNonDeadlockExceptionShortCircuitsWithoutRetry() throws Exception {
        CachePeerRefreshTask task = new CachePeerRefreshTask(5000, 10000, mockFactory);

        try (MockedConstruction<CachePeerAccessor> mocked =
            Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) -> {
                doThrow(new RuntimeException("generic SQL failure"))
                    .when(mock).refresh(anyString(), anyString(), anyLong(), anyString(),
                        anyLong(), anyLong(), any());
            })) {

            invokeRefreshAndElect(task);

            // Non-deadlock: no retry, no further phases => exactly 1 accessor.
            Assert.assertEquals(1, mocked.constructed().size());
            Assert.assertFalse(task.isLeader());
            verify(mockConnection, Mockito.never()).commit();
        }
    }

    @Test
    public void testCleanupDeadlockIgnoredAndElectStillRuns() throws Exception {
        CachePeerRefreshTask task = new CachePeerRefreshTask(5000, 10000, mockFactory);

        try (MockedConstruction<CachePeerAccessor> mocked =
            Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) -> {
                // Phase 2 = cleanup (2nd accessor) deadlocks; retry disabled => swallow.
                if (ctx.getCount() == 2) {
                    when(mock.cleanup(anyLong()))
                        .thenThrow(new RuntimeException("Deadlock in cleanup"));
                }
                when(mock.elect(anyString(), anyLong(), anyLong())).thenReturn(true);
            })) {

            invokeRefreshAndElect(task);

            // refresh + cleanup + elect => 3 accessors; elect still ran and we became leader.
            Assert.assertEquals(3, mocked.constructed().size());
            Assert.assertTrue(task.isLeader());
            // Only refresh and elect committed; cleanup rolled back.
            verify(mockConnection, Mockito.times(2)).commit();
        }
    }

    @Test
    public void testElectDeadlockRetriesAndSucceeds() throws Exception {
        CachePeerRefreshTask task = new CachePeerRefreshTask(5000, 10000, mockFactory);

        try (MockedConstruction<CachePeerAccessor> mocked =
            Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) -> {
                // refresh, cleanup, elect-attempt1 (deadlock), elect-attempt2 (ok)
                if (ctx.getCount() == 3) {
                    when(mock.elect(anyString(), anyLong(), anyLong()))
                        .thenThrow(new RuntimeException("Deadlock during election"));
                } else {
                    when(mock.elect(anyString(), anyLong(), anyLong())).thenReturn(true);
                }
            })) {

            invokeRefreshAndElect(task);

            // refresh + cleanup + 2 elect attempts => 4 accessors.
            Assert.assertEquals(4, mocked.constructed().size());
            Assert.assertTrue(task.isLeader());
            // refresh + cleanup + elect-attempt2 committed = 3 commits.
            verify(mockConnection, Mockito.times(3)).commit();
        }
    }

    @Test
    public void testElectDeadlockExhaustedKeepsPreviousLeaderState() throws Exception {
        CachePeerRefreshTask task = new CachePeerRefreshTask(5000, 10000, mockFactory);

        try (MockedConstruction<CachePeerAccessor> mocked =
            Mockito.mockConstruction(CachePeerAccessor.class, (mock, ctx) -> {
                // Every elect attempt deadlocks.
                when(mock.elect(anyString(), anyLong(), anyLong()))
                    .thenThrow(new RuntimeException("DEADLOCK detected on election"));
            })) {

            invokeRefreshAndElect(task);

            // refresh + cleanup + 2 elect attempts (both fail) => 4 accessors.
            Assert.assertEquals(4, mocked.constructed().size());
            // Elect exhausted => leader state unchanged (still the default false).
            Assert.assertFalse(task.isLeader());
            // Only refresh + cleanup committed; both elect attempts rolled back.
            verify(mockConnection, Mockito.times(2)).commit();
        }
    }

    @Test
    public void testIsDeadlockExceptionMatchesNestedCause() throws Exception {
        // Validate the helper directly: the keyword can live anywhere in the
        // exception chain, case-insensitively.
        java.lang.reflect.Method m = CachePeerRefreshTask.class
            .getDeclaredMethod("isDeadlockException", Throwable.class);
        m.setAccessible(true);

        Assert.assertTrue((boolean) m.invoke(null,
            new RuntimeException("ER_LOCK_DEADLOCK detected")));
        Assert.assertTrue((boolean) m.invoke(null,
            new RuntimeException("Deadlock found when trying to get lock")));
        Assert.assertTrue((boolean) m.invoke(null,
            new RuntimeException("wrapper", new RuntimeException("nested DEADLOCK"))));
        Assert.assertFalse((boolean) m.invoke(null,
            new RuntimeException("generic SQL error")));
        // null-message and null chain must not loop or throw.
        Assert.assertFalse((boolean) m.invoke(null, new RuntimeException((String) null)));
        Assert.assertFalse((boolean) m.invoke(null,
            new Object[] {null}));
    }

    private void invokeRefreshAndElect(CachePeerRefreshTask task) throws Exception {
        Method method = CachePeerRefreshTask.class.getDeclaredMethod("refreshAndElect");
        method.setAccessible(true);
        method.invoke(task);
    }
}
