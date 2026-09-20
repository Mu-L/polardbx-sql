package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PersistentReadWriteLockTest {

    @Test
    public void testTryReadWriteLockBatch() throws SQLException {
        try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection conn = mock(Connection.class);
            Statement statement = mock(Statement.class);
            when(conn.createStatement()).thenReturn(statement);
            mockedMetaDbUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(conn);
            mockedMetaDbUtil.when(() -> MetaDbUtil.insert(anyString(), anyList(), any(Connection.class)))
                .thenReturn(new int[] {1});

            mockedMetaDbUtil.when(() -> MetaDbUtil.tryGetLock(any(Connection.class), any(String.class), anyLong()))
                .thenCallRealMethod();
            mockedMetaDbUtil.when(() -> MetaDbUtil.releaseLock(any(Connection.class), any(String.class)))
                .thenCallRealMethod();

            ResultSet lockRs = mock(ResultSet.class);
            when(lockRs.next()).thenReturn(true);
            when(lockRs.getInt(1)).thenReturn(0).thenReturn(1);
            when(statement.executeQuery(any())).thenReturn(lockRs);

            PersistentReadWriteLock persistentReadWriteLock = PersistentReadWriteLock.create();
            persistentReadWriteLock.tryReadWriteLockBatch("schemaName", "owner", new HashSet<>(), new HashSet<>());
            persistentReadWriteLock.tryReadWriteLockBatch("schemaName", "owner", ImmutableSet.of("d1"),
                new HashSet<>());

            ResultSet emptyLockRs = mock(ResultSet.class);
            when(emptyLockRs.next()).thenReturn(true).thenReturn(false);
            when(emptyLockRs.getInt(1)).thenThrow(new SQLException());
            when(statement.executeQuery(any())).thenReturn(emptyLockRs);
            persistentReadWriteLock.tryReadWriteLockBatch("schemaName", "owner", ImmutableSet.of("d1"),
                new HashSet<>());
        }
    }

    /**
     * Test that when func.apply() throws an exception with empty locks (first func.apply path),
     * the exception is rethrown instead of being swallowed.
     */
    @Test(expected = TddlNestableRuntimeException.class)
    public void testFuncExceptionWithEmptyLocks() throws SQLException {
        try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection conn = mock(Connection.class);
            Statement statement = mock(Statement.class);
            when(conn.createStatement()).thenReturn(statement);
            mockedMetaDbUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(conn);

            PersistentReadWriteLock persistentReadWriteLock = PersistentReadWriteLock.create();
            // empty readLocks and writeLocks triggers the first func.apply() path
            persistentReadWriteLock.tryReadWriteLockBatch("schemaName", "owner",
                new HashSet<>(), new HashSet<>(),
                connection -> {
                    throw new RuntimeException("func failed");
                });
        }
    }

    /**
     * Test that lock-phase exceptions (not from func.apply) still return false for retry.
     */
    @Test
    public void testLockPhaseExceptionReturnsFalse() throws SQLException {
        try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection conn = mock(Connection.class);
            Statement statement = mock(Statement.class);
            when(conn.createStatement()).thenReturn(statement);
            mockedMetaDbUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(conn);
            mockedMetaDbUtil.when(() -> MetaDbUtil.tryGetLock(any(Connection.class), any(String.class), anyLong()))
                .thenCallRealMethod();
            mockedMetaDbUtil.when(() -> MetaDbUtil.releaseLock(any(Connection.class), any(String.class)))
                .thenCallRealMethod();

            // mock GET_LOCK to throw exception (simulates lock-phase failure)
            when(statement.executeQuery(any())).thenThrow(new SQLException("lock failed"));

            PersistentReadWriteLock persistentReadWriteLock = PersistentReadWriteLock.create();
            boolean result = persistentReadWriteLock.tryReadWriteLockBatch("schemaName", "owner",
                new HashSet<>(), ImmutableSet.of("res1"),
                connection -> true);
            Assert.assertFalse("lock-phase exception should return false", result);
        }
    }

    /**
     * Test that when func.apply() throws a deadlock exception,
     * the method returns false (for retry) instead of rethrowing.
     */
    @Test
    public void testFuncDeadlockExceptionReturnsFalse() throws SQLException {
        try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection conn = mock(Connection.class);
            Statement statement = mock(Statement.class);
            when(conn.createStatement()).thenReturn(statement);
            mockedMetaDbUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(conn);

            PersistentReadWriteLock persistentReadWriteLock = PersistentReadWriteLock.create();
            // empty readLocks and writeLocks triggers the first func.apply() path (funcPhase=true)
            boolean result = persistentReadWriteLock.tryReadWriteLockBatch("schemaName", "owner",
                new HashSet<>(), new HashSet<>(),
                connection -> {
                    throw new RuntimeException(
                        "Failed to insert into ddl_engine. Caused by: Deadlock found when trying to get lock");
                });
            Assert.assertFalse("deadlock in func.apply() should return false for retry", result);
        }
    }

    /**
     * Test that a deadlock buried in the exception cause chain is still detected,
     * matching the real-world pattern: TddlRuntimeException wraps SQLException("Deadlock ...").
     */
    @Test
    public void testNestedDeadlockExceptionReturnsFalse() throws SQLException {
        try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection conn = mock(Connection.class);
            Statement statement = mock(Statement.class);
            when(conn.createStatement()).thenReturn(statement);
            mockedMetaDbUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(conn);

            PersistentReadWriteLock persistentReadWriteLock = PersistentReadWriteLock.create();
            boolean result = persistentReadWriteLock.tryReadWriteLockBatch("schemaName", "owner",
                new HashSet<>(), new HashSet<>(),
                connection -> {
                    // Simulate real path: DdlEngineAccessor.insert catches SQLException and wraps it
                    SQLException cause = new SQLException(
                        "Deadlock found when trying to get lock; try restarting transaction");
                    throw new RuntimeException("Failed to insert into the system table `ddl_engine`", cause);
                });
            Assert.assertFalse("nested deadlock in cause chain should return false for retry", result);
        }
    }

    /**
     * Test that when MetaDbUtil.rollback() itself throws (e.g. XConnection rollback failure),
     * the method still returns false instead of propagating the rollback exception.
     */
    @Test
    public void testRollbackFailureStillReturnsFalse() throws SQLException {
        try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection conn = mock(Connection.class);
            Statement statement = mock(Statement.class);
            when(conn.createStatement()).thenReturn(statement);
            mockedMetaDbUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(conn);
            // Make rollback throw, simulating XConnection rollback failure
            mockedMetaDbUtil.when(
                    () -> MetaDbUtil.rollback(any(Connection.class), any(Exception.class), any(), anyString()))
                .thenThrow(new TddlNestableRuntimeException("rollback failed"));

            PersistentReadWriteLock persistentReadWriteLock = PersistentReadWriteLock.create();
            boolean result = persistentReadWriteLock.tryReadWriteLockBatch("schemaName", "owner",
                new HashSet<>(), new HashSet<>(),
                connection -> {
                    throw new RuntimeException(
                        "Deadlock found when trying to get lock; try restarting transaction");
                });
            Assert.assertFalse("rollback failure should not prevent return false", result);
        }
    }
}