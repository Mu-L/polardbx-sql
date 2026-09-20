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

package com.alibaba.polardbx.executor.ddl.newengine.meta;

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.PersistentReadWriteLock;
import com.alibaba.polardbx.gms.metadb.misc.ReadWriteLockAcquireResult;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DDL_SQL_LOAD_REPEATEDLY_MSG;
import static com.alibaba.polardbx.gms.metadb.table.TableInfoManager.PhyInfoSchemaContext.isValidSqlId;

/**
 * For the purpose of avoiding deadlock
 * Always use this class to manage DDL Engine Resources
 */
public class DdlEngineResourceManager {

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    private PersistentReadWriteLock lockManager = PersistentReadWriteLock.create();

    private static final long RETRY_INTERVAL = 1000L;

    private static final Map<String, List<DdlContext>> allLocksTryingToAcquire =
        new ConcurrentHashMap<>(DdlConstants.DEFAULT_LOGICAL_DDL_PARALLELISM * 16);

    /**
     * Check whether the resource is available
     *
     * @return true if available
     */
    public boolean checkResource(Set<String> shared,
                                 Set<String> exclusive) {
        Set<String> resources = Sets.union(shared, exclusive);
        Set<String> blockers = lockManager.queryBlocker(resources);
        return blockers.isEmpty();
    }

    public boolean checkResourcesBelongTo(Long jobId, Set<String> shared, Set<String> exclusive) {
        Set<String> resources = Sets.union(shared, exclusive);
        Set<String> blockers = lockManager.queryBlocker(resources);
        String owner = PersistentReadWriteLock.OWNER_PREFIX + jobId;
        for (String blocker : blockers) {
            if (!blocker.equals(owner)) {
                return false;
            }
        }
        return true;
    }

    public void acquireResource(@NotNull String schemaName,
                                @NotNull long jobId,
                                @NotNull Set<String> shared,
                                @NotNull Set<String> exclusive) {
        acquireResource(schemaName, jobId, __ -> false, shared, exclusive, -1L, (Connection conn) -> true);
    }

    public void acquireResource(@NotNull String schemaName,
                                @NotNull long jobId,
                                @NotNull Predicate shouldInterrupt,
                                @NotNull Set<String> shared,
                                @NotNull Set<String> exclusive,
                                @NotNull long sqlId,
                                @NotNull Function<Connection, Boolean> func) {
        acquireResourceWithCallbacks(schemaName, jobId, shouldInterrupt, shared, exclusive, sqlId, null, func);
    }

    public void acquireResourceWithFirstGrantedCallback(@NotNull String schemaName,
                                                        @NotNull long jobId,
                                                        @NotNull Predicate shouldInterrupt,
                                                        @NotNull Set<String> shared,
                                                        @NotNull Set<String> exclusive,
                                                        @NotNull long sqlId,
                                                        @NotNull Function<Connection, Boolean> firstGrantedFunc) {
        acquireResourceWithCallbacks(schemaName, jobId, shouldInterrupt, shared, exclusive, sqlId, firstGrantedFunc,
            null, true);
    }

    public void acquireResourceWithCallbacks(@NotNull String schemaName,
                                             @NotNull long jobId,
                                             @NotNull Predicate shouldInterrupt,
                                             @NotNull Set<String> shared,
                                             @NotNull Set<String> exclusive,
                                             @NotNull long sqlId,
                                             Function<Connection, Boolean> firstGrantedFunc,
                                             Function<Connection, Boolean> lastGrantedFunc) {
        acquireResourceWithCallbacks(schemaName, jobId, shouldInterrupt, shared, exclusive, sqlId, firstGrantedFunc,
            lastGrantedFunc, false);
    }

    private void acquireResourceWithCallbacks(@NotNull String schemaName,
                                              @NotNull long jobId,
                                              @NotNull Predicate shouldInterrupt,
                                              @NotNull Set<String> shared,
                                              @NotNull Set<String> exclusive,
                                              @NotNull long sqlId,
                                              Function<Connection, Boolean> firstGrantedFunc,
                                              Function<Connection, Boolean> lastGrantedFunc,
                                              boolean invokeFirstCallbackOnWaiting) {
        Preconditions.checkNotNull(schemaName, "schemaName can't be null");
        Preconditions.checkNotNull(shared, "shared resource can't be null");
        Preconditions.checkNotNull(exclusive, "exclusive resource can't be null");
        Pair<Set<String>, Set<String>> rwLocks = inferRwLocks(shared, exclusive);
        Set<String> readLocks = rwLocks.getKey();
        Set<String> writeLocks = rwLocks.getValue();
        String owner = PersistentReadWriteLock.OWNER_PREFIX + jobId;

        final LocalDateTime beginTs = LocalDateTime.now();
        int retryCount = 0;

        if (DynamicConfig.getInstance().enableDdlRwLockFifoWaitingQueue()) {
            acquireResourceWithWaitingQueue(schemaName, owner, shouldInterrupt, readLocks, writeLocks, sqlId,
                firstGrantedFunc, lastGrantedFunc, beginTs, retryCount, invokeFirstCallbackOnWaiting);
            return;
        }

        Function<Connection, Boolean> batchFunc = lastGrantedFunc != null ? lastGrantedFunc :
            (firstGrantedFunc != null ? firstGrantedFunc : (Connection conn) -> true);
        try {
            while (!lockManager.tryReadWriteLockBatch(schemaName, owner, readLocks, writeLocks, batchFunc)) {
                checkIfSqlIdBeforeCheckPoint(sqlId);

                LocalDateTime now = LocalDateTime.now();
                long timeoutMinutes = DynamicConfig.getInstance().getDdlAcquireLockTimeoutMinutes();
                if (now.minusMinutes(timeoutMinutes).isAfter(beginTs)) {
                    throw new TddlNestableRuntimeException("GET DDL LOCK TIMEOUT");
                }

                if (Thread.interrupted() || shouldInterrupt.test(null)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_QUERY_CANCLED);
                }

                try {
                    Thread.sleep(RETRY_INTERVAL);
                } catch (InterruptedException e) {
                    // ignore interrupt
                    Thread.currentThread().interrupt();
                    throw new TddlRuntimeException(ErrorCode.ERR_QUERY_CANCLED);
                }

                //check if there's any failed Job holds the lock
                //if true, don't wait anymore
                Set<String> blockers = lockManager.queryBlocker(Sets.union(shared, exclusive));
                LOGGER.info(String.format(
                    "tryReadWriteLockBatch failed, schemaName:[%s], jobId:[%s], retryCount:[%d], shared:[%s], exclusive:[%s], blockers:[%s]",
                    schemaName, jobId, retryCount++, setToString(shared), setToString(exclusive), setToString(blockers))
                );
                Set<String> ddlBlockers =
                    blockers.stream().filter(e -> StringUtils.startsWith(e, PersistentReadWriteLock.OWNER_PREFIX))
                        .collect(Collectors.toSet());
                if (CollectionUtils.isNotEmpty(ddlBlockers)) {
                    List<DdlEngineRecord> blockerJobRecords = getBlockerJobRecords(schemaName, ddlBlockers);
                    if (CollectionUtils.isEmpty(blockerJobRecords)) {
                        //there are pending locks(which are without jobs).
                        //this situation should not happen
                        //but if we come up with this situation
                        //we release all the pending locks
                        String errMsg = String.format(
                            "found pending locks without DDL jobs, lock id:[%s].",
                            Joiner.on(",").join(ddlBlockers)
                        );
                        LOGGER.error(errMsg);
                        EventLogger.log(EventType.DDL_WARN, errMsg);
                        for (String ddlBlocker : ddlBlockers) {
                            lockManager.unlockReadWriteByOwner(ddlBlocker);
                        }
                    } else {
                        for (DdlEngineRecord record : blockerJobRecords) {
                            DdlState ddlState = DdlState.valueOf(record.state);
                            if (DdlHelper.isTerminated(ddlState)) {
                                StringBuilder sb = new StringBuilder();
                                sb.append(String
                                    .format("Found Paused DDL JOB. You can use 'SHOW DDL %s' to check it for details",
                                        record.jobId));
                                if (record.isSupportContinue() || record.isSupportCancel()) {
                                    sb.append(", ");
                                }
                                if (record.isSupportContinue()) {
                                    sb.append(
                                        String.format("use 'CONTINUE DDL %s' to continue executing it", record.jobId));
                                }
                                if (record.isSupportContinue() && record.isSupportCancel()) {
                                    sb.append(", or ");
                                }
                                if (record.isSupportCancel()) {
                                    sb.append(String.format("use 'CANCEL DDL %s' to cancel it", record.jobId));
                                }
                                sb.append(".");
                                throw new TddlRuntimeException(ErrorCode.ERR_PAUSED_DDL_JOB_EXISTS, sb.toString());
                            }
                        }
                    }
                }
            }
            //exceed the attempt times, still unable to acquire all the locks
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public boolean downGradeWriteLock(Connection connection, long jobId, String writeLock) {
        String owner = PersistentReadWriteLock.OWNER_PREFIX + String.valueOf(jobId);
        return lockManager.downGradeWriteLock(connection, owner, writeLock);
    }

    public int releaseResource(long jobId) {
        String owner = PersistentReadWriteLock.OWNER_PREFIX + String.valueOf(jobId);
        FailPoint.injectCrash("fp_ddl_engine_release_write_lock_crash");
        return lockManager.unlockReadWriteByOwner(owner);
    }

    public int releaseResource(Connection connection, long jobId) {
        String owner = PersistentReadWriteLock.OWNER_PREFIX + String.valueOf(jobId);
        FailPoint.injectCrash("fp_ddl_engine_release_write_lock_crash");
        return lockManager.unlockReadWriteByOwner(connection, owner);
    }

    private void acquireResourceWithWaitingQueue(String schemaName,
                                                 String owner,
                                                 Predicate shouldInterrupt,
                                                 Set<String> readLocks,
                                                 Set<String> writeLocks,
                                                 long sqlId,
                                                 Function<Connection, Boolean> firstGrantedFunc,
                                                 Function<Connection, Boolean> lastGrantedFunc,
                                                 LocalDateTime beginTs,
                                                 int retryCount,
                                                 boolean invokeFirstCallbackOnWaiting) {
        List<String> allLocks = Lists.newArrayList(Sets.union(readLocks, writeLocks));
        allLocks.sort(String::compareTo);
        if (CollectionUtils.isEmpty(allLocks)) {
            Function<Connection, Boolean> resourceCallback =
                getResourceCallback(0, 1, firstGrantedFunc, lastGrantedFunc);
            lockManager.tryReadWriteLockBatch(schemaName, owner, readLocks, writeLocks,
                resourceCallback == null ? (Connection conn) -> true : resourceCallback);
            return;
        }

        Set<String> newlyGrantedResources = new HashSet<>();
        Set<String> waitingResources = new HashSet<>();
        boolean queuedUpdated = false;
        try {
            for (int i = 0; i < allLocks.size(); i++) {
                String resource = allLocks.get(i);
                boolean write = writeLocks.contains(resource);
                Function<Connection, Boolean> resourceCallback =
                    getResourceCallback(i, allLocks.size(), firstGrantedFunc, lastGrantedFunc);
                while (true) {
                    ReadWriteLockAcquireResult acquireResult = lockManager.tryReadWriteLockOneResourceWithWaitingQueue(
                        schemaName, owner, resource, write, resourceCallback,
                        invokeFirstCallbackOnWaiting && i == 0);
                    if (acquireResult == ReadWriteLockAcquireResult.ALREADY_HELD) {
                        waitingResources.remove(resource);
                        break;
                    }
                    if (acquireResult == ReadWriteLockAcquireResult.NEWLY_GRANTED) {
                        waitingResources.remove(resource);
                        newlyGrantedResources.add(resource);
                        break;
                    }
                    waitingResources.add(resource);
                    checkIfSqlIdBeforeCheckPoint(sqlId);

                    LocalDateTime now = LocalDateTime.now();
                    long timeoutMinutes = DynamicConfig.getInstance().getDdlAcquireLockTimeoutMinutes();
                    if (now.minusMinutes(timeoutMinutes).isAfter(beginTs)) {
                        throw new TddlNestableRuntimeException("GET DDL LOCK TIMEOUT");
                    }

                    if (Thread.interrupted() || shouldInterrupt.test(null)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_QUERY_CANCLED);
                    }

                    try {
                        Thread.sleep(RETRY_INTERVAL);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new TddlRuntimeException(ErrorCode.ERR_QUERY_CANCLED);
                    }

                    Set<String> blockers = lockManager.queryBlocker(Sets.newHashSet(resource));
                    int currentRetryCount = ++retryCount;
                    LOGGER.info(String.format(
                        "tryReadWriteLockOneResource failed, schemaName:[%s], owner:[%s], retryCount:[%d], resource:[%s], write:[%s], blockers:[%s]",
                        schemaName, owner, currentRetryCount, resource, write, setToString(blockers))
                    );
                    processResourceBlockers(schemaName, owner, blockers, currentRetryCount);
                }
            }
            queuedUpdated = true;
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        } finally {
            if (!queuedUpdated) {
                lockManager.unlockReadWriteByOwner(owner, newlyGrantedResources, waitingResources);
            }
        }
    }

    private Function<Connection, Boolean> getResourceCallback(int index,
                                                              int size,
                                                              Function<Connection, Boolean> firstGrantedFunc,
                                                              Function<Connection, Boolean> lastGrantedFunc) {
        boolean first = index == 0;
        boolean last = index == size - 1;
        if (first && last && firstGrantedFunc != null && lastGrantedFunc != null) {
            return connection -> firstGrantedFunc.apply(connection) && lastGrantedFunc.apply(connection);
        }
        if (first && firstGrantedFunc != null) {
            return firstGrantedFunc;
        }
        if (last && lastGrantedFunc != null) {
            return lastGrantedFunc;
        }
        return null;
    }

    private void processResourceBlockers(String schemaName,
                                         String owner,
                                         Set<String> blockers,
                                         int retryCount) {
        Set<String> ddlBlockers = blockers.stream()
            .filter(e -> StringUtils.startsWith(e, PersistentReadWriteLock.OWNER_PREFIX))
            .collect(Collectors.toSet());
        if (CollectionUtils.isNotEmpty(ddlBlockers)) {
            List<DdlEngineRecord> blockerJobRecords = getBlockerJobRecords(schemaName, ddlBlockers);
            if (CollectionUtils.isEmpty(blockerJobRecords)) {
                String errMsg = String.format(
                    "found pending locks without DDL jobs, lock id:[%s].",
                    Joiner.on(",").join(ddlBlockers)
                );
                LOGGER.error(errMsg);
                EventLogger.log(EventType.DDL_WARN, errMsg);
                for (String ddlBlocker : ddlBlockers) {
                    lockManager.unlockReadWriteByOwner(ddlBlocker);
                }
                return;
            }
            for (DdlEngineRecord record : blockerJobRecords) {
                DdlState ddlState = DdlState.valueOf(record.state);
                if (DdlHelper.isTerminated(ddlState)) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(String
                        .format("Found Paused DDL JOB. You can use 'SHOW DDL %s' to check it for details",
                            record.jobId));
                    if (record.isSupportContinue() || record.isSupportCancel()) {
                        sb.append(", ");
                    }
                    if (record.isSupportContinue()) {
                        sb.append(String.format("use 'CONTINUE DDL %s' to continue executing it", record.jobId));
                    }
                    if (record.isSupportContinue() && record.isSupportCancel()) {
                        sb.append(", or ");
                    }
                    if (record.isSupportCancel()) {
                        sb.append(String.format("use 'CANCEL DDL %s' to cancel it", record.jobId));
                    }
                    sb.append(".");
                    throw new TddlRuntimeException(ErrorCode.ERR_PAUSED_DDL_JOB_EXISTS, sb.toString());
                }
            }
        }

        int detectionInterval = DynamicConfig.getInstance().getDdlRwLockDeadlockDetectionInterval();
        if (detectionInterval > 0 && retryCount % detectionInterval == 0
            && lockManager.shouldAbortForDeadlock(owner)) {
            LOGGER.warn(String.format(
                "DDL owner %s is selected as victim by DDL RW-lock deadlock detection. schemaName:[%s], retryCount:[%d]",
                owner, schemaName, retryCount));
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED,
                String.format("DDL owner %s is selected as victim by DDL RW-lock deadlock detection.", owner));
        }
    }

    /**
     * infer all the read/write locks need to acquire from resources
     * if a resource exists in both shared and exclusive sets, a write lock is needed
     */
    private Pair<Set<String>, Set<String>> inferRwLocks(Set<String> shared, Set<String> exclusive) {
        Set<String> writeLocks = Sets.newHashSet(exclusive);
        Set<String> readLocks = Sets.newHashSet(shared);
        readLocks = Sets.filter(readLocks, e -> !writeLocks.contains(e));
        return Pair.of(readLocks, writeLocks);
    }

    private List<DdlEngineRecord> getBlockerJobRecords(String schemaName, Set<String> blockerSet) {
        Set<Long> jobIdSet = PersistentReadWriteLock.toJobIdSet(blockerSet);
        List<DdlEngineRecord> result = new DdlEngineAccessorDelegate<List<DdlEngineRecord>>() {
            @Override
            protected List<DdlEngineRecord> invoke() {
                return engineAccessor.query(Lists.newArrayList(jobIdSet));
            }
        }.execute();
        return result;
    }

    private String setToString(Set<String> lockSet) {
        if (CollectionUtils.isEmpty(lockSet)) {
            return "";
        }
        return Joiner.on(",").join(lockSet);
    }

    public static void startAcquiringLock(String schemaName, DdlContext ddlContext) {
        synchronized (allLocksTryingToAcquire) {
            if (!allLocksTryingToAcquire.containsKey(schemaName)) {
                allLocksTryingToAcquire.put(schemaName, new ArrayList<>());
            }
            allLocksTryingToAcquire.get(schemaName).add(ddlContext);
        }
    }

    public static void finishAcquiringLock(String schemaName, DdlContext ddlContext) {
        synchronized (allLocksTryingToAcquire) {
            if (allLocksTryingToAcquire.containsKey(schemaName)) {
                allLocksTryingToAcquire.get(schemaName).remove(ddlContext);
                if (CollectionUtils.isEmpty(allLocksTryingToAcquire.get(schemaName))) {
                    allLocksTryingToAcquire.remove(schemaName);
                }
            }
        }
    }

    public static List<DdlContext> getAllDdlAcquiringLocks(String schemaName) {
        List<DdlContext> result = new ArrayList<>();
        synchronized (allLocksTryingToAcquire) {
            if (CollectionUtils.isEmpty(allLocksTryingToAcquire.get(schemaName))) {
                return result;
            }
            result.addAll(allLocksTryingToAcquire.get(schemaName));
            return result;
        }
    }

    public static void checkIfSqlIdBeforeCheckPoint(Long sqlId) {
        if (isValidSqlId(sqlId)) {
            long processedSqlId = -1L;
            try {
                Long gdnCheckPoint = MetaDbUtil.queryDdlLoadCheckPoint();
                processedSqlId = gdnCheckPoint == null ? 0 : gdnCheckPoint;
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "failed to query cdc checkpoint because of " + e.getMessage());
            }
            if (processedSqlId >= sqlId) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format(DDL_SQL_LOAD_REPEATEDLY_MSG, sqlId, processedSqlId));
            }
        }
    }

}
