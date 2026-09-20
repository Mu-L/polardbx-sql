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

package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.IInnerConnectionManager;
import com.alibaba.polardbx.common.RevisableOrderInvariantHash;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.oss.IDeltaReadOption;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.mpp.split.SpecifiedOssSplit;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplifiedWithChecksum;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.util.concurrent.Futures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_FAIL_CSV_CHECKSUM;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_FAIL_DELETE_CHECKSUM;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_FAIL_DURING_CAL_COLUMNAR_HASH;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_FAIL_DURING_CAL_PRIMARY_HASH;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_FORCE_CAL_IN_NAIVE_METHOD;

/**
 * @author yaozhili
 */
public class CciFastChecker extends AbstractCciChecker {
    private final Lock hashLock = new ReentrantLock();
    /**
     * Record connection id in use.
     * If the checking thread is interrupted, kill these connections.
     */
    private final Set<IInnerConnection> connections = new ConcurrentSkipListSet<>(
        (o1, o2) -> {
            if (o1 == null && o2 == null) {
                return 0;
            }
            if (o1 == null) {
                return -1;
            }
            if (o2 == null) {
                return 1;
            }
            return o1.hashCode() - o2.hashCode();
        }
    );

    private static final String CALCULATE_PRIMARY_HASH =
        "select check_sum_v2(*) as checksum from %s force index(primary)";
    private static final String CALCULATE_COLUMNAR_HASH =
        "select check_sum_v2(*) as checksum from %s force index(%s)";

    protected static final String PRIMARY_HINT =
        "/*+TDDL:ENABLE_ORC_RAW_TYPE_BLOCK=true SOCKET_TIMEOUT=259200000 %s */";
    protected static final String COLUMNAR_HINT =
        "/*+TDDL:WORKLOAD_TYPE=AP ENABLE_COLUMNAR_OPTIMIZER=true OPTIMIZER_TYPE='columnar' "
            + "ENABLE_HTAP=true ENABLE_BLOCK_CACHE=false ENABLE_ORC_RAW_TYPE_BLOCK=true "
            + "SOCKET_TIMEOUT=259200000 MPP_TASK_MAX_RUN_TIME=259200000 %s */";

    public CciFastChecker(String schemaName, String tableName, String indexName) {
        super(schemaName, tableName, indexName);
    }

    @Override
    public void checkSnapshot(ExecutionContext baseEc) throws Throwable {
        /*
        1. crete read view for innodb to prevent purge.
        2. columnar flush to make consistent point.
        3. create columnar trx to prevent purge.
        4. get primary table hash.
        5. close innodb connection.
        6. get columnar table hash.
        7. close columnar connection.
        8. close columnar trx.
         */
        AtomicReference<Pair<Long, Long>> tso = new AtomicReference<>(null);
        ITransaction columnarTrx;
        try (IInnerConnection conn = connManager.getConnection(schemaName)) {
            conn.setTimeZone("+8:00");
            // 1. crete read view for innodb to prevent purge.
            AbstractCciChecker.createReadViewForInnodb(conn, tableName);
            // 2. columnar flush to make consistent point.
            interruptIfDdlCanceled(
                threadPool, baseEc,
                () -> tso.set(getAndWaitColumnarFlush(schemaName, indexName)),
                null);
            log("Check cci using innodb tso " + tso.get().getKey() + ", columnar tso " + tso.get().getValue());
            // 3. create columnar trx to prevent purge.
            columnarTrx = ExecUtils.createColumnarTransaction(schemaName, baseEc, tso.get().getValue());
            // 4. get primary table hash.
            try (Statement stmt = conn.createStatement()) {
                long start = System.nanoTime();

                interruptIfDdlCanceled(
                    threadPool, baseEc,
                    () -> {
                        try {
                            calculatePrimaryChecksum(stmt, baseEc, tso.get().getKey());
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    },
                    () -> {
                        // KILL: force close
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            handleError(e);
                        }
                    });

                log("Primary checksum calculated, costing " + ((System.nanoTime() - start) / 1_000_000) + " ms");
                log("primary checksum: " + primaryHashCode);
            } catch (Throwable t) {
                handleError(t);
                error("Get primary table hash failed.", t);
                columnarTrx.close();
                throw t;
            }
            // 5. close innodb connection.
        }
        try {
            long start = System.nanoTime();

            // 6. get columnar table hash. 7. close columnar connection.
            calculateColumnarChecksum(baseEc, tso.get().getValue(), threadPool, connManager);

            log("Columnar checksum calculated, costing " + ((System.nanoTime() - start) / 1_000_000) + " ms");
            log("columnar checksum: " + columnarHashCode);
        } catch (Throwable t) {
            handleError(t);
            error("Get columnar hash failed.", t);
            throw t;
        } finally {
            // 8. close columnar trx.
            columnarTrx.close();
        }
    }

    protected void calculatePrimaryChecksum(Statement stmt, ExecutionContext baseEc, long tso) throws SQLException {
        if (FailPoint.isKeyEnable(FP_FAIL_DURING_CAL_PRIMARY_HASH)) {
            throw new RuntimeException(FP_FAIL_DURING_CAL_PRIMARY_HASH);
        }
        // Calculate primary checksum in this thread.
        try {
            final String sql = getPrimarySql(baseEc, tso);
            log("primary checksum sql: " + sql);
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                primaryHashCode = rs.getLong("checksum");
            }
        } catch (Throwable t) {
            handleError(t);
            error("Calculate primary table hash failed.", t);
            throw t;
        }
    }

    protected void calculateColumnarChecksum(ExecutionContext baseEc,
                                             long tso,
                                             ServerThreadPool threadPool,
                                             IInnerConnectionManager connectionManager) throws SQLException {
        if (FailPoint.isKeyEnable(FP_FAIL_DURING_CAL_COLUMNAR_HASH)) {
            throw new RuntimeException(FP_FAIL_DURING_CAL_COLUMNAR_HASH);
        }
        // 1. Get all orc/csv files of this CCI.
        long startTime = System.nanoTime();
        final long tableId = getTableId(schemaName, indexName);
        log("[Fast checker] Get table id cost: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");

        startTime = System.nanoTime();
        List<FilesRecordSimplifiedWithChecksum> filesRecords = getFilesRecords(tso, tableId, schemaName);
        log("[Fast checker] Get all files cost: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");

        // 2. Filter out files needed to be process, and by the way calculate cached checksum.
        Map<String, Set<String>> orcFiles = new HashMap<>();
        Set<String> deltaFiles = new HashSet<>();
        final RevisableOrderInvariantHash hasher = new RevisableOrderInvariantHash();
        final Map<String, Set<String>> toBeProcessedOrcFiles = new HashMap<>();
        for (FilesRecordSimplifiedWithChecksum filesRecord : filesRecords) {
            String fileName = filesRecord.fileName;
            String partitionName = filesRecord.partitionName.toLowerCase();
            ColumnarFileType columnarFileType =
                ColumnarFileType.of(fileName.substring(fileName.lastIndexOf('.') + 1));
            switch (columnarFileType) {
            case ORC:
                orcFiles.computeIfAbsent(partitionName, k -> new HashSet<>()).add(fileName);
                hasher.add(filesRecord.checksum).remove(0);
                if (0 != filesRecord.deletedChecksum) {
                    toBeProcessedOrcFiles.computeIfAbsent(partitionName, k -> new HashSet<>()).add(fileName);
                }
                break;
            case CSV:
            case DEL:
                deltaFiles.add(fileName);
                break;
            default:
                log("Increment check found unexpected file: " + fileName);
                break;
            }
        }
        log("all orc files checksum: " + hasher.getResult());

        startTime = System.nanoTime();
        // csv/del file name -> pair(partition name, end pos)
        Map<String, Pair<String, Long>> tuples = new HashMap<>();
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarAppendedFilesAccessor accessor = new ColumnarAppendedFilesAccessor();
            accessor.setConnection(connection);
            accessor.queryByFileNamesAndTso(deltaFiles, tso).forEach(record -> {
                String fileName = record.getFileName();
                long start = record.getAppendOffset();
                long end = start + record.getAppendLength();
                String partName = record.getPartName();
                tuples.put(fileName, new Pair<>(partName, end));
            });
        } catch (Throwable t) {
            error("Failed to diff csv files", t);
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER, t, "Failed to diff csv files");
        }
        log("Get all delta files info cost: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");

        Map<String, IDeltaReadOption> deltas = new HashMap<>();
        AtomicBoolean hasCsvFiles = new AtomicBoolean(false);
        for (Map.Entry<String, Pair<String, Long>> deltaFileEntry : tuples.entrySet()) {
            String fileName = deltaFileEntry.getKey();
            String partName = deltaFileEntry.getValue().getKey();
            long endPos = deltaFileEntry.getValue().getValue();
            deltas.compute(partName, (k, v) -> {
                if (v == null) {
                    v = new SpecifiedOssSplit.DeltaReadWithPositionOption(tso, -1, -1, tableId);
                }
                ColumnarFileType columnarFileType =
                    ColumnarFileType.of(fileName.substring(fileName.lastIndexOf('.') + 1));
                SpecifiedOssSplit.DeltaReadWithPositionOption delta = (SpecifiedOssSplit.DeltaReadWithPositionOption) v;
                switch (columnarFileType) {
                case CSV:
                    hasCsvFiles.set(true);
                    if (delta.getCsvFiles() == null) {
                        delta.setCsvFiles(new ArrayList<>());
                        delta.setCsvStartPos(new ArrayList<>());
                        delta.setCsvEndPos(new ArrayList<>());
                    }
                    delta.getCsvFiles().add(fileName);
                    delta.getCsvStartPos().add(0L);
                    delta.getCsvEndPos().add(endPos);
                    break;
                case DEL:
                    if (delta.getDelFiles() == null) {
                        delta.setDelFiles(new ArrayList<>());
                        delta.setDelBeginPos(new ArrayList<>());
                        delta.setDelEndPos(new ArrayList<>());
                    }
                    delta.getDelFiles().add(fileName);
                    delta.getDelBeginPos().add(0L);
                    delta.getDelEndPos().add(endPos);
                    break;
                default:
                    log("Cci fast check found unexpected file: " + fileName);
                    break;
                }
                return v;
            });
        }

        // 3. RTT 2: Calculate deleted checksum.
        final Future deletedChecksumFuture = toBeProcessedOrcFiles.isEmpty() ? Futures.immediateFuture(null) :
            threadPool.submit(null, null, () -> {
                MDC.put(MDC.MDC_KEY_APP, schemaName);
                calDeletedChecksum(baseEc, connectionManager, hasher, toBeProcessedOrcFiles, deltas);
            });

        // 4. RTT 2: Calculate csv checksum.
        final Future csvFuture = !hasCsvFiles.get() ? Futures.immediateFuture(null) :
            threadPool.submit(null, null, () -> {
                MDC.put(MDC.MDC_KEY_APP, schemaName);
                calCsvChecksum(baseEc, connectionManager, hasher, deltas);
            });

        interruptIfDdlCanceled(
            threadPool, baseEc,
            () -> {
                try {
                    deletedChecksumFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    csvFuture.cancel(true);
                    forceCloseConnections();
                    throw new RuntimeException(e);
                }
                try {
                    csvFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    forceCloseConnections();
                    throw new RuntimeException(e);
                }
            },
            () -> {
                deletedChecksumFuture.cancel(true);
                csvFuture.cancel(true);
                forceCloseConnections();
            });

        columnarHashCode = hasher.getResult();

        if (FailPoint.isKeyEnable(FP_FORCE_CAL_IN_NAIVE_METHOD)
            || (-1 != primaryHashCode && columnarHashCode != primaryHashCode)) {
            log("Columnar checksum not match, innodb checksum: " + primaryHashCode
                + ", columnar checksum: " + columnarHashCode);
            if (baseEc.isEnableCciNaiveCheckIfFastCheckerFailed()) {
                // Use naive method to check again.
                calColumnarChecksumInNaiveMethod(orcFiles, deltas, baseEc, threadPool, connectionManager);
            }
        }
    }

    private void calCsvChecksum(ExecutionContext baseEc, IInnerConnectionManager connectionManager,
                                RevisableOrderInvariantHash hasher, Map<String, IDeltaReadOption> deltas) {
        if (FailPoint.isKeyEnable(FP_FAIL_CSV_CHECKSUM)) {
            throw new RuntimeException(FP_FAIL_CSV_CHECKSUM);
        }
        try (IInnerConnection conn = connectionManager.getConnection(schemaName);
            Statement stmt = conn.createStatement()) {
            connections.add(conn);
            conn.addExecutionContextInjectHook(
                e -> ((ExecutionContext) e).setReadDeltaFiles(deltas)
            );
            String sql = getCsvSql(baseEc);
            log("columnar csv checksum sql: " + sql);
            long startTime = System.nanoTime();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                long csvChecksum = rs.getLong("checksum");
                log("columnar csv checksum: " + csvChecksum);
                hashLock.lock();
                try {
                    hasher.add(csvChecksum).remove(0);
                } finally {
                    hashLock.unlock();
                }
            } else {
                throw new RuntimeException("Not found any csv checksum.");
            }
            log("Calculate csv checksum cost: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");
            conn.clearExecutionContextInjectHooks();
            connections.remove(conn);
        } catch (Throwable t) {
            handleError(t);
            throw new RuntimeException("Failed to calculate csv checksum.", t);
        }
    }

    private void calDeletedChecksum(ExecutionContext baseEc,
                                    IInnerConnectionManager connectionManager,
                                    RevisableOrderInvariantHash hasher,
                                    Map<String, Set<String>> toBeProcessedOrcFiles,
                                    Map<String, IDeltaReadOption> deltas) {
        if (FailPoint.isKeyEnable(FP_FAIL_DELETE_CHECKSUM)) {
            throw new RuntimeException(FP_FAIL_DELETE_CHECKSUM);
        }
        try (IInnerConnection conn = connectionManager.getConnection(schemaName);
            Statement stmt = conn.createStatement()) {
            connections.add(conn);
            conn.addExecutionContextInjectHook(
                (ec) -> {
                    ((ExecutionContext) ec).setReadOrcFiles(toBeProcessedOrcFiles);
                    ((ExecutionContext) ec).setReadDeltaFiles(deltas);
                });
            String sql = getDeletedSql(baseEc);

            log("columnar deleted checksum sql: " + sql);

            long startTime = System.nanoTime();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                long deletedChecksum = rs.getLong("checksum");
                log("columnar deleted checksum: " + deletedChecksum);
                hashLock.lock();
                try {
                    hasher.remove(deletedChecksum).add(0);
                } finally {
                    hashLock.unlock();
                }
            } else {
                throw new RuntimeException("Columnar deleted checksum is empty.");
            }
            log("Calculate deleted checksum cost: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");
            conn.clearExecutionContextInjectHooks();
            connections.remove(conn);
        } catch (Throwable t) {
            handleError(t);
            throw new RuntimeException(t);
        }
    }

    private void calColumnarChecksumInNaiveMethod(Map<String, Set<String>> orcFiles,
                                                  Map<String, IDeltaReadOption> deltas,
                                                  ExecutionContext baseEc, ServerThreadPool threadPool,
                                                  IInnerConnectionManager connectionManager) {
        try (IInnerConnection conn = connectionManager.getConnection(schemaName);
            Statement stmt = conn.createStatement()) {
            conn.addExecutionContextInjectHook(
                (ec) -> {
                    ((ExecutionContext) ec).setReadOrcFiles(orcFiles);
                    ((ExecutionContext) ec).setReadDeltaFiles(deltas);
                });
            String sql = getColumnarNaiveSql(baseEc);
            log("columnar naive checksum sql: " + sql);
            interruptIfDdlCanceled(
                threadPool, baseEc,
                () -> {
                    try {
                        ResultSet rs = stmt.executeQuery(sql);
                        if (rs.next()) {
                            columnarHashCode = rs.getLong("checksum");
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                },
                () -> {
                    // KILL: force close
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        handleError(e);
                        error("Close connection failed,", e);
                    }
                });
            conn.clearExecutionContextInjectHooks();
        } catch (Throwable t) {
            handleError(t);
            error("Calculate columnar checksum in naive method failed.", t);
            throw new RuntimeException("Calculate columnar checksum in naive method failed.");
        }
    }

    private void forceCloseConnections() {
        for (IInnerConnection connection : connections) {
            try {
                // KILL: force close
                connection.close();
            } catch (Throwable t) {
                // ignore
                handleError(t);
            }
        }
    }

    @Override
    protected void log(String msg) {
        SQLRecorderLogger.ddlLogger.warn("[CCI Fast Checker] " + msg);
    }

    @Override
    protected void error(String msg, Throwable t) {
        SQLRecorderLogger.ddlLogger.error("[CCI Fast Checker] " + msg, t);
    }

    @Override
    public boolean getCheckReports(Collection<String> reports) {
        super.getCheckReports(reports);
        boolean success = true;
        if (-1 == primaryHashCode || primaryHashCode != columnarHashCode) {
            // Check fail.
            reports.add("Inconsistency detected: primary hash: " + primaryHashCode
                + ", columnar hash: " + columnarHashCode);
            success = false;
        }
        if (success) {
            reports.clear();
        }
        return success;
    }

    protected String getPrimarySql(ExecutionContext baseEc, long tso) {
        StringBuilder sb = new StringBuilder();
        setBasicHint(baseEc, sb);
        sb.append(" SNAPSHOT_TS=")
            .append(tso);
        sb.append(" TRANSACTION_POLICY=TSO");
        String hint = String.format(PRIMARY_HINT, sb);
        return hint + String.format(CALCULATE_PRIMARY_HASH, tableName);
    }

    protected String getCsvSql(ExecutionContext baseEc) {
        StringBuilder sb = new StringBuilder(" READ_CSV_ONLY=true");
        setBasicHint(baseEc, sb);

        String hint = String.format(COLUMNAR_HINT, sb);
        return hint + String.format(CALCULATE_COLUMNAR_HASH, tableName, indexName);
    }

    protected String getDeletedSql(ExecutionContext baseEc) {
        StringBuilder sb = new StringBuilder(" READ_ORC_ONLY=true ENABLE_OSS_DELETED_SCAN=true ");
        setBasicHint(baseEc, sb);
        String hint = String.format(COLUMNAR_HINT, sb);
        return hint + String.format(CALCULATE_COLUMNAR_HASH, tableName, indexName);
    }

    protected String getColumnarNaiveSql(ExecutionContext baseEc) {
        StringBuilder sb = new StringBuilder(" READ_SPECIFIED_COLUMNAR_FILES=true ");
        setBasicHint(baseEc, sb);
        String hint = String.format(COLUMNAR_HINT, sb);
        return hint + String.format(CALCULATE_COLUMNAR_HASH, tableName, indexName);
    }

    public long getColumnarHashCode() {
        return columnarHashCode;
    }
}
