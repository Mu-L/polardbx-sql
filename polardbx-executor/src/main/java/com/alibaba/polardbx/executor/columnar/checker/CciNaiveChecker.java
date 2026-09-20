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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_FAIL_DURING_CAL_COLUMNAR_HASH;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_FAIL_DURING_CAL_PRIMARY_HASH;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_SLEEP_DURING_CHECK_CCI;

/**
 * @author yaozhili
 */
public class CciNaiveChecker extends AbstractCciChecker {
    private long primaryCount = -1;
    private long columnarCount = -1;
    private long primaryPkHashCode = -1;
    private long columnarPkHashCode = -1;
    private static final String CALCULATE_PRIMARY_HASH =
        "select count(0) as count, check_sum_v2(%s) as pk_checksum, check_sum_v2(*) as checksum "
            + "from %s force index(primary)";
    private static final String CALCULATE_COLUMNAR_HASH =
        "select count(0) as count, check_sum_v2(%s) as pk_checksum, check_sum_v2(*) as checksum "
            + "from %s force index(%s)";
    protected static final String PRIMARY_HINT = "/*+TDDL:SOCKET_TIMEOUT=259200000 %s */";
    protected static final String COLUMNAR_HINT =
        "/*+TDDL:WORKLOAD_TYPE=AP ENABLE_COLUMNAR_OPTIMIZER=true "
            + "OPTIMIZER_TYPE='columnar' ENABLE_HTAP=true SOCKET_TIMEOUT=259200000 "
            + "MPP_TASK_MAX_RUN_TIME=259200000 %s */";

    public CciNaiveChecker(String schemaName, String tableName, String indexName) {
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
            // 1. crete read view for innodb to prevent purge.
            AbstractCciChecker.createReadViewForInnodb(conn, tableName);
            // 2. columnar flush to make consistent point.
            interruptIfDdlCanceled(threadPool, baseEc,
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
                            calPrimaryHashCode(stmt, baseEc, tso.get().getKey());
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

                log("Primary checksum calculated, costing " + ((System.nanoTime() - start) / 1_000_000) + " ms");
            } catch (Throwable t) {
                handleError(t);
                error(String.format("Error occurs when checking primary index %s.%s", tableName, indexName), t);
                columnarTrx.close();
                throw t;
            }
            // 5. close innodb connection.
        }
        // 6. get columnar table hash.
        try (IInnerConnection conn = connManager.getConnection(schemaName);
            Statement stmt = conn.createStatement()) {
            long start = System.nanoTime();

            interruptIfDdlCanceled(
                threadPool, baseEc,
                () -> {
                    try {
                        calColumnarHashCode(stmt, baseEc, tso.get().getValue());
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

            log("Columnar checksum calculated, costing " + ((System.nanoTime() - start) / 1_000_000) + " ms");
            // 7. close columnar connection.
        } catch (Throwable t) {
            handleError(t);
            error(String.format("Error occurs when checking columnar index %s.%s", tableName, indexName), t);
            throw t;
        } finally {
            // 8. close columnar trx.
            columnarTrx.close();
        }
    }

    @Override
    protected void log(String msg) {
        SQLRecorderLogger.ddlLogger.warn("[CCI Naive Checker] " + msg);
    }

    @Override
    protected void error(String msg, Throwable t) {
        SQLRecorderLogger.ddlLogger.error("[CCI Naive Checker] " + msg, t);
    }

    @Override
    public boolean getCheckReports(Collection<String> reports) {
        super.getCheckReports(reports);
        boolean success = true;
        if (-1 == primaryCount || primaryCount != columnarCount) {
            // Check fail.
            reports.add("Inconsistency detected: primary count: " + primaryCount
                + ", columnar count: " + columnarCount);
            success = false;
        }

        if (-1 == primaryPkHashCode || primaryPkHashCode != columnarPkHashCode) {
            // Check fail.
            reports.add("Inconsistency detected: primary pk hash: " + primaryPkHashCode
                + ", columnar pk hash: " + columnarPkHashCode);
            success = false;
        }

        if (-1 == primaryHashCode || primaryHashCode != columnarHashCode) {
            // Check fail.
            reports.add("Inconsistency detected: primary hash: " + primaryHashCode
                + ", columnar hash: " + columnarHashCode);
            success = false;
        }

        if (!success) {
            reports.add("Primary table check sql: " + primaryCheckSql
                + "\nColumnar table check sql: " + columnarCheckSql);
        } else {
            // clear errors if succeeded.
            reports.clear();
        }

        return success;
    }

    /**
     * Calculate checksum hash for primary table, using given tso.
     */
    private void calPrimaryHashCode(Statement stmt, ExecutionContext ec, long tso) throws SQLException {
        if (FailPoint.isKeyEnable(FP_FAIL_DURING_CAL_PRIMARY_HASH)) {
            throw new RuntimeException(FP_FAIL_DURING_CAL_PRIMARY_HASH);
        }
        if (FailPoint.isKeyEnable(FP_SLEEP_DURING_CHECK_CCI)) {
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // Build pk list.
        String pkList = ec
            .getSchemaManager(schemaName)
            .getTable(tableName)
            .getPrimaryKey()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.joining(","));
        // checksum sql for primary table
        String sql = getPrimarySql(ec, tso, pkList);
        log("Check CCI primary sql: " + sql);
        primaryCheckSql = sql;
        // execution plan for this sql
        ResultSet explainRs = stmt.executeQuery("explain " + sql);
        StringBuilder explainResult = new StringBuilder();
        while (explainRs.next()) {
            explainResult.append(explainRs.getString(1)).append("\n");
        }
        log("Check CCI primary sql plan: \n" + explainResult);
        if (!explainResult.toString().contains("LogicalView")) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Check cci plan does not contain LogicalView");
        }

        ResultSet rs = stmt.executeQuery(sql);
        if (rs.next()) {
            primaryCount = rs.getLong("count");
            primaryPkHashCode = rs.getLong("pk_checksum");
            primaryHashCode = rs.getLong("checksum");
        }
    }

    private void calColumnarHashCode(Statement stmt, ExecutionContext ec, long tso) throws SQLException {
        if (FailPoint.isKeyEnable(FP_FAIL_DURING_CAL_COLUMNAR_HASH)) {
            throw new RuntimeException(FP_FAIL_DURING_CAL_COLUMNAR_HASH);
        }
        // Build pk list.
        String pkList = ec
            .getSchemaManager(schemaName)
            .getTable(tableName)
            .getPrimaryKey()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.joining(","));
        // checksum sql for columnar index
        String sql = getColumnarSql(ec, tso, pkList);
        log("Check CCI columnar sql: " + sql);
        columnarCheckSql = sql;
        // execution plan for this sql
        ResultSet explainRs = stmt.executeQuery("explain " + sql);
        StringBuilder explainResult = new StringBuilder();
        while (explainRs.next()) {
            explainResult.append(explainRs.getString(1)).append("\n");
        }
        log("Check CCI columnar sql plan: \n" + explainResult);
        if (!explainResult.toString().contains("OSSTableScan")) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Check cci plan does not contain OSSTableScan");
        }

        ResultSet rs = stmt.executeQuery(sql);
        if (rs.next()) {
            columnarCount = rs.getLong("count");
            columnarPkHashCode = rs.getLong("pk_checksum");
            columnarHashCode = rs.getLong("checksum");
        }
    }

    protected String getColumnarSql(ExecutionContext ec, long tso, String pkList) {
        // Build hint.
        StringBuilder sb = new StringBuilder();
        setBasicHint(ec, sb);
        sb.append(" SNAPSHOT_TS=")
            .append(tso)
            .append(" ");
        String hint = String.format(COLUMNAR_HINT, sb);
        String sql = String.format(CALCULATE_COLUMNAR_HASH, pkList, tableName, indexName);
        return hint + sql;
    }

    protected String getPrimarySql(ExecutionContext ec, long tso, String pkList) {
        // Build hint.
        StringBuilder sb = new StringBuilder();
        setBasicHint(ec, sb);
        sb.append(" SNAPSHOT_TS=")
            .append(tso)
            .append(" ");
        sb.append(" TRANSACTION_POLICY=TSO");
        String hint = String.format(PRIMARY_HINT, sb);
        String sql = String.format(CALCULATE_PRIMARY_HASH, pkList, tableName);
        return hint + sql;
    }
}
