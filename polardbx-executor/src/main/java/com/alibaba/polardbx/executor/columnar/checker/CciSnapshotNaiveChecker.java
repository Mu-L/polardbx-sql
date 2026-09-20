package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

public class CciSnapshotNaiveChecker extends CciNaiveChecker {
    private static final String CALCULATE_PRIMARY_HASH =
        "select count(0) as count, check_sum_v2(%s) as pk_checksum, check_sum_v2(*) as checksum "
            + "from %s as of tso %s force index(primary)";
    private static final String CALCULATE_COLUMNAR_HASH =
        "select count(0) as count, check_sum_v2(%s) as pk_checksum, check_sum_v2(*) as checksum "
            + "from %s as of tso %s force index(%s)";
    private final long primaryTso;
    private final long columnarTso;

    public CciSnapshotNaiveChecker(String schemaName, String tableName, String indexName,
                                   long primaryTso, long columnarTso) {
        super(schemaName, tableName, indexName);
        this.primaryTso = primaryTso;
        this.columnarTso = columnarTso;
    }

    @Override
    protected void log(String msg) {
        SQLRecorderLogger.ddlLogger.warn("[CCI Snapshot Naive Checker] " + msg);
    }

    @Override
    protected void error(String msg, Throwable t) {
        SQLRecorderLogger.ddlLogger.error("[CCI Snapshot Naive Checker] " + msg, t);
    }

    @Override
    protected String getColumnarSql(ExecutionContext ec, long tso, String pkList) {
        // Build hint.
        StringBuilder sb = new StringBuilder();
        setBasicHint(ec, sb);
        String hint = String.format(COLUMNAR_HINT, sb);

        String sql = String.format(CALCULATE_COLUMNAR_HASH, pkList, tableName, tso, indexName);

        return hint + sql;
    }

    @Override
    protected String getPrimarySql(ExecutionContext ec, long tso, String pkList) {
        // Build hint.
        StringBuilder sb = new StringBuilder();
        setBasicHint(ec, sb);
        sb.append(" TRANSACTION_POLICY=TSO");
        String hint = String.format(PRIMARY_HINT, sb);

        String sql = String.format(CALCULATE_PRIMARY_HASH, pkList, tableName, tso);

        return hint + sql;
    }
}
