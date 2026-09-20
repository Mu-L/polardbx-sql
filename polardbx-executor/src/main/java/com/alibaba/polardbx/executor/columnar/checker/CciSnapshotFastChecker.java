package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

/**
 * @author yaozhili
 */
public class CciSnapshotFastChecker extends CciFastChecker {
    private static final String CALCULATE_PRIMARY_HASH =
        "select check_sum_v2(*) as checksum from %s as of tso %s force index(primary)";

    private final long primaryTso;
    private final long columnarTso;

    public CciSnapshotFastChecker(String schemaName, String tableName, String indexName,
                                  long primaryTso, long columnarTso) {
        super(schemaName, tableName, indexName);
        this.primaryTso = primaryTso;
        this.columnarTso = columnarTso;
    }

    @Override
    protected void log(String msg) {
        SQLRecorderLogger.ddlLogger.warn("[CCI Snapshot Fast Checker] " + msg);
    }

    @Override
    protected void error(String msg, Throwable t) {
        SQLRecorderLogger.ddlLogger.error("[CCI Snapshot Fast Checker] " + msg, t);
    }

    @Override
    protected String getPrimarySql(ExecutionContext baseEc, long tso) {
        StringBuilder sb = new StringBuilder();
        setBasicHint(baseEc, sb);

        sb.append(" TRANSACTION_POLICY=TSO");
        String hint = String.format(PRIMARY_HINT, sb);
        return hint + String.format(CALCULATE_PRIMARY_HASH, tableName, tso);
    }
}
