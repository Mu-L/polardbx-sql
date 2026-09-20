package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.columnar.StagingFlushTask;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

/**
 * Synchronously drain this CN's SEALED staging seqs once — a single pass of the
 * background flush loop, with per-seq fault isolation (one unflushable seq never
 * blocks the others).
 *
 * <p>Usage: {@code CALL polardbx.force_rotate_staging()} rotates + flushes the
 * current active seq; this procedure instead flushes ALL already-SEALED seqs
 * owned by this CN in one shot — preferred for tests / ops that need to force a
 * flush cycle without rotating.
 */
public class ForceFlushStagingProcedure extends BaseInnerProcedure {

    @Override
    void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        cursor.addColumn("FLUSHED", DataTypes.StringType);
        cursor.addColumn("CLEANED", DataTypes.StringType);
        cursor.addColumn("KEPT", DataTypes.StringType);
        cursor.addColumn("FAILED", DataTypes.StringType);

        try {
            StagingFlushTask.FlushRoundStats stats = new StagingFlushTask().forceFlushAllSealed();
            cursor.addRow(new Object[] {
                String.valueOf(stats.flushed),
                String.valueOf(stats.cleaned),
                String.valueOf(stats.kept),
                String.valueOf(stats.failed)});
        } catch (Exception e) {
            throw new RuntimeException("force_flush_staging failed: " + e.getMessage(), e);
        }
    }
}
