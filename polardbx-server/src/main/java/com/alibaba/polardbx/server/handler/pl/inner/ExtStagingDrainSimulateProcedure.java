package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.ExtStagingDrainStartTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.ExtStagingDrainWaitTask;
import com.alibaba.polardbx.executor.sync.ExtStagingClearDrainingSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Diagnostic / testing procedure that directly executes the ext-staging drain
 * pipeline (DrainStart + DrainWait) without going through DDL engine scheduling.
 *
 * <p>Usage: {@code CALL polardbx.ext_staging_drain_simulate('dn_id_1,dn_id_2')}
 *
 * <p>This avoids topology changes, DDL job lifecycle, and rebalance side effects,
 * making it safe for integration testing under NotThreadSafe.
 */
public class ExtStagingDrainSimulateProcedure extends BaseInnerProcedure {

    private static final Logger logger = LoggerFactory.getLogger(ExtStagingDrainSimulateProcedure.class);

    @Override
    void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        cursor.addColumn("PHASE", DataTypes.StringType);
        cursor.addColumn("RESULT", DataTypes.StringType);
        cursor.addColumn("MESSAGE", DataTypes.StringType);

        List<SQLExpr> params = statement.getParameters();
        if (params.isEmpty()) {
            cursor.addRow(
                new Object[] {"ERROR", "FAIL", "Usage: CALL polardbx.ext_staging_drain_simulate('dn_id1,dn_id2')"});
            return;
        }

        // Parse comma-separated DN IDs from the first parameter
        String rawDnIds = params.get(0).toString().replace("'", "").replace("\"", "").trim();
        List<String> dnIds = Arrays.stream(rawDnIds.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());

        if (dnIds.isEmpty()) {
            cursor.addRow(new Object[] {"ERROR", "FAIL", "No valid DN IDs provided"});
            return;
        }

        String schema = DefaultDbSchema.NAME;
        boolean drainCompleted = false;

        try {
            // Phase 1: DrainStart — broadcast exclude-DN to all CNs
            try {
                ExtStagingDrainStartTask startTask = new ExtStagingDrainStartTask(schema, dnIds);
                startTask.executeDrainStart();
                cursor.addRow(new Object[] {"DRAIN_START", "OK", "Broadcast exclude-DN completed for: " + rawDnIds});
            } catch (Exception e) {
                logger.error("ext_staging_drain_simulate: DrainStart failed", e);
                cursor.addRow(new Object[] {"DRAIN_START", "FAIL", e.getMessage()});
                return;
            }

            // Phase 2: DrainWait — poll until drained, sweep, evict cache
            try {
                ExtStagingDrainWaitTask waitTask = new ExtStagingDrainWaitTask(schema, dnIds);
                waitTask.executeDrainWait();
                cursor.addRow(new Object[] {"DRAIN_WAIT", "OK", "Drain wait completed for: " + rawDnIds});
            } catch (Exception e) {
                logger.error("ext_staging_drain_simulate: DrainWait failed", e);
                cursor.addRow(new Object[] {"DRAIN_WAIT", "FAIL", e.getMessage()});
                return;
            }

            drainCompleted = true;
            cursor.addRow(new Object[] {"DONE", "OK", "Full drain simulate completed for: " + rawDnIds});
        } finally {
            if (!drainCompleted) {
                clearDrainBarrier(dnIds);
            }
        }
    }

    private void clearDrainBarrier(List<String> dnIds) {
        try {
            SyncManagerHelper.syncThrowExceptions(
                new ExtStagingClearDrainingSyncAction(dnIds), DefaultDbSchema.NAME, SyncScope.ALL);
        } catch (Exception e) {
            logger.error("ext_staging_drain_simulate: failed to clear drain barrier for " + dnIds, e);
            throw new RuntimeException("failed to clear ext-staging drain barrier for " + dnIds, e);
        }
    }
}
