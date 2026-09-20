package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.matrix.jdbc.TResultSet;
import com.alibaba.polardbx.server.QueryResultHandler;
import com.alibaba.polardbx.server.ServerConnection;
import com.google.common.collect.ImmutableMap;

import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.ADD_DN_CCL_RULE;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.CLEAR_ALL_DN_CCL_RULES;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_AUTO_SNAPSHOT_CONFIG;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.CHAIN_GLOBAL_ARCHIVE;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.CHAIN_HIST_ARCHIVE;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.CHECK_CHAIN_ALL;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.CHECK_CHAIN_GLOBAL;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.CHECK_CHAIN_HIST;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.CHECK_CHAIN_VALID;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_BACKUP;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_FLUSH;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_GENERATE_SNAPSHOTS;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_IGNORE;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_ROLLBACK;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_SET_CONFIG;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_SNAPSHOT_FILES;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_UNIGNORE;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.DRAIN_HANGING_TRX;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.EXT_STAGING_DRAIN_SIMULATE;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.FORCE_ROTATE_STAGING;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.FORCE_FLUSH_STAGING;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.FORCE_EXT_COLUMN_MAPPING_MIGRATION;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.FIX_GDN_TRX_POLICY;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.SHOW_JDBC_URL;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.RECORD_JDBC_URL;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.TRIGGER_SYNC_POINT_TRX;

/**
 * @author yaozhili
 */
public class InnerProcedureHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(InnerProcedureHandler.class);

    private static final ImmutableMap<String, BaseInnerProcedure> INNER_PROCEDURES;

    static {
        INNER_PROCEDURES = ImmutableMap.<String, BaseInnerProcedure>builder()
            .put(TRIGGER_SYNC_POINT_TRX, new TriggerSyncPointTrx())
            .put(COLUMNAR_FLUSH, new ColumnarFlushProcedure())
            .put(COLUMNAR_BACKUP, new ColumnarBackupProcedure())
            .put(COLUMNAR_SNAPSHOT_FILES, new ColumnarSnapshotFilesProcedure())
            .put(COLUMNAR_SET_CONFIG, new ColumnarSetConfigProcedure())
            .put(COLUMNAR_ROLLBACK, new ColumnarRollbackProcedure())
            .put(COLUMNAR_GENERATE_SNAPSHOTS, new ColumnarGenerateSnapshots())
            .put(COLUMNAR_AUTO_SNAPSHOT_CONFIG, new ColumnarAutoSnapshotConfigProcedure())
            .put(COLUMNAR_IGNORE, new ColumnarIgnoreProcedure())
            .put(COLUMNAR_UNIGNORE, new ColumnarUnignoreProcedure())
            .put(RECORD_JDBC_URL, new JdbcUrlAddProcedure())
            .put(ADD_DN_CCL_RULE, new AddDnCclRuleProcedure())
            .put(SHOW_JDBC_URL, new JdbcUrlShowProcedure())
            .put(CLEAR_ALL_DN_CCL_RULES, new ClearAllDnCclProcedure())
            .put(CHECK_CHAIN_HIST, new CheckChainHistProcedure())
            .put(CHECK_CHAIN_GLOBAL, new CheckChainGlobalProcedure())
            .put(CHECK_CHAIN_VALID, new CheckChainValidProcedure())
            .put(CHECK_CHAIN_ALL, new CheckChainAllProcedure())
            .put(CHAIN_HIST_ARCHIVE, new ChainHistArchiveProcedure())
            .put(CHAIN_GLOBAL_ARCHIVE, new ChainGlobalArchiveProcedure())
            .put(DRAIN_HANGING_TRX, new DrainHangingTrxProcedure())
            .put(FIX_GDN_TRX_POLICY, new FixGdnTrxPolicyProcedure())
            .put(EXT_STAGING_DRAIN_SIMULATE, new ExtStagingDrainSimulateProcedure())
            .put(FORCE_ROTATE_STAGING, new ForceRotateStagingProcedure())
            .put(FORCE_FLUSH_STAGING, new ForceFlushStagingProcedure())
            .put(FORCE_EXT_COLUMN_MAPPING_MIGRATION, new ForceExtColumnMappingMigrationProcedure())
            .build();
    }

    public static void handle(SQLCallStatement statement,
                              ServerConnection c,
                              boolean hashMore) {
        String procedureName = statement.getProcedureName().getSimpleName().toLowerCase();
        BaseInnerProcedure procedure = INNER_PROCEDURES.get(procedureName);
        if (null != procedure) {
            // Execute inner procedure.
            ResultCursor cursor = procedure.getResultCursor(c, statement, procedureName);
            // Send result.
            sendResult(c, hashMore, cursor);
        } else {
            c.writeErrMessage(ErrorCode.ERR_PROCEDURE_NOT_FOUND,
                "Not found any inner procedure " + procedureName);
        }
    }

    private static void sendResult(ServerConnection c, boolean hashMore, ResultCursor cursor) {
        QueryResultHandler queryResultHandler = c.createResultHandler(hashMore);
        TResultSet resultSet = new TResultSet(cursor, null);
        try {
            queryResultHandler.sendSelectResult(resultSet, new AtomicLong(0), Long.MAX_VALUE);
        } catch (Throwable t) {
            c.writeErrMessage(ErrorCode.ERR_PROCEDURE_EXECUTE, t.getMessage());
        } finally {
            try {
                // 释放cursor持有的资源（如spill临时文件）
                resultSet.close();
            } catch (Throwable t) {
                LOGGER.warn("Failed to close inner procedure result cursor", t);
            }
        }
        queryResultHandler.sendPacketEnd(false);
    }
}
