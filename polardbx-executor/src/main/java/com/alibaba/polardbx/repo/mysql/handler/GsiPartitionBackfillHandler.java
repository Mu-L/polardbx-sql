package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.backfill.Loader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.gsi.BackfillExecutor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.GsiPartitionBackfill;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

/**
 * Process backfill for GSI. GsiPkRangeBackfillHandler extends CalciteHandlerCommon
 * because we're going to reuse `executeWithConcurrentPolicy` to execute
 * INSERTs.
 */
public class GsiPartitionBackfillHandler extends HandlerCommon {

    private static final Logger LOG = SQLRecorderLogger.ddlLogger;

    public GsiPartitionBackfillHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        GsiPartitionBackfill backfill = (GsiPartitionBackfill) logicalPlan;
        String schemaName = backfill.getSchemaName();
        String baseTableName = backfill.getBaseTableName();
        List<String> indexNames = backfill.getIndexNames();
        List<String> modifyStringColumns = backfill.getModifyStringColumns();
        boolean modifyColumn = backfill.isOnlineModifyColumn();
        List<String> partitionList = backfill.getPartitionList();

        BackfillExecutor backfillExecutor = new BackfillExecutor((List<RelNode> inputs,
                                                                  ExecutionContext executionContext1) -> {
            QueryConcurrencyPolicy queryConcurrencyPolicy = getQueryConcurrencyPolicy(executionContext1);
            if (Loader.canUseBackfillReturning(executionContext1, schemaName)) {
                queryConcurrencyPolicy = QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;
            }
            List<Cursor> inputCursors = new ArrayList<>(inputs.size());
            executeWithConcurrentPolicy(executionContext1, inputs, queryConcurrencyPolicy, inputCursors, schemaName);
            return inputCursors;
        });

        boolean useBinary = executionContext.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY);
        boolean omcForce = executionContext.getParamManager().getBoolean(ConnectionParams.OMC_FORCE_TYPE_CONVERSION);
        boolean canUseReturning = Loader.canUseBackfillReturning(executionContext, schemaName);

        // online modify column, does not clear sql_mode
        if (modifyColumn) {
            executionContext = setChangeSetApplySqlMode(executionContext.copy());
            if (!useBinary && !omcForce) {
                // select + insert, need encoding
                upgradeEncoding(executionContext, schemaName, baseTableName);
            }
            // 暂时不使用 backfill insert ignore returning 优化，因为无法处理 sql_mode 严格模式行为
            canUseReturning = false;
        } else {
            executionContext = clearSqlMode(executionContext.copy());
            if (!useBinary) {
                upgradeEncoding(executionContext, schemaName, baseTableName);
            }
        }

        executionContext.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, MetricLevel.SQL.metricLevel);

        PhyTableOperationUtil.disableIntraGroupParallelism(schemaName, executionContext);

        // Force master first and following will copy this EC.
        executionContext.getExtraCmds().put(ConnectionProperties.MASTER, true);
        int affectRows;
        if (backfill.isAddColumnsBackfill() || backfill.isMirrorCopy() || backfill.isUseChangeSet()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "backfill by partition is not support here!");
        } else {
            // Normal creating GSI.
            assert 1 == indexNames.size();
            affectRows =
                backfillExecutor.backfill(schemaName, baseTableName, indexNames.get(0), useBinary, false,
                    canUseReturning, modifyStringColumns, null, partitionList, modifyColumn, 1, executionContext);
        }

        return new AffectRowCursor(affectRows);
    }
}
