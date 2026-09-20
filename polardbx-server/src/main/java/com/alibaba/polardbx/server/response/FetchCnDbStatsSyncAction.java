package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.stats.MatrixStatistics;

import java.util.Map;

public class FetchCnDbStatsSyncAction implements ISyncAction {
    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("CN_DB_STATS");

        // Add all columns from ShowStats
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("NAME", DataTypes.StringType);
        result.addColumn("NET_IN", DataTypes.LongType);
        result.addColumn("NET_OUT", DataTypes.LongType);
        result.addColumn("ACTIVE_CONNECTION", DataTypes.LongType);
        result.addColumn("CONNECTION_COUNT", DataTypes.LongType);
        result.addColumn("TIME_COST", DataTypes.LongType);
        result.addColumn("REQUEST_COUNT", DataTypes.LongType);
        result.addColumn("QUERY_COUNT", DataTypes.LongType);
        result.addColumn("INSERT_COUNT", DataTypes.LongType);
        result.addColumn("DELETE_COUNT", DataTypes.LongType);
        result.addColumn("UPDATE_COUNT", DataTypes.LongType);
        result.addColumn("REPLACE_COUNT", DataTypes.LongType);
        result.addColumn("RUNNING_COUNT", DataTypes.LongType);
        result.addColumn("PHYSICAL_REQUEST_COUNT", DataTypes.LongType);
        result.addColumn("PHYSICAL_TIME_COST", DataTypes.LongType);
        result.addColumn("ERROR_COUNT", DataTypes.LongType);
        result.addColumn("VIOLATION_ERROR_COUNT", DataTypes.LongType);
        result.addColumn("MULTI_DB_COUNT", DataTypes.LongType);
        result.addColumn("TEMP_TABLE_COUNT", DataTypes.LongType);
        result.addColumn("JOIN_MULTI_DB_COUNT", DataTypes.LongType);
        result.addColumn("AGGREGATE_MULTI_DB_COUNT", DataTypes.LongType);
        result.addColumn("HINT_COUNT", DataTypes.LongType);
        result.addColumn("SLOW_REQUEST", DataTypes.LongType);
        result.addColumn("PHYSICAL_SLOW_REQUEST", DataTypes.LongType);
        result.addColumn("TRANS_COUNT_XA", DataTypes.LongType);
        result.addColumn("TRANS_COUNT_BEST_EFFORT", DataTypes.LongType);
        result.addColumn("TRANS_COUNT_TSO", DataTypes.LongType);
        result.addColumn("CCL_KILL", DataTypes.LongType);
        result.addColumn("CCL_RUN", DataTypes.LongType);
        result.addColumn("CCL_WAIT", DataTypes.LongType);
        result.addColumn("CCL_WAIT_KILL", DataTypes.LongType);
        result.addColumn("CCL_RESCHEDULE", DataTypes.LongType);
        result.addColumn("TP_WORKLOAD", DataTypes.LongType);
        result.addColumn("AP_WORKLOAD", DataTypes.LongType);
        result.addColumn("LOCAL_NUM", DataTypes.LongType);
        result.addColumn("CLUSTER_NUM", DataTypes.LongType);

        boolean returnRealActiveConnNum = DynamicConfig.getInstance().isReturnRealActiveConnNum();

        // Get all schemas from CobarServer
        Map<String, SchemaConfig> schemas = CobarServer.getInstance().getConfig().getSchemas();
        for (SchemaConfig schema : schemas.values()) {
            if (!schema.getDataSource().isInited() ||
                SystemDbHelper.CDC_DB_NAME.equalsIgnoreCase(schema.getName())) {
                continue;
            }

            TDataSource ds = schema.getDataSource();
            MatrixStatistics stats = ds.getStatistics();
            ServerThreadPool exec = CobarServer.getInstance().getServerExecutor();
            long activeConnection = stats.activeConnection.get();
            if (!returnRealActiveConnNum) {
                activeConnection = Math.max(0, stats.activeConnection.get());
            }

            result.addRow(new Object[] {
                TddlNode.getHost() + ":" + TddlNode.getPort(),
                ds.getSchemaName(),
                stats.netIn,
                stats.netOut,
                activeConnection,
                stats.connectionCount.get(),
                stats.timeCost,
                stats.request,
                stats.query,
                stats.insert,
                stats.delete,
                stats.update,
                stats.replace,
                exec.getTaskCountBySchemaName(ds.getSchemaName()),
                stats.physicalRequest.get(),
                stats.physicalTimeCost.get(),
                stats.errorCount,
                stats.integrityConstraintViolationErrorCount,
                stats.multiDBCount,
                stats.tempTableCount,
                stats.joinMultiDBCount,
                stats.aggregateMultiDBCount,
                stats.hintCount,
                stats.slowRequest,
                stats.physicalSlowRequest,
                stats.getTransactionStats().countXA.get(),
                stats.getTransactionStats().countBestEffort.get(),
                stats.getTransactionStats().countTSO.get(),
                stats.cclKill,
                stats.cclRun,
                stats.cclWait,
                stats.cclWaitKill,
                stats.cclReschedule,
                stats.tpLoad,
                stats.apLoad,
                stats.local,
                stats.cluster
            });
        }

        return result;
    }
}