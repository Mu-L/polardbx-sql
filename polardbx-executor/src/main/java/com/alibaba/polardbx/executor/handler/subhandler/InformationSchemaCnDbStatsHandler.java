package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaCnDbStats;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

public class InformationSchemaCnDbStatsHandler extends BaseVirtualViewSubClassHandler {

    private static Class fetchCnDbStatsSyncClass;

    public InformationSchemaCnDbStatsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    static {
        try {
            fetchCnDbStatsSyncClass =
                Class.forName("com.alibaba.polardbx.server.response.FetchCnDbStatsSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaCnDbStats;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        try {
            ISyncAction fetchCnDbStatsSyncAction =
                (ISyncAction) fetchCnDbStatsSyncClass
                    .getConstructor()
                    .newInstance();
            List<List<Map<String, Object>>> results =
                SyncManagerHelper.syncIgnoreExceptions(fetchCnDbStatsSyncAction, SystemDbHelper.INFO_SCHEMA_DB_NAME,
                    SyncScope.CURRENT_ONLY);
            handleResult(results, cursor);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return cursor;
    }

    public static void handleResult(List<List<Map<String, Object>>> results, ArrayResultCursor cursor) {
        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                cursor.addRow(new Object[] {
                    DataTypes.StringType.convertFrom(row.get("COMPUTE_NODE")),
                    DataTypes.StringType.convertFrom(row.get("NAME")),
                    DataTypes.LongType.convertFrom(row.get("NET_IN")),
                    DataTypes.LongType.convertFrom(row.get("NET_OUT")),
                    DataTypes.LongType.convertFrom(row.get("ACTIVE_CONNECTION")),
                    DataTypes.LongType.convertFrom(row.get("CONNECTION_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("TIME_COST")),
                    DataTypes.LongType.convertFrom(row.get("REQUEST_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("QUERY_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("INSERT_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("DELETE_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("UPDATE_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("REPLACE_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("RUNNING_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("PHYSICAL_REQUEST_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("PHYSICAL_TIME_COST")),
                    DataTypes.LongType.convertFrom(row.get("ERROR_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("VIOLATION_ERROR_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("MULTI_DB_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("TEMP_TABLE_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("JOIN_MULTI_DB_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("AGGREGATE_MULTI_DB_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("HINT_COUNT")),
                    DataTypes.LongType.convertFrom(row.get("SLOW_REQUEST")),
                    DataTypes.LongType.convertFrom(row.get("PHYSICAL_SLOW_REQUEST")),
                    DataTypes.LongType.convertFrom(row.get("TRANS_COUNT_XA")),
                    DataTypes.LongType.convertFrom(row.get("TRANS_COUNT_BEST_EFFORT")),
                    DataTypes.LongType.convertFrom(row.get("TRANS_COUNT_TSO")),
                    DataTypes.LongType.convertFrom(row.get("CCL_KILL")),
                    DataTypes.LongType.convertFrom(row.get("CCL_RUN")),
                    DataTypes.LongType.convertFrom(row.get("CCL_WAIT")),
                    DataTypes.LongType.convertFrom(row.get("CCL_WAIT_KILL")),
                    DataTypes.LongType.convertFrom(row.get("CCL_RESCHEDULE")),
                    DataTypes.LongType.convertFrom(row.get("TP_WORKLOAD")),
                    DataTypes.LongType.convertFrom(row.get("AP_WORKLOAD")),
                    DataTypes.LongType.convertFrom(row.get("LOCAL_NUM")),
                    DataTypes.LongType.convertFrom(row.get("CLUSTER_NUM"))
                });
            }
        }
    }
}
