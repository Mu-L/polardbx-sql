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
import com.alibaba.polardbx.optimizer.view.InformationSchemaCnStatus;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

public class InformationSchemaCnStatusHandler extends BaseVirtualViewSubClassHandler {

    private static Class fetchCnStatusSyncClass;

    public InformationSchemaCnStatusHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    static {
        // Only supported on server, this is a temporary solution
        try {
            fetchCnStatusSyncClass =
                Class.forName("com.alibaba.polardbx.server.response.FetchCnStatusSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaCnStatus;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        try {
            ISyncAction fetchCnStatusSyncAction =
                (ISyncAction) fetchCnStatusSyncClass
                    .getConstructor()
                    .newInstance();
            List<List<Map<String, Object>>> results =
                SyncManagerHelper.syncIgnoreExceptions(fetchCnStatusSyncAction, SystemDbHelper.INFO_SCHEMA_DB_NAME,
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
                final String node = DataTypes.StringType.convertFrom(row.get("NODE"));
                final Long cpuCore = DataTypes.LongType.convertFrom(row.get("CPU_CORE"));
                final Long heapUsed = DataTypes.LongType.convertFrom(row.get("HEAP_USED"));
                final Long heapFree = DataTypes.LongType.convertFrom(row.get("HEAP_FREE"));
                final Long nonHeapUsed = DataTypes.LongType.convertFrom(row.get("NON_HEAP_USED"));
                final Long spillUsage = DataTypes.LongType.convertFrom(row.get("SPILL_USAGE"));
                final Long logUsage = DataTypes.LongType.convertFrom(row.get("LOG_USAGE"));
                final Long appConn = DataTypes.LongType.convertFrom(row.get("APP_CONN"));
                final Long toDnClientNum = DataTypes.LongType.convertFrom(row.get("TO_DN_CLIENT_NUM"));
                final Long toDnIdleSession = DataTypes.LongType.convertFrom(row.get("TO_DN_IDLE_SESSION"));
                final Long toDnWorkingSession = DataTypes.LongType.convertFrom(row.get("TO_DN_WORKING_SESSION"));

                cursor.addRow(new Object[] {
                    node,
                    cpuCore,
                    heapUsed,
                    heapFree,
                    nonHeapUsed,
                    spillUsage,
                    logUsage,
                    appConn,
                    toDnClientNum,
                    toDnIdleSession,
                    toDnWorkingSession
                });
            }
        }
    }
}