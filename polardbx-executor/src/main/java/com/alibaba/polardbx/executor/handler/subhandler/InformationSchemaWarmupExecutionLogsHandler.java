package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaWarmupExecutionLogs;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

public class InformationSchemaWarmupExecutionLogsHandler extends BaseVirtualViewSubClassHandler {

    private static final Class WARMUP_EXECUTION_LOG_SYNC_ACTION_CLASS;

    static {
        try {
            WARMUP_EXECUTION_LOG_SYNC_ACTION_CLASS =
                Class.forName("com.alibaba.polardbx.executor.sync.WarmupExecutionLogSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }
    
    public InformationSchemaWarmupExecutionLogsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        final ISyncAction syncAction;
        try {
            syncAction = (ISyncAction) WARMUP_EXECUTION_LOG_SYNC_ACTION_CLASS.getConstructor().newInstance();
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
        List<List<Map<String, Object>>> results = SyncManagerHelper.syncIgnoreExceptions(syncAction, SyncScope.CURRENT_ONLY);

        for (List<Map<String, Object>> rs : results) {
            if (rs == null) {
                continue;
            }
            for (Map<String, Object> row : rs) {
                cursor.addRow(new Object[] {
                    DataTypes.LongType.convertFrom(row.get("TASK_ID")),
                    DataTypes.VarcharType.convertFrom(row.get("STATUS")),
                    DataTypes.VarcharType.convertFrom(row.get("CRON_EXPR")),
                    DataTypes.VarcharType.convertFrom(row.get("CRON_EXEC_TIME")),
                    DataTypes.DatetimeType.convertFrom(row.get("START_TIME")),
                    DataTypes.DatetimeType.convertFrom(row.get("FINISH_TIME")),
                    DataTypes.LongType.convertFrom(row.get("TIME_COST")),
                    DataTypes.VarcharType.convertFrom(row.get("INST_ID")),
                    DataTypes.VarcharType.convertFrom(row.get("SCHEMA_NAME")),
                    DataTypes.VarcharType.convertFrom(row.get("SQL_DEF")),
                    DataTypes.VarcharType.convertFrom(row.get("IO_MESSAGE")),
                    DataTypes.VarcharType.convertFrom(row.get("HOST_PORT"))
                });
            }
        }
        return cursor;
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaWarmupExecutionLogs;
    }
}
