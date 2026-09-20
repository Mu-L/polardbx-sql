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
import com.alibaba.polardbx.optimizer.view.InformationSchemaCnMemoryPool;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

public class InformationSchemaCnMemoryPoolHandler extends BaseVirtualViewSubClassHandler {

    private static Class fetchCnMemoryPoolSyncClass;
    public InformationSchemaCnMemoryPoolHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            fetchCnMemoryPoolSyncClass =
                Class.forName("com.alibaba.polardbx.server.response.FetchCnMemoryPoolSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }


    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaCnMemoryPool;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        try {
            ISyncAction syncAction =
                (ISyncAction) fetchCnMemoryPoolSyncClass
                    .getConstructor()
                    .newInstance();
            List<List<Map<String, Object>>> results =
                SyncManagerHelper.syncIgnoreExceptions(syncAction, SystemDbHelper.INFO_SCHEMA_DB_NAME,
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
                final String computeNode = DataTypes.StringType.convertFrom(row.get("COMPUTE_NODE"));
                final String name = DataTypes.StringType.convertFrom(row.get("NAME"));
                final Long usedBytes = DataTypes.LongType.convertFrom(row.get("USED_BYTES"));
                final Long limitBytes = DataTypes.LongType.convertFrom(row.get("LIMIT_BYTES"));
                final String info = DataTypes.StringType.convertFrom(row.get("INFO"));

                cursor.addRow(new Object[] {
                    computeNode,
                    name,
                    usedBytes,
                    limitBytes,
                    info
                });
            }
        }
    }
}
