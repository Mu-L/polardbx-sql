package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.CollectStatisticProgressSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaCollectStatisticProgress;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

/**
 * @author pangzhaoxing
 */
public class InformationSchemaCollectStatisticProgressHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaCollectStatisticProgressHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaCollectStatisticProgress;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        List<List<Map<String, Object>>> syncRes =
            SyncManagerHelper.syncIgnoreExceptions(new CollectStatisticProgressSyncAction(), SyncScope.ALL);

        ArrayResultCursor result = new ArrayResultCursor("CollectStatisticProgresses");
        result.addColumn("connection_id", DataTypes.LongType);
        result.addColumn("collect_sql", DataTypes.StringType);
        result.addColumn("status", DataTypes.StringType);
        result.addColumn("start_time", DataTypes.DatetimeType);
        result.addColumn("enable_hll", DataTypes.StringType);
        result.addColumn("statistic_parallelism", DataTypes.IntegerType);
        result.addColumn("hll_dn_parallelism", DataTypes.IntegerType);
        result.addColumn("table_count", DataTypes.IntegerType);
        result.addColumn("success_count", DataTypes.IntegerType);
        result.addColumn("fail_count", DataTypes.IntegerType);
        result.addColumn("progress", DataTypes.DoubleType);
        result.addColumn("running_hll_task", DataTypes.StringType);
        for (List<Map<String, Object>> res : syncRes) {
            for (Map<String, Object> map : res) {
                result.addRow(new Object[] {
                    map.get("connection_id"),
                    map.get("collect_sql"),
                    map.get("status"),
                    map.get("start_time"),
                    map.get("enable_hll"),
                    map.get("statistic_parallelism"),
                    map.get("hll_dn_parallelism"),
                    map.get("table_count"),
                    map.get("success_count"),
                    map.get("fail_count"),
                    map.get("progress"),
                    map.get("running_hll_task")
                });
            }
        }
        return result;
    }
}
