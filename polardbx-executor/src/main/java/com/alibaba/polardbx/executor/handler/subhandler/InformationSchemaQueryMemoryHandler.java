package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.memory.GlobalMemoryTrackerManager;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.QueryContext;
import com.alibaba.polardbx.executor.mpp.execution.SqlTaskManager;
import com.alibaba.polardbx.executor.mpp.execution.TaskManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaQueryMemory;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;

public class InformationSchemaQueryMemoryHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaQueryMemoryHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaQueryMemory;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        GlobalMemoryTrackerManager globalMemoryTrackerManager = MemoryTrackerManager.getGlobalMemoryTrackerManager();

        List<Object[]> results = globalMemoryTrackerManager.dumpQueryLevelTableResult();

        TaskManager taskManager = ((MppServer) ServiceProvider.getInstance().getServer()).getTaskManager();
        if (taskManager instanceof SqlTaskManager) {
            SqlTaskManager sqlTaskManager = (SqlTaskManager) taskManager;

            for (Object[] result : results) {
                String queryId = String.valueOf(result[0]);
                QueryContext queryContext = sqlTaskManager.getQueryContext(queryId);
                result[4] = queryContext.getOriginalSql();
            }
        }

        for (Object[] row : results) {
            cursor.addRow(row);
        }

        return cursor;
    }
}