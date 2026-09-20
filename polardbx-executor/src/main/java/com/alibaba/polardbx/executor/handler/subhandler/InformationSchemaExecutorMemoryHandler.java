package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.memory.GlobalMemoryTrackerManager;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaExecutorMemory;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;

public class InformationSchemaExecutorMemoryHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaExecutorMemoryHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaExecutorMemory;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        GlobalMemoryTrackerManager globalMemoryTrackerManager = MemoryTrackerManager.getGlobalMemoryTrackerManager();

        List<Object[]> results = globalMemoryTrackerManager.dumpTableResult();

        for (Object[] row : results) {
            cursor.addRow(row);
        }

        return cursor;
    }
}