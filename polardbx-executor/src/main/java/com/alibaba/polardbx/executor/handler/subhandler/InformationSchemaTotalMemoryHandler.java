package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.memory.GlobalMemoryTrackerManager;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.orc.PreheatMetaManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTotalMemory;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;

public class InformationSchemaTotalMemoryHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaTotalMemoryHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTotalMemory;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        GlobalMemoryTrackerManager globalMemoryTrackerManager = MemoryTrackerManager.getGlobalMemoryTrackerManager();

        Object[] executorMemoryUsage = globalMemoryTrackerManager.dumpTotalUsage();
        cursor.addRow(executorMemoryUsage);

        Object[] blockCacheMemoryUsage = BlockCacheManager.getInstance().dumpMemoryUsage();
        cursor.addRow(blockCacheMemoryUsage);

        Object[] preheatMetaMemoryUsage = PreheatMetaManager.getInstance().dumpTotalUsage();
        cursor.addRow(preheatMetaMemoryUsage);

        List<Object[]> columnarManagerMemoryUsage = DynamicColumnarManager.getInstance().dumpMemoryUsage();
        for (int i = 0; i < columnarManagerMemoryUsage.size(); i++) {
            cursor.addRow(columnarManagerMemoryUsage.get(i));
        }

        return cursor;
    }
}
