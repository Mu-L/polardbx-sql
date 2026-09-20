package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.LongUtil;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.memory.AdaptiveMemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;

import java.util.ArrayList;
import java.util.List;

public class FetchCnMemoryPoolSyncAction implements ISyncAction {
    @Override
    public ResultCursor sync() {

        ArrayResultCursor result = new ArrayResultCursor("CN_MEMORY_POOL");

        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("NAME", DataTypes.StringType);
        result.addColumn("USED_BYTES", DataTypes.LongType);
        result.addColumn("LIMIT_BYTES", DataTypes.LongType);
        result.addColumn("INFO", DataTypes.StringType);

        List<MemoryPool> memoryPools = collectMemoryPools();
        for (MemoryPool pool : memoryPools) {

            long limit = pool.getMaxLimit();
            if (limit == MemorySetting.UNLIMITED_SIZE) {
                // -1 represents unlimited
                limit = -1;
            }

            String info = "";
            if (pool instanceof AdaptiveMemoryPool) {
                info = info + " [lowWater=" + ((AdaptiveMemoryPool) pool).getMinLimit() + ",highWater="
                    + ((AdaptiveMemoryPool) pool).getMaxLimit() + "]";
            }

            result.addRow(new Object[] {
                TddlNode.getHost() + ":" + TddlNode.getPort(),
                pool.getFullName(),
                pool.getMemoryUsage(),
                limit,
                info
            });
        }

        return result;
    }

    private static List<MemoryPool> collectMemoryPools() {
        return collectMemoryPools(MemoryManager.getInstance().getGlobalMemoryPool());
    }

    private static List<MemoryPool> collectMemoryPools(MemoryPool root) {
        List<MemoryPool> results = new ArrayList<>();
        results.add(root);
        for (MemoryPool childPool : root.getChildren().values()) {
            results.addAll(collectMemoryPools(childPool));
        }
        return results;
    }
}
