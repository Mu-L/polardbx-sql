package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.common.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterTableGroupSplitPartitionItemPreparedData extends AlterTableGroupItemPreparedData {

    public AlterTableGroupSplitPartitionItemPreparedData(String schemaName, String tableName) {
        super(schemaName, tableName);
    }
}
