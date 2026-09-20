package com.alibaba.polardbx.executor.ddl.job.builder.tablegroup;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import org.apache.calcite.rel.core.DDL;

public class AlterTableGroupSplitPartitionItemBuilder extends AlterTableGroupItemBuilder {

    AlterTableGroupSplitPartitionPreparedData parentPreparedData;

    public AlterTableGroupSplitPartitionItemBuilder(DDL ddl, AlterTableGroupItemPreparedData preparedData,
                                                    ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    public void setParentPreparedData(
        AlterTableGroupSplitPartitionPreparedData parentPreparedData) {
        this.parentPreparedData = parentPreparedData;
    }
}
