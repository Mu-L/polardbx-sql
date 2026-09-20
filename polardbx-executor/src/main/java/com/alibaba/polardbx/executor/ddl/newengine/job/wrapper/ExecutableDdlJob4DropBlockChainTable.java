package com.alibaba.polardbx.executor.ddl.newengine.job.wrapper;

import com.alibaba.polardbx.executor.ddl.job.task.basic.DeleteGlobalChainDataTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableHideTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.google.common.collect.Lists;
import lombok.Data;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * plain drop table
 */
@Data
public class ExecutableDdlJob4DropBlockChainTable extends ExecutableDdlJob {

    private DropPartitionTableValidateTask validateTask;
    private DropTableHideTableMetaTask dropTableHideTableMetaTask;
    private DdlTask phyDdlTask;
    private DropPartitionTableRemoveMetaTask removeMetaTask;
    private TableSyncTask tableSyncTask;
    private DeleteGlobalChainDataTask deleteGlobalChainDataTask;

    public static ExecutableDdlJob4DropBlockChainTable buildFrom(ExecutableDdlJob4DropPartitionTable temp,
                                                                 String schemaName, String tableName,
                                                                 String blockChainHistoryTableName) {
        ExecutableDdlJob4DropBlockChainTable ret = new ExecutableDdlJob4DropBlockChainTable();
        ret.setValidateTask(temp.getValidateTask());
        ret.setDropTableHideTableMetaTask(temp.getDropTableHideTableMetaTask());
        ret.setPhyDdlTask(temp.getPhyDdlTask());
        ret.setRemoveMetaTask(temp.getRemoveMetaTask());
        ret.setTableSyncTask(temp.getTableSyncTask());
        ret.setDeleteGlobalChainDataTask(new DeleteGlobalChainDataTask(schemaName, tableName));

        ret.addSequentialTasks(Lists.newArrayList(
            temp.getValidateTask(),
            temp.getDropTableHideTableMetaTask(),
            new TableSyncTask(schemaName, blockChainHistoryTableName),
            temp.getPhyDdlTask(),
            temp.getRemoveMetaTask(),
            temp.getTableSyncTask(),
            ret.getDeleteGlobalChainDataTask()
        ).stream().filter(Objects::nonNull).collect(Collectors.toList()));

        return ret;
    }

}
