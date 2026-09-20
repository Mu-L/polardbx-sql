package com.alibaba.polardbx.executor.ddl.newengine.job.wrapper;

import com.alibaba.polardbx.executor.ddl.job.task.basic.*;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.google.common.collect.Lists;
import lombok.Data;

import java.util.Objects;
import java.util.stream.Collectors;

@Data
public class ExecutableDdlJob4CreatePartitionTableNoCdcMark extends ExecutableDdlJob {

    private CreatePartitionTableValidateTask createPartitionTableValidateTask;
    private CreateTableAddTablesPartitionInfoMetaTask createTableAddTablesPartitionInfoMetaTask;
    private CreateTablePhyDdlTask createTablePhyDdlTask;
    private CreateTableAddTablesMetaTask createTableAddTablesMetaTask;
    private CreateTableShowTableMetaTask createTableShowTableMetaTask;
    private TableSyncTask tableSyncTask;
    private CreateArchiveTableEventLogTask createArchiveTableEventLogTask;

    public static ExecutableDdlJob4CreatePartitionTableNoCdcMark buildFrom(ExecutableDdlJob4CreatePartitionTable temp) {
        ExecutableDdlJob4CreatePartitionTableNoCdcMark ret = new ExecutableDdlJob4CreatePartitionTableNoCdcMark();

        ret.setCreatePartitionTableValidateTask(temp.getCreatePartitionTableValidateTask());
        ret.setCreateTableAddTablesPartitionInfoMetaTask(temp.getCreateTableAddTablesPartitionInfoMetaTask());
        ret.setCreateTablePhyDdlTask(temp.getCreateTablePhyDdlTask());
        ret.setCreateTableAddTablesMetaTask(temp.getCreateTableAddTablesMetaTask());
        ret.setCreateTableShowTableMetaTask(temp.getCreateTableShowTableMetaTask());
        ret.setTableSyncTask(temp.getTableSyncTask());
        ret.setCreateArchiveTableEventLogTask(temp.getCreateArchiveTableEventLogTask());

        ret.addSequentialTasks(Lists.newArrayList(
                temp.getCreatePartitionTableValidateTask(),
                temp.getCreateTableAddTablesPartitionInfoMetaTask(),
                temp.getCreateTablePhyDdlTask(),
                temp.getCreateTableAddTablesMetaTask(),
                temp.getCreateTableShowTableMetaTask(),
                temp.getTableSyncTask(),
                temp.getCreateArchiveTableEventLogTask()
        ).stream().filter(Objects::nonNull).collect(Collectors.toList()));

        return ret;
    }
}