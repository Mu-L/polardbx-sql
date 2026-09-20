package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMceExternalizeDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.CleanBlobCacheForTableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.RegisterBlobColumnMappingTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceAddAddrColumnTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceBlobRefMd5CheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceChangeReadModeTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceChangeWriteModeTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceChangeWriteOnlyAddrTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceDropContentColumnTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceInPlaceBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceExternalizeTargetValidator;
import com.alibaba.polardbx.executor.ddl.job.task.mce.McePauseBeforeReadCutoverTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class MceExternalizeJobFactory extends DdlJobFactory {

    private final String schemaName;
    private final String tableName;
    private final String columnName;
    private final String addrColumnName;
    private final String originalType;
    private final String userCommentOverride;
    private final boolean needAddColumn;
    private final Boolean pauseBeforeReadCutoverOverride;
    private final long ddlVersionId;
    private final TableMeta tableMeta;

    public MceExternalizeJobFactory(String schemaName, String tableName, String columnName,
                                    String originalType, boolean needAddColumn,
                                    long ddlVersionId, TableMeta tableMeta) {
        this(schemaName, tableName, columnName, originalType, null, needAddColumn, ddlVersionId, tableMeta);
    }

    public MceExternalizeJobFactory(String schemaName, String tableName, String columnName,
                                    String originalType, String userCommentOverride, boolean needAddColumn,
                                    long ddlVersionId, TableMeta tableMeta) {
        this(schemaName, tableName, columnName, originalType, userCommentOverride, needAddColumn, null,
            ddlVersionId, tableMeta);
    }

    public MceExternalizeJobFactory(String schemaName, String tableName, String columnName,
                                    String originalType, String userCommentOverride, boolean needAddColumn,
                                    Boolean pauseBeforeReadCutoverOverride, long ddlVersionId, TableMeta tableMeta) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
        this.originalType = originalType != null ? originalType : "LONGTEXT";
        this.userCommentOverride = userCommentOverride;
        this.needAddColumn = needAddColumn;
        this.pauseBeforeReadCutoverOverride = pauseBeforeReadCutoverOverride;
        this.ddlVersionId = ddlVersionId;
        this.tableMeta = tableMeta;
    }

    @Override
    protected void validate() {
        MceExternalizeTargetValidator.validate(tableMeta, schemaName, tableName, columnName, originalType);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob job = new ExecutableDdlJob();
        List<DdlTask> tasks = new ArrayList<>();

        if (needAddColumn) {
            tasks.add(new MceAddAddrColumnTask(schemaName, tableName, columnName, originalType, userCommentOverride));
            tasks.add(new TableSyncTask(schemaName, tableName));
        }

        // Keep mapping ownership in the dedicated task. On rollback the reverse order is intentional:
        // MceChangeWriteModeTask first restores NONE and completes its all-CN MDL barrier, then the register task
        // marks only its owned mapping DROP, and finally this first cache task invalidates table-id caches on all CNs.
        tasks.add(new CleanBlobCacheForTableSyncTask(schemaName, tableName, true));
        tasks.add(new RegisterBlobColumnMappingTask(schemaName, tableName,
            Collections.singletonList(columnName)));
        tasks.add(new MceChangeWriteModeTask(schemaName, tableName, columnName));
        tasks.add(new CleanBlobCacheForTableSyncTask(schemaName, tableName, true));
        tasks.add(new TableSyncTask(schemaName, tableName));

        tasks.add(new MceInPlaceBackfillTask(schemaName, tableName, columnName, addrColumnName));
        tasks.add(new MceBlobRefMd5CheckTask(schemaName, tableName, columnName, addrColumnName));
        tasks.add(new McePauseBeforeReadCutoverTask(schemaName, tableName, columnName,
            pauseBeforeReadCutoverOverride));

        tasks.add(new MceChangeReadModeTask(schemaName, tableName, columnName));
        tasks.add(new TableSyncTask(schemaName, tableName));

        // Switch CDC logical meta while READ_ADDR still dual-writes content and addr. After this
        // marker CDC rebuilds logical body from body_addr_; publishing it before ADDR_ONLY avoids
        // any row-event window where CN writes only addr but CDC still reads the content column.
        tasks.add(new CdcMceExternalizeDdlMarkTask(schemaName, tableName, columnName, originalType));

        tasks.add(new MceChangeWriteOnlyAddrTask(schemaName, tableName, columnName));
        tasks.add(new TableSyncTask(schemaName, tableName));

        MceDropContentColumnTask dropContentColumnTask =
            new MceDropContentColumnTask(schemaName, tableName, columnName, ddlVersionId);
        dropContentColumnTask.setExceptionAction(DdlExceptionAction.PAUSE);
        tasks.add(dropContentColumnTask);
        tasks.add(new TableSyncTask(schemaName, tableName));

        job.addSequentialTasks(tasks);
        return job;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        // MCE externalize is a single-table ALTER-like DDL. Keep the resource granularity aligned
        // with other DRDS single-table DDLs (ALTER/CREATE INDEX/TRUNCATE): table-level exclusive
        // lock is enough to block concurrent topology or rule changes on the target table without
        // stalling unrelated DDLs in the same schema.
        resources.add(concatWithDot(schemaName, tableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}
