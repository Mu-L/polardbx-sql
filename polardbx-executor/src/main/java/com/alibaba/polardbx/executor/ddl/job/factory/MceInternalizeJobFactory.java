package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMceInternalizeDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.CleanBlobCacheForTableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceAddContentColumnTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceContentBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceContentMd5CheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceDropAddrColumnTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceInternalizeChangeReadModeTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceInternalizeChangeWriteContentOnlyTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceInternalizeChangeWriteModeTask;
import com.alibaba.polardbx.executor.ddl.job.task.mce.MceInternalizeTargetValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Internalize job: convert a terminal externalized column back to a plain column (the reverse of
 * {@link MceExternalizeJobFactory}). Triggered by a plain
 * {@code ALTER TABLE t MODIFY COLUMN body <ORIGINAL_TYPE>} (no EXTERNALIZE keyword) whose target
 * column is a terminal externalized column and whose declared type exactly matches the stored
 * {@code ext_type}. Intended as the gray-release escape hatch.
 * <p>
 * Reverse pipeline (the MCE state machine traversed backwards, no new states):
 * <pre>
 * EXTERNALIZED → READ_ADDR → DUAL_WRITE → NONE
 * </pre>
 * Task pipeline:
 * <ol>
 *   <li>{@link MceAddContentColumnTask} — physical-only ADD of the plaintext content column; no
 *       MetaDB change, no TableSync needed (CN cannot see the column yet).</li>
 *   <li>{@link MceInternalizeChangeWriteModeTask} — one MetaDB transaction atomically flips the
 *       terminal single-record layout to the standard READ_ADDR two-record layout; dual-write on,
 *       reads stay on FETCH_BLOB(addr). Last CANCELable stretch ends after the checker.</li>
 *   <li>{@link MceContentBackfillTask} — fetch payloads through the full read path and CAS the
 *       plaintext back per row.</li>
 *   <li>{@link MceContentMd5CheckTask} — DN-side verification of every restored row against the
 *       addr-embedded rawSize/rawMd5 before reads may trust the content column.</li>
 *   <li>{@link MceInternalizeChangeReadModeTask} — read cutover to plaintext; disables CANCEL in
 *       the same transaction (point of no return).</li>
 *   <li>{@link CdcMceInternalizeDdlMarkTask} — switch CDC logical meta back to the plaintext
 *       column while content and addr are still dual-written.</li>
 *   <li>{@link MceInternalizeChangeWriteContentOnlyTask} — stop maintaining the addr column.</li>
 *   <li>{@link MceDropAddrColumnTask} — staging-drain hard gate, physical DROP of the addr column
 *       and the final MetaDB cutover; explicit PAUSE on failure, converges via CONTINUE.</li>
 * </ol>
 */
public class MceInternalizeJobFactory extends DdlJobFactory {

    private final String schemaName;
    private final String tableName;
    private final String columnName;
    private final String addrColumnName;
    private final String originalType;
    private final String userComment;
    private final long ddlVersionId;
    private final TableMeta tableMeta;

    public MceInternalizeJobFactory(String schemaName, String tableName, String columnName,
                                    String originalType, String userComment,
                                    long ddlVersionId, TableMeta tableMeta) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
        this.originalType = originalType;
        this.userComment = userComment;
        this.ddlVersionId = ddlVersionId;
        this.tableMeta = tableMeta;
    }

    @Override
    protected void validate() {
        MceInternalizeTargetValidator.validate(tableMeta, schemaName, tableName, columnName, originalType);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob job = new ExecutableDdlJob();
        List<DdlTask> tasks = new ArrayList<>();

        tasks.add(new MceAddContentColumnTask(schemaName, tableName, columnName, originalType, userComment));

        tasks.add(new CleanBlobCacheForTableSyncTask(schemaName, tableName, true));
        tasks.add(new MceInternalizeChangeWriteModeTask(schemaName, tableName, columnName));
        tasks.add(new CleanBlobCacheForTableSyncTask(schemaName, tableName, true));
        tasks.add(new TableSyncTask(schemaName, tableName));

        tasks.add(new MceContentBackfillTask(schemaName, tableName, columnName, addrColumnName));
        tasks.add(new MceContentMd5CheckTask(schemaName, tableName, columnName, addrColumnName));

        tasks.add(new MceInternalizeChangeReadModeTask(schemaName, tableName, columnName));
        tasks.add(new TableSyncTask(schemaName, tableName));

        // Switch CDC logical meta while DUAL_WRITE still maintains both columns. After this marker
        // CDC reads plaintext body directly; publishing it before content-only avoids any row-event
        // window where CN stops updating addr but CDC still interprets addr as the logical column.
        tasks.add(new CdcMceInternalizeDdlMarkTask(schemaName, tableName, columnName, originalType));

        tasks.add(new MceInternalizeChangeWriteContentOnlyTask(schemaName, tableName, columnName));
        tasks.add(new TableSyncTask(schemaName, tableName));

        MceDropAddrColumnTask dropAddrColumnTask =
            new MceDropAddrColumnTask(schemaName, tableName, columnName, ddlVersionId);
        dropAddrColumnTask.setExceptionAction(DdlExceptionAction.PAUSE);
        tasks.add(dropAddrColumnTask);
        tasks.add(new CleanBlobCacheForTableSyncTask(schemaName, tableName, true));
        tasks.add(new TableSyncTask(schemaName, tableName));

        job.addSequentialTasks(tasks);
        return job;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        // Internalize is the reverse of a single-table externalize ALTER. Match other DRDS
        // single-table DDLs by taking only the target table's exclusive resource; this still
        // blocks concurrent topology/rule changes on the same table while allowing unrelated
        // tables in the schema to run their DDLs.
        resources.add(concatWithDot(schemaName, tableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}
