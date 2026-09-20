package com.alibaba.polardbx.executor.ddl.job.task.recyclebin;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.scheduler.SchedulePolicy;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessor;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;

import java.sql.Connection;
import java.util.List;

public class PurgeRecycleBinTask {

    private static volatile PurgeRecycleBinTask instance;

    private boolean init = false;

    public static PurgeRecycleBinTask getInstance() {
        if (instance == null) {
            synchronized (FileSystemManager.class) {
                if (instance == null) {
                    instance = new PurgeRecycleBinTask();
                }
            }
        }
        return instance;
    }

    private PurgeRecycleBinTask() {

    }

    public synchronized void init(ParamManager paramManager) {
        if (!init) {
            String cronExpr = paramManager.getString(ConnectionParams.PURGE_RECYCLEBIN_CRON_EXPR);
            ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
                DefaultDbSchema.NAME,
                null,
                "purge_recycle_bin_schedule_task",
                ScheduledJobExecutorType.PURGE_RECYLE_BIN,
                cronExpr,
                "+08:00",
                SchedulePolicy.WAIT
            );
            try (Connection metaDbConnection = MetaDbDataSource.getInstance().getConnection()) {

                ScheduledJobsAccessor scheduledJobsAccessor = new ScheduledJobsAccessor();
                scheduledJobsAccessor.setConnection(metaDbConnection);
                List<ScheduledJobsRecord> scheduledJobsRecordList =
                    scheduledJobsAccessor.query(DefaultDbSchema.NAME, "purge_recycle_bin_schedule_task");
                if (scheduledJobsRecordList.isEmpty()) {
                    TableMetaChanger.addScheduledJob(metaDbConnection, scheduledJobsRecord);
                } else if (!scheduledJobsRecordList.get(0).getScheduleExpr().equalsIgnoreCase(cronExpr)) {
                    TableMetaChanger.updateScheduledJob(metaDbConnection, scheduledJobsRecord);
                }
            } catch (Throwable t) {
                throw new TddlNestableRuntimeException(t);
            }
            init = true;
        }
    }
}
