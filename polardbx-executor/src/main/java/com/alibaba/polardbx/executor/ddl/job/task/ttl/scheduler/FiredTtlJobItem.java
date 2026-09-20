package com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler;

import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;

public class FiredTtlJobItem {

    /**
     * fired_time#schema_name#table_group_id
     */
    public final static String FIRED_TTL_JOB_TG_KEY_TEMPLATE = "%s#%s#%s";

    protected ExecutableScheduledJob job;
    protected TableMeta tableMeta;
    protected String jobItemTgKey = "";

    public FiredTtlJobItem(ExecutableScheduledJob job, TableMeta tableMeta) {
        this.job = job;
        this.tableMeta = tableMeta;
        this.jobItemTgKey = buildJobItemTgKey(job, tableMeta);
    }

    public static String buildJobItemTgKey(ExecutableScheduledJob job, TableMeta tableMeta) {
        String jobItemTgKey = String.format(FIRED_TTL_JOB_TG_KEY_TEMPLATE, job.getFireTime(), job.getTableSchema(),
            tableMeta.getPartitionInfo().getTableGroupId());
        return jobItemTgKey;
    }

    public ExecutableScheduledJob getJob() {
        return job;
    }

    public void setJob(ExecutableScheduledJob job) {
        this.job = job;
    }

    public TableMeta getTableMeta() {
        return tableMeta;
    }

    public void setTableMeta(TableMeta tableMeta) {
        this.tableMeta = tableMeta;
    }

    public String getJobItemTgKey() {
        return jobItemTgKey;
    }
}
