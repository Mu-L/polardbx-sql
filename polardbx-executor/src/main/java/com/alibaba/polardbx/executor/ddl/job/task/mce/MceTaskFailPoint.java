package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.sync.FailPointDisableSyncAction;
import com.alibaba.polardbx.executor.sync.FailPointEnableSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class MceTaskFailPoint {

    public static final String FP_MCE_BEFORE_CHANGE_WRITE_MODE = "FP_MCE_BEFORE_CHANGE_WRITE_MODE";
    public static final String FP_MCE_BEFORE_BACKFILL = "FP_MCE_BEFORE_BACKFILL";
    public static final String FP_MCE_AFTER_BACKFILL_BATCH = "FP_MCE_AFTER_BACKFILL_BATCH";
    public static final String FP_MCE_FAIL_AFTER_REMOTE_UPLOAD = "FP_MCE_FAIL_AFTER_REMOTE_UPLOAD";
    public static final String FP_MCE_FAIL_AFTER_BACKFILL_UPDATE_BATCH =
        "FP_MCE_FAIL_AFTER_BACKFILL_UPDATE_BATCH";
    public static final String FP_MCE_FAIL_BEFORE_MD5_CHECK = "FP_MCE_FAIL_BEFORE_MD5_CHECK";
    public static final String FP_MCE_AFTER_MD5_CHECK_BATCH = "FP_MCE_AFTER_MD5_CHECK_BATCH";
    public static final String FP_MCE_AFTER_PHYSICAL_DDL = "FP_MCE_AFTER_PHYSICAL_DDL";
    public static final String FP_MCE_PAUSE_GATE_FAIL_ONCE = "FP_MCE_PAUSE_GATE_FAIL_ONCE";
    public static final String FP_MCE_BEFORE_WRITE_ONLY_ADDR = "FP_MCE_BEFORE_WRITE_ONLY_ADDR";
    public static final String FP_MCE_BEFORE_DROP_CONTENT = "FP_MCE_BEFORE_DROP_CONTENT";
    public static final String FP_MCE_AFTER_DROP_CONTENT_PARTITION = "FP_MCE_AFTER_DROP_CONTENT_PARTITION";
    public static final String FP_MCE_INTERNALIZE_AFTER_ADD_CONTENT = "FP_MCE_INTERNALIZE_AFTER_ADD_CONTENT";
    public static final String FP_MCE_INTERNALIZE_BEFORE_CHANGE_WRITE_MODE =
        "FP_MCE_INTERNALIZE_BEFORE_CHANGE_WRITE_MODE";
    public static final String FP_MCE_INTERNALIZE_BEFORE_BACKFILL = "FP_MCE_INTERNALIZE_BEFORE_BACKFILL";
    public static final String FP_MCE_INTERNALIZE_AFTER_BACKFILL_BATCH =
        "FP_MCE_INTERNALIZE_AFTER_BACKFILL_BATCH";
    public static final String FP_MCE_INTERNALIZE_FAIL_BEFORE_CHECK = "FP_MCE_INTERNALIZE_FAIL_BEFORE_CHECK";
    public static final String FP_MCE_INTERNALIZE_BEFORE_CHANGE_READ_MODE =
        "FP_MCE_INTERNALIZE_BEFORE_CHANGE_READ_MODE";
    public static final String FP_MCE_INTERNALIZE_BEFORE_DROP_ADDR = "FP_MCE_INTERNALIZE_BEFORE_DROP_ADDR";

    private static final Set<String> FIRED_ONCE = ConcurrentHashMap.newKeySet();

    private MceTaskFailPoint() {
    }

    public static void pauseWhileEnabled(String key, ExecutionContext executionContext) {
        FailPoint.injectFromHint(key, executionContext, (k, v) -> {
            String reachedKey = k + "_REACHED";
            SyncManagerHelper.syncWithDefaultDb(
                new FailPointEnableSyncAction(reachedKey, "true"), SyncScope.ALL);
            try {
                while (FailPoint.isKeyEnable(k)) {
                    try {
                        Thread.sleep(200L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            } finally {
                SyncManagerHelper.syncWithDefaultDb(
                    new FailPointDisableSyncAction(reachedKey), SyncScope.ALL);
            }
        });
    }

    public static void failAfterPhysicalTables(String key, int completedPhysicalTables,
                                               ExecutionContext executionContext) {
        FailPoint.injectFromHint(key, executionContext, (k, v) -> {
            int threshold = Integer.parseInt(v);
            if (completedPhysicalTables >= threshold) {
                FailPoint.throwException(String.format(
                    "injected failure after %d physical tables at: [%s]", completedPhysicalTables, key));
            }
        });
    }

    /**
     * Fail the first execution of one task only. Unlike process-global failpoints, the hint is
     * carried by this DDL's execution context and the once token is scoped by job/task id, so
     * concurrently running test DDLs cannot trigger each other's injection.
     */
    public static void failOnce(String key, long jobId, long taskId, ExecutionContext executionContext) {
        if (!isEnabled(key, executionContext)) {
            return;
        }
        String token = onceToken(key, jobId, taskId);
        if (FIRED_ONCE.add(token)) {
            throw new RuntimeException(String.format(
                "injected one-shot failure for job=%d, task=%d at: [%s]", jobId, taskId, key));
        }
    }

    public static boolean isEnabled(String key, ExecutionContext executionContext) {
        return executionContext != null && executionContext.getExtraCmds().get(key) != null;
    }

    public static void clearFailOnce(String key, long jobId, long taskId) {
        FIRED_ONCE.remove(onceToken(key, jobId, taskId));
    }

    private static String onceToken(String key, long jobId, long taskId) {
        return key + ':' + jobId + ':' + taskId;
    }
}
