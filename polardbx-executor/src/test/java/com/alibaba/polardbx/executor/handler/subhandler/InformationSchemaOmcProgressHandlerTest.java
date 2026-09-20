package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.ddl.job.task.omc.OmcPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillStatus;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class InformationSchemaOmcProgressHandlerTest {

    @Test
    public void testCompletedCleanupUsesTotalScannedRows() {
        Assert.assertEquals(1000, InformationSchemaOmcProgressHandler.adjustFinishRowCount(
            true, BackfillStatus.SUCCESS.getValue(), 200, 1000));
    }

    @Test
    public void testRunningCleanupKeepsCurrentScannedRows() {
        Assert.assertEquals(600, InformationSchemaOmcProgressHandler.adjustFinishRowCount(
            true, BackfillStatus.RUNNING.getValue(), 600, 1000));
    }

    @Test
    public void testNormalOmcKeepsAffectedRows() {
        Assert.assertEquals(200, InformationSchemaOmcProgressHandler.adjustFinishRowCount(
            false, BackfillStatus.SUCCESS.getValue(), 200, 1000));
    }

    @Test
    public void testIdentifyCleanupTaskFromPersistedTaskRecord() {
        OmcPhyDdlTask cleanupTask = new OmcPhyDdlTask(
            "s", "t", "ALTER TABLE `t` ENGINE=INNODB", "ALTER TABLE t REBUILD CLEANUP WHERE id > 10",
            Collections.emptyMap(), false, "((id > 10) IS NOT TRUE)");
        cleanupTask.setJobId(1L);
        cleanupTask.setTaskId(2L);
        cleanupTask.setRootJobId(1L);
        Assert.assertTrue(InformationSchemaOmcProgressHandler.isRebuildCleanupTask(
            TaskHelper.toDdlEngineTaskRecord(cleanupTask)));

        OmcPhyDdlTask normalTask = new OmcPhyDdlTask(
            "s", "t", "ALTER TABLE `t` ENGINE=INNODB", "ALTER TABLE t MODIFY COLUMN c BIGINT",
            Collections.emptyMap(), false);
        normalTask.setJobId(3L);
        normalTask.setTaskId(4L);
        normalTask.setRootJobId(3L);
        Assert.assertFalse(InformationSchemaOmcProgressHandler.isRebuildCleanupTask(
            TaskHelper.toDdlEngineTaskRecord(normalTask)));
    }

    @Test
    public void testInvalidPersistedTaskRecordIsNotCleanup() {
        DdlEngineTaskRecord invalidTaskRecord = new DdlEngineTaskRecord();
        invalidTaskRecord.taskId = 5L;
        invalidTaskRecord.name = "UnknownTask";
        invalidTaskRecord.value = "{}";
        Assert.assertFalse(InformationSchemaOmcProgressHandler.isRebuildCleanupTask(invalidTaskRecord));
    }
}
