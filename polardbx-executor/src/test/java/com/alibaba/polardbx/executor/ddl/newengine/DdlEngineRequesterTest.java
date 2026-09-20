package com.alibaba.polardbx.executor.ddl.newengine;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineResourceManager;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DdlEngineRequesterTest {

    @Test
    public void testTryRollbackOrContinueJobRemovesInitialJob() {
        ExecutionContext ec = new ExecutionContext();
        DdlEngineRecord record = new DdlEngineRecord();
        record.jobId = 123L;
        record.schemaName = "test_schema";
        record.state = DdlState.INITIAL.name();
        DdlContext acquiringContext = new DdlContext();
        acquiringContext.setJobId(123L);

        try (MockedConstruction<DdlJobManager> mockedJobManager = Mockito.mockConstruction(
            DdlJobManager.class,
            (mock, context) -> {
                when(mock.fetchRecordByJobId(anyLong())).thenReturn(record);
                when(mock.removeInitialJob(anyLong())).thenReturn(true);
            });
            MockedStatic<DdlEngineResourceManager> mockedResourceManager = mockStatic(DdlEngineResourceManager.class)) {
            mockedResourceManager.when(() -> DdlEngineResourceManager.getAllDdlAcquiringLocks("test_schema"))
                .thenReturn(Collections.singletonList(acquiringContext));

            Boolean result = DdlEngineRequester.tryRollbackOrContinueJob(123L, ec);

            Assert.assertTrue(result);
            Assert.assertTrue(acquiringContext.isClientConnectionReset());
            // The reset signal must be set on ExecutionContext before removing the INITIAL job.
            Assert.assertTrue(ec.isDdlClientConnectionReset());
            Mockito.verify(mockedJobManager.constructed().get(0)).removeInitialJob(123L);
        }
    }

    @Test
    public void testTryRollbackOrContinueJobRefetchesWhenInitialJobUpgraded() {
        ExecutionContext ec = new ExecutionContext();
        DdlEngineRecord initialRecord = new DdlEngineRecord();
        initialRecord.jobId = 123L;
        initialRecord.schemaName = "test_schema";
        initialRecord.state = DdlState.INITIAL.name();
        DdlEngineRecord upgradedRecord = new DdlEngineRecord();
        upgradedRecord.jobId = 123L;
        upgradedRecord.schemaName = "test_schema";
        upgradedRecord.state = DdlState.RUNNING.name();
        upgradedRecord.supportedCommands = 0;

        try (MockedConstruction<DdlJobManager> mockedJobManager = Mockito.mockConstruction(
            DdlJobManager.class,
            (mock, context) -> {
                when(mock.fetchRecordByJobId(anyLong())).thenReturn(initialRecord, upgradedRecord);
                when(mock.removeInitialJob(anyLong())).thenReturn(false);
            });
            MockedStatic<DdlEngineResourceManager> mockedResourceManager = mockStatic(DdlEngineResourceManager.class)) {
            mockedResourceManager.when(() -> DdlEngineResourceManager.getAllDdlAcquiringLocks("test_schema"))
                .thenReturn(Collections.emptyList());

            Boolean result = DdlEngineRequester.tryRollbackOrContinueJob(123L, ec);

            Assert.assertTrue(result);
            Assert.assertTrue(ec.isDdlClientConnectionReset());
            // The record has been upgraded by storeJobImpl; it should be re-fetched.
            Mockito.verify(mockedJobManager.constructed().get(0), Mockito.times(2)).fetchRecordByJobId(123L);
        }
    }

    @Test
    public void testTryRollbackOrContinueJobMarksAcquiringContextResetWhenRecordMissing() {
        ExecutionContext ec = new ExecutionContext();

        try (MockedConstruction<DdlJobManager> ignored = Mockito.mockConstruction(
            DdlJobManager.class,
            (mock, context) -> when(mock.fetchRecordByJobId(anyLong())).thenReturn(null))) {
            Boolean result = DdlEngineRequester.tryRollbackOrContinueJob(123L, ec);

            Assert.assertFalse(result);
            // The signal is set on ExecutionContext directly: DdlContext may not exist yet.
            Assert.assertTrue(ec.isDdlClientConnectionReset());
        }
    }
}