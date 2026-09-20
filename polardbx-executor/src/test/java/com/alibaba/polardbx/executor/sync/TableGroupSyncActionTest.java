package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TableGroupSyncActionTest {

    @Test
    public void testSyncWithUnavailableContextThrowsContextualError() {
        TableGroupSyncAction action = new TableGroupSyncAction("ut_nonexistent_schema_for_tg_sync", "ut_tg");
        try {
            action.sync();
            Assert.fail("expected TddlRuntimeException when optimizer context is unavailable");
        } catch (TddlRuntimeException e) {
            String message = String.valueOf(e.getMessage());
            Assert.assertTrue("error message should contain the schema name, but got: " + message,
                message.contains("ut_nonexistent_schema_for_tg_sync"));
            Assert.assertTrue("error message should contain the table group name, but got: " + message,
                message.contains("ut_tg"));
        }
    }

    @Test
    public void testSyncWithFailPointInjectedContextUnavailable() {
        TableGroupSyncAction action = new TableGroupSyncAction("ut_nonexistent_schema_for_tg_sync", "ut_tg");
        FailPoint.enable(FailPointKey.FP_TABLE_GROUP_SYNC_CONTEXT_NULL, "true");
        try {
            action.sync();
            Assert.fail("expected TddlRuntimeException when the failpoint is injected");
        } catch (TddlRuntimeException e) {
            String message = String.valueOf(e.getMessage());
            if (FailPoint.isAssertEnable()) {
                Assert.assertTrue("error message should carry the failpoint marker, but got: " + message,
                    message.contains("[failpoint]"));
            }
            Assert.assertTrue("error message should contain the schema name, but got: " + message,
                message.contains("ut_nonexistent_schema_for_tg_sync"));
        } finally {
            FailPoint.disable(FailPointKey.FP_TABLE_GROUP_SYNC_CONTEXT_NULL);
        }
    }

    @Test
    public void testSyncWithAvailableContextReloadsTableGroup() {
        TableGroupSyncAction action = new TableGroupSyncAction("ut_schema_with_context", "ut_tg_ok");
        OptimizerContext optimizerContext = mock(OptimizerContext.class);
        TableGroupInfoManager tableGroupInfoManager = mock(TableGroupInfoManager.class);
        when(optimizerContext.getTableGroupInfoManager()).thenReturn(tableGroupInfoManager);
        try (MockedStatic<OptimizerContext> contextMock = mockStatic(OptimizerContext.class)) {
            contextMock.when(() -> OptimizerContext.getContext("ut_schema_with_context"))
                .thenReturn(optimizerContext);
            action.sync();
        }
        verify(tableGroupInfoManager).reloadTableGroupByGroupName("ut_schema_with_context", "ut_tg_ok");
    }
}
