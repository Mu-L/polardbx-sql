package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.sql.SqlKind;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CdcRebuildCleanupMarkTaskTest {

    @Test
    public void testProtectedMark() {
        String ddl = "ALTER TABLE t REBUILD CLEANUP WHERE status = 'deleted'";
        ExecutionContext executionContext = Mockito.mock(ExecutionContext.class);
        DdlContext ddlContext = new DdlContext();
        ddlContext.setDdlStmt(ddl);
        ddlContext.setDdlType(DdlType.ALTER_TABLE);
        Mockito.when(executionContext.getDdlContext()).thenReturn(ddlContext);

        CdcRebuildCleanupMarkTask task = Mockito.spy(new CdcRebuildCleanupMarkTask("s", "t"));
        Mockito.doNothing().when(task).updateSupportedCommands(true, false, null);

        try (MockedStatic<CdcManagerHelper> managerMock = Mockito.mockStatic(CdcManagerHelper.class);
            MockedStatic<CdcMarkUtil> markUtilMock = Mockito.mockStatic(CdcMarkUtil.class)) {
            CdcManagerHelper manager = Mockito.mock(CdcManagerHelper.class);
            managerMock.when(CdcManagerHelper::getInstance).thenReturn(manager);
            markUtilMock.when(() -> CdcMarkUtil.buildExtendParameter(executionContext))
                .thenReturn(Collections.emptyMap());

            task.duringTransaction(null, executionContext);

            verify(manager, times(1)).notifyDdlNew(
                eq("s"),
                eq("t"),
                eq(SqlKind.REBUILD_CLEANUP.name()),
                eq(ddl),
                eq(DdlType.ALTER_TABLE),
                any(),
                any(),
                eq(CdcDdlMarkVisibility.Protected),
                eq(Collections.emptyMap()));
        }
    }
}
