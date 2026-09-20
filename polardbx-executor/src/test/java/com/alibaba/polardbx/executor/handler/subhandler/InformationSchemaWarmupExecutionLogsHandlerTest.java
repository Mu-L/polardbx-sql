package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.scheduler.executor.warmup.WarmupTaskManager;
import com.alibaba.polardbx.executor.sync.ISyncManager;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.WarmupExecutionLogSyncAction;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class InformationSchemaWarmupExecutionLogsHandlerTest {
    ExecutionContext executionContext;
    private ArrayResultCursor resultCursor;

    @Before
    public void setup() {
        executionContext = new ExecutionContext();
        executionContext.setSchemaName("test_db");

        resultCursor = new ArrayResultCursor("WARMUP_EXECUTION_LOGS");

        resultCursor.addColumn("TASK_ID", DataTypes.LongType);
        resultCursor.addColumn("STATUS", DataTypes.VarcharType);
        resultCursor.addColumn("CRON_EXPR", DataTypes.VarcharType);
        resultCursor.addColumn("CRON_EXEC_TIME", DataTypes.VarcharType);
        resultCursor.addColumn("START_TIME", DataTypes.DatetimeType);
        resultCursor.addColumn("FINISH_TIME", DataTypes.DatetimeType);
        resultCursor.addColumn("TIME_COST", DataTypes.LongType);
        resultCursor.addColumn("INST_ID", DataTypes.VarcharType);
        resultCursor.addColumn("SCHEMA_NAME", DataTypes.VarcharType);
        resultCursor.addColumn("SQL_DEF", DataTypes.VarcharType);
        resultCursor.addColumn("IO_MESSAGE", DataTypes.VarcharType);
        resultCursor.addColumn("HOST_PORT", DataTypes.VarcharType);

        resultCursor.initMeta();
    }

    @Test
    public void test() {
        try (MockedStatic<ExtensionLoader> extensionLoaderMockedStatic =
            Mockito.mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic =
                Mockito.mockStatic(SyncManagerHelper.class);
            MockedStatic<WarmupTaskManager> warmupTaskManagerMockedStatic =
                Mockito.mockStatic(WarmupTaskManager.class)) {

            // Mock ExtensionLoader first to provide ISyncManager for SyncManagerHelper static init
            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(eq(ISyncManager.class)))
                .thenReturn(Mockito.mock(ISyncManager.class));

            // Mock WarmupTaskManager
            WarmupTaskManager warmupTaskManager = Mockito.mock(WarmupTaskManager.class);
            warmupTaskManagerMockedStatic.when(WarmupTaskManager::getInstance).thenReturn(warmupTaskManager);

            Map<WarmupTaskManager.SnapshotState, List<Object[]>> snapshot = new HashMap<>();
            snapshot.put(WarmupTaskManager.SnapshotState.FINISHED, ImmutableList.of(new Object[] {
                    14, "FINISHED", "*/1 * * * *", "2025-01-07T13:53+08:00[Asia/Shanghai]", " 2025-01-07 13:52:10",
                    "2025-01-07 13:52:33",
                    22883, "pxc-xxx", "test_db", "SELECT * FROM test", "OSSTableScan: 56880438 bytes", "127.0.0.1:3306"
                })
            );

            snapshot.put(WarmupTaskManager.SnapshotState.RUNNING, ImmutableList.of(new Object[] {
                    14, "RUNNING", "*/1 * * * *", "2025-01-07T13:53+08:00[Asia/Shanghai]", " 2025-01-07 13:52:10",
                    "2025-01-07 13:52:33",
                    22883, "pxc-xxx", "test_db", "SELECT * FROM test", "OSSTableScan: 56880438 bytes", "127.0.0.1:3306"
                })
            );
            snapshot.put(WarmupTaskManager.SnapshotState.QUEUED, ImmutableList.of(new Object[] {
                    14, "QUEUED", "*/1 * * * *", "2025-01-07T13:53+08:00[Asia/Shanghai]", " 2025-01-07 13:52:10",
                    "2025-01-07 13:52:33",
                    22883, "pxc-xxx", "test_db", "SELECT * FROM test", "OSSTableScan: 56880438 bytes", "127.0.0.1:3306"
                })
            );
            snapshot.put(WarmupTaskManager.SnapshotState.FAILED, ImmutableList.of(new Object[] {
                    14, "FAILED", "*/1 * * * *", "2025-01-07T13:53+08:00[Asia/Shanghai]", " 2025-01-07 13:52:10",
                    "2025-01-07 13:52:33",
                    22883, "pxc-xxx", "test_db", "SELECT * FROM test", "OSSTableScan: 56880438 bytes", "127.0.0.1:3306"
                })
            );

            Mockito.when(warmupTaskManager.getSnapShot()).thenReturn(snapshot);

            WarmupExecutionLogSyncAction syncAction = new WarmupExecutionLogSyncAction();
            ResultCursor syncResultCursor = syncAction.sync();

            List<Map<String, Object>> localResult = ExecUtils.resultSetToList(syncResultCursor);

            // Use 2-arg overload: syncIgnoreExceptions(IGmsSyncAction, SyncScope)
            syncManagerHelperMockedStatic.when(
                () -> SyncManagerHelper.syncIgnoreExceptions(any(IGmsSyncAction.class), any(SyncScope.class))
            ).thenReturn(
                ImmutableList.of(localResult, localResult, localResult, localResult) // 4 node.
            );

            InformationSchemaWarmupExecutionLogsHandler handler =
                new InformationSchemaWarmupExecutionLogsHandler(Mockito.mock(VirtualViewHandler.class));

            Cursor cursor = handler.handle(Mockito.mock(VirtualView.class), executionContext, resultCursor);
            Row row;
            while ((row = cursor.next()) != null) {
                System.out.println(row);
            }
        }
    }
}
