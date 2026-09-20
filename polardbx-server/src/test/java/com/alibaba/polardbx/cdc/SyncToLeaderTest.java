package com.alibaba.polardbx.cdc;

import com.alibaba.polardbx.common.cdc.CdcDDLContext;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.topology.NodeInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.calcite.sql.SqlKind;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.alibaba.polardbx.common.cdc.ICdcManager.CDC_IS_CCI;
import static com.alibaba.polardbx.common.cdc.ICdcManager.CDC_MARK_RECORD_COMMIT_TSO;
import static com.alibaba.polardbx.common.cdc.ICdcManager.DDL_ID;

public class SyncToLeaderTest {
    @Test
    public void testSyncToLeader() throws NoSuchFieldException, IllegalAccessException {
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = Mockito.mockStatic(
                SyncManagerHelper.class)) {
            Connection connection = Mockito.mock(Connection.class);
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(connection);
            NodeInfoRecord nodeInfoRecord = new NodeInfoRecord();
            nodeInfoRecord.ip = "127.0.0.1";
            nodeInfoRecord.port = 12345;
            List<NodeInfoRecord> nodeInfoRecords = ImmutableList.of(nodeInfoRecord);
            metaDbUtilMock.when(() -> MetaDbUtil.query(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(nodeInfoRecords);

            CdcManagerHelper cdcManagerHelper = CdcManagerHelper.getInstance();
            Field cdcManagerFiled = cdcManagerHelper.getClass().getDeclaredField("cdcManager");
            cdcManagerFiled.setAccessible(true);
            CdcManager cdcManager = (CdcManager) cdcManagerFiled.get(cdcManagerHelper);
            ExecutorService executorService = Mockito.mock(ExecutorService.class);
            Field executorServiceField = cdcManager.getClass().getDeclaredField("managerCoreExecutor");
            executorServiceField.setAccessible(true);
            executorServiceField.set(cdcManager, executorService);
            Mockito.when(executorService.submit(Mockito.any(Runnable.class))).then(
                invocation -> {
                    Object[] args = invocation.getArguments();
                    ((Runnable) args[0]).run();
                    return Futures.immediateFuture(null);
                }
            );
            List<Map<String, Object>> result = ImmutableList.of(ImmutableMap.of("COMMIT_TSO", 1234L));
            syncManagerHelperMockedStatic.when(
                () -> SyncManagerHelper.sync(Mockito.any(IGmsSyncAction.class), Mockito.anyString(),
                    Mockito.anyString())).thenReturn(result);

            Map<String, Object> params = new HashMap<>();
            params.put(CDC_MARK_RECORD_COMMIT_TSO, "true");
            params.put(DDL_ID, 1L);
            params.put(ConnectionProperties.DDL_ON_PRIMARY_GSI_TYPE, "CCI");
            params.put(CDC_IS_CCI, true);
            CdcDDLContext context = new CdcDDLContext("polardbx", "polardbx_function",
                SqlKind.PROCEDURE_CALL.name(),
                "call polardbx.columnar_flush()", CdcDdlMarkVisibility.Protected, null, null,
                DdlType.IGNORED, true, params, null, false, null, null, null, null, false);

            Assert.assertNull(context.getCommitTso());
            cdcManagerHelper.syncToLeaderMarkDdl(context);
            Assert.assertEquals(1234L, context.getCommitTso().longValue());
        }
    }
}
