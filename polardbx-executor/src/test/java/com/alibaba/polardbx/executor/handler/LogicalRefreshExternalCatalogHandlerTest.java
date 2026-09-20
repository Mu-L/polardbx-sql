package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ExternalCatalogSyncAction;
import com.alibaba.polardbx.executor.sync.ISyncManager;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.sql.SqlRefreshExternalCatalog;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalRefreshExternalCatalogHandlerTest {

    private static final String CATALOG_NAME = "cat1";

    @Test
    public void testRefreshTableThrowsUnsupportedBeforePrivilegeAndSync() {
        LogicalDal logicalDal = mockLogicalDal(new SqlRefreshExternalCatalog(SqlParserPos.ZERO,
            CATALOG_NAME, "db1", "tb1"));
        ExecutionContext executionContext = mock(ExecutionContext.class);
        LogicalRefreshExternalCatalogHandler handler = newHandler();

        try (MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class)) {
            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(ISyncManager.class))
                .thenReturn(mock(ISyncManager.class));
            try (MockedStatic<PolarPrivilegeUtils> privilegeUtilsMockedStatic = mockStatic(PolarPrivilegeUtils.class);
                MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class)) {
                try {
                    handler.handle(logicalDal, executionContext);
                    fail("Expected unsupported table refresh error");
                } catch (TddlRuntimeException e) {
                    assertEquals(ErrorCode.ERR_EXTERNAL_TABLE, e.getErrorCodeType());
                    assertThat(e.getMessage(), containsString("REFRESH EXTERNAL TABLE is not supported"));
                }

                privilegeUtilsMockedStatic.verifyNoInteractions();
                syncManagerHelperMockedStatic.verifyNoInteractions();
            }
        }
    }

    @Test
    public void testRefreshCatalogChecksPrivilegeBroadcastsRefreshAndReturnsZeroAffectedRows() {
        LogicalDal logicalDal = mockLogicalDal(new SqlRefreshExternalCatalog(SqlParserPos.ZERO,
            CATALOG_NAME, null, null));
        ExecutionContext executionContext = mock(ExecutionContext.class);
        AtomicReference<IGmsSyncAction> actionRef = new AtomicReference<>();
        LogicalRefreshExternalCatalogHandler handler = newHandler();

        try (MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class)) {
            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(ISyncManager.class))
                .thenReturn(mock(ISyncManager.class));
            try (MockedStatic<PolarPrivilegeUtils> privilegeUtilsMockedStatic = mockStatic(PolarPrivilegeUtils.class);
                MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class)) {
                syncManagerHelperMockedStatic.when(
                        () -> SyncManagerHelper.syncThrowExceptions(Mockito.any(IGmsSyncAction.class),
                            Mockito.eq(SyncScope.ALL)))
                    .thenAnswer(invocation -> {
                        actionRef.set(invocation.getArgument(0));
                        return null;
                    });

                Cursor cursor = handler.handle(logicalDal, executionContext);

                privilegeUtilsMockedStatic.verify(
                    () -> PolarPrivilegeUtils.checkInstancePrivilege(PrivilegePoint.ALTER, executionContext));
                syncManagerHelperMockedStatic.verify(
                    () -> SyncManagerHelper.syncThrowExceptions(Mockito.any(IGmsSyncAction.class),
                        Mockito.eq(SyncScope.ALL)));
                assertThat(cursor, instanceOf(AffectRowCursor.class));
                assertArrayEquals(new int[] {0}, ((AffectRowCursor) cursor).getAffectRows());
                assertThat(actionRef.get(), instanceOf(ExternalCatalogSyncAction.class));
                ExternalCatalogSyncAction action = (ExternalCatalogSyncAction) actionRef.get();
                assertEquals(CATALOG_NAME, action.getCatalogName());
                assertEquals("REFRESH", action.getAction());
            }
        }
    }

    @Test
    public void testRefreshCatalogPropagatesSyncFailure() {
        LogicalDal logicalDal = mockLogicalDal(new SqlRefreshExternalCatalog(SqlParserPos.ZERO,
            CATALOG_NAME, null, null));
        ExecutionContext executionContext = mock(ExecutionContext.class);
        TddlRuntimeException syncFailure = new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "sync failed");
        LogicalRefreshExternalCatalogHandler handler = newHandler();

        try (MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class)) {
            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(ISyncManager.class))
                .thenReturn(mock(ISyncManager.class));
            try (MockedStatic<PolarPrivilegeUtils> privilegeUtilsMockedStatic = mockStatic(PolarPrivilegeUtils.class);
                MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class)) {
                syncManagerHelperMockedStatic.when(
                        () -> SyncManagerHelper.syncThrowExceptions(Mockito.any(IGmsSyncAction.class),
                            Mockito.eq(SyncScope.ALL)))
                    .thenThrow(syncFailure);

                try {
                    handler.handle(logicalDal, executionContext);
                    fail("Expected sync failure to propagate");
                } catch (TddlRuntimeException e) {
                    assertSame(syncFailure, e);
                }

                privilegeUtilsMockedStatic.verify(
                    () -> PolarPrivilegeUtils.checkInstancePrivilege(PrivilegePoint.ALTER, executionContext));
                syncManagerHelperMockedStatic.verify(
                    () -> SyncManagerHelper.syncThrowExceptions(Mockito.any(IGmsSyncAction.class),
                        Mockito.eq(SyncScope.ALL)));
            }
        }
    }

    private LogicalRefreshExternalCatalogHandler newHandler() {
        return new LogicalRefreshExternalCatalogHandler(mock(IRepository.class));
    }

    private LogicalDal mockLogicalDal(SqlRefreshExternalCatalog refreshNode) {
        LogicalDal logicalDal = mock(LogicalDal.class);
        when(logicalDal.getNativeSqlNode()).thenReturn(refreshNode);
        return logicalDal;
    }
}
