package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class CheckTableMetaVersionSyncActionTest {

    private static final String SCHEMA_NAME = "test_schema";
    private static final String TABLE_NAME = "test_table";

    private MockedStatic<OptimizerContext> optimizerContextMockedStatic;
    private MockedStatic<MetaDbUtil> metaDbUtilMockedStatic;
    private MockedStatic<TableInfoManager> tableInfoManagerMockedStatic;

    private SchemaManager schemaManager;
    private TableMeta tableMeta;
    private Connection metaDbConn;

    @Before
    public void setUp() {
        schemaManager = Mockito.mock(SchemaManager.class);
        tableMeta = Mockito.mock(TableMeta.class);
        metaDbConn = Mockito.mock(Connection.class);

        OptimizerContext optimizerContext = Mockito.mock(OptimizerContext.class);
        Mockito.when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
        Mockito.when(schemaManager.getTableWithNull(TABLE_NAME)).thenReturn(tableMeta);

        optimizerContextMockedStatic = Mockito.mockStatic(OptimizerContext.class);
        optimizerContextMockedStatic.when(() -> OptimizerContext.getContext(SCHEMA_NAME)).thenReturn(optimizerContext);

        metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
        metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(metaDbConn);

        tableInfoManagerMockedStatic = Mockito.mockStatic(TableInfoManager.class);
    }

    @After
    public void tearDown() {
        if (optimizerContextMockedStatic != null) {
            optimizerContextMockedStatic.close();
        }
        if (metaDbUtilMockedStatic != null) {
            metaDbUtilMockedStatic.close();
        }
        if (tableInfoManagerMockedStatic != null) {
            tableInfoManagerMockedStatic.close();
        }
    }

    @Test
    public void testSyncReturnsNullWhenVersionMatches() {
        Mockito.when(tableMeta.getVersion()).thenReturn(1L);
        tableInfoManagerMockedStatic
            .when(() -> TableInfoManager.checkTableVersion(anyString(), anyString(), any(Connection.class)))
            .thenReturn(1L);

        CheckTableMetaVersionSyncAction action = new CheckTableMetaVersionSyncAction(SCHEMA_NAME, TABLE_NAME);
        Assert.assertNull(action.sync());
    }

    @Test
    public void testSyncReturnsNullWhenTableMetaMissing() {
        Mockito.when(schemaManager.getTableWithNull(TABLE_NAME)).thenReturn(null);

        CheckTableMetaVersionSyncAction action = new CheckTableMetaVersionSyncAction(SCHEMA_NAME, TABLE_NAME);
        Assert.assertNull(action.sync());
    }

    @Test
    public void testRethrowsVersionMismatchTddlRuntimeException() {
        Mockito.when(tableMeta.getVersion()).thenReturn(1L);
        tableInfoManagerMockedStatic
            .when(() -> TableInfoManager.checkTableVersion(anyString(), anyString(), any(Connection.class)))
            .thenReturn(2L);

        CheckTableMetaVersionSyncAction action = new CheckTableMetaVersionSyncAction(SCHEMA_NAME, TABLE_NAME);
        try {
            action.sync();
            Assert.fail("expected TddlRuntimeException for version mismatch");
        } catch (TddlRuntimeException e) {
            Assert.assertEquals(ErrorCode.ERR_CHECK_TABLE_META_VERSION, e.getErrorCodeType());
            Assert.assertTrue(e.getMessage().contains("is not equal to the version"));
            Assert.assertFalse(e.getMessage().contains("failed to check tableMeta version"));
        }
    }

    @Test
    public void testWrapsNonTddlException() {
        Mockito.when(tableMeta.getVersion()).thenReturn(1L);
        tableInfoManagerMockedStatic
            .when(() -> TableInfoManager.checkTableVersion(anyString(), anyString(), any(Connection.class)))
            .thenThrow(new RuntimeException("metadb unavailable"));

        CheckTableMetaVersionSyncAction action = new CheckTableMetaVersionSyncAction(SCHEMA_NAME, TABLE_NAME);
        try {
            action.sync();
            Assert.fail("expected TddlRuntimeException wrapping the original failure");
        } catch (TddlRuntimeException e) {
            Assert.assertEquals(ErrorCode.ERR_CHECK_TABLE_META_VERSION, e.getErrorCodeType());
            Assert.assertTrue(e.getMessage().contains("failed to check tableMeta version"));
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
        }
    }
}
