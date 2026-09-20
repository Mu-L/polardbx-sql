package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.common.RecycleBinManager;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalDropTableHandlerTest {

    private MockedStatic<OptimizerContext> mockNonExternalizedTable(String schema, String table) {
        MockedStatic<OptimizerContext> mockedStaticOptimizerContext = mockStatic(OptimizerContext.class);
        OptimizerContext optimizerContext = mock(OptimizerContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        mockedStaticOptimizerContext.when(() -> OptimizerContext.getContext(schema)).thenReturn(optimizerContext);
        when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
        when(schemaManager.getTable(table)).thenReturn(tableMeta);
        when(schemaManager.getTableWithNull(table)).thenReturn(tableMeta);
        when(tableMeta.hasExternalizedColumn()).thenReturn(false);
        when(tableMeta.hasColumnInMceLifecycle()).thenReturn(false);
        return mockedStaticOptimizerContext;
    }

    /**
     * Test buildDdlJob for non-partitioned database with GSI
     */
    @Test
    public void testBuildDdlJobForNonPartitionedDbWithGsi() {
        String schema = "test_schema";
        String table = "test_table";
        String appName = "test_app";

        // Mock the handler to avoid executing internal methods
        LogicalDropTableHandler handler = mock(LogicalDropTableHandler.class);
        when(handler.buildDdlJob(any(), any())).thenCallRealMethod();
        when(handler.buildDropTableWithGsiJob(any(), any())).thenReturn(mock(ExecutableDdlJob.class));

        ExecutionContext ec = new ExecutionContext(schema);
        ec.setAppName(appName);
        LogicalDropTable logicalDropTable = mock(LogicalDropTable.class);
        when(logicalDropTable.getSchemaName()).thenReturn(schema);
        when(logicalDropTable.getTableName()).thenReturn(table);
        when(logicalDropTable.isWithGsi()).thenReturn(true);
        when(logicalDropTable.isPurge()).thenReturn(false);

        ParamManager pm = mock(ParamManager.class);
        ec.setParamManager(pm);
        when(pm.getBoolean(eq(ConnectionParams.ENABLE_RECYCLEBIN))).thenReturn(false);
        when(pm.getBoolean(eq(ConnectionParams.IMPORT_TABLE))).thenReturn(false);

        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        DropTableWithGsiPreparedData data = mock(DropTableWithGsiPreparedData.class);
        when(logicalDropTable.getDropTableWithGsiPreparedData()).thenReturn(data);

        // Mock RecycleBin and RecycleBinManager
        RecycleBin recycleBin = mock(RecycleBin.class);
        when(recycleBin.genName()).thenReturn("recycle_bin_name");

        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class);
            MockedStatic<TtlUtil> mockedStaticTtlUtil = mockStatic(TtlUtil.class);
            MockedStatic<LogicalDropTableHandler> mockedStaticLogicalDropTableHandler = mockStatic(
                LogicalDropTableHandler.class);
            MockedStatic<DdlUtils> mockedStaticDdlUtils = mockStatic(DdlUtils.class);
            MockedStatic<CheckOSSArchiveUtil> mockedStaticCheckOSSArchiveUtil = mockStatic(CheckOSSArchiveUtil.class);
            MockedStatic<RecycleBinManager> mockedStaticRecycleBinManager = mockStatic(RecycleBinManager.class);
            MockedStatic<OptimizerContext> mockedStaticOptimizerContext = mockNonExternalizedTable(schema, table)) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            mockedStaticTtlUtil.when(
                    () -> TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(anyString(), anyString(), any()))
                .thenReturn(false);
            // Ignore tryForbidDropTableOperationIfNeed method
            mockedStaticLogicalDropTableHandler.when(
                    () -> LogicalDropTableHandler.tryForbidDropTableOperationIfNeed(any(), anyString(), anyString()))
                .then(invocation -> {
                    // Do nothing to skip the method execution
                    return null;
                });
            // Ignore generateVersionId method
            mockedStaticDdlUtils.when(() -> DdlUtils.generateVersionId(any()))
                .thenReturn(1L);
// Ignore checkWithoutOSS method
            mockedStaticCheckOSSArchiveUtil.when(() -> CheckOSSArchiveUtil.checkWithoutOSS(anyString(), anyString()))
                .thenReturn(true);
            // Mock RecycleBinManager.getInstance().getByAppName()
            RecycleBinManager recycleBinManager = mock(RecycleBinManager.class);
            mockedStaticRecycleBinManager.when(RecycleBinManager::getInstance).thenReturn(recycleBinManager);
            when(recycleBinManager.getByAppName(anyString())).thenReturn(recycleBin);

            // Non-partitioned database
            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(false);

            // Should go through buildDropTableWithGsiJob path
            DdlJob ddlJob = handler.buildDdlJob(logicalDropTable, ec);

            // Verify that we got a job (not null)
            Assert.assertTrue(ddlJob != null);
        }
    }

    /**
     * Test buildDdlJob for non-partitioned database without GSI with recycle bin
     */
    @Test
    public void testBuildDdlJobForNonPartitionedDbWithoutGsiWithRecycleBin() {
        String schema = "test_schema";
        String table = "test_table";

        // Mock the handler to avoid executing internal methods
        LogicalDropTableHandler handler = mock(LogicalDropTableHandler.class);
        when(handler.buildDdlJob(any(), any())).thenCallRealMethod();
        when(handler.handleRecycleBin(any(), any())).thenReturn(mock(ExecutableDdlJob.class));

        ExecutionContext ec = new ExecutionContext(schema);
        ec.setAppName("test_app");
        LogicalDropTable logicalDropTable = mock(LogicalDropTable.class);
        when(logicalDropTable.getSchemaName()).thenReturn(schema);
        when(logicalDropTable.getTableName()).thenReturn(table);
        when(logicalDropTable.isWithGsi()).thenReturn(false);
        when(logicalDropTable.isPurge()).thenReturn(false);
        when(logicalDropTable.ifExists()).thenReturn(false);

        ParamManager pm = mock(ParamManager.class);
        ec.setParamManager(pm);
        when(pm.getBoolean(eq(ConnectionParams.ENABLE_RECYCLEBIN))).thenReturn(true);
        when(pm.getBoolean(eq(ConnectionParams.IMPORT_TABLE))).thenReturn(false);

        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        DropTableWithGsiPreparedData data = mock(DropTableWithGsiPreparedData.class);
        when(logicalDropTable.getDropTableWithGsiPreparedData()).thenReturn(data);

        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class);
            MockedStatic<TtlUtil> mockedStaticTtlUtil = mockStatic(TtlUtil.class);
            MockedStatic<LogicalCommonDdlHandler> mockedStaticLogicalCommonDdlHandler = mockStatic(
                LogicalCommonDdlHandler.class);
            MockedStatic<LogicalDropTableHandler> mockedStaticLogicalDropTableHandler = mockStatic(
                LogicalDropTableHandler.class);
            MockedStatic<DdlUtils> mockedStaticDdlUtils = mockStatic(DdlUtils.class);
            MockedStatic<CheckOSSArchiveUtil> mockedStaticCheckOSSArchiveUtil = mockStatic(CheckOSSArchiveUtil.class);
            MockedStatic<OptimizerContext> mockedStaticOptimizerContext = mockNonExternalizedTable(schema, table)) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            mockedStaticTtlUtil.when(
                    () -> TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(anyString(), anyString(), any()))
                .thenReturn(false);
            mockedStaticLogicalCommonDdlHandler.when(
                    () -> LogicalCommonDdlHandler.isAvailableForRecycleBin(eq(schema), eq(table), eq(ec)))
                .thenReturn(true);
            // Ignore tryForbidDropTableOperationIfNeed method
            mockedStaticLogicalDropTableHandler.when(
                    () -> LogicalDropTableHandler.tryForbidDropTableOperationIfNeed(any(), anyString(), anyString()))
                .then(invocation -> {
                    // Do nothing to skip the method execution
                    return null;
                });
            // Ignore generateVersionId method
            mockedStaticDdlUtils.when(() -> DdlUtils.generateVersionId(any()))
                .thenReturn(1L);
            // Ignore checkWithoutOSS method
            mockedStaticCheckOSSArchiveUtil.when(() -> CheckOSSArchiveUtil.checkWithoutOSS(anyString(), anyString()))
                .thenReturn(true);

            // Non-partitioned database
            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(false);

            // Should go through handleRecycleBin path
            DdlJob ddlJob = handler.buildDdlJob(logicalDropTable, ec);

            // Verify that we got a job (not null)
            Assert.assertTrue(ddlJob != null);
        }
    }

    /**
     * Test buildDdlJob for non-partitioned database without GSI without recycle bin
     */
    @Test
    public void testBuildDdlJobForNonPartitionedDbWithoutGsiWithoutRecycleBin() {
        String schema = "test_schema";
        String table = "test_table";

        // Mock the handler to avoid executing internal methods
        LogicalDropTableHandler handler = mock(LogicalDropTableHandler.class);
        when(handler.buildDdlJob(any(), any())).thenCallRealMethod();
        when(handler.buildDropTableJob(any(), any())).thenReturn(mock(ExecutableDdlJob.class));

        ExecutionContext ec = new ExecutionContext(schema);
        LogicalDropTable logicalDropTable = mock(LogicalDropTable.class);
        when(logicalDropTable.getSchemaName()).thenReturn(schema);
        when(logicalDropTable.getTableName()).thenReturn(table);
        when(logicalDropTable.isWithGsi()).thenReturn(false);
        when(logicalDropTable.isPurge()).thenReturn(true);
        when(logicalDropTable.ifExists()).thenReturn(false);

        ParamManager pm = mock(ParamManager.class);
        ec.setParamManager(pm);
        when(pm.getBoolean(eq(ConnectionParams.ENABLE_RECYCLEBIN))).thenReturn(true);
        when(pm.getBoolean(eq(ConnectionParams.IMPORT_TABLE))).thenReturn(false);

        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        DropTableWithGsiPreparedData data = mock(DropTableWithGsiPreparedData.class);
        when(logicalDropTable.getDropTableWithGsiPreparedData()).thenReturn(data);

        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class);
            MockedStatic<TtlUtil> mockedStaticTtlUtil = mockStatic(TtlUtil.class);
            MockedStatic<LogicalCommonDdlHandler> mockedStaticLogicalCommonDdlHandler = mockStatic(
                LogicalCommonDdlHandler.class);
            MockedStatic<LogicalDropTableHandler> mockedStaticLogicalDropTableHandler = mockStatic(
                LogicalDropTableHandler.class);
            MockedStatic<DdlUtils> mockedStaticDdlUtils = mockStatic(DdlUtils.class);
            MockedStatic<CheckOSSArchiveUtil> mockedStaticCheckOSSArchiveUtil = mockStatic(CheckOSSArchiveUtil.class)) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            mockedStaticTtlUtil.when(
                    () -> TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(anyString(), anyString(), any()))
                .thenReturn(false);
            mockedStaticLogicalCommonDdlHandler.when(
                    () -> LogicalCommonDdlHandler.isAvailableForRecycleBin(eq(schema), eq(table), eq(ec)))
                .thenReturn(false);
            // Ignore tryForbidDropTableOperationIfNeed method
            mockedStaticLogicalDropTableHandler.when(
                    () -> LogicalDropTableHandler.tryForbidDropTableOperationIfNeed(any(), anyString(), anyString()))
                .then(invocation -> {
                    // Do nothing to skip the method execution
                    return null;
                });
            // Ignore generateVersionId method
            mockedStaticDdlUtils.when(() -> DdlUtils.generateVersionId(any()))
                .thenReturn(1L);
            // Ignore checkWithoutOSS method
            mockedStaticCheckOSSArchiveUtil.when(() -> CheckOSSArchiveUtil.checkWithoutOSS(anyString(), anyString()))
                .thenReturn(true);

            // Non-partitioned database
            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(false);

            // Should go through buildDropTableJob path
            DdlJob ddlJob = handler.buildDdlJob(logicalDropTable, ec);

            // Verify that we got a job (not null)
            Assert.assertTrue(ddlJob != null);
        }
    }

    /**
     * Test buildDdlJob for partitioned database with GSI
     */
    @Test
    public void testBuildDdlJobForPartitionedDbWithGsi() {
        String schema = "test_schema";
        String table = "test_table";

        // Mock the handler to avoid executing internal methods
        LogicalDropTableHandler handler = mock(LogicalDropTableHandler.class);
        when(handler.buildDdlJob(any(), any())).thenCallRealMethod();
        when(handler.buildDropPartitionTableWithGsiJob(any(), any())).thenReturn(mock(ExecutableDdlJob.class));

        ExecutionContext ec = new ExecutionContext(schema);
        LogicalDropTable logicalDropTable = mock(LogicalDropTable.class);
        when(logicalDropTable.getSchemaName()).thenReturn(schema);
        when(logicalDropTable.getTableName()).thenReturn(table);
        when(logicalDropTable.isWithGsi()).thenReturn(true);
        when(logicalDropTable.isPurge()).thenReturn(false);
        when(logicalDropTable.ifExists()).thenReturn(false);

        ParamManager pm = mock(ParamManager.class);
        ec.setParamManager(pm);
        when(pm.getBoolean(eq(ConnectionParams.ENABLE_RECYCLEBIN))).thenReturn(false);
        when(pm.getBoolean(eq(ConnectionParams.IMPORT_TABLE))).thenReturn(false);

        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        DropTableWithGsiPreparedData data = mock(DropTableWithGsiPreparedData.class);
        when(logicalDropTable.getDropTableWithGsiPreparedData()).thenReturn(data);

        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class);
            MockedStatic<TtlUtil> mockedStaticTtlUtil = mockStatic(TtlUtil.class);
            MockedStatic<LogicalDropTableHandler> mockedStaticLogicalDropTableHandler = mockStatic(
                LogicalDropTableHandler.class);
            MockedStatic<DdlUtils> mockedStaticDdlUtils = mockStatic(DdlUtils.class);
            MockedStatic<CheckOSSArchiveUtil> mockedStaticCheckOSSArchiveUtil = mockStatic(CheckOSSArchiveUtil.class);
            MockedStatic<OptimizerContext> mockedStaticOptimizerContext = mockStatic(OptimizerContext.class)) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            mockedStaticTtlUtil.when(
                    () -> TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(anyString(), anyString(), any()))
                .thenReturn(false);
            // Ignore tryForbidDropTableOperationIfNeed method
            mockedStaticLogicalDropTableHandler.when(
                    () -> LogicalDropTableHandler.tryForbidDropTableOperationIfNeed(any(), anyString(), anyString()))
                .then(invocation -> {
                    // Do nothing to skip the method execution
                    return null;
                });
            // Ignore generateVersionId method
            mockedStaticDdlUtils.when(() -> DdlUtils.generateVersionId(any()))
                .thenReturn(1L);
            // Ignore checkWithoutOSS method
            mockedStaticCheckOSSArchiveUtil.when(() -> CheckOSSArchiveUtil.checkWithoutOSS(anyString(), anyString()))
                .thenReturn(true);
            // Mock OptimizerContext for appendDropShadowTableSubJobIfNeeded
            mockedStaticOptimizerContext.when(() -> OptimizerContext.getContext(anyString()))
                .thenReturn(null);

            // Partitioned database
            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(true);

            // Should go through buildDropPartitionTableWithGsiJob path
            DdlJob ddlJob = handler.buildDdlJob(logicalDropTable, ec);

            // Verify that we got a job (not null)
            Assert.assertTrue(ddlJob != null);
        }
    }

    /**
     * Test buildDdlJob for partitioned database with file storage engine
     */
    @Test
    public void testBuildDdlJobForPartitionedDbWithFileStorage() {
        String schema = "test_schema";
        String table = "test_table";

        // Mock the handler to avoid executing internal methods
        LogicalDropTableHandler handler = mock(LogicalDropTableHandler.class);
        when(handler.buildDdlJob(any(), any())).thenCallRealMethod();
        when(handler.buildRecycleFileStorageTableJob(any(), any())).thenReturn(mock(ExecutableDdlJob.class));

        ExecutionContext ec = new ExecutionContext(schema);
        String appName = "test_app";
        ec.setAppName(appName);
        LogicalDropTable logicalDropTable = mock(LogicalDropTable.class);
        when(logicalDropTable.getSchemaName()).thenReturn(schema);
        when(logicalDropTable.getTableName()).thenReturn(table);
        when(logicalDropTable.isWithGsi()).thenReturn(false);
        when(logicalDropTable.isPurge()).thenReturn(false);
        when(logicalDropTable.ifExists()).thenReturn(false);

        ParamManager pm = mock(ParamManager.class);
        ec.setParamManager(pm);
        when(pm.getBoolean(eq(ConnectionParams.ENABLE_RECYCLEBIN))).thenReturn(true);
        when(pm.getBoolean(eq(ConnectionParams.IMPORT_TABLE))).thenReturn(false);
        when(pm.getBoolean(eq(ConnectionParams.PURGE_FILE_STORAGE_TABLE))).thenReturn(false);

        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        DropTableWithGsiPreparedData data = mock(DropTableWithGsiPreparedData.class);
        when(logicalDropTable.getDropTableWithGsiPreparedData()).thenReturn(data);

        OptimizerContext optimizerContext = mock(OptimizerContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);

        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class);
            MockedStatic<TtlUtil> mockedStaticTtlUtil = mockStatic(TtlUtil.class);
            MockedStatic<OptimizerContext> mockedStaticOptimizerContext = mockStatic(OptimizerContext.class);
            MockedStatic<LogicalDropTableHandler> mockedStaticLogicalDropTableHandler = mockStatic(
                LogicalDropTableHandler.class);
            MockedStatic<DdlUtils> mockedStaticDdlUtils = mockStatic(DdlUtils.class);
            MockedStatic<RecycleBinManager> mockedStaticRecycleBinManager = mockStatic(RecycleBinManager.class);
            MockedStatic<CheckOSSArchiveUtil> mockedStaticCheckOSSArchiveUtil = mockStatic(CheckOSSArchiveUtil.class)) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            mockedStaticTtlUtil.when(
                    () -> TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(anyString(), anyString(), any()))
                .thenReturn(false);
            mockedStaticOptimizerContext.when(() -> OptimizerContext.getContext(eq(schema)))
                .thenReturn(optimizerContext);
            // Ignore tryForbidDropTableOperationIfNeed method
            mockedStaticLogicalDropTableHandler.when(
                    () -> LogicalDropTableHandler.tryForbidDropTableOperationIfNeed(any(), anyString(), anyString()))
                .then(invocation -> {
                    // Do nothing to skip the method execution
                    return null;
                });
            RecycleBinManager recycleBinManager = mock(RecycleBinManager.class);
            mockedStaticRecycleBinManager.when(RecycleBinManager::getInstance).thenReturn(recycleBinManager);
            when(recycleBinManager.getByAppName(anyString())).thenReturn(mock(RecycleBin.class));

            // Ignore generateVersionId method
            mockedStaticDdlUtils.when(() -> DdlUtils.generateVersionId(any()))
                .thenReturn(1L);
            // Ignore checkWithoutOSS method
            mockedStaticCheckOSSArchiveUtil.when(() -> CheckOSSArchiveUtil.checkWithoutOSS(anyString(), anyString()))
                .thenReturn(true);

            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
            when(schemaManager.getTable(anyString())).thenReturn(tableMeta);
            when(tableMeta.getEngine()).thenReturn(Engine.INNODB); // This will be changed to a file storage engine

            // Partitioned database
            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(true);

            // Try with file storage engine
            when(tableMeta.getEngine()).thenReturn(Engine.OSS);

            // Should go through buildRecycleFileStorageTableJob path
            DdlJob ddlJob = handler.buildDdlJob(logicalDropTable, ec);

            // Verify that we got a job (not null)
            Assert.assertTrue(ddlJob != null);
        }
    }

    /**
     * Test buildDdlJob for partitioned database without GSI with recycle bin
     */
    @Test
    public void testBuildDdlJobForPartitionedDbWithoutGsiWithRecycleBin() {
        String schema = "test_schema";
        String table = "test_table";

        // Mock the handler to avoid executing internal methods
        LogicalDropTableHandler handler = mock(LogicalDropTableHandler.class);
        when(handler.buildDdlJob(any(), any())).thenCallRealMethod();
        when(handler.handleRecycleBin(any(), any())).thenReturn(mock(ExecutableDdlJob.class));

        ExecutionContext ec = new ExecutionContext(schema);
        LogicalDropTable logicalDropTable = mock(LogicalDropTable.class);
        when(logicalDropTable.getSchemaName()).thenReturn(schema);
        when(logicalDropTable.getTableName()).thenReturn(table);
        when(logicalDropTable.isWithGsi()).thenReturn(false);
        when(logicalDropTable.isPurge()).thenReturn(false);
        when(logicalDropTable.ifExists()).thenReturn(false);

        ParamManager pm = mock(ParamManager.class);
        ec.setParamManager(pm);
        when(pm.getBoolean(eq(ConnectionParams.ENABLE_RECYCLEBIN))).thenReturn(true);
        when(pm.getBoolean(eq(ConnectionParams.IMPORT_TABLE))).thenReturn(false);

        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        DropTableWithGsiPreparedData data = mock(DropTableWithGsiPreparedData.class);
        when(logicalDropTable.getDropTableWithGsiPreparedData()).thenReturn(data);

        OptimizerContext optimizerContext = mock(OptimizerContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);

        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class);
            MockedStatic<TtlUtil> mockedStaticTtlUtil = mockStatic(TtlUtil.class);
            MockedStatic<OptimizerContext> mockedStaticOptimizerContext = mockStatic(OptimizerContext.class);
            MockedStatic<LogicalCommonDdlHandler> mockedStaticLogicalCommonDdlHandler = mockStatic(
                LogicalCommonDdlHandler.class);
            MockedStatic<LogicalDropTableHandler> mockedStaticLogicalDropTableHandler = mockStatic(
                LogicalDropTableHandler.class);
            MockedStatic<DdlUtils> mockedStaticDdlUtils = mockStatic(DdlUtils.class);
            MockedStatic<CheckOSSArchiveUtil> mockedStaticCheckOSSArchiveUtil = mockStatic(CheckOSSArchiveUtil.class)) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            mockedStaticTtlUtil.when(
                    () -> TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(anyString(), anyString(), any()))
                .thenReturn(false);
            mockedStaticOptimizerContext.when(() -> OptimizerContext.getContext(eq(schema)))
                .thenReturn(optimizerContext);
            mockedStaticLogicalCommonDdlHandler.when(
                    () -> LogicalCommonDdlHandler.isAvailableForRecycleBin(eq(schema), eq(table), eq(ec)))
                .thenReturn(true);
            // Ignore tryForbidDropTableOperationIfNeed method
            mockedStaticLogicalDropTableHandler.when(
                    () -> LogicalDropTableHandler.tryForbidDropTableOperationIfNeed(any(), anyString(), anyString()))
                .then(invocation -> {
                    // Do nothing to skip the method execution
                    return null;
                });
            // Ignore generateVersionId method
            mockedStaticDdlUtils.when(() -> DdlUtils.generateVersionId(any()))
                .thenReturn(1L);
            // Ignore checkWithoutOSS method
            mockedStaticCheckOSSArchiveUtil.when(() -> CheckOSSArchiveUtil.checkWithoutOSS(anyString(), anyString()))
                .thenReturn(true);

            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
            when(schemaManager.getTable(anyString())).thenReturn(tableMeta);
            when(tableMeta.getEngine()).thenReturn(Engine.INNODB);

            // Partitioned database
            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(true);

            // Should go through handleRecycleBin path
            DdlJob ddlJob = handler.buildDdlJob(logicalDropTable, ec);

            // Verify that we got a job (not null)
            Assert.assertTrue(ddlJob != null);
        }
    }

    /**
     * Test buildDdlJob for partitioned database without GSI without recycle bin
     */
    @Test
    public void testBuildDdlJobForPartitionedDbWithoutGsiWithoutRecycleBin() {
        String schema = "test_schema";
        String table = "test_table";

        // Mock the handler to avoid executing internal methods
        LogicalDropTableHandler handler = mock(LogicalDropTableHandler.class);
        when(handler.buildDdlJob(any(), any())).thenCallRealMethod();
        when(handler.buildDropPartitionTableJob(any(), any())).thenReturn(mock(ExecutableDdlJob.class));

        ExecutionContext ec = new ExecutionContext(schema);
        LogicalDropTable logicalDropTable = mock(LogicalDropTable.class);
        when(logicalDropTable.getSchemaName()).thenReturn(schema);
        when(logicalDropTable.getTableName()).thenReturn(table);
        when(logicalDropTable.isWithGsi()).thenReturn(false);
        when(logicalDropTable.isPurge()).thenReturn(true);
        when(logicalDropTable.ifExists()).thenReturn(false);

        ParamManager pm = mock(ParamManager.class);
        ec.setParamManager(pm);
        when(pm.getBoolean(eq(ConnectionParams.ENABLE_RECYCLEBIN))).thenReturn(true);
        when(pm.getBoolean(eq(ConnectionParams.IMPORT_TABLE))).thenReturn(false);

        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        DropTableWithGsiPreparedData data = mock(DropTableWithGsiPreparedData.class);
        when(logicalDropTable.getDropTableWithGsiPreparedData()).thenReturn(data);

        OptimizerContext optimizerContext = mock(OptimizerContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);

        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class);
            MockedStatic<TtlUtil> mockedStaticTtlUtil = mockStatic(TtlUtil.class);
            MockedStatic<OptimizerContext> mockedStaticOptimizerContext = mockStatic(OptimizerContext.class);
            MockedStatic<LogicalCommonDdlHandler> mockedStaticLogicalCommonDdlHandler = mockStatic(
                LogicalCommonDdlHandler.class);
            MockedStatic<LogicalDropTableHandler> mockedStaticLogicalDropTableHandler = mockStatic(
                LogicalDropTableHandler.class);
            MockedStatic<DdlUtils> mockedStaticDdlUtils = mockStatic(DdlUtils.class);
            MockedStatic<CheckOSSArchiveUtil> mockedStaticCheckOSSArchiveUtil = mockStatic(CheckOSSArchiveUtil.class)) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            mockedStaticTtlUtil.when(
                    () -> TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(anyString(), anyString(), any()))
                .thenReturn(false);
            mockedStaticOptimizerContext.when(() -> OptimizerContext.getContext(eq(schema)))
                .thenReturn(optimizerContext);
            mockedStaticLogicalCommonDdlHandler.when(
                    () -> LogicalCommonDdlHandler.isAvailableForRecycleBin(eq(schema), eq(table), eq(ec)))
                .thenReturn(false);
            // Ignore tryForbidDropTableOperationIfNeed method
            mockedStaticLogicalDropTableHandler.when(
                    () -> LogicalDropTableHandler.tryForbidDropTableOperationIfNeed(any(), anyString(), anyString()))
                .then(invocation -> {
                    // Do nothing to skip the method execution
                    return null;
                });
            // Ignore generateVersionId method
            mockedStaticDdlUtils.when(() -> DdlUtils.generateVersionId(any()))
                .thenReturn(1L);
            // Ignore checkWithoutOSS method
            mockedStaticCheckOSSArchiveUtil.when(() -> CheckOSSArchiveUtil.checkWithoutOSS(anyString(), anyString()))
                .thenReturn(true);

            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
            when(schemaManager.getTable(anyString())).thenReturn(tableMeta);
            when(tableMeta.getEngine()).thenReturn(Engine.INNODB);

            // Partitioned database
            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(true);

            // Should go through buildDropPartitionTableJob path
            DdlJob ddlJob = handler.buildDdlJob(logicalDropTable, ec);

            // Verify that we got a job (not null)
            Assert.assertTrue(ddlJob != null);
        }
    }
}