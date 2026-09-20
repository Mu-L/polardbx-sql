package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRepartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalOptimizeTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalTruncateTable;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableSetTableGroup;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableExchangePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupAddTable;
import org.apache.calcite.sql.SqlAlterTableGroupRenamePartition;
import org.apache.calcite.sql.SqlAlterTableRepartition;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlCreateTableGroup;
import org.apache.calcite.sql.SqlDropTableGroup;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOptimizeTableDdl;
import org.apache.calcite.sql.SqlRenameTable;
import org.apache.calcite.sql.SqlRenameTables;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DdlFixedResourcesHandlerTest {

    private static final String SCHEMA_NAME = "test_schema";
    private static final String TABLE_NAME = "test_table";

    @Test
    public void testConcatWithDot() {
        Assert.assertEquals("test_table", LogicalCommonDdlHandler.concatWithDot(null, "test_table"));
        Assert.assertEquals("test_table", LogicalCommonDdlHandler.concatWithDot("", "test_table"));
        Assert.assertEquals("test_schema.test_table",
            LogicalCommonDdlHandler.concatWithDot("test_schema", "test_table"));
    }

    @Test
    public void testCreateTableAddsExclusiveResourceForNormalCreate() {
        LogicalCreateTableHandler target = new LogicalCreateTableHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SqlCreateTable sqlCreateTable = mock(SqlCreateTable.class);
        ExecutionContext executionContext = mockExecutionContext(false);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getTableName()).thenReturn(TABLE_NAME);
        when(ddlPlan.getDdlType()).thenReturn(DdlType.CREATE_TABLE);
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlCreateTable);
        when(sqlCreateTable.isWithImplicitTableGroup()).thenReturn(false);
        when(sqlCreateTable.isSelect()).thenReturn(false);
        when(sqlCreateTable.getLikeTableName()).thenReturn(null);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(sharedResources.contains(SCHEMA_NAME));
        Assert.assertTrue(exclusiveResources.contains("test_schema.test_table"));
        Assert.assertTrue(tableVersions.isEmpty());
    }

    @Test
    public void testCreateTableSkipsTableResourceWhenTableGroupLockIsAcquired() {
        LogicalCreateTableHandler target = new LogicalCreateTableHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SqlCreateTable sqlCreateTable = mock(SqlCreateTable.class);
        ExecutionContext executionContext = mockExecutionContext(true);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getTableName()).thenReturn(TABLE_NAME);
        when(ddlPlan.getDdlType()).thenReturn(DdlType.CREATE_TABLE);
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlCreateTable);
        when(sqlCreateTable.isWithImplicitTableGroup()).thenReturn(false);
        when(sqlCreateTable.isSelect()).thenReturn(false);
        when(sqlCreateTable.getLikeTableName()).thenReturn(null);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(sharedResources.contains(SCHEMA_NAME));
        Assert.assertFalse(exclusiveResources.contains("test_schema.test_table"));
        Assert.assertTrue(tableVersions.isEmpty());
    }

    @Test
    public void testCreateTableLikeAddsSourceSchemaAndTableResourceAndVersion() {
        LogicalCreateTableHandler target = new LogicalCreateTableHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SqlCreateTable sqlCreateTable = mock(SqlCreateTable.class);
        ExecutionContext executionContext = mockExecutionContext(false);
        SchemaManager sourceSchemaManager = mock(SchemaManager.class);
        TableMeta sourceTableMeta = mock(TableMeta.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getTableName()).thenReturn(TABLE_NAME);
        when(ddlPlan.getDdlType()).thenReturn(DdlType.CREATE_TABLE);
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlCreateTable);
        when(executionContext.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(executionContext.getSchemaManager("source_schema")).thenReturn(sourceSchemaManager);
        when(sourceSchemaManager.getTableWithNull("source_table")).thenReturn(sourceTableMeta);
        when(sourceTableMeta.getVersion()).thenReturn(123L);
        when(sqlCreateTable.isWithImplicitTableGroup()).thenReturn(false);
        when(sqlCreateTable.isSelect()).thenReturn(false);
        when(sqlCreateTable.getLikeTableName()).thenReturn(identifier("source_schema", "source_table"));

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(sharedResources.contains(SCHEMA_NAME));
        Assert.assertTrue(sharedResources.contains("source_schema"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.test_table"));
        Assert.assertTrue(exclusiveResources.contains("source_schema.source_table"));
        Assert.assertEquals(Long.valueOf(123L), tableVersions.get("source_table"));
    }

    @Test
    public void testDropIndexAddsTableResourceAndVersion() {
        LogicalDropIndexHandler target = new LogicalDropIndexHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getTableName()).thenReturn(TABLE_NAME);
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(schemaManager);
        when(schemaManager.getTable(TABLE_NAME)).thenReturn(tableMeta);
        when(tableMeta.getVersion()).thenReturn(88L);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.contains("test_schema.test_table"));
        Assert.assertEquals(Long.valueOf(88L), tableVersions.get(TABLE_NAME));
    }

    @Test
    public void testAlterTableSetTableGroupSkipsPrimaryTableForGsi() throws Exception {
        LogicalAlterTableSetTableGroupHandler target =
            new LogicalAlterTableSetTableGroupHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        AlterTableSetTableGroup relDdl = mock(AlterTableSetTableGroup.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        setRelDdl(ddlPlan, relDdl);
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getTableName()).thenReturn(TABLE_NAME);
        when(relDdl.getTableGroupName()).thenReturn("target_tg");
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull(TABLE_NAME)).thenReturn(tableMeta);
        when(tableMeta.isGsi()).thenReturn(true);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.contains("test_schema.target_tg"));
        Assert.assertFalse(exclusiveResources.contains("test_schema.test_table"));
        Assert.assertTrue(tableVersions.isEmpty());
    }

    @Test
    public void testAlterTableSetTableGroupAddsPrimaryTableForNonGsi() throws Exception {
        LogicalAlterTableSetTableGroupHandler target =
            new LogicalAlterTableSetTableGroupHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        AlterTableSetTableGroup relDdl = mock(AlterTableSetTableGroup.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        setRelDdl(ddlPlan, relDdl);
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getTableName()).thenReturn(TABLE_NAME);
        when(relDdl.getTableGroupName()).thenReturn("target_tg");
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull(TABLE_NAME)).thenReturn(tableMeta);
        when(tableMeta.isGsi()).thenReturn(false);
        when(tableMeta.getVersion()).thenReturn(99L);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.contains("test_schema.target_tg"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.test_table"));
        Assert.assertEquals(Long.valueOf(99L), tableVersions.get(TABLE_NAME));
    }

    @Test
    public void testRenameTableAddsSourceTargetResourcesAndSourceVersion() {
        LogicalRenameTableHandler target = new LogicalRenameTableHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SqlRenameTable sqlRenameTable = mock(SqlRenameTable.class);
        SqlIdentifier sourceTable = mock(SqlIdentifier.class);
        SqlIdentifier targetTable = mock(SqlIdentifier.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlRenameTable);
        when(sqlRenameTable.getOriginTableName()).thenReturn(sourceTable);
        when(sqlRenameTable.getOriginNewTableName()).thenReturn(targetTable);
        when(sourceTable.getLastName()).thenReturn("old_table");
        when(targetTable.getLastName()).thenReturn("new_table");
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull("old_table")).thenReturn(tableMeta);
        when(tableMeta.getVersion()).thenReturn(100L);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.contains("test_schema.old_table"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.new_table"));
        Assert.assertEquals(Long.valueOf(100L), tableVersions.get("old_table"));
    }

    @Test
    public void testSimpleTableVersionHandlersAddTableResourceAndVersion() {
        List<LogicalCommonDdlHandler> handlers = Arrays.asList(
            new LogicalCreateIndexHandler(mock(IRepository.class)),
            new LogicalInsertOverwriteHandler(mock(IRepository.class)),
            new LogicalAlterTableSplitPartitionHandler(mock(IRepository.class)),
            new LogicalAlterTableExpandPartitionsHandler(mock(IRepository.class)),
            new LogicalAlterTableToggleFullScanHandler(mock(IRepository.class)),
            new LogicalAlterTableRenamePartitionHandler(mock(IRepository.class)),
            new LogicalAlterTablePartitionCountHandler(mock(IRepository.class)),
            new LogicalAlterTableRemovePartitioningHandler(mock(IRepository.class)),
            new LogicalAlterTableRemoveAutoPartitionHandler(mock(IRepository.class))
        );

        for (LogicalCommonDdlHandler handler : handlers) {
            assertTableResourceAndVersion(handler);
        }
    }

    @Test
    public void testDropTableAddsOnlyTableResource() {
        LogicalDropTableHandler target = new LogicalDropTableHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mockBaseDdlOperation();
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();

        target.prepareFixedResources(ddlPlan, mock(ExecutionContext.class), sharedResources, exclusiveResources,
            tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.contains("test_schema.test_table"));
        Assert.assertTrue(tableVersions.isEmpty());
    }

    @Test
    public void testAlterTableLocksGsiTableAndNormalTable() {
        LogicalAlterTableHandler target = new LogicalAlterTableHandler(mock(IRepository.class));
        LogicalAlterTable ddlPlan = mock(LogicalAlterTable.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta gsiTableMeta = mock(TableMeta.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getTableName()).thenReturn(TABLE_NAME);
        when(ddlPlan.isCleanupExpiredData()).thenReturn(false);
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull(TABLE_NAME)).thenReturn(gsiTableMeta);
        when(gsiTableMeta.isGsi()).thenReturn(true);
        when(gsiTableMeta.getVersion()).thenReturn(99L);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.contains("test_schema.test_table"));
        Assert.assertEquals(Long.valueOf(99L), tableVersions.get(TABLE_NAME));

        TableMeta normalTableMeta = mock(TableMeta.class);
        when(schemaManager.getTableWithNull(TABLE_NAME)).thenReturn(normalTableMeta);
        when(normalTableMeta.isGsi()).thenReturn(false);
        when(normalTableMeta.getVersion()).thenReturn(101L);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(exclusiveResources.contains("test_schema.test_table"));
        Assert.assertEquals(Long.valueOf(101L), tableVersions.get(TABLE_NAME));
    }

    @Test
    public void testTruncateTableRespectsRecycleBinAndPurge() {
        LogicalTruncateTableHandler target = new LogicalTruncateTableHandler(mock(IRepository.class));
        LogicalTruncateTable ddlPlan = mock(LogicalTruncateTable.class);
        ExecutionContext executionContext = mockExecutionContextWithRecycleBin(true);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getTableName()).thenReturn(TABLE_NAME);
        when(ddlPlan.isPurge()).thenReturn(false);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.isEmpty());
        Assert.assertTrue(tableVersions.isEmpty());

        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(ddlPlan.isPurge()).thenReturn(true);
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull(TABLE_NAME)).thenReturn(tableMeta);
        when(tableMeta.getVersion()).thenReturn(102L);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(exclusiveResources.contains("test_schema.test_table"));
        Assert.assertEquals(Long.valueOf(102L), tableVersions.get(TABLE_NAME));
    }

    @Test
    public void testRenameTablesAddsAllSourceAndTargetResources() {
        LogicalRenameTablesHandler target = new LogicalRenameTablesHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SqlRenameTables sqlRenameTables = mock(SqlRenameTables.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta firstTableMeta = mock(TableMeta.class);
        TableMeta secondTableMeta = mock(TableMeta.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlRenameTables);
        when(sqlRenameTables.getTableNameList()).thenReturn(Arrays.asList(
            org.apache.calcite.util.Pair.of(identifier("old_a"), identifier("new_a")),
            org.apache.calcite.util.Pair.of(identifier("old_b"), identifier("new_b"))));
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull("old_a")).thenReturn(firstTableMeta);
        when(schemaManager.getTableWithNull("old_b")).thenReturn(secondTableMeta);
        when(firstTableMeta.getVersion()).thenReturn(1L);
        when(secondTableMeta.getVersion()).thenReturn(2L);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(exclusiveResources.contains("test_schema.old_a"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.new_a"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.old_b"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.new_b"));
        Assert.assertEquals(Long.valueOf(1L), tableVersions.get("old_a"));
        Assert.assertEquals(Long.valueOf(2L), tableVersions.get("old_b"));
    }

    @Test
    public void testOptimizeTableAddsResourcesForExplicitAndDefaultSchemas() {
        LogicalOptimizeTableHandler target = new LogicalOptimizeTableHandler(mock(IRepository.class));
        LogicalOptimizeTable ddlPlan = mock(LogicalOptimizeTable.class);
        SqlOptimizeTableDdl sqlOptimizeTableDdl = mock(SqlOptimizeTableDdl.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager defaultSchemaManager = mock(SchemaManager.class);
        SchemaManager explicitSchemaManager = mock(SchemaManager.class);
        TableMeta defaultTableMeta = mock(TableMeta.class);
        TableMeta explicitTableMeta = mock(TableMeta.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        SqlIdentifier defaultTable = identifier("opt_a");
        SqlIdentifier explicitTable = identifier("other_schema", "opt_b");
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlOptimizeTableDdl);
        when(sqlOptimizeTableDdl.getTableNames()).thenReturn(Arrays.asList(defaultTable, explicitTable));
        when(executionContext.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(defaultSchemaManager);
        when(executionContext.getSchemaManager("other_schema")).thenReturn(explicitSchemaManager);
        when(defaultSchemaManager.getTableWithNull("opt_a")).thenReturn(defaultTableMeta);
        when(explicitSchemaManager.getTableWithNull("opt_b")).thenReturn(explicitTableMeta);
        when(defaultTableMeta.getVersion()).thenReturn(11L);
        when(explicitTableMeta.getVersion()).thenReturn(12L);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(exclusiveResources.contains("test_schema.opt_a"));
        Assert.assertTrue(exclusiveResources.contains("other_schema.opt_b"));
        Assert.assertEquals(Long.valueOf(11L), tableVersions.get("opt_a"));
        Assert.assertEquals(Long.valueOf(12L), tableVersions.get("opt_b"));
    }

    @Test
    public void testDropTableGroupAndPushDownUdfAddSpecialResources() {
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SqlDropTableGroup sqlDropTableGroup = mock(SqlDropTableGroup.class);
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlDropTableGroup);
        when(sqlDropTableGroup.getTableGroupName()).thenReturn("target_tg");

        new LogicalDropTableGroupHandler(mock(IRepository.class))
            .prepareFixedResources(ddlPlan, mock(ExecutionContext.class), sharedResources, exclusiveResources,
                tableVersions);
        new LogicalPushDownUdfHanlder(mock(IRepository.class))
            .prepareFixedResources(ddlPlan, mock(ExecutionContext.class), sharedResources, exclusiveResources,
                tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.contains("test_schema.target_tg"));
        Assert.assertTrue(exclusiveResources.contains(PlConstants.PUSH_DOWN_UDF));
        Assert.assertTrue(tableVersions.isEmpty());
    }

    @Test
    public void testAlterTableGroupAddTableLocksTablesAndTableGroup() {
        LogicalAlterTableGroupAddTableHandler target =
            new LogicalAlterTableGroupAddTableHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SqlAlterTableGroup sqlAlterTableGroup = mock(SqlAlterTableGroup.class);
        SqlAlterTableGroupAddTable sqlAddTable = mock(SqlAlterTableGroupAddTable.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta firstTableMeta = mock(TableMeta.class);
        TableMeta secondTableMeta = mock(TableMeta.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlAlterTableGroup);
        when(sqlAlterTableGroup.getTableGroupName()).thenReturn(identifier("target_tg"));
        when(sqlAlterTableGroup.getAlters()).thenReturn(Collections.singletonList(sqlAddTable));
        when(sqlAddTable.getTables()).thenReturn(Arrays.asList(identifier("table_a"), identifier("table_b")));
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull("table_a")).thenReturn(firstTableMeta);
        when(schemaManager.getTableWithNull("table_b")).thenReturn(secondTableMeta);
        when(firstTableMeta.isGsi()).thenReturn(false);
        when(secondTableMeta.isGsi()).thenReturn(false);
        when(firstTableMeta.getVersion()).thenReturn(21L);
        when(secondTableMeta.getVersion()).thenReturn(22L);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(exclusiveResources.contains("test_schema.table_a"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.table_b"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.target_tg"));
        Assert.assertEquals(Long.valueOf(21L), tableVersions.get("table_a"));
        Assert.assertEquals(Long.valueOf(22L), tableVersions.get("table_b"));
    }

    @Test
    public void testAlterTableGroupRenamePartitionLocksTableGroupAndPartitions() {
        LogicalAlterTableGroupRenamePartitionHandler target =
            new LogicalAlterTableGroupRenamePartitionHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SqlAlterTableGroup sqlAlterTableGroup = mock(SqlAlterTableGroup.class);
        SqlAlterTableGroupRenamePartition sqlRenamePartition = mock(SqlAlterTableGroupRenamePartition.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlAlterTableGroup);
        when(sqlAlterTableGroup.getTableGroupName()).thenReturn(identifier("target_tg"));
        when(sqlAlterTableGroup.getAlters()).thenReturn(Collections.singletonList(sqlRenamePartition));
        when(sqlRenamePartition.getChangePartitionsPair()).thenReturn(Arrays.asList(
            com.alibaba.polardbx.common.utils.Pair.of("p1", "p1_new"),
            com.alibaba.polardbx.common.utils.Pair.of("p2", "p2_new")));

        target.prepareFixedResources(ddlPlan, mock(ExecutionContext.class), sharedResources, exclusiveResources,
            tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.contains("test_schema.target_tg"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.target_tg.p1"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.target_tg.p1_new"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.target_tg.p2"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.target_tg.p2_new"));
        Assert.assertTrue(tableVersions.isEmpty());
    }

    @Test
    public void testSimpleAlterTableGroupHandlersLockTableGroup() {
        List<LogicalCommonDdlHandler> handlers = Arrays.asList(
            new LogicalAlterTableGroupDropPartitionHandler(mock(IRepository.class)),
            new LogicalAlterTableGroupMergePartitionHandler(mock(IRepository.class)),
            new LogicalAlterTableGroupOptimizePartitionHandler(mock(IRepository.class)),
            new LogicalAlterTableGroupReorgPartitionHandler(mock(IRepository.class)),
            new LogicalAlterTableGroupSplitPartitionHandler(mock(IRepository.class))
        );

        for (LogicalCommonDdlHandler handler : handlers) {
            BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
            SqlAlterTableGroup sqlAlterTableGroup = mock(SqlAlterTableGroup.class);
            Set<String> sharedResources = new HashSet<>();
            Set<String> exclusiveResources = new HashSet<>();
            Map<String, Long> tableVersions = new HashMap<>();
            when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
            when(ddlPlan.getNativeSqlNode()).thenReturn(sqlAlterTableGroup);
            when(sqlAlterTableGroup.getTableGroupName()).thenReturn(identifier("target_tg"));

            handler.prepareFixedResources(ddlPlan, mock(ExecutionContext.class), sharedResources, exclusiveResources,
                tableVersions);

            Assert.assertTrue(sharedResources.isEmpty());
            Assert.assertTrue(exclusiveResources.contains("test_schema.target_tg"));
            Assert.assertTrue(tableVersions.isEmpty());
        }
    }

    @Test
    public void testEmptyPrepareFixedResourcesHandlersKeepResourcesUnchanged() {
        BaseDdlOperation ddlPlan = mockBaseDdlOperation();
        ExecutionContext executionContext = mock(ExecutionContext.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        List<LogicalCommonDdlHandler> handlers = Arrays.asList(
            new LogicalCreateMaterializedViewHandler(mock(IRepository.class)),
            new LogicalDropMaterializedViewHandler(mock(IRepository.class)),
            new LogicalAlterTableGroupAddPartitionProxyHandler(mock(IRepository.class)),
            new LogicalAlterTableGroupModifyPartitionProxyHandler(mock(IRepository.class))
        );

        for (LogicalCommonDdlHandler handler : handlers) {
            handler.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources,
                tableVersions);
        }

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.isEmpty());
        Assert.assertTrue(tableVersions.isEmpty());
    }

    @Test
    public void testCreateAndDropViewAddViewResource() {
        List<LogicalCommonDdlHandler> handlers = Arrays.asList(
            new LogicalCreateViewHandler(mock(IRepository.class)),
            new LogicalDropViewHandler(mock(IRepository.class))
        );

        for (LogicalCommonDdlHandler handler : handlers) {
            BaseDdlOperation ddlPlan = mockBaseDdlOperation();
            Set<String> sharedResources = new HashSet<>();
            Set<String> exclusiveResources = new HashSet<>();
            Map<String, Long> tableVersions = new HashMap<>();

            handler.prepareFixedResources(ddlPlan, mock(ExecutionContext.class), sharedResources, exclusiveResources,
                tableVersions);

            Assert.assertTrue(sharedResources.isEmpty());
            Assert.assertTrue(exclusiveResources.contains("test_schema.test_table"));
            Assert.assertTrue(tableVersions.isEmpty());
        }
    }

    @Test
    public void testCreateTableGroupAddsTableGroupResource() {
        LogicalCreateTableGroupHandler target = new LogicalCreateTableGroupHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SqlCreateTableGroup sqlCreateTableGroup = mock(SqlCreateTableGroup.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlCreateTableGroup);
        when(sqlCreateTableGroup.getTableGroupName()).thenReturn("create_tg");

        target.prepareFixedResources(ddlPlan, mock(ExecutionContext.class), sharedResources, exclusiveResources,
            tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.contains("test_schema.create_tg"));
        Assert.assertTrue(tableVersions.isEmpty());
    }

    @Test
    public void testAlterTableRepartitionLocksTableAndIndex() throws Exception {
        LogicalAlterTableRepartitionHandler target =
            new LogicalAlterTableRepartitionHandler(mock(IRepository.class));
        LogicalAlterTableRepartition ddlPlan = mock(LogicalAlterTableRepartition.class);
        DDL relDdl = mock(DDL.class);
        SqlAlterTableRepartition sqlAlterTableRepartition = mock(SqlAlterTableRepartition.class);
        SqlAddIndex sqlAddIndex = mock(SqlAddIndex.class);
        SqlIdentifier indexName = identifier("g_i");
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        setRelDdl(ddlPlan, relDdl);
        relDdl.sqlNode = sqlAlterTableRepartition;
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getTableName()).thenReturn(TABLE_NAME);
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull(TABLE_NAME)).thenReturn(tableMeta);
        when(tableMeta.getVersion()).thenReturn(201L);
        when(sqlAlterTableRepartition.getAlters()).thenReturn(Collections.singletonList(sqlAddIndex));
        when(sqlAddIndex.getIndexName()).thenReturn(indexName);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.contains("test_schema.test_table"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.g_i"));
        Assert.assertEquals(Long.valueOf(201L), tableVersions.get(TABLE_NAME));
    }

    @Test
    public void testExchangePartitionLocksSourceAndTargetTables() {
        LogicalAlterTableExchangePartitionHandler target =
            new LogicalAlterTableExchangePartitionHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SqlAlterTable sqlAlterTable = mock(SqlAlterTable.class);
        SqlAlterTableExchangePartition sqlExchangePartition = mock(SqlAlterTableExchangePartition.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getTableName()).thenReturn("source_table");
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlAlterTable);
        when(sqlAlterTable.getAlters()).thenReturn(Collections.singletonList(sqlExchangePartition));
        when(sqlExchangePartition.getTableName()).thenReturn(identifier("target_table"));
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull("source_table")).thenReturn(tableMeta);
        when(tableMeta.getVersion()).thenReturn(202L);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.contains("test_schema.source_table"));
        Assert.assertTrue(exclusiveResources.contains("test_schema.target_table"));
        Assert.assertEquals(Long.valueOf(202L), tableVersions.get("source_table"));
    }

    private ExecutionContext mockExecutionContext(boolean acquireCreateTableGroupLock) {
        ExecutionContext executionContext = mock(ExecutionContext.class);
        ParamManager paramManager = mock(ParamManager.class);
        when(executionContext.getParamManager()).thenReturn(paramManager);
        when(paramManager.getBoolean(ConnectionParams.ACQUIRE_CREATE_TABLE_GROUP_LOCK))
            .thenReturn(acquireCreateTableGroupLock);
        return executionContext;
    }

    private ExecutionContext mockExecutionContextWithRecycleBin(boolean enableRecycleBin) {
        ExecutionContext executionContext = mock(ExecutionContext.class);
        ParamManager paramManager = mock(ParamManager.class);
        when(executionContext.getParamManager()).thenReturn(paramManager);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_RECYCLEBIN)).thenReturn(enableRecycleBin);
        return executionContext;
    }

    private void assertTableResourceAndVersion(LogicalCommonDdlHandler target) {
        BaseDdlOperation ddlPlan = mockBaseDdlOperation();
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> sharedResources = new HashSet<>();
        Set<String> exclusiveResources = new HashSet<>();
        Map<String, Long> tableVersions = new HashMap<>();
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull(TABLE_NAME)).thenReturn(tableMeta);
        when(tableMeta.getVersion()).thenReturn(77L);

        target.prepareFixedResources(ddlPlan, executionContext, sharedResources, exclusiveResources, tableVersions);

        Assert.assertTrue(sharedResources.isEmpty());
        Assert.assertTrue(exclusiveResources.contains("test_schema.test_table"));
        Assert.assertEquals(Long.valueOf(77L), tableVersions.get(TABLE_NAME));
    }

    private BaseDdlOperation mockBaseDdlOperation() {
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        when(ddlPlan.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(ddlPlan.getTableName()).thenReturn(TABLE_NAME);
        return ddlPlan;
    }

    private SqlIdentifier identifier(String name) {
        return new SqlIdentifier(name, SqlParserPos.ZERO);
    }

    private SqlIdentifier identifier(String schemaName, String tableName) {
        return new SqlIdentifier(Arrays.asList(schemaName, tableName), SqlParserPos.ZERO);
    }

    private void setRelDdl(BaseDdlOperation ddlPlan, DDL relDdl) throws Exception {
        Field field = BaseDdlOperation.class.getField("relDdl");
        field.set(ddlPlan, relDdl);
    }
}
