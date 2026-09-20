package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalOptimizeTable;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOptimizeTableDdl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DdlTableVersionChangedTest {

    private static final String SCHEMA_NAME = "test_schema";
    private static final String OTHER_SCHEMA = "other_schema";
    private static final String TABLE_NAME = "test_table";

    @Test
    public void testDefaultHookReturnsTrueWhenVersionIncreased() {
        LogicalCommonDdlHandler target = new LogicalDropTableGroupHandler(mock(IRepository.class));
        ExecutionContext executionContext = mock(ExecutionContext.class);
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SchemaManager latestSchemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(TABLE_NAME, 10L);
        when(latestSchemaManager.getTableWithNull(TABLE_NAME)).thenReturn(tableMeta);
        when(tableMeta.getVersion()).thenReturn(11L);

        try (MockedStatic<OptimizerContext> mocked = Mockito.mockStatic(OptimizerContext.class)) {
            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            mocked.when(() -> OptimizerContext.getContext(SCHEMA_NAME)).thenReturn(optimizerContext);
            when(optimizerContext.getLatestSchemaManager()).thenReturn(latestSchemaManager);

            Assert.assertTrue(target.tableVersionChanged(ddlPlan, executionContext, tableVersions, SCHEMA_NAME));
        }
    }

    @Test
    public void testDefaultHookReturnsFalseWhenVersionUnchanged() {
        LogicalCommonDdlHandler target = new LogicalDropTableGroupHandler(mock(IRepository.class));
        ExecutionContext executionContext = mock(ExecutionContext.class);
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SchemaManager latestSchemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(TABLE_NAME, 10L);
        when(latestSchemaManager.getTableWithNull(TABLE_NAME)).thenReturn(tableMeta);
        when(tableMeta.getVersion()).thenReturn(10L);

        try (MockedStatic<OptimizerContext> mocked = Mockito.mockStatic(OptimizerContext.class)) {
            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            mocked.when(() -> OptimizerContext.getContext(SCHEMA_NAME)).thenReturn(optimizerContext);
            when(optimizerContext.getLatestSchemaManager()).thenReturn(latestSchemaManager);

            Assert.assertFalse(target.tableVersionChanged(ddlPlan, executionContext, tableVersions, SCHEMA_NAME));
        }
    }

    @Test
    public void testDefaultHookReturnsTrueWhenTableDropped() {
        LogicalCommonDdlHandler target = new LogicalDropTableGroupHandler(mock(IRepository.class));
        ExecutionContext executionContext = mock(ExecutionContext.class);
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SchemaManager latestSchemaManager = mock(SchemaManager.class);
        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(TABLE_NAME, 10L);
        when(latestSchemaManager.getTableWithNull(TABLE_NAME)).thenReturn(null);

        try (MockedStatic<OptimizerContext> mocked = Mockito.mockStatic(OptimizerContext.class)) {
            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            mocked.when(() -> OptimizerContext.getContext(SCHEMA_NAME)).thenReturn(optimizerContext);
            when(optimizerContext.getLatestSchemaManager()).thenReturn(latestSchemaManager);

            Assert.assertTrue(target.tableVersionChanged(ddlPlan, executionContext, tableVersions, SCHEMA_NAME));
        }
    }

    @Test
    public void testDefaultHookEmptyMapComparesSchemaManagerIdentity() {
        LogicalCommonDdlHandler target = new LogicalDropTableGroupHandler(mock(IRepository.class));
        ExecutionContext executionContext = mock(ExecutionContext.class);
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SchemaManager ecSchemaManager = mock(SchemaManager.class);
        SchemaManager latestSchemaManager = mock(SchemaManager.class);
        Map<String, Long> tableVersions = new HashMap<>();
        when(executionContext.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(executionContext.getSchemaManager()).thenReturn(ecSchemaManager);

        try (MockedStatic<OptimizerContext> mocked = Mockito.mockStatic(OptimizerContext.class)) {
            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            mocked.when(() -> OptimizerContext.getContext(SCHEMA_NAME)).thenReturn(optimizerContext);
            when(optimizerContext.getLatestSchemaManager()).thenReturn(latestSchemaManager);

            Assert.assertTrue(target.tableVersionChanged(ddlPlan, executionContext, tableVersions, SCHEMA_NAME));

            when(optimizerContext.getLatestSchemaManager()).thenReturn(ecSchemaManager);
            Assert.assertFalse(target.tableVersionChanged(ddlPlan, executionContext, tableVersions, SCHEMA_NAME));
        }
    }

    @Test
    public void testCreateTableLikeHookDetectsSourceTableVersionChangeInOtherSchema() {
        LogicalCreateTableHandler target = new LogicalCreateTableHandler(mock(IRepository.class));
        BaseDdlOperation ddlPlan = mock(BaseDdlOperation.class);
        SqlCreateTable sqlCreateTable = mock(SqlCreateTable.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager sourceSnapshot = mock(SchemaManager.class);
        SchemaManager sourceLatest = mock(SchemaManager.class);
        TableMeta oldMeta = mock(TableMeta.class);
        TableMeta latestMeta = mock(TableMeta.class);
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlCreateTable);
        when(sqlCreateTable.getLikeTableName()).thenReturn(identifier(OTHER_SCHEMA, "source_table"));
        when(executionContext.getSchemaManager(OTHER_SCHEMA)).thenReturn(sourceSnapshot);
        when(sourceSnapshot.getTableWithNull("source_table")).thenReturn(oldMeta);
        when(oldMeta.getVersion()).thenReturn(10L);
        when(sourceLatest.getTableWithNull("source_table")).thenReturn(latestMeta);
        when(latestMeta.getVersion()).thenReturn(11L);

        try (MockedStatic<OptimizerContext> mocked = Mockito.mockStatic(OptimizerContext.class)) {
            OptimizerContext sourceContext = mock(OptimizerContext.class);
            mocked.when(() -> OptimizerContext.getContext(OTHER_SCHEMA)).thenReturn(sourceContext);
            when(sourceContext.getLatestSchemaManager()).thenReturn(sourceLatest);

            Assert.assertTrue(target.tableVersionChanged(ddlPlan, executionContext, tableVersions, SCHEMA_NAME));
        }
    }

    @Test
    public void testOptimizeHookDetectsVersionChangeInOtherSchema() {
        LogicalOptimizeTableHandler target = new LogicalOptimizeTableHandler(mock(IRepository.class));
        LogicalOptimizeTable ddlPlan = mock(LogicalOptimizeTable.class);
        SqlOptimizeTableDdl sqlOptimizeTableDdl = mock(SqlOptimizeTableDdl.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager defaultSnapshot = mock(SchemaManager.class);
        SchemaManager otherSnapshot = mock(SchemaManager.class);
        SchemaManager defaultLatest = mock(SchemaManager.class);
        SchemaManager otherLatest = mock(SchemaManager.class);
        TableMeta oldMetaA = mock(TableMeta.class);
        TableMeta latestMetaA = mock(TableMeta.class);
        TableMeta oldMetaB = mock(TableMeta.class);
        TableMeta latestMetaB = mock(TableMeta.class);
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlOptimizeTableDdl);
        when(sqlOptimizeTableDdl.getTableNames())
            .thenReturn(Arrays.asList(identifier("opt_a"), identifier(OTHER_SCHEMA, "opt_b")));
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(defaultSnapshot);
        when(executionContext.getSchemaManager(OTHER_SCHEMA)).thenReturn(otherSnapshot);
        // opt_a unchanged in the default schema
        when(defaultSnapshot.getTableWithNull("opt_a")).thenReturn(oldMetaA);
        when(oldMetaA.getVersion()).thenReturn(11L);
        when(defaultLatest.getTableWithNull("opt_a")).thenReturn(latestMetaA);
        when(latestMetaA.getVersion()).thenReturn(11L);
        // opt_b version bumped in the other schema
        when(otherSnapshot.getTableWithNull("opt_b")).thenReturn(oldMetaB);
        when(oldMetaB.getVersion()).thenReturn(12L);
        when(otherLatest.getTableWithNull("opt_b")).thenReturn(latestMetaB);
        when(latestMetaB.getVersion()).thenReturn(13L);

        try (MockedStatic<OptimizerContext> mocked = Mockito.mockStatic(OptimizerContext.class)) {
            OptimizerContext defaultContext = mock(OptimizerContext.class);
            OptimizerContext otherContext = mock(OptimizerContext.class);
            mocked.when(() -> OptimizerContext.getContext(SCHEMA_NAME)).thenReturn(defaultContext);
            mocked.when(() -> OptimizerContext.getContext(OTHER_SCHEMA)).thenReturn(otherContext);
            when(defaultContext.getLatestSchemaManager()).thenReturn(defaultLatest);
            when(otherContext.getLatestSchemaManager()).thenReturn(otherLatest);

            Assert.assertTrue(target.tableVersionChanged(ddlPlan, executionContext, tableVersions, SCHEMA_NAME));
        }
    }

    @Test
    public void testOptimizeHookReturnsFalseWhenAllUnchanged() {
        LogicalOptimizeTableHandler target = new LogicalOptimizeTableHandler(mock(IRepository.class));
        LogicalOptimizeTable ddlPlan = mock(LogicalOptimizeTable.class);
        SqlOptimizeTableDdl sqlOptimizeTableDdl = mock(SqlOptimizeTableDdl.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager defaultSnapshot = mock(SchemaManager.class);
        SchemaManager otherSnapshot = mock(SchemaManager.class);
        SchemaManager defaultLatest = mock(SchemaManager.class);
        SchemaManager otherLatest = mock(SchemaManager.class);
        TableMeta oldMeta = mock(TableMeta.class);
        TableMeta latestMeta = mock(TableMeta.class);
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlOptimizeTableDdl);
        when(sqlOptimizeTableDdl.getTableNames())
            .thenReturn(Arrays.asList(identifier("opt_a"), identifier(OTHER_SCHEMA, "opt_b")));
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(defaultSnapshot);
        when(executionContext.getSchemaManager(OTHER_SCHEMA)).thenReturn(otherSnapshot);
        when(defaultSnapshot.getTableWithNull("opt_a")).thenReturn(oldMeta);
        when(otherSnapshot.getTableWithNull("opt_b")).thenReturn(oldMeta);
        when(oldMeta.getVersion()).thenReturn(11L);
        when(latestMeta.getVersion()).thenReturn(11L);

        try (MockedStatic<OptimizerContext> mocked = Mockito.mockStatic(OptimizerContext.class)) {
            OptimizerContext defaultContext = mock(OptimizerContext.class);
            OptimizerContext otherContext = mock(OptimizerContext.class);
            mocked.when(() -> OptimizerContext.getContext(SCHEMA_NAME)).thenReturn(defaultContext);
            mocked.when(() -> OptimizerContext.getContext(OTHER_SCHEMA)).thenReturn(otherContext);
            when(defaultContext.getLatestSchemaManager()).thenReturn(defaultLatest);
            when(otherContext.getLatestSchemaManager()).thenReturn(otherLatest);
            when(defaultLatest.getTableWithNull("opt_a")).thenReturn(latestMeta);
            when(otherLatest.getTableWithNull("opt_b")).thenReturn(latestMeta);

            Assert.assertFalse(target.tableVersionChanged(ddlPlan, executionContext, tableVersions, SCHEMA_NAME));
        }
    }

    @Test
    public void testOptimizeHookDetectsTableDroppedInOtherSchema() {
        LogicalOptimizeTableHandler target = new LogicalOptimizeTableHandler(mock(IRepository.class));
        LogicalOptimizeTable ddlPlan = mock(LogicalOptimizeTable.class);
        SqlOptimizeTableDdl sqlOptimizeTableDdl = mock(SqlOptimizeTableDdl.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager otherSnapshot = mock(SchemaManager.class);
        SchemaManager otherLatest = mock(SchemaManager.class);
        TableMeta oldMeta = mock(TableMeta.class);
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlOptimizeTableDdl);
        when(sqlOptimizeTableDdl.getTableNames())
            .thenReturn(Arrays.asList(identifier(OTHER_SCHEMA, "opt_b")));
        when(executionContext.getSchemaManager(OTHER_SCHEMA)).thenReturn(otherSnapshot);
        when(otherSnapshot.getTableWithNull("opt_b")).thenReturn(oldMeta);
        when(oldMeta.getVersion()).thenReturn(11L);
        when(otherLatest.getTableWithNull("opt_b")).thenReturn(null);

        try (MockedStatic<OptimizerContext> mocked = Mockito.mockStatic(OptimizerContext.class)) {
            OptimizerContext otherContext = mock(OptimizerContext.class);
            mocked.when(() -> OptimizerContext.getContext(OTHER_SCHEMA)).thenReturn(otherContext);
            when(otherContext.getLatestSchemaManager()).thenReturn(otherLatest);

            Assert.assertTrue(target.tableVersionChanged(ddlPlan, executionContext, tableVersions, SCHEMA_NAME));
        }
    }

    @Test
    public void testOptimizeHookSkipsTableAbsentFromSnapshot() {
        LogicalOptimizeTableHandler target = new LogicalOptimizeTableHandler(mock(IRepository.class));
        LogicalOptimizeTable ddlPlan = mock(LogicalOptimizeTable.class);
        SqlOptimizeTableDdl sqlOptimizeTableDdl = mock(SqlOptimizeTableDdl.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager defaultSnapshot = mock(SchemaManager.class);
        Map<String, Long> tableVersions = new HashMap<>();
        when(ddlPlan.getNativeSqlNode()).thenReturn(sqlOptimizeTableDdl);
        when(sqlOptimizeTableDdl.getTableNames()).thenReturn(Arrays.asList(identifier("opt_a")));
        when(executionContext.getSchemaManager(SCHEMA_NAME)).thenReturn(defaultSnapshot);
        when(defaultSnapshot.getTableWithNull("opt_a")).thenReturn(null);

        Assert.assertFalse(target.tableVersionChanged(ddlPlan, executionContext, tableVersions, SCHEMA_NAME));
    }

    private SqlIdentifier identifier(String name) {
        return new SqlIdentifier(name, SqlParserPos.ZERO);
    }

    private SqlIdentifier identifier(String schemaName, String tableName) {
        return new SqlIdentifier(Arrays.asList(schemaName, tableName), SqlParserPos.ZERO);
    }
}
