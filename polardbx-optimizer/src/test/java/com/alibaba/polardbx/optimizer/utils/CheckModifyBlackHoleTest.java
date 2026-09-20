package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckModifyBlackHoleTest {

    @Test
    public void testCheckModifyBlackHole_updateWithBlackHoleEngine() {
        TableModify tableModify = mock(TableModify.class);
        when(tableModify.isUpdate()).thenReturn(true);
        when(tableModify.isDelete()).thenReturn(false);

        RelOptTable targetTable = mock(RelOptTable.class);
        when(targetTable.getQualifiedName()).thenReturn(Arrays.asList("test_schema", "test_table"));
        when(tableModify.getTargetTables()).thenReturn(Collections.singletonList(targetTable));

        ExecutionContext ec = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(ec.getSchemaManager("test_schema")).thenReturn(schemaManager);
        when(schemaManager.getTable("test_table")).thenReturn(tableMeta);
        when(tableMeta.getEngine()).thenReturn(Engine.BLACKHOLE);

        try (MockedStatic<RelUtils> mockedRelUtils = Mockito.mockStatic(RelUtils.class)) {
            mockedRelUtils.when(() -> RelUtils.getQualifiedTableName(any(RelOptTable.class)))
                .thenReturn(Pair.of("test_schema", "test_table"));

            boolean result = CheckModifyLimitation.checkModifyBlackHole(tableModify, ec);
            Assert.assertTrue(result);
        }
    }

    @Test
    public void testCheckModifyBlackHole_updateWithInnodbEngine() {
        TableModify tableModify = mock(TableModify.class);
        when(tableModify.isUpdate()).thenReturn(true);
        when(tableModify.isDelete()).thenReturn(false);

        RelOptTable targetTable = mock(RelOptTable.class);
        when(targetTable.getQualifiedName()).thenReturn(Arrays.asList("test_schema", "test_table"));
        when(tableModify.getTargetTables()).thenReturn(Collections.singletonList(targetTable));

        ExecutionContext ec = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(ec.getSchemaManager("test_schema")).thenReturn(schemaManager);
        when(schemaManager.getTable("test_table")).thenReturn(tableMeta);
        when(tableMeta.getEngine()).thenReturn(Engine.INNODB);

        try (MockedStatic<RelUtils> mockedRelUtils = Mockito.mockStatic(RelUtils.class)) {
            mockedRelUtils.when(() -> RelUtils.getQualifiedTableName(any(RelOptTable.class)))
                .thenReturn(Pair.of("test_schema", "test_table"));

            boolean result = CheckModifyLimitation.checkModifyBlackHole(tableModify, ec);
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testCheckModifyBlackHole_deleteWithBlackHoleEngine() {
        TableModify tableModify = mock(TableModify.class);
        when(tableModify.isUpdate()).thenReturn(false);
        when(tableModify.isDelete()).thenReturn(true);

        RelOptTable targetTable = mock(RelOptTable.class);
        when(targetTable.getQualifiedName()).thenReturn(Arrays.asList("test_schema", "test_table"));
        when(tableModify.getTargetTables()).thenReturn(Collections.singletonList(targetTable));

        ExecutionContext ec = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(ec.getSchemaManager("test_schema")).thenReturn(schemaManager);
        when(schemaManager.getTable("test_table")).thenReturn(tableMeta);
        when(tableMeta.getEngine()).thenReturn(Engine.BLACKHOLE);

        try (MockedStatic<RelUtils> mockedRelUtils = Mockito.mockStatic(RelUtils.class)) {
            mockedRelUtils.when(() -> RelUtils.getQualifiedTableName(any(RelOptTable.class)))
                .thenReturn(Pair.of("test_schema", "test_table"));

            boolean result = CheckModifyLimitation.checkModifyBlackHole(tableModify, ec);
            Assert.assertTrue(result);
        }
    }

    @Test
    public void testCheckModifyBlackHole_insertShouldReturnFalse() {
        TableModify tableModify = mock(TableModify.class);
        when(tableModify.isUpdate()).thenReturn(false);
        when(tableModify.isDelete()).thenReturn(false);

        ExecutionContext ec = mock(ExecutionContext.class);

        boolean result = CheckModifyLimitation.checkModifyBlackHole(tableModify, ec);
        Assert.assertFalse(result);
    }

    @Test
    public void testCheckModifyBlackHole_multipleTablesOneBlackHole() {
        TableModify tableModify = mock(TableModify.class);
        when(tableModify.isUpdate()).thenReturn(true);
        when(tableModify.isDelete()).thenReturn(false);

        RelOptTable table1 = mock(RelOptTable.class);
        when(table1.getQualifiedName()).thenReturn(Arrays.asList("schema1", "table1"));
        RelOptTable table2 = mock(RelOptTable.class);
        when(table2.getQualifiedName()).thenReturn(Arrays.asList("schema2", "table2"));
        when(tableModify.getTargetTables()).thenReturn(Arrays.asList(table1, table2));

        ExecutionContext ec = mock(ExecutionContext.class);
        SchemaManager schemaManager1 = mock(SchemaManager.class);
        SchemaManager schemaManager2 = mock(SchemaManager.class);
        TableMeta tableMeta1 = mock(TableMeta.class);
        TableMeta tableMeta2 = mock(TableMeta.class);
        when(ec.getSchemaManager("schema1")).thenReturn(schemaManager1);
        when(ec.getSchemaManager("schema2")).thenReturn(schemaManager2);
        when(schemaManager1.getTable("table1")).thenReturn(tableMeta1);
        when(schemaManager2.getTable("table2")).thenReturn(tableMeta2);
        when(tableMeta1.getEngine()).thenReturn(Engine.INNODB);
        when(tableMeta2.getEngine()).thenReturn(Engine.BLACKHOLE);

        try (MockedStatic<RelUtils> mockedRelUtils = Mockito.mockStatic(RelUtils.class)) {
            mockedRelUtils.when(() -> RelUtils.getQualifiedTableName(table1))
                .thenReturn(Pair.of("schema1", "table1"));
            mockedRelUtils.when(() -> RelUtils.getQualifiedTableName(table2))
                .thenReturn(Pair.of("schema2", "table2"));

            boolean result = CheckModifyLimitation.checkModifyBlackHole(tableModify, ec);
            Assert.assertTrue(result);
        }
    }

    @Test
    public void testCheckModifyBlackHole_multipleTablesNoBlackHole() {
        TableModify tableModify = mock(TableModify.class);
        when(tableModify.isUpdate()).thenReturn(true);
        when(tableModify.isDelete()).thenReturn(false);

        RelOptTable table1 = mock(RelOptTable.class);
        when(table1.getQualifiedName()).thenReturn(Arrays.asList("schema1", "table1"));
        RelOptTable table2 = mock(RelOptTable.class);
        when(table2.getQualifiedName()).thenReturn(Arrays.asList("schema2", "table2"));
        when(tableModify.getTargetTables()).thenReturn(Arrays.asList(table1, table2));

        ExecutionContext ec = mock(ExecutionContext.class);
        SchemaManager schemaManager1 = mock(SchemaManager.class);
        SchemaManager schemaManager2 = mock(SchemaManager.class);
        TableMeta tableMeta1 = mock(TableMeta.class);
        TableMeta tableMeta2 = mock(TableMeta.class);
        when(ec.getSchemaManager("schema1")).thenReturn(schemaManager1);
        when(ec.getSchemaManager("schema2")).thenReturn(schemaManager2);
        when(schemaManager1.getTable("table1")).thenReturn(tableMeta1);
        when(schemaManager2.getTable("table2")).thenReturn(tableMeta2);
        when(tableMeta1.getEngine()).thenReturn(Engine.INNODB);
        when(tableMeta2.getEngine()).thenReturn(Engine.INNODB);

        try (MockedStatic<RelUtils> mockedRelUtils = Mockito.mockStatic(RelUtils.class)) {
            mockedRelUtils.when(() -> RelUtils.getQualifiedTableName(table1))
                .thenReturn(Pair.of("schema1", "table1"));
            mockedRelUtils.when(() -> RelUtils.getQualifiedTableName(table2))
                .thenReturn(Pair.of("schema2", "table2"));

            boolean result = CheckModifyLimitation.checkModifyBlackHole(tableModify, ec);
            Assert.assertFalse(result);
        }
    }
}
