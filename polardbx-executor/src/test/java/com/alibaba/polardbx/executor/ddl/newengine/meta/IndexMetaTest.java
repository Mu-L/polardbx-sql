package com.alibaba.polardbx.executor.ddl.newengine.meta;

import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.google.common.collect.Lists;
import com.google.common.truth.Truth;
import org.apache.calcite.sql.SqlAlterTable;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexMetaTest {

    @Test
    public void testBuildIndexMetaByAddColumns() {
        TableMeta tableMeta = mock(TableMeta.class);
        ColumnMeta columnMetaA = mock(ColumnMeta.class);
        ColumnMeta columnMetaB = mock(ColumnMeta.class);

        when(columnMetaA.getName()).thenReturn("a");
        when(columnMetaB.getName()).thenReturn("b");
        when(columnMetaA.isNullable()).thenReturn(true);
        when(columnMetaB.isNullable()).thenReturn(false);

        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(columnMetaA);
        columnMetas.add(columnMetaB);

        List<String> columnNames = new ArrayList<>();
        columnNames.add("a");
        columnNames.add("b");

        Map<String, String> isNullable = new HashMap<>();
        isNullable.put("a", "YES");
        isNullable.put("b", "NO");

        when(tableMeta.getPhysicalColumns()).thenReturn(columnMetas);

        List<GsiMetaManager.IndexRecord> records =
            GsiUtils.buildIndexMetaByAddColumns(columnNames, "wumu", "t1", "gsi1", 1,
                IndexStatus.ABSENT, isNullable, 0);

        Assert.assertEquals(records.size(), 2);
        Assert.assertNull(records.get(0).getIndexType());
        Assert.assertEquals("", records.get(0).getIndexComment());
        Assert.assertNull(records.get(1).getIndexType());
        Assert.assertEquals("", records.get(1).getIndexComment());
    }

    @Test
    public void testBuildIndexMetaByAddColumns2() {
        List<GsiMetaManager.IndexRecord> indexRecords = new ArrayList<>();
        SqlAlterTable alterTable = mock(SqlAlterTable.class);
        when(alterTable.getAlters()).thenReturn(new ArrayList<>());

        Map<SqlAlterTable.ColumnOpt, List<String>> map = new HashMap<>();
        List<String> columnNames = new ArrayList<>();
        columnNames.add("x");
        columnNames.add("y");
        map.put(SqlAlterTable.ColumnOpt.ADD, columnNames);
        when(alterTable.getColumnOpts()).thenReturn(map);

        GsiUtils.buildIndexMetaByAddColumns(indexRecords, alterTable, "wumu", "t1", "gsi1", 1, IndexStatus.ABSENT);

        Assert.assertEquals(indexRecords.size(), 2);
        Assert.assertNull(indexRecords.get(0).getIndexType());
        Assert.assertEquals("", indexRecords.get(0).getIndexComment());
        Assert.assertNull(indexRecords.get(1).getIndexType());
        Assert.assertEquals("", indexRecords.get(1).getIndexComment());
    }

    @Test
    public void testBuildPkForRecords() {
        List<String> primaryKeys;
        List<IndexColumnMeta> primaryKeysExt;
        boolean hasPrimaryKey;
        List<IndexesRecord> records = new ArrayList<>();
        IndexesRecord r1 = new IndexesRecord();
        r1.tableSchema = "test";
        r1.tableName = "example2";
        r1.nonUnique = 0L;
        r1.indexSchema = "test";
        r1.indexName = "PRIMARY";
        r1.seqInIndex = 1;
        r1.columnName = "id";
        r1.collation = "D";
        r1.cardinality = 0L;
        r1.subPart = 0;
        records.add(r1);

        IndexesRecord r2 = new IndexesRecord();
        r2.tableSchema = "test";
        r2.tableName = "example2";
        r2.nonUnique = 0L;
        r2.indexSchema = "test";
        r2.indexName = "PRIMARY";
        r2.seqInIndex = 2;
        r2.columnName = "b";
        r2.collation = "A";
        r2.cardinality = 0L;
        r2.subPart = 30;
        records.add(r2);

        IndexesRecord r3 = new IndexesRecord();
        r3.tableSchema = "test";
        r3.tableName = "example2";
        r3.nonUnique = 1L;
        r3.indexSchema = "test";
        r3.indexName = "pre";
        r3.seqInIndex = 1;
        r3.columnName = "name";
        r3.collation = "A";
        r3.cardinality = 0L;
        r3.subPart = 10;
        records.add(r3);

        IndexesRecord r4 = new IndexesRecord();
        r4.tableSchema = "test";
        r4.tableName = "example2";
        r4.nonUnique = 1L;
        r4.indexSchema = "test";
        r4.indexName = "gen";
        r4.seqInIndex = 1;
        r4.columnName = "NULL";
        r4.collation = "A";
        r4.cardinality = 0L;
        r4.subPart = 0;
        records.add(r4);

        Map<String, ColumnMeta> columnMetaMap = new HashMap<>();
        columnMetaMap.put("id", mockColumn("id"));
        columnMetaMap.put("name", mockColumn("name"));
        columnMetaMap.put("a", mockColumn("a"));
        columnMetaMap.put("b", mockColumn("b"));
        primaryKeys = new ArrayList<>();
        primaryKeysExt = new ArrayList<>();
        hasPrimaryKey =
            GmsTableMetaManager.buildPkForRecords(records, columnMetaMap, "example2", primaryKeys, primaryKeysExt);

        Truth.assertThat(hasPrimaryKey).isTrue();
        Truth.assertThat(primaryKeys.size()).isEqualTo(2);
        Truth.assertThat(primaryKeys.get(0)).isEqualTo("id");
        Truth.assertThat(primaryKeys.get(1)).isEqualTo("b");
        Truth.assertThat(primaryKeysExt.size()).isEqualTo(2);
        Truth.assertThat(primaryKeysExt.get(0).toString()).isEqualTo("id DESC");
        Truth.assertThat(primaryKeysExt.get(1).toString()).isEqualTo("b(30) ASC");

        primaryKeys = new ArrayList<>();
        primaryKeysExt = new ArrayList<>();
        hasPrimaryKey =
            GmsTableMetaManager.buildPkForRecords(Lists.newArrayList(), columnMetaMap, "example2", primaryKeys,
                primaryKeysExt);
        Truth.assertThat(hasPrimaryKey).isFalse();
        Truth.assertThat(primaryKeys).isEmpty();
        Truth.assertThat(primaryKeysExt).isEmpty();

        primaryKeys = new ArrayList<>();
        primaryKeysExt = new ArrayList<>();
        hasPrimaryKey =
            GmsTableMetaManager.buildPkForRecords(Lists.newArrayList(new IndexesRecord()), columnMetaMap, "example2",
                primaryKeys, primaryKeysExt);
        Truth.assertThat(hasPrimaryKey).isFalse();
        Truth.assertThat(primaryKeys.size()).isEqualTo(1);
        Truth.assertThat(primaryKeys.get(0)).isNull();
        Truth.assertThat(primaryKeysExt).isEmpty();
    }

    @Test
    public void testBuildPkForResultSet() throws SQLException {
        List<String> primaryKeys;
        List<IndexColumnMeta> primaryKeysExt;
        boolean hasPrimaryKey;
        List<IndexesRecord> records = new ArrayList<>();
        IndexesRecord r2 = new IndexesRecord();
        r2.tableSchema = "test";
        r2.tableName = "example2";
        r2.nonUnique = 1L;
        r2.indexSchema = "test";
        r2.indexName = "gen";
        r2.seqInIndex = 1;
        r2.columnName = "NULL";
        r2.collation = "A";
        r2.cardinality = 0L;
        r2.subPart = 0;
        records.add(r2);

        IndexesRecord r5 = new IndexesRecord();
        r5.tableSchema = "test";
        r5.tableName = "example2";
        r5.nonUnique = 0L;
        r5.indexSchema = "test";
        r5.indexName = "PRIMARY";
        r5.seqInIndex = 2;
        r5.columnName = "b";
        r5.collation = "A";
        r5.cardinality = 0L;
        r5.subPart = 30;
        records.add(r5);

        IndexesRecord r7 = new IndexesRecord();
        r7.tableSchema = "test";
        r7.tableName = "example2";
        r7.nonUnique = 0L;
        r7.indexSchema = "test";
        r7.indexName = "PRIMARY";
        r7.seqInIndex = 1;
        r7.columnName = "id";
        r7.collation = "D";
        r7.cardinality = 0L;
        r7.subPart = 0;
        records.add(r7);

        IndexesRecord r8 = new IndexesRecord();
        r8.tableSchema = "test";
        r8.tableName = "example2";
        r8.nonUnique = 1L;
        r8.indexSchema = "test";
        r8.indexName = "pre";
        r8.seqInIndex = 1;
        r8.columnName = "name";
        r8.collation = "A";
        r8.cardinality = 0L;
        r8.subPart = 10;
        records.add(r8);

        ResultSet resultSet = mock(ResultSet.class);
        Iterator<IndexesRecord> iterator = records.iterator();
        AtomicReference<IndexesRecord> current = new AtomicReference<>();

        when(resultSet.next()).thenAnswer(invocation -> {
            if (iterator.hasNext()) {
                current.set(iterator.next());
                return true;
            }
            return false;
        });

        doAnswer(inv -> {
            String col = inv.getArgument(0);
            if ("Key_name".equalsIgnoreCase(col)) {
                return current.get().indexName;
            }
            if ("COLUMN_NAME".equalsIgnoreCase(col)) {
                return current.get().columnName;
            }
            if ("COLLATION".equalsIgnoreCase(col)) {
                return current.get().collation;
            }
            return null; // 其他列名的默认行为
        }).when(resultSet).getString(anyString());
        doAnswer(inv -> {
            String col = inv.getArgument(0);
            if ("SUB_PART".equalsIgnoreCase(col)) {
                return current.get().subPart;
            }
            return null; // 其他列名的默认行为
        }).when(resultSet).getLong(anyString());

        doAnswer(inv -> {
            String col = inv.getArgument(0);
            if ("Seq_in_index".equalsIgnoreCase(col)) {
                return (int) current.get().seqInIndex;
            }
            return null; // 其他列名的默认行为
        }).when(resultSet).getInt(anyString());
        Map<String, ColumnMeta> columnMetaMap = new HashMap<>();
        columnMetaMap.put("id", mockColumn("id"));
        columnMetaMap.put("name", mockColumn("name"));
        columnMetaMap.put("a", mockColumn("a"));
        columnMetaMap.put("b", mockColumn("b"));

        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(metaData.getColumnName(anyInt())).thenReturn("mock");
        primaryKeys = new ArrayList<>();
        primaryKeysExt = new ArrayList<>();
        hasPrimaryKey =
            GmsTableMetaManager.buildPkForResultSet(resultSet, metaData, columnMetaMap, "example2", true, primaryKeys,
                primaryKeysExt);
        Truth.assertThat(hasPrimaryKey).isTrue();
        Truth.assertThat(primaryKeys.size()).isEqualTo(2);
        Truth.assertThat(primaryKeys.get(0)).isEqualTo("id");
        Truth.assertThat(primaryKeys.get(1)).isEqualTo("b");
        Truth.assertThat(primaryKeysExt.size()).isEqualTo(2);
        Truth.assertThat(primaryKeysExt.get(0).toString()).isEqualTo("id DESC");
        Truth.assertThat(primaryKeysExt.get(1).toString()).isEqualTo("b(30) ASC");

        primaryKeys = new ArrayList<>();
        primaryKeysExt = new ArrayList<>();
        hasPrimaryKey =
            GmsTableMetaManager.buildPkForResultSet(resultSet, metaData, columnMetaMap, "example2", true, primaryKeys,
                primaryKeysExt);
        Truth.assertThat(hasPrimaryKey).isFalse();
        Truth.assertThat(primaryKeys.size()).isEqualTo(1);
        Truth.assertThat(primaryKeys.get(0)).isEqualTo("mock");
        Truth.assertThat(primaryKeysExt).isEmpty();
    }

    ColumnMeta mockColumn(String columnName) {
        ColumnMeta meta = mock(ColumnMeta.class);
        when(meta.getName()).thenReturn(columnName);
        return meta;
    }
}
