package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ConnectorTableBuilderTest {

    private List<ColumnMeta> sampleColumns() {
        Field f1 = new Field("t", "id", DataTypes.IntegerType);
        Field f2 = new Field("t", "name", DataTypes.VarcharType);
        return Arrays.asList(
            new ColumnMeta("t", "id", null, f1),
            new ColumnMeta("t", "name", null, f2)
        );
    }

    @Test
    public void testLegacyConstructorStillWorks() {
        List<ColumnMeta> cols = sampleColumns();
        ConnectorTable ct = new ConnectorTable(cols, Collections.singletonList("dt"));
        assertEquals(2, ct.columns.size());
        assertEquals(1, ct.partitionColumns.size());
        assertNull(ct.comment);
        assertTrue(ct.primaryKey.isEmpty());
        assertTrue(ct.properties.isEmpty());
        assertEquals(-1, ct.estimatedRowCount);
    }

    @Test
    public void testBuilderMinimal() {
        ConnectorTable ct = ConnectorTable.builder(sampleColumns()).build();
        assertEquals(2, ct.columns.size());
        assertTrue(ct.partitionColumns.isEmpty());
        assertEquals(-1, ct.estimatedRowCount);
    }

    @Test
    public void testBuilderFull() {
        Map<String, String> props = new HashMap<>();
        props.put("format", "parquet");
        ConnectorTable ct = ConnectorTable.builder(sampleColumns())
            .partitionColumns(Arrays.asList("dt", "region"))
            .comment("User events table")
            .primaryKey(Collections.singletonList("id"))
            .properties(props)
            .estimatedRowCount(1_000_000)
            .build();
        assertEquals("User events table", ct.comment);
        assertEquals(Collections.singletonList("id"), ct.primaryKey);
        assertEquals("parquet", ct.properties.get("format"));
        assertEquals(1_000_000, ct.estimatedRowCount);
        assertEquals(2, ct.partitionColumns.size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testColumnsUnmodifiable() {
        List<ColumnMeta> mutableCols = new ArrayList<>(sampleColumns());
        ConnectorTable ct = ConnectorTable.builder(mutableCols).build();
        ct.columns.add(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPartitionColumnsUnmodifiable() {
        List<String> mutableParts = new ArrayList<>();
        mutableParts.add("dt");
        ConnectorTable ct = ConnectorTable.builder(sampleColumns())
            .partitionColumns(mutableParts)
            .build();
        ct.partitionColumns.add("extra");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPrimaryKeyUnmodifiable() {
        List<String> mutablePk = new ArrayList<>();
        mutablePk.add("id");
        ConnectorTable ct = ConnectorTable.builder(sampleColumns())
            .primaryKey(mutablePk)
            .build();
        ct.primaryKey.add("extra");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPropertiesUnmodifiable() {
        Map<String, String> props = new HashMap<>();
        props.put("k", "v");
        ConnectorTable ct = ConnectorTable.builder(sampleColumns())
            .properties(props)
            .build();
        ct.properties.put("new", "val");
    }
}
