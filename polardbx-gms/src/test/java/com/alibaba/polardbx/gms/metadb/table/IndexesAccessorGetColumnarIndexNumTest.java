package com.alibaba.polardbx.gms.metadb.table;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IndexesAccessorGetColumnarIndexNumTest {

    private IndexesAccessor accessor;

    @Before
    public void setUp() {
        accessor = Mockito.spy(new IndexesAccessor());
    }

    private IndexesRecord makeColumnarRecord(String indexName, long indexStatus) {
        IndexesRecord record = new IndexesRecord();
        record.indexName = indexName;
        record.indexStatus = indexStatus;
        record.indexLocation = IndexesRecord.GLOBAL_INDEX;
        record.flag = IndexesRecord.FLAG_COLUMNAR;
        return record;
    }

    private IndexesRecord makeNonColumnarRecord(String indexName, long indexStatus) {
        IndexesRecord record = new IndexesRecord();
        record.indexName = indexName;
        record.indexStatus = indexStatus;
        record.indexLocation = IndexesRecord.GLOBAL_INDEX;
        record.flag = 0;
        return record;
    }

    @Test
    public void testDistinctByIndexName() {
        List<IndexesRecord> records = Arrays.asList(
            makeColumnarRecord("cci_order_date", IndexStatus.PUBLIC.getValue()),
            makeColumnarRecord("cci_order_date", IndexStatus.PUBLIC.getValue()),
            makeColumnarRecord("cci_order_date", IndexStatus.PUBLIC.getValue())
        );
        Mockito.doReturn(records).when(accessor).query("test_db", "test_table");

        long count = accessor.getColumnarIndexNum("test_db", "test_table");
        Assert.assertEquals(1L, count);
    }

    @Test
    public void testFilterNonPublicStatus() {
        List<IndexesRecord> records = Arrays.asList(
            makeColumnarRecord("cci_active", IndexStatus.PUBLIC.getValue()),
            makeColumnarRecord("cci_creating", IndexStatus.CREATING.getValue()),
            makeColumnarRecord("cci_absent", IndexStatus.ABSENT.getValue())
        );
        Mockito.doReturn(records).when(accessor).query("test_db", "test_table");

        long count = accessor.getColumnarIndexNum("test_db", "test_table");
        Assert.assertEquals(1L, count);
    }

    @Test
    public void testMultiColumnCciCountsAsOne() {
        List<IndexesRecord> records = Arrays.asList(
            makeColumnarRecord("cci_composite", IndexStatus.PUBLIC.getValue()),
            makeColumnarRecord("cci_composite", IndexStatus.PUBLIC.getValue()),
            makeColumnarRecord("cci_composite", IndexStatus.PUBLIC.getValue()),
            makeColumnarRecord("cci_another", IndexStatus.PUBLIC.getValue()),
            makeColumnarRecord("cci_another", IndexStatus.PUBLIC.getValue())
        );
        Mockito.doReturn(records).when(accessor).query("test_db", "test_table");

        long count = accessor.getColumnarIndexNum("test_db", "test_table");
        Assert.assertEquals(2L, count);
    }

    @Test
    public void testNonColumnarNotCounted() {
        List<IndexesRecord> records = Arrays.asList(
            makeColumnarRecord("cci_valid", IndexStatus.PUBLIC.getValue()),
            makeNonColumnarRecord("idx_local", IndexStatus.PUBLIC.getValue())
        );
        Mockito.doReturn(records).when(accessor).query("test_db", "test_table");

        long count = accessor.getColumnarIndexNum("test_db", "test_table");
        Assert.assertEquals(1L, count);
    }

    @Test
    public void testNullRecords() {
        Mockito.doReturn(null).when(accessor).query("test_db", "test_table");

        long count = accessor.getColumnarIndexNum("test_db", "test_table");
        Assert.assertEquals(0L, count);
    }

    @Test
    public void testEmptyRecords() {
        Mockito.doReturn(new ArrayList<>()).when(accessor).query("test_db", "test_table");

        long count = accessor.getColumnarIndexNum("test_db", "test_table");
        Assert.assertEquals(0L, count);
    }

    @Test
    public void testDroppedIndexNotCounted() {
        List<IndexesRecord> records = Arrays.asList(
            makeColumnarRecord("cci_active", IndexStatus.PUBLIC.getValue()),
            makeColumnarRecord("cci_dropping", IndexStatus.DROP_WRITE_ONLY.getValue()),
            makeColumnarRecord("cci_dropped", IndexStatus.DROP_DELETE_ONLY.getValue())
        );
        Mockito.doReturn(records).when(accessor).query("test_db", "test_table");

        long count = accessor.getColumnarIndexNum("test_db", "test_table");
        Assert.assertEquals(1L, count);
    }
}
