package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TableInfoManagerVectorIndexesTest {

    @After
    public void tearDown() {
        InstanceVersion.setSupportsVectorIndexes(false);
    }

    @Test
    public void testVectorIndexesAreAppendedWithoutChangingOrdinaryIndexes() {
        IndexesInfoSchemaRecord primary = ordinary("PRIMARY", "id", "BTREE");
        IndexesInfoSchemaRecord ordinary = ordinary("idx_tag", "tag", "BTREE");

        List<IndexesInfoSchemaRecord> result = TableInfoManager.appendVectorIndexes(
            Arrays.asList(primary, ordinary),
            Arrays.asList(VectorIndexMetaParserTest.vector(
                "vec_embedding", "embedding", "EUCLIDEAN", 3L, "6", "40")));

        Assert.assertEquals(3, result.size());
        Assert.assertSame(primary, result.get(0));
        Assert.assertSame(ordinary, result.get(1));
        Assert.assertEquals("vec_embedding", result.get(2).indexName);
        Assert.assertEquals("VECTOR", result.get(2).indexType);
        Assert.assertEquals("", result.get(2).nullable);
        Assert.assertEquals("", result.get(2).comment);
        Assert.assertEquals("M=6, DISTANCE=EUCLIDEAN, EF_CONSTRUCTION=40, DIM=3",
            result.get(2).indexComment);
    }

    @Test
    public void testVectorMetadataIsSkippedForSystemSchemas() {
        Assert.assertFalse(TableInfoManager.shouldFetchVectorIndexes("information_schema"));
        Assert.assertFalse(TableInfoManager.shouldFetchVectorIndexes("PERFORMANCE_SCHEMA"));
        Assert.assertFalse(TableInfoManager.shouldFetchVectorIndexes("mysql"));
        Assert.assertFalse(TableInfoManager.shouldFetchVectorIndexes("sys"));
        Assert.assertFalse(TableInfoManager.shouldFetchVectorIndexes(null));
        Assert.assertTrue(TableInfoManager.shouldFetchVectorIndexes("application_db"));
    }

    @Test
    public void testEmptyVectorResultKeepsOrdinaryListUnchanged() {
        List<IndexesInfoSchemaRecord> ordinary = Arrays.asList(ordinary("idx_tag", "tag", "BTREE"));

        Assert.assertSame(ordinary, TableInfoManager.appendVectorIndexes(ordinary, null));
        Assert.assertSame(ordinary, TableInfoManager.appendVectorIndexes(ordinary, Arrays.asList()));
    }

    @Test
    public void testVectorQueryIsSkippedWhenStorageDoesNotSupportIt() throws Exception {
        TableInfoManager target = new TableInfoManager();
        IndexesAccessor mockIndexesAccessor = mock(IndexesAccessor.class);
        VectorIndexesAccessor mockVectorIndexesAccessor = mock(VectorIndexesAccessor.class);
        DataSource mockDataSource = mock(DataSource.class);
        List<IndexesInfoSchemaRecord> ordinary = Collections.singletonList(ordinary("idx_tag", "tag", "BTREE"));
        setAccessor(target, "indexesAccessor", mockIndexesAccessor);
        setAccessor(target, "vectorIndexesAccessor", mockVectorIndexesAccessor);
        when(mockIndexesAccessor.queryInfoSchema("app_db", "t", mockDataSource)).thenReturn(ordinary);
        InstanceVersion.setSupportsVectorIndexes(false);

        List<IndexesInfoSchemaRecord> result = invokeFetchIndexMeta(
            target, "app_db", "t", mockDataSource);

        Assert.assertSame(ordinary, result);
        verifyNoInteractions(mockVectorIndexesAccessor);
    }

    @Test
    public void testVectorQueryRunsWhenStorageSupportsIt() throws Exception {
        TableInfoManager target = new TableInfoManager();
        IndexesAccessor mockIndexesAccessor = mock(IndexesAccessor.class);
        VectorIndexesAccessor mockVectorIndexesAccessor = mock(VectorIndexesAccessor.class);
        DataSource mockDataSource = mock(DataSource.class);
        List<IndexesInfoSchemaRecord> ordinary = Collections.singletonList(ordinary("idx_tag", "tag", "BTREE"));
        List<VectorIndexesInfoSchemaRecord> vectors = Collections.singletonList(
            VectorIndexMetaParserTest.vector("vec_embedding", "embedding", "EUCLIDEAN", 3L, "6", "40"));
        setAccessor(target, "indexesAccessor", mockIndexesAccessor);
        setAccessor(target, "vectorIndexesAccessor", mockVectorIndexesAccessor);
        when(mockIndexesAccessor.queryInfoSchema("app_db", "t", mockDataSource)).thenReturn(ordinary);
        when(mockVectorIndexesAccessor.query("app_db", "t", mockDataSource)).thenReturn(vectors);
        InstanceVersion.setSupportsVectorIndexes(true);

        List<IndexesInfoSchemaRecord> result = invokeFetchIndexMeta(
            target, "app_db", "t", mockDataSource);

        Assert.assertEquals(2, result.size());
        Assert.assertEquals("vec_embedding", result.get(1).indexName);
        verify(mockVectorIndexesAccessor).query("app_db", "t", mockDataSource);
    }

    @Test
    public void testSystemSchemaSkipsVectorQueryEvenWhenStorageSupportsIt() throws Exception {
        TableInfoManager target = new TableInfoManager();
        IndexesAccessor mockIndexesAccessor = mock(IndexesAccessor.class);
        VectorIndexesAccessor mockVectorIndexesAccessor = mock(VectorIndexesAccessor.class);
        DataSource mockDataSource = mock(DataSource.class);
        List<IndexesInfoSchemaRecord> ordinary = Collections.singletonList(ordinary("PRIMARY", "id", "BTREE"));
        setAccessor(target, "indexesAccessor", mockIndexesAccessor);
        setAccessor(target, "vectorIndexesAccessor", mockVectorIndexesAccessor);
        when(mockIndexesAccessor.queryInfoSchema("mysql", "t", mockDataSource)).thenReturn(ordinary);
        InstanceVersion.setSupportsVectorIndexes(true);

        List<IndexesInfoSchemaRecord> result = invokeFetchIndexMeta(
            target, "mysql", "t", mockDataSource);

        Assert.assertSame(ordinary, result);
        verifyNoInteractions(mockVectorIndexesAccessor);
    }

    @Test
    public void testFirstColumnVectorQueryHonorsStorageSupport() throws Exception {
        TableInfoManager target = new TableInfoManager();
        IndexesAccessor mockIndexesAccessor = mock(IndexesAccessor.class);
        VectorIndexesAccessor mockVectorIndexesAccessor = mock(VectorIndexesAccessor.class);
        DataSource mockDataSource = mock(DataSource.class);
        List<IndexesInfoSchemaRecord> ordinary = Collections.singletonList(ordinary("idx_tag", "tag", "BTREE"));
        List<VectorIndexesInfoSchemaRecord> vectors = Collections.singletonList(
            VectorIndexMetaParserTest.vector("vec_embedding", "embedding", "EUCLIDEAN", 3L, "6", "40"));
        setAccessor(target, "indexesAccessor", mockIndexesAccessor);
        setAccessor(target, "vectorIndexesAccessor", mockVectorIndexesAccessor);
        when(mockIndexesAccessor.queryInfoSchemaByFirstColumn("app_db", "t", "embedding", mockDataSource))
            .thenReturn(ordinary);
        when(mockVectorIndexesAccessor.queryByFirstColumn("app_db", "t", "embedding", mockDataSource))
            .thenReturn(vectors);

        InstanceVersion.setSupportsVectorIndexes(false);
        Assert.assertSame(ordinary, invokeFetchIndexMetaByFirstColumn(
            target, "app_db", "t", "embedding", mockDataSource));
        verify(mockVectorIndexesAccessor, never())
            .queryByFirstColumn("app_db", "t", "embedding", mockDataSource);

        InstanceVersion.setSupportsVectorIndexes(true);
        List<IndexesInfoSchemaRecord> result = invokeFetchIndexMetaByFirstColumn(
            target, "app_db", "t", "embedding", mockDataSource);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("vec_embedding", result.get(1).indexName);
        verify(mockVectorIndexesAccessor)
            .queryByFirstColumn("app_db", "t", "embedding", mockDataSource);
    }

    private static List<IndexesInfoSchemaRecord> invokeFetchIndexMeta(TableInfoManager target,
                                                                      String schema,
                                                                      String table,
                                                                      DataSource dataSource) throws Exception {
        Method method = TableInfoManager.class.getDeclaredMethod(
            "fetchIndexMetaFromInfoSchema", String.class, String.class, DataSource.class);
        method.setAccessible(true);
        return (List<IndexesInfoSchemaRecord>) method.invoke(target, schema, table, dataSource);
    }

    private static List<IndexesInfoSchemaRecord> invokeFetchIndexMetaByFirstColumn(TableInfoManager target,
                                                                                   String schema,
                                                                                   String table,
                                                                                   String column,
                                                                                   DataSource dataSource)
        throws Exception {
        Method method = TableInfoManager.class.getDeclaredMethod(
            "fetchIndexMetaByFirstColumnFromInfoSchema", String.class, String.class, String.class, DataSource.class);
        method.setAccessible(true);
        return (List<IndexesInfoSchemaRecord>) method.invoke(target, schema, table, column, dataSource);
    }

    private static void setAccessor(TableInfoManager target, String fieldName, Object accessor) throws Exception {
        Field field = TableInfoManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, accessor);
    }

    private static IndexesInfoSchemaRecord ordinary(String name, String column, String type) {
        IndexesInfoSchemaRecord record = new IndexesInfoSchemaRecord();
        record.tableSchema = "phy_db";
        record.tableName = "phy_table";
        record.indexSchema = "phy_db";
        record.indexName = name;
        record.columnName = column;
        record.seqInIndex = 1;
        record.indexType = type;
        return record;
    }
}
