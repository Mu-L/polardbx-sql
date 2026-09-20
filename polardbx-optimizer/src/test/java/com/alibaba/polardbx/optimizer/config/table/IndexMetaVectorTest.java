package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.gms.metadb.table.VectorIndexMeta;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexMetaVectorTest {

    @Test
    public void testTypedVectorMetadataIsRetained() {
        ColumnMeta column = mock(ColumnMeta.class);
        when(column.getName()).thenReturn("embedding");
        VectorIndexMeta vectorMeta = new VectorIndexMeta(VectorIndexMeta.Algorithm.HNSW,
            VectorIndexMeta.Metric.COSINE, 128, 16, 100);

        IndexMeta indexMeta = new IndexMeta("t", Collections.singletonList(column), Collections.emptyList(),
            IndexType.VECTOR, Relationship.NONE, true, false, false, "vec", vectorMeta);

        Assert.assertSame(vectorMeta, indexMeta.getVectorIndexMeta());
        Assert.assertEquals(VectorIndexMeta.Metric.COSINE, indexMeta.getVectorIndexMeta().getMetric());
        Assert.assertEquals(128, indexMeta.getVectorIndexMeta().getDimension());
    }

    @Test
    public void testOrdinaryIndexHasNoVectorMetadata() {
        ColumnMeta column = mock(ColumnMeta.class);
        when(column.getName()).thenReturn("tag");

        IndexMeta indexMeta = new IndexMeta("t", Collections.singletonList(column), Collections.emptyList(),
            IndexType.BTREE, Relationship.NONE, true, false, false, "idx_tag");

        Assert.assertNull(indexMeta.getVectorIndexMeta());
    }

    @Test
    public void testTypedVectorMetadataSurvivesSerialization() throws Exception {
        VectorIndexMeta vectorMeta = new VectorIndexMeta(VectorIndexMeta.Algorithm.HNSW,
            VectorIndexMeta.Metric.INNER_PRODUCT, 256, 32, 120);
        IndexMeta indexMeta = new IndexMeta("t", Collections.emptyList(), Collections.emptyList(),
            IndexType.VECTOR, Relationship.NONE, true, false, false, "vec", vectorMeta);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(indexMeta);
        }
        IndexMeta restored;
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (IndexMeta) input.readObject();
        }

        Assert.assertEquals(IndexType.VECTOR, restored.getIndexType());
        Assert.assertEquals(vectorMeta, restored.getVectorIndexMeta());
    }
}
