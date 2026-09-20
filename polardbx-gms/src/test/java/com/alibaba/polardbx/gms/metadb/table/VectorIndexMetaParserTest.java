package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import org.junit.Assert;
import org.junit.Test;

public class VectorIndexMetaParserTest {

    @Test
    public void testStructuredInformationSchemaRecord() {
        VectorIndexesInfoSchemaRecord vector = vector("vec_a", "embedding", "EUCLIDEAN", 3L, "006", "040");

        VectorIndexMeta meta = VectorIndexMetaParser.fromInfoSchemaRecord(vector);
        IndexesInfoSchemaRecord index = VectorIndexMetaParser.toIndexesInfoSchemaRecord(vector);

        Assert.assertEquals(VectorIndexMeta.Algorithm.HNSW, meta.getAlgorithm());
        Assert.assertEquals(VectorIndexMeta.Metric.EUCLIDEAN, meta.getMetric());
        Assert.assertEquals(3, meta.getDimension());
        Assert.assertEquals(6, meta.getM());
        Assert.assertEquals(40, meta.getEfConstruction());
        Assert.assertEquals("phy_db", index.tableSchema);
        Assert.assertEquals("phy_table", index.tableName);
        Assert.assertEquals("vec_a", index.indexName);
        Assert.assertEquals("embedding", index.columnName);
        Assert.assertEquals("VECTOR", index.indexType);
        Assert.assertEquals("", index.nullable);
        Assert.assertEquals("", index.comment);
        Assert.assertEquals("M=6, DISTANCE=EUCLIDEAN, EF_CONSTRUCTION=40, DIM=3", index.indexComment);
    }

    @Test
    public void testParseDurableIndexComment() {
        VectorIndexMeta meta = VectorIndexMetaParser.parseVectorIndexMeta(
            "M=6, DISTANCE=EUCLIDEAN, EF_CONSTRUCTION=40, DIM=3");
        Assert.assertEquals(VectorIndexMeta.Metric.EUCLIDEAN, meta.getMetric());
        Assert.assertEquals("M=6, DISTANCE=EUCLIDEAN, EF_CONSTRUCTION=40, DIM=3",
            VectorIndexMetaParser.toCanonicalIndexComment(meta));
    }

    @Test
    public void testInvalidStructuredFieldsFail() {
        VectorIndexesInfoSchemaRecord[] invalid = {
            vector("vec", "v", "UNKNOWN", 3L, "6", "40"),
            vector("vec", "v", "EUCLIDEAN", null, "6", "40"),
            vector("vec", "v", "EUCLIDEAN", 3L, "2", "40"),
            vector("vec", "v", "EUCLIDEAN", 3L, "6", "x")
        };
        for (VectorIndexesInfoSchemaRecord record : invalid) {
            try {
                VectorIndexMetaParser.toIndexesInfoSchemaRecord(record);
                Assert.fail("Expected invalid vector information schema record to fail");
            } catch (TddlRuntimeException expected) {
                Assert.assertNotNull(expected.getMessage());
            }
        }
    }

    @Test
    public void testMergeRejectsDifferentResolvedMetadata() {
        VectorIndexMeta existing = VectorIndexMetaParser.parseVectorIndexMeta(
            "M=6, DISTANCE=EUCLIDEAN, EF_CONSTRUCTION=40, DIM=3");
        try {
            VectorIndexMetaParser.mergeVectorIndexMeta(existing,
                "M=6, DISTANCE=COSINE, EF_CONSTRUCTION=40, DIM=3", "vec");
            Assert.fail("Expected inconsistent rows of one vector index to fail");
        } catch (TddlRuntimeException expected) {
            Assert.assertTrue(expected.getMessage().contains("conflicting"));
        }
    }

    @Test
    public void testMergeKeepsExistingMetadataForEquivalentRows() {
        VectorIndexMeta existing = VectorIndexMetaParser.parseVectorIndexMeta(
            "M=6, DISTANCE=EUCLIDEAN, EF_CONSTRUCTION=40, DIM=3");
        Assert.assertSame(existing, VectorIndexMetaParser.mergeVectorIndexMeta(existing, "", "vec"));
        Assert.assertEquals(existing, VectorIndexMetaParser.mergeVectorIndexMeta(existing,
            "DIM=3, EF_CONSTRUCTION=40, DISTANCE=euclidean, M=6", "vec"));
    }

    @Test
    public void testMalformedDurableCommentsFail() {
        assertParseFails(" ");
        assertParseFails("M=6, DISTANCE=EUCLIDEAN, EF_CONSTRUCTION=40");
        assertParseFails("M=6, M=7, DISTANCE=EUCLIDEAN, EF_CONSTRUCTION=40, DIM=3");
        assertParseFails("M=6, DISTANCE=EUCLIDEAN, EF_CONSTRUCTION=40, DIM=3, UNKNOWN=1");
        assertParseFails("M=, DISTANCE=EUCLIDEAN, EF_CONSTRUCTION=40, DIM=3");
    }

    private static void assertParseFails(String comment) {
        try {
            VectorIndexMetaParser.parseVectorIndexMeta(comment);
            Assert.fail("Expected invalid vector index comment to fail: " + comment);
        } catch (TddlRuntimeException expected) {
            Assert.assertNotNull(expected.getMessage());
        }
    }

    static VectorIndexesInfoSchemaRecord vector(String name, String column, String metric, Long dimension,
                                                String m, String efConstruction) {
        VectorIndexesInfoSchemaRecord record = new VectorIndexesInfoSchemaRecord();
        record.tableSchema = "phy_db";
        record.tableName = "phy_table";
        record.indexName = name;
        record.columnName = column;
        record.algorithm = "HNSW";
        record.metricType = metric;
        record.dimension = dimension;
        record.m = m;
        record.efConstruction = efConstruction;
        return record;
    }
}
