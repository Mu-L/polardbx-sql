package com.alibaba.polardbx.qatest.dql.auto.vector;

import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class VectorShowIndexMetadataTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorShowIndexMetadataTest.class);

    @org.junit.BeforeClass
    public static void setUpDatabase() throws Exception {
        assumeMysql80Dn();
        dropTestDatabase(DATABASE_NAME);
        createTestDatabase(DATABASE_NAME);
    }

    @org.junit.AfterClass
    public static void tearDownDatabase() throws Exception {
        dropTestDatabase(DATABASE_NAME);
    }

    @Test
    public void testOrdinaryAndMultipleVectorIndexesShareMetadataRefresh() throws Exception {
        String table = "t_mixed_local_indexes";
        dropTableIfExists(table);
        try (Statement statement = tddlConnection.createStatement()) {
            statement.execute("CREATE TABLE " + table + " ("
                + "id BIGINT PRIMARY KEY, "
                + "tag VARCHAR(32), "
                + "embedding_a VECTOR(3), "
                + "embedding_b VECTOR(4), "
                + "KEY idx_tag(tag), "
                + "VECTOR INDEX vec_a(embedding_a), "
                + "VECTOR INDEX vec_b(embedding_b)"
                + ") SINGLE");

            Map<String, Integer> counts = new HashMap<>();
            Map<String, String> types = new HashMap<>();
            try (ResultSet resultSet = statement.executeQuery("SHOW INDEX FROM " + table)) {
                while (resultSet.next()) {
                    String name = resultSet.getString("Key_name");
                    counts.put(name.toLowerCase(), counts.getOrDefault(name.toLowerCase(), 0) + 1);
                    types.put(name.toLowerCase(), resultSet.getString("Index_type"));
                }
            }

            Assert.assertEquals(Integer.valueOf(1), counts.get("primary"));
            Assert.assertEquals(Integer.valueOf(1), counts.get("idx_tag"));
            Assert.assertEquals(Integer.valueOf(1), counts.get("vec_a"));
            Assert.assertEquals(Integer.valueOf(1), counts.get("vec_b"));
            Assert.assertEquals("BTREE", types.get("idx_tag").toUpperCase());
            assertVectorType(types.get("vec_a"));
            assertVectorType(types.get("vec_b"));
        } finally {
            dropTableIfExists(table);
        }
    }

    private static void assertVectorType(String type) {
        Assert.assertNotNull(type);
        Assert.assertEquals("Unexpected vector index type", "VECTOR", type.toUpperCase());
    }

}
