package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Regression coverage for DN v3 explicit VECTOR functions and CN fallback.
 */
public class VectorDnV3FunctionTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorDnV3FunctionTest.class);

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

    private static final double DELTA = 1E-9;

    @Test
    public void testInnerProductAndDimensionWithoutTable() throws SQLException {
        String sql = "SELECT "
            + "VEC_DISTANCE_INNER_PRODUCT(VEC_FROMTEXT('[1,2,3]'), VEC_FROMTEXT('[4,5,6]')) AS distance, "
            + "VECTOR_DIM(VEC_FROMTEXT('[1,2,3]')) AS dimension FROM DUAL";
        try (ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(-32.0D, resultSet.getDouble("distance"), DELTA);
            Assert.assertEquals(3L, resultSet.getLong("dimension"));
        }
    }

    @Test
    public void testInnerProductOnVectorColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE vec_dn_v3_function (id BIGINT PRIMARY KEY, embedding VECTOR(3)) SINGLE");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO vec_dn_v3_function VALUES (1, VEC_FROMTEXT('[1,2,3]'))");
        String sql = "SELECT VEC_DISTANCE_INNER_PRODUCT(embedding, VEC_FROMTEXT('[4,5,6]')) AS distance, "
            + "VECTOR_DIM(embedding) AS dimension FROM vec_dn_v3_function WHERE id = 1";
        try (ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(-32.0D, resultSet.getDouble("distance"), DELTA);
            Assert.assertEquals(3L, resultSet.getLong("dimension"));
        }
    }
}
