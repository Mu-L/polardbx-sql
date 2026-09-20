package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;

/**
 * DN v3.0 vector-index DDL option contract through CN.
 */
public class VectorIndexDnV3DdlTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexDnV3DdlTest.class);

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
    public void testAllDdlEntrypointsPreserveEfAndInnerProduct() throws Exception {
        assertDdlRoundTrip(
            "t_vec_v3_inline",
            "CREATE TABLE t_vec_v3_inline (id BIGINT PRIMARY KEY, v VECTOR(3), "
                + "VECTOR INDEX vi(v) M=6 EF_CONSTRUCTION=40 DISTANCE=INNER_PRODUCT) SINGLE",
            null);

        assertDdlRoundTrip(
            "t_vec_v3_alter",
            "CREATE TABLE t_vec_v3_alter (id BIGINT PRIMARY KEY, v VECTOR(3)) SINGLE",
            "ALTER TABLE t_vec_v3_alter ADD VECTOR INDEX vi(v) "
                + "DISTANCE=INNER_PRODUCT M=6 EF_CONSTRUCTION=40");

        assertDdlRoundTrip(
            "t_vec_v3_create",
            "CREATE TABLE t_vec_v3_create (id BIGINT PRIMARY KEY, v VECTOR(3)) SINGLE",
            "CREATE VECTOR INDEX vi ON t_vec_v3_create(v) "
                + "EF_CONSTRUCTION=40 DISTANCE=INNER_PRODUCT M=6");
    }

    private void assertDdlRoundTrip(String tableName, String createTable, String createIndex) throws Exception {
        dropTableIfExists(tableName);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
            if (createIndex != null) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);
            }

            try (Statement statement = tddlConnection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW CREATE TABLE " + tableName)) {
                Assert.assertTrue(resultSet.next());
                String ddl = resultSet.getString(2).toUpperCase();
                Assert.assertTrue(ddl, ddl.contains("VECTOR"));
                Assert.assertTrue(ddl, ddl.contains("INNER_PRODUCT"));
                Assert.assertTrue(ddl, ddl.contains("M=6") || ddl.contains("M = 6"));
                Assert.assertTrue(ddl,
                    ddl.contains("EF_CONSTRUCTION=40") || ddl.contains("EF_CONSTRUCTION = 40"));
            }
        } finally {
            dropTableIfExists(tableName);
        }
    }
}
