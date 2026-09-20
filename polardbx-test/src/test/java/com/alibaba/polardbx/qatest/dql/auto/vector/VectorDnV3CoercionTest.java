package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * DN v3 VECTOR function aliases, CAST/JSON coercion, and parameter regression.
 */
public class VectorDnV3CoercionTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorDnV3CoercionTest.class);

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

    private static final String TABLE_NAME = "vec_dn_v3_coercion";

    @Before
    public void initTable() {
        dropTableIfExists(TABLE_NAME);
    }

    @Test
    public void testInputAndOutputAliases() throws SQLException {
        String sql = "SELECT FROM_VECTOR(TO_VECTOR('[1,2,3]')) AS first_value, "
            + "VECTOR_TO_STRING(STRING_TO_VECTOR('[4,5,6]')) AS second_value FROM DUAL";
        try (ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("[1,2,3]", resultSet.getString("first_value"));
            Assert.assertEquals("[4,5,6]", resultSet.getString("second_value"));
        }
    }

    @Test
    public void testTextAndJsonCast() throws SQLException {
        String sql = "SELECT VEC_TOTEXT(CAST('[1,2,3]' AS VECTOR(3))) AS text_vector, "
            + "VEC_TOTEXT(CAST(JSON_ARRAY(4,5,6) AS VECTOR(3))) AS json_vector, "
            + "VECTOR_DIM(CAST('[7,8]' AS VECTOR)) AS inferred_dimension FROM DUAL";
        try (ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection)) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("[1,2,3]", resultSet.getString("text_vector"));
            Assert.assertEquals("[4,5,6]", resultSet.getString("json_vector"));
            Assert.assertEquals(2L, resultSet.getLong("inferred_dimension"));
        }
    }

    @Test
    public void testJsonNullAndInvalidShapes() throws SQLException {
        try (ResultSet resultSet = JdbcUtil.executeQuery(
            "SELECT CAST(CAST('null' AS JSON) AS VECTOR) AS value FROM DUAL", tddlConnection)) {
            Assert.assertTrue(resultSet.next());
            Assert.assertNull(resultSet.getBytes("value"));
        }
        assertQueryFails("SELECT CAST(JSON_OBJECT('x', 1) AS VECTOR) FROM DUAL");
        assertQueryFails("SELECT CAST(JSON_ARRAY(JSON_ARRAY(1), 2) AS VECTOR) FROM DUAL");
        assertQueryFails("SELECT CAST(JSON_ARRAY(1, '2') AS VECTOR) FROM DUAL");
        assertQueryFails("SELECT CAST('[1,2]' AS VECTOR(3)) FROM DUAL");
    }

    @Test
    public void testPreparedStringParameterIsPassedToDnUnchanged() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " (id BIGINT PRIMARY KEY, embedding VECTOR(3)) SINGLE");
        try (PreparedStatement statement = tddlConnection.prepareStatement(
            "INSERT INTO " + TABLE_NAME + " VALUES (?, ?)")) {
            statement.setLong(1, 1L);
            statement.setString(2, "[1,2,3]");
            statement.executeUpdate();
            Assert.fail("Expected DN to reject a raw string parameter for VECTOR");
        } catch (SQLException expected) {
            Assert.assertTrue(expected.getMessage().contains("Incorrect vector value"));
        }

        try (PreparedStatement statement = tddlConnection.prepareStatement(
            "INSERT INTO " + TABLE_NAME + " VALUES (?, VEC_FROMTEXT(?))")) {
            statement.setLong(1, 1L);
            statement.setString(2, "[1,2,3]");
            Assert.assertEquals(1, statement.executeUpdate());
        }
        try (ResultSet resultSet = JdbcUtil.executeQuery(
            "SELECT VEC_TOTEXT(embedding) FROM " + TABLE_NAME + " WHERE id = 1", tddlConnection)) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("[1,2,3]", resultSet.getString(1));
        }

        try (PreparedStatement statement = tddlConnection.prepareStatement(
            "UPDATE " + TABLE_NAME + " SET embedding = VEC_FROMTEXT(?) WHERE id = ?")) {
            statement.setString(1, "[3,2,1]");
            statement.setLong(2, 1L);
            Assert.assertEquals(1, statement.executeUpdate());
        }
        try (ResultSet resultSet = JdbcUtil.executeQuery(
            "SELECT VEC_TOTEXT(embedding) FROM " + TABLE_NAME + " WHERE id = 1", tddlConnection)) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("[3,2,1]", resultSet.getString(1));
        }

        try (PreparedStatement statement = tddlConnection.prepareStatement(
            "INSERT INTO " + TABLE_NAME + " VALUES (?, VEC_FROMTEXT(?))")) {
            statement.setLong(1, 2L);
            statement.setString(2, "[1,2]");
            statement.executeUpdate();
            Assert.fail("Expected VECTOR dimension mismatch");
        } catch (SQLException expected) {
            Assert.assertFalse(expected.getMessage().isEmpty());
        }
    }

    private void assertQueryFails(String sql) {
        try (java.sql.Statement statement = tddlConnection.createStatement();
            ResultSet ignored = statement.executeQuery(sql)) {
            Assert.fail("Expected query to fail: " + sql);
        } catch (SQLException expected) {
            Assert.assertFalse(expected.getMessage().isEmpty());
        }
    }
}
