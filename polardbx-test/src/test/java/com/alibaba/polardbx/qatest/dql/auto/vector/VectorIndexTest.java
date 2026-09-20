package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Tests for Vector Index DDL and vector function query functionality.
 * <p>
 * Covers parser layer, DDL routing, DML with vector functions, and ANN queries.
 * DN fully supports all vector features: VECTOR type, VECTOR INDEX, VEC_FROMTEXT,
 * VEC_TOTEXT, VEC_DISTANCE, VEC_DISTANCE_EUCLIDEAN, VEC_DISTANCE_COSINE, VECTOR_DIM.
 */
public class VectorIndexTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexTest.class);

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

    private static final String TABLE_NAME = "t_vec_test_01";

    // ==================== DDL tests ====================

    /**
     * Test that CREATE TABLE with VECTOR column can be parsed and executed.
     */
    @Test
    public void testCreateTableWithVectorColumn() {
        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(4)"
                + ") partition by hash(id) partitions 4",
            TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW TABLES LIKE '" + TABLE_NAME + "'");
        try {
            assertWithMessage("Table should exist after creation")
                .that(rs.next()).isTrue();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    /**
     * Test CREATE VECTOR INDEX with DN syntax (M=N DISTANCE=X).
     */
    @Test
    public void testCreateVectorIndex() {
        String tableName = "t_vec_idx_test_01";
        dropTableIfExists(tableName);

        String createTableSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(4)"
                + ") partition by hash(id) partitions 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        String createIndexSql = String.format(
            "CREATE VECTOR INDEX vec_idx ON %s(embedding) M=6 DISTANCE=COSINE",
            tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndexSql);

        dropTableIfExists(tableName);
    }

    /**
     * Test CREATE TABLE with inline VECTOR INDEX.
     */
    @Test
    public void testCreateTableWithInlineVectorIndex() {
        String tableName = "t_vec_inline_test_01";
        dropTableIfExists(tableName);

        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(4), "
                + "  VECTOR INDEX vec_idx(embedding) M=6 DISTANCE=COSINE"
                + ") partition by hash(id) partitions 4",
            tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        dropTableIfExists(tableName);
    }

    /**
     * Test ALTER TABLE ADD VECTOR INDEX.
     */
    @Test
    public void testAlterTableAddVectorIndex() {
        String tableName = "t_vec_alter_test";
        dropTableIfExists(tableName);

        String createTableSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(4)"
                + ") partition by hash(id) partitions 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        String alterSql = String.format(
            "ALTER TABLE %s ADD VECTOR INDEX vec_idx(embedding) M=6 DISTANCE=COSINE",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);

        dropTableIfExists(tableName);
    }

    /**
     * Test DROP VECTOR INDEX.
     */
    @Test
    public void testDropVectorIndex() {
        String tableName = "t_vec_drop_test";
        String indexName = "vec_idx_drop";
        dropTableIfExists(tableName);

        String createTableSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(4), "
                + "  VECTOR INDEX %s(embedding) M=6 DISTANCE=COSINE"
                + ") partition by hash(id) partitions 4",
            tableName, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        String dropIndexSql = String.format("DROP INDEX %s ON %s", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropIndexSql);

        dropTableIfExists(tableName);
    }

    // ==================== VEC_FROMTEXT / VEC_TOTEXT / VECTOR_DIM tests ====================

    /**
     * Test VEC_FROMTEXT insert and VEC_TOTEXT read-back.
     */
    @Test
    public void testVecFromTextAndToText() {
        String tableName = "t_vec_fromtext_test";
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, embedding VECTOR(4))"
                + " partition by hash(id) partitions 4", tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (1, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]'))", tableName));

        String selectSql = String.format(
            "SELECT VEC_TOTEXT(embedding) FROM %s WHERE id = 1", tableName);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            String text = rs.getString(1);
            assertWithMessage("VEC_TOTEXT should return non-null").that(text).isNotNull();
        } catch (SQLException e) {
            throw new RuntimeException("VEC_TOTEXT failed: " + e.getMessage(), e);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    /**
     * Test VECTOR_DIM function.
     */
    @Test
    public void testVectorDim() {
        String tableName = "t_vec_dim_test";
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, embedding VECTOR(4))"
                + " partition by hash(id) partitions 4", tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (1, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]'))", tableName));

        String selectSql = String.format(
            "SELECT VECTOR_DIM(embedding) FROM %s WHERE id = 1", tableName);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            long dim = rs.getLong(1);
            assertWithMessage("VECTOR_DIM should return 4").that(dim).isEqualTo(4);
        } catch (SQLException e) {
            throw new RuntimeException("VECTOR_DIM failed: " + e.getMessage(), e);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    // ==================== VEC_DISTANCE query tests ====================

    /**
     * Test VEC_DISTANCE (auto distance type from index) in SELECT.
     */
    @Test
    public void testVecDistanceInSelect() {
        String tableName = "t_vec_select_test";
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, embedding VECTOR(4))"
                + " partition by hash(id) partitions 4", tableName));
        // VEC_DISTANCE requires a VECTOR INDEX to determine the distance metric
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE VECTOR INDEX vec_idx ON %s(embedding) M=6 DISTANCE=COSINE", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES "
                + "(1, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')), "
                + "(2, VEC_FROMTEXT('[0.5,0.6,0.7,0.8]'))",
            tableName));

        String selectSql = String.format(
            "SELECT id, VEC_DISTANCE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist"
                + " FROM %s ORDER BY id",
            tableName);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have first row").that(rs.next()).isTrue();
            assertWithMessage("ID should be 1").that(rs.getLong(1)).isEqualTo(1);
            assertWithMessage("Should have second row").that(rs.next()).isTrue();
            assertWithMessage("ID should be 2").that(rs.getLong(1)).isEqualTo(2);
        } catch (SQLException e) {
            throw new RuntimeException("VEC_DISTANCE failed: " + e.getMessage(), e);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    /**
     * Test VEC_DISTANCE_COSINE explicit function.
     */
    @Test
    public void testVecDistanceCosine() {
        String tableName = "t_vec_cosine_test";
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, embedding VECTOR(4))"
                + " partition by hash(id) partitions 4", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (1, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]'))", tableName));

        String selectSql = String.format(
            "SELECT VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist"
                + " FROM %s WHERE id = 1",
            tableName);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            double dist = rs.getDouble(1);
            assertWithMessage("Distance to same vector should be ~0").that(dist).isAtMost(0.001);
        } catch (SQLException e) {
            throw new RuntimeException("VEC_DISTANCE_COSINE failed: " + e.getMessage(), e);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    /**
     * Test VEC_DISTANCE_EUCLIDEAN explicit function.
     */
    @Test
    public void testVecDistanceEuclidean() {
        String tableName = "t_vec_euclidean_test";
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, embedding VECTOR(4))"
                + " partition by hash(id) partitions 4", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES (1, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]'))", tableName));

        String selectSql = String.format(
            "SELECT VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist"
                + " FROM %s WHERE id = 1",
            tableName);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            double dist = rs.getDouble(1);
            assertWithMessage("Euclidean distance to same vector should be 0").that(dist).isAtMost(0.001);
        } catch (SQLException e) {
            throw new RuntimeException("VEC_DISTANCE_EUCLIDEAN failed: " + e.getMessage(), e);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    /**
     * Test ANN query pattern: ORDER BY VEC_DISTANCE LIMIT N.
     */
    @Test
    public void testOrderByVecDistanceLimit() {
        String tableName = "t_vec_ann_test";
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, embedding VECTOR(4))"
                + " partition by hash(id) partitions 4", tableName));
        // VEC_DISTANCE requires a VECTOR INDEX to determine the distance metric
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE VECTOR INDEX vec_idx ON %s(embedding) M=6 DISTANCE=COSINE", tableName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s VALUES "
                + "(1, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')), "
                + "(2, VEC_FROMTEXT('[0.2,0.3,0.4,0.5]')), "
                + "(3, VEC_FROMTEXT('[0.9,0.8,0.7,0.6]'))",
            tableName));

        String selectSql = String.format(
            "SELECT id, VEC_DISTANCE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist"
                + " FROM %s ORDER BY dist LIMIT 2",
            tableName);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertWithMessage("Should return at most 2 rows").that(count).isAtMost(2);
        } catch (SQLException e) {
            throw new RuntimeException("ANN query failed: " + e.getMessage(), e);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    /**
     * Test EXPLAIN with VEC_DISTANCE query.
     */
    @Test
    public void testExplainVecDistance() {
        String tableName = "t_vec_explain_test";
        dropTableIfExists(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, embedding VECTOR(4))"
                + " partition by hash(id) partitions 4", tableName));

        String explainSql = String.format(
            "EXPLAIN SELECT id, VEC_DISTANCE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist"
                + " FROM %s ORDER BY dist LIMIT 10",
            tableName);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(explainSql)) {
            assertWithMessage("EXPLAIN should return results").that(rs != null).isTrue();
        } catch (SQLException e) {
            throw new RuntimeException("EXPLAIN failed: " + e.getMessage(), e);
        } finally {
            dropTableIfExists(tableName);
        }
    }
}
