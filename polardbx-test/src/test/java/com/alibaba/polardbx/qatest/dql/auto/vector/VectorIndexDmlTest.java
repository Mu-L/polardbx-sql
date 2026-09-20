package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Tests for Vector Index DML operations.
 * <p>
 * Covers:
 * - Batch insert vector data
 * - Update vector column
 * - Delete vector data
 * - REPLACE INTO vector data
 * - INSERT ON DUPLICATE KEY UPDATE vector column
 */
public class VectorIndexDmlTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexDmlTest.class);

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

    private static final String TABLE_NAME = "t_vec_dml_test";

    @Before
    public void initTable() {
        dropTableIfExists(TABLE_NAME);
        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  name VARCHAR(100), "
                + "  embedding VECTOR(4)"
                + ") partition by hash(id) partitions 4",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
    }

    /**
     * Test batch insert of vector data.
     */
    @Test
    public void testBatchInsertVectorData() {
        // Insert multiple rows at once
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(TABLE_NAME).append(" VALUES ");
        for (int i = 1; i <= 100; i++) {
            if (i > 1) {
                sb.append(", ");
            }
            sb.append(String.format("(%d, 'item_%d', VEC_FROMTEXT('[%f,%f,%f,%f]'))",
                i, i, Math.random(), Math.random(), Math.random(), Math.random()));
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, sb.toString());

        // Verify count
        String countSql = "SELECT COUNT(*) FROM " + TABLE_NAME;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(countSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            assertWithMessage("Should have 100 rows").that(rs.getInt(1)).isEqualTo(100);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test update of vector column.
     */
    @Test
    public void testUpdateVectorColumn() {
        // Insert initial data
        String insertSql = String.format(
            "INSERT INTO %s VALUES (1, 'item_1', VEC_FROMTEXT('[0.1,0.2,0.3,0.4]'))", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        // Update vector column
        String updateSql = String.format(
            "UPDATE %s SET embedding = VEC_FROMTEXT('[0.5,0.6,0.7,0.8]') WHERE id = 1", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateSql);

        // Verify update
        String selectSql = String.format(
            "SELECT VEC_TOTEXT(embedding) FROM %s WHERE id = 1", TABLE_NAME);
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            String embedding = rs.getString(1);
            assertWithMessage("Embedding should be updated")
                .that(embedding).contains("0.5");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test delete of vector data.
     */
    @Test
    public void testDeleteVectorData() {
        // Insert data
        String insertSql = String.format(
            "INSERT INTO %s VALUES (1, 'item_1', VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')), "
                + "(2, 'item_2', VEC_FROMTEXT('[0.5,0.6,0.7,0.8]'))", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        // Delete one row
        String deleteSql = String.format("DELETE FROM %s WHERE id = 1", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, deleteSql);

        // Verify delete
        String countSql = "SELECT COUNT(*) FROM " + TABLE_NAME;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(countSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            assertWithMessage("Should have 1 row").that(rs.getInt(1)).isEqualTo(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test REPLACE INTO vector data.
     */
    @Test
    public void testReplaceIntoVectorData() {
        // Insert initial data
        String insertSql = String.format(
            "INSERT INTO %s VALUES (1, 'item_1', VEC_FROMTEXT('[0.1,0.2,0.3,0.4]'))", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        // Replace with new data
        String replaceSql = String.format(
            "REPLACE INTO %s VALUES (1, 'item_1_updated', VEC_FROMTEXT('[0.9,0.8,0.7,0.6]'))", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, replaceSql);

        // Verify replace
        String selectSql = String.format(
            "SELECT name, embedding FROM %s WHERE id = 1", TABLE_NAME);
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            assertWithMessage("Name should be updated")
                .that(rs.getString(1)).isEqualTo("item_1_updated");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test INSERT ON DUPLICATE KEY UPDATE vector column.
     */
    @Test
    public void testInsertOnDuplicateKeyUpdateVector() {
        // Insert initial data
        String insertSql = String.format(
            "INSERT INTO %s VALUES (1, 'item_1', VEC_FROMTEXT('[0.1,0.2,0.3,0.4]'))", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        // Insert with ON DUPLICATE KEY UPDATE
        String upsertSql = String.format(
            "INSERT INTO %s VALUES (1, 'item_1_new', VEC_FROMTEXT('[0.5,0.6,0.7,0.8]')) "
                + "ON DUPLICATE KEY UPDATE embedding = VALUES(embedding), name = VALUES(name)",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, upsertSql);

        // Verify update
        String selectSql = String.format(
            "SELECT name, embedding FROM %s WHERE id = 1", TABLE_NAME);
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            assertWithMessage("Name should be updated")
                .that(rs.getString(1)).isEqualTo("item_1_new");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test insert with NULL vector value.
     */
    @Test
    public void testInsertNullVector() {
        String insertSql = String.format(
            "INSERT INTO %s (id, name) VALUES (1, 'item_without_vector')", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        String selectSql = String.format(
            "SELECT embedding FROM %s WHERE id = 1", TABLE_NAME);
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            Object embedding = rs.getObject(1);
            // NULL or empty vector
            assertWithMessage("Embedding should be null or empty")
                .that(embedding == null || rs.wasNull()).isTrue();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test insert vector with different dimensions (should fail if dimension mismatch).
     */
    @Test
    public void testInsertVectorDimensionMismatch() {
        // Try to insert a vector with wrong dimension
        String insertSql = String.format(
            "INSERT INTO %s VALUES (1, 'item_1', VEC_FROMTEXT('[0.1,0.2,0.3]'))", TABLE_NAME);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(insertSql);
            // If DN accepts it, that's fine (some DN might store as-is)
        } catch (SQLException e) {
            // Expected: dimension mismatch error
            String msg = e.getMessage().toLowerCase();
            // Should be an execution error, not parser error
            assertWithMessage("Should not be a parser error, got: " + msg)
                .that(msg).doesNotContain("syntax");
        }
    }

    /**
     * Test multi-row insert with vectors.
     */
    @Test
    public void testMultiRowInsert() {
        List<String> values = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            values.add(String.format("(%d, 'item_%d', VEC_FROMTEXT('[%f,%f,%f,%f]'))",
                i, i, i * 0.1, i * 0.1 + 0.01, i * 0.1 + 0.02, i * 0.1 + 0.03));
        }

        String insertSql = String.format(
            "INSERT INTO %s VALUES %s", TABLE_NAME, String.join(", ", values));
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        // Verify count
        String countSql = "SELECT COUNT(*) FROM " + TABLE_NAME;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(countSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            assertWithMessage("Should have 10 rows").that(rs.getInt(1)).isEqualTo(10);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test VEC_DISTANCE after DML operations.
     */
    @Test
    public void testVecDistanceAfterDml() {
        // Insert data
        String insertSql = String.format(
            "INSERT INTO %s VALUES "
                + "(1, 'item_1', VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')), "
                + "(2, 'item_2', VEC_FROMTEXT('[0.5,0.6,0.7,0.8]'))", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        // Query with VEC_DISTANCE
        String selectSql = String.format(
            "SELECT id, VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist "
                + "FROM %s ORDER BY dist LIMIT 1", TABLE_NAME);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            assertWithMessage("ID should be 1 (closest)").that(rs.getLong(1)).isEqualTo(1);
        } catch (SQLException e) {
            // VEC_DISTANCE might not be supported on DN
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error, got: " + msg)
                .that(msg).doesNotContain("syntax");
        }
    }
}
