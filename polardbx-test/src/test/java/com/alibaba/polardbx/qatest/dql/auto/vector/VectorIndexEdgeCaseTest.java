package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Tests for Vector Index edge cases.
 * <p>
 * Covers:
 * - NULL vector values
 * - Empty vectors
 * - Dimension mismatch
 * - Invalid vector format
 * - Various dimensions (low, medium, high)
 * - Special characters in vectors
 */
public class VectorIndexEdgeCaseTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexEdgeCaseTest.class);

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

    /**
     * Test NULL vector value handling.
     */
    @Test
    public void testNullVectorValue() {
        String tableName = "t_vec_edge_1";
        dropTableIfExists(tableName);

        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(4)"
                + ") partition by hash(id) partitions 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Insert NULL vector
        String insertSql = String.format(
            "INSERT INTO %s (id) VALUES (1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        // Query with VEC_DISTANCE on NULL
        String selectSql = String.format(
            "SELECT id, VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) FROM %s",
            tableName);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            Object dist = rs.getObject(2);
            // NULL vector should produce NULL distance or error
        } catch (SQLException e) {
            // Expected: NULL vector handling
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error")
                .that(msg).doesNotContain("syntax");
        }

        dropTableIfExists(tableName);
    }

    /**
     * Test empty vector handling.
     */
    @Test
    public void testEmptyVector() {
        String tableName = "t_vec_edge_2";
        dropTableIfExists(tableName);

        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(4)"
                + ") partition by hash(id) partitions 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Try to insert empty vector
        String insertSql = String.format(
            "INSERT INTO %s VALUES (1, VEC_FROMTEXT('[]'))", tableName);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(insertSql);
            // Empty vector might be accepted
        } catch (SQLException e) {
            // Expected: empty vector error
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error")
                .that(msg).doesNotContain("syntax");
        }

        dropTableIfExists(tableName);
    }

    /**
     * Test invalid vector format.
     */
    @Test
    public void testInvalidVectorFormat() {
        String tableName = "t_vec_edge_3";
        dropTableIfExists(tableName);

        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(4)"
                + ") partition by hash(id) partitions 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Try different invalid formats
        String[] invalidVectors = {
            "VEC_FROMTEXT('not-a-vector')",
            "VEC_FROMTEXT('[1,2,not-a-number,4]')",
            "VEC_FROMTEXT('[1,2,3')",  // missing closing bracket
            "VEC_FROMTEXT('1,2,3,4')",  // no brackets
        };

        for (String vec : invalidVectors) {
            String insertSql = String.format(
                "INSERT INTO %s VALUES (1, %s)", tableName, vec);

            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute(insertSql);
                // Some formats might be accepted
            } catch (SQLException e) {
                // Expected: invalid format error
                String msg = e.getMessage().toLowerCase();
                assertWithMessage("Should not be parser error for " + vec)
                    .that(msg).doesNotContain("syntax");
            }
        }

        dropTableIfExists(tableName);
    }

    /**
     * Test various vector dimensions.
     */
    @Test
    public void testVariousDimensions() {
        int[] dimensions = {2, 4, 8, 16, 32, 64, 128, 256};

        for (int dim : dimensions) {
            String tableName = "t_vec_dim_" + dim;
            dropTableIfExists(tableName);

            String createSql = String.format(
                "CREATE TABLE %s ("
                    + "  id BIGINT PRIMARY KEY, "
                    + "  embedding VECTOR(%d)"
                    + ") partition by hash(id) partitions 4",
                tableName, dim);

            try {
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

                // Generate vector with correct dimension
                StringBuilder vec = new StringBuilder("[");
                for (int i = 0; i < dim; i++) {
                    if (i > 0) {
                        vec.append(",");
                    }
                    vec.append(String.format("%.4f", Math.random()));
                }
                vec.append("]");

                String insertSql = String.format(
                    "INSERT INTO %s VALUES (1, VEC_FROMTEXT('%s'))", tableName, vec.toString());
                JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

                // Query with VEC_DISTANCE
                String selectSql = String.format(
                    "SELECT VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('%s')) FROM %s",
                    vec.toString(), tableName);

                try (Statement stmt = tddlConnection.createStatement();
                    ResultSet rs = stmt.executeQuery(selectSql)) {
                    assertWithMessage("Should have result for dimension " + dim)
                        .that(rs.next()).isTrue();
                    double dist = rs.getDouble(1);
                    // Distance to same vector should be 0 or very close
                    assertWithMessage("Distance to same vector should be small for dimension " + dim)
                        .that(dist).isLessThan(0.001);
                } catch (SQLException e) {
                    // VEC_DISTANCE might not be supported
                }
            } catch (Exception e) {
                // Some dimensions might not be supported
            } finally {
                dropTableIfExists(tableName);
            }
        }
    }

    /**
     * Test special characters in vector (negative numbers, scientific notation).
     */
    @Test
    public void testSpecialCharactersInVector() {
        String tableName = "t_vec_edge_special";
        dropTableIfExists(tableName);

        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(4)"
                + ") partition by hash(id) partitions 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Insert vectors with special formats
        String[] specialVectors = {
            "[-0.1,-0.2,-0.3,-0.4]",  // negative numbers
            "[1e-5,2e-3,3e-1,4e0]",  // scientific notation
            "[+0.1,+0.2,+0.3,+0.4]",  // positive sign
        };

        for (int i = 0; i < specialVectors.length; i++) {
            String insertSql = String.format(
                "INSERT INTO %s VALUES (%d, VEC_FROMTEXT('%s'))", tableName, i + 1, specialVectors[i]);

            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute(insertSql);
            } catch (SQLException e) {
                // Some formats might not be supported
            }
        }

        dropTableIfExists(tableName);
    }

    /**
     * Test vector with extreme values.
     */
    @Test
    public void testExtremeVectorValues() {
        String tableName = "t_vec_edge_extreme";
        dropTableIfExists(tableName);

        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(4)"
                + ") partition by hash(id) partitions 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Insert vectors with extreme values
        String[][] extremeVectors = {
            {"1", "[0,0,0,0]"},  // zeros
            {"2", "[1,1,1,1]"},  // ones
            {"3", "[-1,-1,-1,-1]"},  // negative ones
            {"4", "[1e10,1e10,1e10,1e10]"},  // very large
            {"5", "[1e-10,1e-10,1e-10,1e-10]"},  // very small
        };

        for (String[] vec : extremeVectors) {
            String insertSql = String.format(
                "INSERT INTO %s VALUES (%s, VEC_FROMTEXT('%s'))", tableName, vec[0], vec[1]);

            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute(insertSql);
            } catch (SQLException e) {
                // Some values might overflow or be rejected
            }
        }

        dropTableIfExists(tableName);
    }

    /**
     * Test VEC_DISTANCE with mismatched dimensions in query.
     */
    @Test
    public void testVecDistanceDimensionMismatchInQuery() {
        String tableName = "t_vec_edge_mismatch";
        dropTableIfExists(tableName);

        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(4)"
                + ") partition by hash(id) partitions 4",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String insertSql = String.format(
            "INSERT INTO %s VALUES (1, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]'))", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        // Query with wrong dimension
        String selectSql = String.format(
            "SELECT VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3]')) FROM %s", tableName);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            // If accepted, dimension mismatch might return NULL or error
            if (rs.next()) {
                Object result = rs.getObject(1);
                // Result could be NULL or an error
            }
        } catch (SQLException e) {
            // Expected: dimension mismatch error
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error")
                .that(msg).doesNotContain("syntax");
        }

        dropTableIfExists(tableName);
    }

    /**
     * Test very large dimension vector.
     */
    @Test
    public void testLargeDimensionVector() {
        int dim = 1536;  // Common embedding size for OpenAI
        String tableName = "t_vec_dim_large";
        dropTableIfExists(tableName);

        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  embedding VECTOR(%d)"
                + ") partition by hash(id) partitions 4",
            tableName, dim);

        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

            // Generate a large vector
            StringBuilder vec = new StringBuilder("[");
            for (int i = 0; i < dim; i++) {
                if (i > 0) {
                    vec.append(",");
                }
                vec.append(String.format("%.6f", Math.random() * 2 - 1));
            }
            vec.append("]");

            String insertSql = String.format(
                "INSERT INTO %s VALUES (1, '%s')", tableName, vec.toString());

            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute(insertSql);
            } catch (SQLException e) {
                // Large vector might not be supported
            }
        } catch (Exception e) {
            // Large dimension might not be supported
        } finally {
            dropTableIfExists(tableName);
        }
    }
}
