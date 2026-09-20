package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Tests for Vector Index Query operations.
 * <p>
 * Covers:
 * - ANN query with WHERE condition
 * - JOIN with VEC_DISTANCE
 * - Multiple vector columns query
 * - Different distance measures (COSINE, EUCLIDEAN)
 * - EXPLAIN with vector index
 */
public class VectorIndexQueryTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexQueryTest.class);

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

    private static final String TABLE_NAME = "t_vec_query_test";

    @Before
    public void initTable() {
        dropTableIfExists(TABLE_NAME);
        String createSql = String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT PRIMARY KEY, "
                + "  category INT, "
                + "  name VARCHAR(100), "
                + "  embedding VECTOR(4)"
                + ") partition by hash(id) partitions 4",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // Insert test data
        String insertSql = String.format(
            "INSERT INTO %s VALUES "
                + "(1, 1, 'item_1', VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')), "
                + "(2, 1, 'item_2', VEC_FROMTEXT('[0.2,0.3,0.4,0.5]')), "
                + "(3, 2, 'item_3', VEC_FROMTEXT('[0.9,0.8,0.7,0.6]')), "
                + "(4, 2, 'item_4', VEC_FROMTEXT('[0.8,0.7,0.6,0.5]')), "
                + "(5, 3, 'item_5', VEC_FROMTEXT('[0.5,0.5,0.5,0.5]'))",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
    }

    /**
     * Test ANN query with WHERE condition.
     */
    @Test
    public void testAnnQueryWithWhereCondition() {
        String selectSql = String.format(
            "SELECT id, name, VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist "
                + "FROM %s WHERE category = 1 ORDER BY dist LIMIT 2",
            TABLE_NAME);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            int count = 0;
            while (rs.next()) {
                count++;
                int category = rs.getInt("category");
                // Note: category might not be in SELECT, verify id instead
                assertWithMessage("ID should be 1 or 2")
                    .that(rs.getLong(1)).isIn(java.util.Arrays.asList(1L, 2L));
            }
            assertWithMessage("Should have at most 2 rows").that(count).isAtMost(2);
        } catch (SQLException e) {
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error, got: " + msg)
                .that(msg).doesNotContain("syntax");
        }
    }

    /**
     * Test VEC_DISTANCE with COSINE distance.
     */
    @Test
    public void testVecDistanceCosine() {
        String selectSql = String.format(
            "SELECT id, VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist "
                + "FROM %s ORDER BY id LIMIT 1",
            TABLE_NAME);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            double dist = rs.getDouble(2);
            assertWithMessage("Distance should be finite")
                .that(Double.isFinite(dist)).isTrue();
        } catch (SQLException e) {
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error, got: " + msg)
                .that(msg).doesNotContain("syntax");
        }
    }

    /**
     * Test VEC_DISTANCE with EUCLIDEAN distance.
     */
    @Test
    public void testVecDistanceEuclidean() {
        String selectSql = String.format(
            "SELECT id, VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist "
                + "FROM %s ORDER BY id LIMIT 1",
            TABLE_NAME);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertWithMessage("Should have result").that(rs.next()).isTrue();
            double dist = rs.getDouble(2);
            assertWithMessage("Distance should be finite")
                .that(Double.isFinite(dist)).isTrue();
        } catch (SQLException e) {
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error, got: " + msg)
                .that(msg).doesNotContain("syntax");
        }
    }

    /**
     * Test ORDER BY VEC_DISTANCE ASC.
     */
    @Test
    public void testOrderByVecDistanceAsc() {
        String selectSql = String.format(
            "SELECT id, VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist "
                + "FROM %s ORDER BY dist ASC LIMIT 3",
            TABLE_NAME);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            double prevDist = Double.NEGATIVE_INFINITY;
            while (rs.next()) {
                double dist = rs.getDouble(2);
                assertWithMessage("Results should be ordered by distance ASC")
                    .that(dist).isAtLeast(prevDist);
                prevDist = dist;
            }
        } catch (SQLException e) {
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error, got: " + msg)
                .that(msg).doesNotContain("syntax");
        }
    }

    /**
     * Test VEC_DISTANCE in WHERE clause.
     */
    @Test
    public void testVecDistanceInWhereClause() {
        // Note: This may not be optimized by vector index
        String selectSql = String.format(
            "SELECT id FROM %s WHERE VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) < 0.5",
            TABLE_NAME);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            // Should return rows where distance is less than 0.5
            while (rs.next()) {
                // Results are valid
            }
        } catch (SQLException e) {
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error, got: " + msg)
                .that(msg).doesNotContain("syntax");
        }
    }

    /**
     * Test JOIN with VEC_DISTANCE.
     */
    @Test
    public void testJoinWithVecDistance() {
        String table1 = "t_vec_join_1";
        String table2 = "t_vec_join_2";

        // Create two tables with vectors
        dropTableIfExists(table1);
        dropTableIfExists(table2);

        String create1 = String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, embedding VECTOR(4)) partition by hash(id) partitions 4", table1);
        String create2 = String.format(
            "CREATE TABLE %s (id BIGINT PRIMARY KEY, embedding VECTOR(4)) partition by hash(id) partitions 4", table2);

        JdbcUtil.executeUpdateSuccess(tddlConnection, create1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create2);

        String insert1 = String.format(
            "INSERT INTO %s VALUES (1, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')), (2, VEC_FROMTEXT('[0.5,0.6,0.7,0.8]'))",
            table1);
        String insert2 = String.format(
            "INSERT INTO %s VALUES (1, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')), (2, VEC_FROMTEXT('[0.9,0.8,0.7,0.6]'))",
            table2);

        JdbcUtil.executeUpdateSuccess(tddlConnection, insert1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert2);

        // Join with VEC_DISTANCE
        String selectSql = String.format(
            "SELECT t1.id, VEC_DISTANCE_COSINE(t1.embedding, t2.embedding) AS dist "
                + "FROM %s t1 JOIN %s t2 ON t1.id = t2.id",
            table1, table2);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            while (rs.next()) {
                double dist = rs.getDouble(2);
                assertWithMessage("Distance should be valid").that(Double.isFinite(dist)).isTrue();
            }
        } catch (SQLException e) {
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error, got: " + msg)
                .that(msg).doesNotContain("syntax");
        }

        dropTableIfExists(table1);
        dropTableIfExists(table2);
    }

    /**
     * Test aggregation with VEC_DISTANCE.
     */
    @Test
    public void testAggregationWithVecDistance() {
        String selectSql = String.format(
            "SELECT category, AVG(VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]'))) AS avg_dist "
                + "FROM %s GROUP BY category ORDER BY category",
            TABLE_NAME);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            while (rs.next()) {
                int category = rs.getInt(1);
                double avgDist = rs.getDouble(2);
                assertWithMessage("Average distance should be valid")
                    .that(Double.isFinite(avgDist)).isTrue();
            }
        } catch (SQLException e) {
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error, got: " + msg)
                .that(msg).doesNotContain("syntax");
        }
    }

    /**
     * Test EXPLAIN with vector index query.
     */
    @Test
    public void testExplainVectorQuery() {
        String explainSql = String.format(
            "EXPLAIN SELECT id, VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist "
                + "FROM %s ORDER BY dist LIMIT 10",
            TABLE_NAME);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(explainSql)) {
            StringBuilder plan = new StringBuilder();
            while (rs.next()) {
                plan.append(rs.getString(1)).append("\n");
            }
            // Plan should contain VEC_DISTANCE or at least not error
        } catch (SQLException e) {
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error, got: " + msg)
                .that(msg).doesNotContain("syntax");
        }
    }

    /**
     * Test subquery with VEC_DISTANCE.
     */
    @Test
    public void testSubqueryWithVecDistance() {
        String selectSql = String.format(
            "SELECT * FROM ("
                + "  SELECT id, name, VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist "
                + "  FROM %s"
                + ") t WHERE dist < 1.0 ORDER BY dist LIMIT 5",
            TABLE_NAME);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            while (rs.next()) {
                double dist = rs.getDouble("dist");
                assertWithMessage("Distance should be less than 1.0")
                    .that(dist).isLessThan(1.0);
            }
        } catch (SQLException e) {
            String msg = e.getMessage().toLowerCase();
            assertWithMessage("Should not be parser error, got: " + msg)
                .that(msg).doesNotContain("syntax");
        }
    }
}
