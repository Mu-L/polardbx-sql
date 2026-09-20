package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for Vector Index integration with Global Secondary Index (GSI).
 * Validates that vector indexes can coexist and work correctly with GSI.
 */
public class VectorIndexGsiTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorIndexGsiTest.class);

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

    private static final String TABLE_NAME = "vec_gsi_test";
    private static final String GSI_NAME = "g_i_vec_gsi";
    private static final String VEC_IDX_NAME = "vec_idx_gsi";

    private static final List<String> GSI_LIST = ImmutableList.of(GSI_NAME);

    @Before
    public void initTable() {
        dropTableWithGsi(TABLE_NAME, GSI_LIST);
    }

    /**
     * Test creating a table with both vector index and GSI.
     */
    @Test
    public void testCreateTableWithVectorIndexAndGsi() throws Exception {
        final String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL AUTO_INCREMENT,\n"
                + "  name VARCHAR(100),\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id),\n"
                + "  GLOBAL INDEX {1} (name) PARTITION BY HASH(name) PARTITIONS 3,\n"
                + "  VECTOR INDEX {2} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, GSI_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert test data (start from 1 to avoid AUTO_INCREMENT converting 0 to 1)
        for (int i = 1; i <= 20; i++) {
            String embedding = String.format("[%d.0, %d.5, %d.2, %d.8]", i, i, i, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, name, embedding) VALUES ({1}, ''name_{1}'', VEC_FROMTEXT(''{2}''))",
                TABLE_NAME, i, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Query using GSI
        String gsiQuery = MessageFormat.format(
            "SELECT id, name FROM {0} WHERE name = ''name_5''", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(gsiQuery, tddlConnection)) {
            Assert.assertTrue("GSI query should return result", rs.next());
            Assert.assertEquals(5, rs.getLong("id"));
        }

        // Query using vector index with VEC_DISTANCE
        String vecQuery = MessageFormat.format(
            "SELECT id, name, VEC_DISTANCE(embedding, VEC_FROMTEXT(''[5.0, 5.0, 5.0, 5.0]'')) as dist "
                + "FROM {0} ORDER BY dist LIMIT 3", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(vecQuery, tddlConnection)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals("Vector query should return 3 results", 3, count);
        }

        // Check GSI integrity
        gsiIntegrityCheck(GSI_NAME);
    }

    /**
     * Test adding GSI to a table that already has vector index.
     */
    @Test
    public void testAddGsiToTableWithVectorIndex() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  category VARCHAR(50),\n"
                + "  embedding VECTOR(8),\n"
                + "  PRIMARY KEY (id),\n"
                + "  VECTOR INDEX {1} (embedding) DISTANCE=EUCLIDEAN\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert initial data
        for (int i = 0; i < 10; i++) {
            String embedding = buildVectorString(8, i * 0.1);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, category, embedding) VALUES ({1},  ''cat_{2}'', VEC_FROMTEXT(''{3}''))",
                TABLE_NAME, i, i % 3, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Add GSI
        String addGsi = MessageFormat.format(
            "ALTER TABLE {0} ADD GLOBAL INDEX {1} (category) PARTITION BY HASH(category) PARTITIONS 3",
            TABLE_NAME, GSI_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, addGsi);

        // Insert more data after GSI added
        for (int i = 10; i < 20; i++) {
            String embedding = buildVectorString(8, i * 0.1);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, category, embedding) VALUES ({1},  ''cat_{2}'', VEC_FROMTEXT(''{3}''))",
                TABLE_NAME, i, i % 3, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Verify both indexes work
        String gsiQuery = MessageFormat.format(
            "SELECT COUNT(*) FROM {0} WHERE category = ''cat_1''", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(gsiQuery, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getInt(1) > 0);
        }

        String vecQuery = MessageFormat.format(
            "SELECT id FROM {0} ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT(''[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]'')) LIMIT 5",
            TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(vecQuery, tddlConnection)) {
            Assert.assertTrue("Vector query should return results", rs.next());
        }

        gsiIntegrityCheck(GSI_NAME);
    }

    /**
     * Test creating a table with both GSI and vector index inline.
     * Note: ALTER TABLE ADD VECTOR INDEX is not supported because the vector column
     * cannot be used as a partition key. Use inline VECTOR INDEX in CREATE TABLE instead.
     */
    @Test
    public void testAddVectorIndexToTableWithGsi() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  user_id BIGINT,\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id),\n"
                + "  GLOBAL INDEX {1} (user_id) PARTITION BY HASH(user_id) PARTITIONS 4,\n"
                + "  VECTOR INDEX {2} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, GSI_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert data
        for (int i = 0; i < 25; i++) {
            String embedding = buildVectorString(4, i * 0.2);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, user_id, embedding) VALUES ({1}, {2}, VEC_FROMTEXT(''{3}''))",
                TABLE_NAME, i, i % 5, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Verify GSI works
        String gsiQuery = MessageFormat.format(
            "SELECT id, user_id FROM {0} WHERE user_id = 2", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(gsiQuery, tddlConnection)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertTrue("GSI query should return results", count > 0);
        }

        // Verify vector query works
        String vecQuery = MessageFormat.format(
            "SELECT id, VEC_DISTANCE(embedding, VEC_FROMTEXT(''[1.0, 1.0, 1.0, 1.0]'')) as dist "
                + "FROM {0} ORDER BY dist LIMIT 3", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(vecQuery, tddlConnection)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals("Vector query should return 3 results", 3, count);
        }

        gsiIntegrityCheck(GSI_NAME);
    }

    /**
     * Test DML operations on table with both vector index and GSI.
     */
    @Test
    public void testDmlWithBothIndexes() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  name VARCHAR(50),\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id),\n"
                + "  GLOBAL INDEX {1} (name) PARTITION BY HASH(name) PARTITIONS 3,\n"
                + "  VECTOR INDEX {2} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, GSI_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert
        for (int i = 0; i < 10; i++) {
            String embedding = buildVectorString(4, i * 0.1);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, name, embedding) VALUES ({1}, ''name_{1}'', VEC_FROMTEXT(''{2}''))",
                TABLE_NAME, i, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Update affecting GSI column
        String updateGsi = MessageFormat.format(
            "UPDATE {0} SET name = ''updated_5'' WHERE id = 5", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateGsi);

        // Update affecting vector column
        String updateVec = MessageFormat.format(
            "UPDATE {0} SET embedding = VEC_FROMTEXT(''[1.0, 2.0, 3.0, 4.0]'') WHERE id = 5", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateVec);

        // Delete
        String delete = MessageFormat.format("DELETE FROM {0} WHERE id = 9", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, delete);

        // Verify data consistency
        String countQuery = MessageFormat.format("SELECT COUNT(*) FROM {0}", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(countQuery, tddlConnection)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(9, rs.getInt(1));
        }

        gsiIntegrityCheck(GSI_NAME);
    }

    /**
     * Test clustered GSI with vector index.
     */
    @Test
    public void testClusteredGsiWithVectorIndex() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  status VARCHAR(20),\n"
                + "  embedding VECTOR(4),\n"
                + "  data VARCHAR(100),\n"
                + "  PRIMARY KEY (id),\n"
                + "  CLUSTERED INDEX {1} (status) PARTITION BY HASH(status) PARTITIONS 3,\n"
                + "  VECTOR INDEX {2} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, GSI_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert test data
        String[] statuses = {"active", "pending", "inactive"};
        for (int i = 0; i < 30; i++) {
            String embedding = buildVectorString(4, i * 0.05);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, status, embedding, data) VALUES ({1}, ''{2}'', VEC_FROMTEXT(''{3}''), ''data_{1}'')",
                TABLE_NAME, i, statuses[i % 3], embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Query using clustered GSI
        String cgsiQuery = MessageFormat.format(
            "SELECT id, status, data FROM {0} WHERE status = ''active''", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(cgsiQuery, tddlConnection)) {
            int count = 0;
            while (rs.next()) {
                count++;
                Assert.assertEquals("active", rs.getString("status"));
            }
            Assert.assertTrue("Should have active records", count > 0);
        }

        gsiIntegrityCheck(GSI_NAME);
    }

    /**
     * Test unique GSI with vector index.
     */
    @Test
    public void testUniqueGsiWithVectorIndex() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  email VARCHAR(100),\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id),\n"
                + "  UNIQUE GLOBAL INDEX {1} (email) PARTITION BY HASH(email) PARTITIONS 3,\n"
                + "  VECTOR INDEX {2} (embedding) DISTANCE=EUCLIDEAN\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, GSI_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert with unique emails
        for (int i = 0; i < 10; i++) {
            String embedding = buildVectorString(4, i * 0.15);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, email, embedding) VALUES ({1}, ''user{1}@test.com'', VEC_FROMTEXT(''{2}''))",
                TABLE_NAME, i, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Try duplicate email - should fail
        String dupInsert = MessageFormat.format(
            "INSERT INTO {0} (id, email, embedding) VALUES (100, ''user5@test.com'', VEC_FROMTEXT(''[1.0, 2.0, 3.0, 4.0]''))",
            TABLE_NAME);
        JdbcUtil.executeFailed(tddlConnection, dupInsert, "Duplicate");

        gsiIntegrityCheck(GSI_NAME);
    }

    /**
     * Test invisible GSI with vector index.
     */
    @Test
    public void testInvisibleGsiWithVectorIndex() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  category VARCHAR(50),\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id),\n"
                + "  GLOBAL INDEX {1} (category) PARTITION BY HASH(category) PARTITIONS 3 INVISIBLE,\n"
                + "  VECTOR INDEX {2} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, GSI_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert data
        for (int i = 0; i < 10; i++) {
            String embedding = buildVectorString(4, i * 0.1);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, category, embedding) VALUES ({1},  ''cat_{2}'', VEC_FROMTEXT(''{3}''))",
                TABLE_NAME, i, i % 3, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Make GSI visible
        String alterVisible = MessageFormat.format(
            "ALTER TABLE {0} ALTER INDEX {1} VISIBLE", TABLE_NAME, GSI_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterVisible);

        // Now GSI should be usable
        String gsiQuery = MessageFormat.format(
            "SELECT id, category FROM {0} WHERE category = ''cat_1''", TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(gsiQuery, tddlConnection)) {
            Assert.assertTrue(rs.next());
        }

        gsiIntegrityCheck(GSI_NAME);
    }

    /**
     * Test DROP INDEX for GSI while keeping vector index.
     */
    @Test
    public void testDropGsiKeepVectorIndex() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  name VARCHAR(50),\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id),\n"
                + "  GLOBAL INDEX {1} (name) PARTITION BY HASH(name) PARTITIONS 3,\n"
                + "  VECTOR INDEX {2} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, GSI_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert data
        for (int i = 0; i < 5; i++) {
            String embedding = buildVectorString(4, i);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, name, embedding) VALUES ({1}, ''name_{1}'', VEC_FROMTEXT(''{2}''))",
                TABLE_NAME, i, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Drop GSI
        String dropGsi = MessageFormat.format(
            "ALTER TABLE {0} DROP INDEX {1}", TABLE_NAME, GSI_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropGsi);

        // Vector index should still work
        String vecQuery = MessageFormat.format(
            "SELECT id FROM {0} ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT(''[0.0, 0.0, 0.0, 0.0]'')) LIMIT 3",
            TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(vecQuery, tddlConnection)) {
            Assert.assertTrue("Vector query should still work", rs.next());
        }
    }

    /**
     * Test query with both GSI lookup and vector distance calculation.
     */
    @Test
    public void testCombinedGsiAndVectorQuery() throws Exception {
        String createTable = MessageFormat.format(
            "CREATE PARTITION TABLE {0} (\n"
                + "  id BIGINT NOT NULL,\n"
                + "  tag VARCHAR(50),\n"
                + "  embedding VECTOR(4),\n"
                + "  PRIMARY KEY (id),\n"
                + "  GLOBAL INDEX {1} (tag) PARTITION BY HASH(tag) PARTITIONS 3,\n"
                + "  VECTOR INDEX {2} (embedding) DISTANCE=COSINE\n"
                + ") PARTITION BY HASH(id) PARTITIONS 4",
            TABLE_NAME, GSI_NAME, VEC_IDX_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Insert data
        for (int i = 0; i < 30; i++) {
            String embedding = buildVectorString(4, i * 0.03);
            String sql = MessageFormat.format(
                "INSERT INTO {0} (id, tag, embedding) VALUES ({1}, ''tag_{2}'', VEC_FROMTEXT(''{3}''))",
                TABLE_NAME, i, i % 5, embedding);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        // Query combining GSI filter and vector similarity
        String combinedQuery = MessageFormat.format(
            "SELECT id, tag, VEC_DISTANCE(embedding, VEC_FROMTEXT(''[0.1, 0.2, 0.3, 0.4]'')) as dist "
                + "FROM {0} WHERE tag = ''tag_1'' ORDER BY dist LIMIT 5",
            TABLE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(combinedQuery, tddlConnection)) {
            int count = 0;
            while (rs.next()) {
                count++;
                Assert.assertEquals("tag_1", rs.getString("tag"));
            }
            Assert.assertTrue("Should have results", count > 0);
        }

        gsiIntegrityCheck(GSI_NAME);
    }

    private String buildVectorString(int dim, double baseValue) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < dim; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(String.format("%.2f", baseValue + i * 0.01));
        }
        sb.append("]");
        return sb.toString();
    }

    private void gsiIntegrityCheck(String index) throws Exception {
        final String createTable = JdbcUtil.showCreateTable(tddlConnection, index);
        final String tableName = createTable.substring("CREATE TABLE `".length(), createTable.indexOf("` ("));

        final String CHECK_HINT =
            "/*+TDDL: cmd_extra(GSI_CHECK_PARALLELISM=4, GSI_CHECK_BATCH_SIZE=1024, GSI_CHECK_SPEED_LIMITATION=-1)*/";
        final ResultSet rs = JdbcUtil
            .executeQuery(CHECK_HINT + "check global index " + tableName, tddlConnection);
        List<String> result = JdbcUtil.getStringResult(rs, false)
            .stream()
            .map(row -> row.get(row.size() - 1))
            .collect(Collectors.toList());
        System.out.println("Checker: " + result.get(result.size() - 1));
        Assert.assertTrue(result.get(result.size() - 1).contains("OK"));
    }
}
