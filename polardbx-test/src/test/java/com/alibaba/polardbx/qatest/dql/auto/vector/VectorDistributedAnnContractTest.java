package com.alibaba.polardbx.qatest.dql.auto.vector;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Contract tests for distributed ANN planning and execution.
 */
public class VectorDistributedAnnContractTest extends VectorIndexTestBase {

    private static final String DATABASE_NAME = databaseNameFor(VectorDistributedAnnContractTest.class);

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

    private static final String TABLE_NAME = "t_vec_ann_contract";
    private static final String FILTER_TABLE_NAME = "t_vec_ann_filter";
    private static final String VECTOR_INDEX_NAME = "vi_ann_contract";

    @Before
    public void prepareTables() {
        dropTableIfExists(FILTER_TABLE_NAME);
        dropTableIfExists(TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " ("
                + "id BIGINT NOT NULL PRIMARY KEY, "
                + "category INT NOT NULL, "
                + "name VARCHAR(64), "
                + "embedding VECTOR(2), "
                + "INDEX idx_category(category), "
                + "VECTOR INDEX " + VECTOR_INDEX_NAME
                + "(embedding) M=6 EF_CONSTRUCTION=40 DISTANCE=EUCLIDEAN"
                + ") PARTITION BY HASH(id) PARTITIONS 4");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + FILTER_TABLE_NAME + " ("
                + "id BIGINT NOT NULL PRIMARY KEY, "
                + "filter_key INT NOT NULL, "
                + "keep_flag INT NOT NULL"
                + ") PARTITION BY HASH(filter_key) PARTITIONS 3");

        StringBuilder vectors = new StringBuilder("INSERT INTO ").append(TABLE_NAME)
            .append(" (id, category, name, embedding) VALUES ");
        StringBuilder filters = new StringBuilder("INSERT INTO ").append(FILTER_TABLE_NAME)
            .append(" (id, filter_key, keep_flag) VALUES ");
        for (int id = 1; id <= 64; id++) {
            if (id > 1) {
                vectors.append(',');
                filters.append(',');
            }
            int distance = (id + 1) / 2;
            vectors.append('(').append(id).append(',').append(id % 4).append(", 'row_")
                .append(id).append("', VEC_FROMTEXT('[").append(distance).append(",0]'))");
            filters.append('(').append(id).append(',').append(id % 3).append(',')
                .append(id >= 50 ? 1 : 0).append(')');
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, vectors.toString());
        JdbcUtil.executeUpdateSuccess(tddlConnection, filters.toString());
    }

    @Test
    public void testLiteralAndPreparedOffsetUseExactGlobalOrder() throws Exception {
        String orderedQuery = "SELECT id FROM " + TABLE_NAME
            + " ORDER BY VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0,0]')), id LIMIT 7, 6";
        Assert.assertEquals(Arrays.asList(8L, 9L, 10L, 11L, 12L, 13L), queryIds(orderedQuery));

        String preparedQuery = "SELECT id FROM " + TABLE_NAME
            + " ORDER BY VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT(?)), id LIMIT ?, ?";
        try (PreparedStatement ps = tddlConnection.prepareStatement(preparedQuery)) {
            ps.setString(1, "[0,0]");
            ps.setInt(2, 7);
            ps.setInt(3, 6);
            Assert.assertEquals(Arrays.asList(8L, 9L, 10L, 11L, 12L, 13L), queryIds(ps));
        }
    }

    @Test
    public void testPushableFilterAndTieBreakerAreGloballyExact() throws Exception {
        String query = "SELECT id FROM " + TABLE_NAME
            + " WHERE category = 1"
            + " ORDER BY VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0,0]')), id LIMIT 2, 5";
        Assert.assertEquals(Arrays.asList(9L, 13L, 17L, 21L, 25L), queryIds(query));
    }

    @Test
    public void testHintsAndLocalOffsetContractReachPhysicalSql() throws Exception {
        assertPhysicalHintAndLimit("FORCE INDEX(" + VECTOR_INDEX_NAME + ")", "VEC_DISTANCE");
        assertPhysicalHintAndLimit("USE INDEX(" + VECTOR_INDEX_NAME + ")", "VEC_DISTANCE");
        assertPhysicalHintAndLimit("IGNORE INDEX(" + VECTOR_INDEX_NAME + ")", "VEC_DISTANCE_EUCLIDEAN");
    }

    @Test
    public void testPartitionPruningOccursBeforeAnnShardExecution() throws Exception {
        String query = "SELECT id FROM " + TABLE_NAME + " FORCE INDEX(" + VECTOR_INDEX_NAME + ")"
            + " WHERE id = 17"
            + " ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT('[0,0]')), id LIMIT 1";
        Assert.assertEquals(Arrays.asList(17L), queryIds(query));

        executeTrace(query);
        List<Pair<String, String>> physicalTrace = physicalTraceFor(TABLE_NAME);
        Assert.assertEquals("Partition pruning should execute exactly one physical table query: " + physicalTrace,
            1, physicalTrace.size());
    }

    @Test
    public void testCnJoinFilterCannotTruncateAnnCandidatesEarly() throws Exception {
        String query = "SELECT a.id FROM " + TABLE_NAME + " a"
            + " JOIN " + FILTER_TABLE_NAME + " f ON a.id = f.id"
            + " WHERE f.keep_flag = 1"
            + " ORDER BY VEC_DISTANCE_EUCLIDEAN(a.embedding, VEC_FROMTEXT('[0,0]')), a.id LIMIT 5";
        Assert.assertEquals(Arrays.asList(50L, 51L, 52L, 53L, 54L), queryIds(query));
    }

    private void assertPhysicalHintAndLimit(String hint, String distanceFunction) throws Exception {
        String query = "SELECT id FROM " + TABLE_NAME + " " + hint
            + " WHERE category >= 0"
            + " ORDER BY " + distanceFunction + "(embedding, VEC_FROMTEXT('[0,0]')), id LIMIT 3, 5";
        Assert.assertEquals(Arrays.asList(4L, 5L, 6L, 7L, 8L), queryIds(query));

        executeTrace(query);
        List<Pair<String, String>> physicalTrace = physicalTraceFor(TABLE_NAME);
        Assert.assertTrue("Expected physical statements for " + hint, !physicalTrace.isEmpty());
        for (Pair<String, String> trace : physicalTrace) {
            String sql = trace.getKey();
            String params = trace.getValue();
            String normalized = sql.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
            Assert.assertTrue("Physical SQL lost index hint " + hint + ": " + sql,
                normalized.contains(hint.toUpperCase(Locale.ROOT)));
            Assert.assertTrue("Physical SQL lost distance expression: " + sql,
                normalized.contains(distanceFunction));
            Assert.assertTrue("Physical SQL lost pushable filter: " + sql,
                normalized.contains("CATEGORY"));
            Assert.assertFalse("Local OFFSET must be zero, not the global offset: " + sql,
                normalized.matches(".*LIMIT 3\\s*,.*"));
            boolean literalFetch = normalized.matches(".*LIMIT (?:0\\s*,\\s*)?8(?:\\D.*|$)");
            boolean parameterizedFetch = normalized.matches(".*LIMIT \\?(?:\\D.*|$)")
                && params != null && params.matches(".*(?:\\[|,\\s*)8\\s*\\]$");
            Assert.assertTrue("Local fetch must be OFFSET + FETCH = 8: " + sql + ", params: " + params,
                literalFetch || parameterizedFetch);
        }
    }

    private void executeTrace(String query) throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("TRACE " + query);
        }
    }

    private List<Pair<String, String>> physicalTraceFor(String logicalTable) throws Exception {
        List<Pair<String, String>> trace = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW TRACE")) {
            while (rs.next()) {
                String statement = rs.getString("STATEMENT");
                if (statement != null && statement.toUpperCase(Locale.ROOT)
                    .contains(logicalTable.toUpperCase(Locale.ROOT))) {
                    trace.add(Pair.of(statement, rs.getString("PARAMS")));
                }
            }
        }
        return trace;
    }

    private List<Long> queryIds(String query) throws Exception {
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(query)) {
            return collectIds(rs);
        }
    }

    private List<Long> queryIds(PreparedStatement ps) throws Exception {
        try (ResultSet rs = ps.executeQuery()) {
            return collectIds(rs);
        }
    }

    private List<Long> collectIds(ResultSet rs) throws Exception {
        List<Long> ids = new ArrayList<>();
        while (rs.next()) {
            ids.add(rs.getLong(1));
        }
        return ids;
    }
}
