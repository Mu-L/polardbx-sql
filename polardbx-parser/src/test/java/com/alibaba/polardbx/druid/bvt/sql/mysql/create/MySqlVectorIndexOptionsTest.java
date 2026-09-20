package com.alibaba.polardbx.druid.bvt.sql.mysql.create;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MySqlVectorIndexOptionsTest {

    @Test
    public void testAllDdlEntrypointsRoundTripVectorOptions() {
        assertRoundTrip(
            "create table t (id bigint primary key, v vector(3), "
                + "vector index vi(v) m=3 ef_construction=5 distance=inner_product)");
        assertRoundTrip(
            "alter table t add vector index vi(v) distance=cosine ef_construction=1000 m=200");
        assertRoundTrip(
            "create vector index vi on t(v) ef_construction=40 distance=euclidean m=16");
    }

    @Test
    public void testMultipleVectorIndexesRoundTrip() {
        List<SQLStatement> statements = SQLUtils.toStatementList(
            "create table t (id bigint primary key, tag varchar(32), v1 vector(3), v2 vector(4), "
                + "key idx_tag(tag), vector index vi1(v1) m=6 ef_construction=40 distance=euclidean, "
                + "vector index vi2(v2) m=8 ef_construction=80 distance=cosine)",
            JdbcConstants.MYSQL);
        Assert.assertEquals(1, statements.size());
        String output = SQLUtils.toMySqlString(statements.get(0)).toUpperCase();
        Assert.assertTrue(output, output.contains("VECTOR INDEX VI1"));
        Assert.assertTrue(output, output.contains("VECTOR INDEX VI2"));
        Assert.assertTrue(output, output.contains("KEY IDX_TAG"));
    }

    @Test
    public void testVectorOptionBoundaries() {
        assertRoundTrip("create vector index vi on t(v) m=3 ef_construction=5 distance=cosine");
        assertRoundTrip("create vector index vi on t(v) m=200 ef_construction=1000 distance=euclidean");

        assertParseFails("create vector index vi on t(v) m=2");
        assertParseFails("create vector index vi on t(v) m=201");
        assertParseFails("create vector index vi on t(v) ef_construction=4");
        assertParseFails("create vector index vi on t(v) ef_construction=1001");
    }

    @Test
    public void testInvalidVectorOptionsAreRejected() {
        assertParseFails("create vector index vi on t(v) distance=manhattan");
        assertParseFails("create vector index vi on t(v) m=16 m=20");
        assertParseFails("create vector index vi on t(v) ef_construction=40 ef_construction=80");
        assertParseFails("create vector index vi on t(v) with (distance=cosine, m=16)");
        assertParseFails("create vector index vi on t(v1, v2) distance=cosine");
        assertParseFails("create vector index vi on t(v) distance=cosine invisible");
        assertParseFails("create global vector index vi on t(v) distance=cosine");
        assertParseFails("alter table t add vector index if not exists vi(v) distance=cosine");
        assertParseFails("drop vector index vi on t");
    }

    private static void assertRoundTrip(String sql) {
        List<SQLStatement> statements = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);
        Assert.assertEquals(1, statements.size());
        String output = SQLUtils.toMySqlString(statements.get(0)).toUpperCase();
        Assert.assertTrue(output, output.contains("VECTOR"));
        Assert.assertTrue(output, output.contains("M="));
        Assert.assertTrue(output, output.contains("EF_CONSTRUCTION="));
        Assert.assertTrue(output, output.contains("DISTANCE="));
    }

    private static void assertParseFails(String sql) {
        try {
            SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);
            Assert.fail("Expected parser failure for: " + sql);
        } catch (ParserException expected) {
            // Expected.
        }
    }
}
