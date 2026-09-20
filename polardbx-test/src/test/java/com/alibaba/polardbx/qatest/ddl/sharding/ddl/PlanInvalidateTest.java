package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

/**
 * @author fangwu
 */
public class PlanInvalidateTest extends DDLBaseNewDBTestCase {
    private String schemaName;

    public PlanInvalidateTest() {
        Random r = new Random();
        schemaName = "statistic_invalidate_test_" + Math.abs(r.nextInt(1000));
    }

    @After
    public void clean() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + schemaName);
        }
    }

    /**
     * test plan in plan cache would be invalidated by drop table
     */
    @Test
    public void testPlanCacheInvalidatedByDropTable() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        Connection c4 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // make select plan into plan cache
            String sql = "select * from " + tableName + " where id=15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("HitCache:true") && explain.contains("Source:PLAN_CACHE"));

            // drop table
            c3 = prepareConnection(schemaName);
            c3.createStatement().execute("drop table if exists " + tableName);

            // create table
            c4 = prepareConnection(schemaName);
            c4.createStatement().execute(createTable);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("HitCache:false") && explain.contains("Source:PLAN_CACHE"));
        } finally {
            if (c1 != null) {
                c1.close();
            }
            if (c2 != null) {
                c2.close();
            }
            if (c3 != null) {
                c3.close();
            }
            if (c4 != null) {
                c4.close();
            }
        }
    }

    /**
     * test plan in plan cache would be invalidated by drop table
     */
    @Test
    public void testPlanCacheInvalidatedByAlterTable() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // make select plan into plan cache
            String sql = "select * from " + tableName + " where id=15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("HitCache:true") && explain.contains("Source:PLAN_CACHE"));

            // alter table
            c3 = prepareConnection(schemaName);
            String alterSql = "alter table " + tableName + " add index(name)";
            c3.createStatement().execute(alterSql);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("HitCache:false") && explain.contains("Source:PLAN_CACHE"));
        } finally {
            if (c1 != null) {
                c1.close();
            }
            if (c2 != null) {
                c2.close();
            }
            if (c3 != null) {
                c3.close();
            }
        }
    }

    /**
     * test plan in plan cache would be invalidated by drop database
     */
    @Test
    public void testPlanCacheInvalidatedByDropDatabase() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        Connection c4 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // make select plan into plan cache
            String sql = "select * from " + tableName + " where id=15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("HitCache:true") && explain.contains("Source:PLAN_CACHE"));

            // drop and recreate database
            c3 = getPolardbxConnection();
            c3.createStatement().execute("drop database if exists " + schemaName);
            c3.createStatement().execute("create database if not exists " + schemaName);

            // create table
            c4 = prepareConnection(schemaName);
            c4.createStatement().execute(createTable);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("HitCache:false") && explain.contains("Source:PLAN_CACHE"));
        } finally {
            if (c1 != null) {
                c1.close();
            }
            if (c2 != null) {
                c2.close();
            }
            if (c3 != null) {
                c3.close();
            }
            if (c4 != null) {
                c4.close();
            }
        }
    }

    /**
     * test plan in baseline would be invalidated by drop database
     */
    @Test
    public void testBaselineInvalidatedByDropDatabase() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        Connection c4 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // make select plan into baseline
            String sql = "baseline add sql /*TDDL:a()*/select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            sql = "select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:SPM_ACCEPT"));

            // drop and recreate database
            c3 = getPolardbxConnection();
            c3.createStatement().execute("drop database if exists " + schemaName);
            c3.createStatement().execute("create database if not exists " + schemaName);

            // create table
            c4 = prepareConnection(schemaName);
            c4.createStatement().execute(createTable);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:PLAN_CACHE"));
        } finally {
            if (c1 != null) {
                c1.close();
            }
            if (c2 != null) {
                c2.close();
            }
            if (c3 != null) {
                c3.close();
            }
            if (c4 != null) {
                c4.close();
            }
        }
    }

    /**
     * test plan in baseline would be invalidated by drop table
     */
    @Test
    public void testBaselineInvalidatedByDropTable() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        Connection c4 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // make select plan into baseline
            String sql = "baseline add sql /*TDDL:a()*/select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            sql = "select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:SPM_ACCEPT"));

            // drop and recreate database
            c3 = prepareConnection(schemaName);
            c3.createStatement().execute("drop table if exists " + tableName);

            // create table
            c4 = prepareConnection(schemaName);
            c4.createStatement().execute(createTable);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:PLAN_CACHE"));
        } finally {
            if (c1 != null) {
                c1.close();
            }
            if (c2 != null) {
                c2.close();
            }
            if (c3 != null) {
                c3.close();
            }
            if (c4 != null) {
                c4.close();
            }
        }
    }

    /**
     * test fixed plan in baseline would be invalidated by drop table
     */
    @Test
    public void testBaselineFixedInvalidatedByDropTable() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        Connection c4 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // make select plan into baseline
            String sql = "baseline fix sql /*TDDL:a()*/select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            sql = "select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:SPM_FIX"));

            // drop and recreate database
            c3 = prepareConnection(schemaName);
            c3.createStatement().execute("drop table if exists " + tableName);

            // create table
            c4 = prepareConnection(schemaName);
            c4.createStatement().execute(createTable);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:PLAN_CACHE"));
        } finally {
            if (c1 != null) {
                c1.close();
            }
            if (c2 != null) {
                c2.close();
            }
            if (c3 != null) {
                c3.close();
            }
            if (c4 != null) {
                c4.close();
            }
        }
    }

    /**
     * test fixed plan in baseline would be rebuilt by alter table
     */
    @Test
    public void testBaselineFixedInvalidatedByAlterTable() throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + Math.abs(r.nextInt());
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;

        try (Connection c = getPolardbxConnection()) {
            // prepare schema
            c1 = getPolardbxConnection();
            c1.createStatement().execute("create database if not exists " + schemaName);

            c.createStatement().execute("use " + schemaName);

            // prepare table
            c2 = prepareConnection(schemaName);
            c2.createStatement().execute("drop table if exists " + tableName);
            c2.createStatement().execute(createTable);

            // make select plan into baseline
            String sql = "baseline fix sql /*TDDL:a()*/select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            sql = "select * from " + tableName + " where id>15";
            c.createStatement().executeQuery(sql);
            String explain = getExplainResult(c, sql);

            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:SPM_FIX"));

            // drop and recreate database
            c3 = prepareConnection(schemaName);
            String alterSql = "alter table " + tableName + " add index(name)";
            c3.createStatement().execute(alterSql);

            // check plan if exists
            explain = getExplainResult(c, sql);
            logger.info(explain);
            Assert.assertTrue(explain.contains("Source:SPM_FIX_DDL_HASHCODE_UPDATE"));
        } finally {
            if (c1 != null) {
                c1.close();
            }
            if (c2 != null) {
                c2.close();
            }
            if (c3 != null) {
                c3.close();
            }
        }
    }

    /**
     * AONE 85060543: SPM FIXED PLAN rebuild must keep the local INDEX hint, and must correctly
     * re-bind shard-key/filter parameters and LIMIT offset/count, under a plain Statement
     * (literal SQL, no bind params).
     */
    @Test
    public void testFixedPlanRebuildKeepsIndexHintAndParamsUnderStatement() throws SQLException {
        FixedPlanRebuildFixture f = buildFixedPlanRebuildFixture("stmt");
        try {
            // use distinct bind values before/after DDL to detect stale/misordered params
            assertStatementRows(f, 15, "left", 0, 2, new int[] {20, 30});

            bumpTableVersion(f);

            // after DDL: different bind values than before DDL
            assertStatementRows(f, 35, "left", 1, 2, new int[] {50, 60});
        } finally {
            f.close();
        }
    }

    /**
     * AONE 85060543: same scenario as
     * {@link #testFixedPlanRebuildKeepsIndexHintAndParamsUnderStatement()} but driven through a
     * client-side (emulated) JDBC PreparedStatement, i.e. no useServerPrepStmts, so the same
     * PreparedStatement object is reused across the DDL without going through COM_STMT_EXECUTE.
     */
    @Test
    public void testFixedPlanRebuildKeepsIndexHintAndParamsUnderClientPrepare() throws SQLException {
        FixedPlanRebuildFixture f = buildFixedPlanRebuildFixture("client");
        try (PreparedStatement ps = f.paramConn.prepareStatement(f.parameterizedSql)) {
            assertPreparedRows(f, ps, 15, "left", 0, 2, new int[] {20, 30});

            bumpTableVersion(f);

            assertPreparedRows(f, ps, 45, "left", 1, 2, new int[] {60, 70});
        } finally {
            f.close();
        }
    }

    /**
     * AONE 85060543: same scenario as
     * {@link #testFixedPlanRebuildKeepsIndexHintAndParamsUnderStatement()} but driven through a
     * real server-side PreparedStatement (useServerPrepStmts=true), reusing the same
     * PreparedStatement across the DDL so a re-execute after the table version bump goes through
     * COM_STMT_EXECUTE -> SPM FIXED PLAN rebuild with a live PreparedStmtCache.
     */
    @Test
    public void testFixedPlanRebuildKeepsIndexHintAndParamsUnderServerPrepare() throws SQLException {
        FixedPlanRebuildFixture f = buildFixedPlanRebuildFixture("server");
        try (PreparedStatement ps = f.paramConn.prepareStatement(f.parameterizedSql)) {
            assertPreparedRows(f, ps, 15, "left", 0, 2, new int[] {20, 30});

            bumpTableVersion(f);

            // re-execute same PreparedStatement, triggering FIXED PLAN rebuild via server prepare path
            assertPreparedRows(f, ps, 55, "left", 1, 2, new int[] {70, 80});
        } finally {
            f.close();
        }
    }

    /**
     * Shared fixture: a sharded table with a named local index, rows id=10,20,...,100 with
     * name="name_<id>", and an SPM FIXED plan built with the local INDEX hint over a SQL that
     * mixes a shard-key/filter bind param (id>?), an equality filter bind param (name=?), a plain
     * SQL constant (1=1) to exercise reparameterization numbering/order, and LIMIT ?,?.
     */
    private FixedPlanRebuildFixture buildFixedPlanRebuildFixture(String suffix) throws SQLException {
        Random r = new Random();
        String tableName = "plan_invalidate_test_" + suffix + "_" + Math.abs(r.nextInt());
        String indexName = "idx_test_name";
        String createTable = "create table if not exists " + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id)";

        Connection ddlConn = getPolardbxConnection();
        ddlConn.createStatement().execute("create database if not exists " + schemaName);
        ddlConn.createStatement().execute("use " + schemaName);
        ddlConn.createStatement().execute("drop table if exists " + tableName);
        ddlConn.createStatement().execute(createTable);
        ddlConn.createStatement().execute("alter table " + tableName + " add index " + indexName + "(name)");
        for (int id = 10; id <= 200; id += 10) {
            String name = id <= 100 ? "left" : "right";
            ddlConn.createStatement()
                .execute("insert into " + tableName + " (id, name) values (" + id + ", '" + name + "')");
        }

        String parameterizedSql =
            "select id, name from " + tableName + " where id>? and 1=1 and name=? order by id limit ?,?";
        String literalTemplate =
            "select id, name from " + tableName + " where id>%d and 1=1 and name='%s' order by id limit %d,%d";

        // fix baseline with local index hint, matched by shape against the parameterized query above
        String fixSql = "baseline fix sql /*+TDDL:INDEX(" + tableName + "," + indexName + ")*/"
            + "select id, name from " + tableName + " where id>15 and 1=1 and name='left' order by id limit 0,2";
        ddlConn.createStatement().executeQuery(fixSql);
        String checkSql = String.format(literalTemplate, 15, "left", 0, 2);
        String explain = getExplainResult(ddlConn, checkSql);
        logger.info(explain);
        Assert.assertTrue("baseline fix should be accepted as SPM_FIX before any DDL rebuild",
            explain.contains("SPM_FIX"));

        Connection paramConn = "server".equals(suffix)
            ? getPolardbxConnectionWithExtraParams("&useServerPrepStmts=true")
            : getPolardbxConnection();
        paramConn.createStatement().execute("use " + schemaName);

        return new FixedPlanRebuildFixture(ddlConn, paramConn, tableName, indexName, parameterizedSql,
            literalTemplate);
    }

    private void bumpTableVersion(FixedPlanRebuildFixture f) throws SQLException {
        // bump table version to trigger SPM_FIX_DDL_HASHCODE_UPDATE on next access
        f.ddlConn.createStatement().execute("alter table " + f.tableName + " add index idx_test_name_"
            + Math.abs(new Random().nextInt()) + "(name)");
    }

    private void assertStatementRows(FixedPlanRebuildFixture f, int idBound, String name, int offset, int fetch,
                                     int[] expectedIds) throws SQLException {
        String sql = String.format(f.literalTemplate, idBound, name, offset, fetch);
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("use " + schemaName);
            ResultSet rs = c.createStatement().executeQuery(sql);
            assertRowsAndOrder(rs, expectedIds, name);
        }
        assertRebuiltPlanKeepsIndexHint(f, sql);
    }

    private void assertPreparedRows(FixedPlanRebuildFixture f, PreparedStatement ps, int idBound, String name,
                                    int offset, int fetch, int[] expectedIds) throws SQLException {
        ps.setInt(1, idBound);
        ps.setString(2, name);
        ps.setInt(3, offset);
        ps.setInt(4, fetch);
        ResultSet rs = ps.executeQuery();
        assertRowsAndOrder(rs, expectedIds, name);

        String checkSql = String.format(f.literalTemplate, idBound, name, offset, fetch);
        assertRebuiltPlanKeepsIndexHint(f, checkSql);
    }

    private void assertRowsAndOrder(ResultSet rs, int[] expectedIds, String expectedName) throws SQLException {
        try {
            int i = 0;
            while (rs.next()) {
                Assert.assertTrue("returned more rows than expected: " + expectedIds.length,
                    i < expectedIds.length);
                Assert.assertEquals("row order/value mismatch at position " + i, expectedIds[i], rs.getInt("id"));
                Assert.assertEquals("routed row should match the bound name filter", expectedName,
                    rs.getString("name"));
                i++;
            }
            Assert.assertEquals("returned row count mismatch", expectedIds.length, i);
        } finally {
            rs.close();
        }
    }

    private void assertRebuiltPlanKeepsIndexHint(FixedPlanRebuildFixture f, String checkSql) throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("use " + schemaName);
            String explain = getExplainResult(c, checkSql).toUpperCase().replace("`", "");
            logger.info(explain);
            Assert.assertFalse("rebuilt plan should not degrade to DirectTableOperation, losing the index hint",
                explain.contains("DIRECTTABLEOPERATION"));
            String forceIndexClause = "FORCE INDEX(" + f.indexName.toUpperCase() + ")";
            Assert.assertTrue(
                "rebuilt physical SQL should keep the local index hint as " + forceIndexClause
                    + ", but got: " + explain,
                explain.contains(forceIndexClause));
        }
    }

    private Connection prepareConnection(String schema) throws SQLException {
        Connection c = getPolardbxConnection();
        c.createStatement().execute("use " + schema);
        return c;
    }

    private static final class FixedPlanRebuildFixture {
        final Connection ddlConn;
        final Connection paramConn;
        final String tableName;
        final String indexName;
        final String parameterizedSql;
        final String literalTemplate;

        FixedPlanRebuildFixture(Connection ddlConn, Connection paramConn, String tableName, String indexName,
                                String parameterizedSql, String literalTemplate) {
            this.ddlConn = ddlConn;
            this.paramConn = paramConn;
            this.tableName = tableName;
            this.indexName = indexName;
            this.parameterizedSql = parameterizedSql;
            this.literalTemplate = literalTemplate;
        }

        void close() throws SQLException {
            if (ddlConn != null) {
                ddlConn.close();
            }
            if (paramConn != null) {
                paramConn.close();
            }
        }
    }
}
