package com.alibaba.polardbx.qatest.ddl.auto.omc30.rebuildcleanup;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

@CdcIgnore(ignoreReason = "REBUILD CLEANUP 不产生逐行 DELETE binlog，CDC 下游数据校验会不一致")
public class RebuildCleanupBasicTest extends RebuildCleanupTestBase {

    private static final String DATABASE_NAME = "rebuild_cleanup_basic_test";

    @Override
    protected String databaseName() {
        return DATABASE_NAME;
    }

    @Test
    public void testCleanupPredicateKeepsFalseAndUnknownRows() {
        String cleanupSql = forceOmc30Hint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where status = 'deleted'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, cleanupSql);

        Assert.assertEquals(0,
            queryCount("select count(*) from " + TABLE_NAME + " where status = 'deleted'"));
        Assert.assertEquals(8, queryCount("select count(*) from " + TABLE_NAME));
        Assert.assertEquals("Rows for which the cleanup predicate is UNKNOWN must be kept",
            2, queryCount("select count(*) from " + TABLE_NAME + " where status is null"));
    }

    @Test
    public void testInvalidPredicateIsRejectedWithoutChangingData() {
        String sql = forceOmc30Hint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where missing_status = 'deleted' dry run";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "does not exist");
        Assert.assertEquals(12, queryCount("select count(*) from " + TABLE_NAME));
    }

    @Test
    public void testAlwaysFalseAndAlwaysTruePredicateBoundaries() {
        String keepAllSql = forceOmc30Hint()
            + "alter table " + TABLE_NAME + " rebuild cleanup where 1 = 0";
        JdbcUtil.executeUpdateSuccess(tddlConnection, keepAllSql);

        Assert.assertEquals("An always-false cleanup predicate must retain every row",
            12, queryCount("select count(*) from " + TABLE_NAME));

        String cleanupAllSql = forceOmc30Hint()
            + "alter table " + TABLE_NAME + " rebuild cleanup where 1 = 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, cleanupAllSql);

        Assert.assertEquals("An always-true cleanup predicate must remove every row",
            0, queryCount("select count(*) from " + TABLE_NAME));
    }

    @Test
    public void testEmptyTableAndBigintBoundaryValues() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "truncate table " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + TABLE_NAME + " values "
                + "(-9223372036854775808, 'edge', 'min-bigint'),"
                + "(0, null, 'null-status'),"
                + "(9223372036854775807, 'edge', 'max-bigint')");

        String cleanupSql = forceOmc30Hint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where id in (-9223372036854775808, 9223372036854775807) or status is null";
        JdbcUtil.executeUpdateSuccess(tddlConnection, cleanupSql);

        Assert.assertEquals(0,
            queryCount("select count(*) from " + TABLE_NAME
                + " where id in (-9223372036854775808, 9223372036854775807)"));
        Assert.assertEquals(0, queryCount("select count(*) from " + TABLE_NAME + " where status is null"));
        Assert.assertEquals(0, queryCount("select count(*) from " + TABLE_NAME));

        String emptyCleanupSql = forceOmc30Hint()
            + "alter table " + TABLE_NAME + " rebuild cleanup where 1 = 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, emptyCleanupSql);
        Assert.assertEquals("Cleaning an empty table must succeed", 0,
            queryCount("select count(*) from " + TABLE_NAME));
    }
}
