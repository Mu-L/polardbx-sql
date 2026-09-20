package com.alibaba.polardbx.qatest.ddl.auto.omc30.rebuildcleanup;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

@CdcIgnore(ignoreReason = "REBUILD CLEANUP 不产生逐行 DELETE binlog，CDC 下游数据校验会不一致")
public class RebuildCleanupGsiTest extends RebuildCleanupTestBase {

    private static final String DATABASE_NAME = "rebuild_cleanup_gsi_test";

    @Override
    protected String databaseName() {
        return DATABASE_NAME;
    }

    @Test
    public void testTableWithGsiRequiresForceHintAndRebuildsGsi() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create global index cleanup_orders_gsi on " + TABLE_NAME
                + " (status) covering (remark) partition by hash(status) partitions 3");

        String dryRunSql = forceOmc30Hint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where status = 'deleted' dry run";
        JdbcUtil.executeUpdateFailed(tddlConnection, dryRunSql, "does not support GSI");

        String cleanupSql = forceOmc30Hint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where status = 'deleted'";
        JdbcUtil.executeUpdateFailed(tddlConnection, cleanupSql, "does not support GSI");

        Assert.assertEquals(12, queryCount("select count(*) from " + TABLE_NAME));
        Assert.assertEquals(4,
            queryCount("select count(*) from " + TABLE_NAME + " where status = 'deleted'"));

        String forcedCleanupSql = forceOmc30WithGsiHint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where status = 'deleted'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, forcedCleanupSql);

        Assert.assertEquals(8, queryCount("select count(*) from " + TABLE_NAME));
        Assert.assertEquals(0,
            queryCount("select count(*) from " + TABLE_NAME + " where status = 'deleted'"));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, TABLE_NAME, "cleanup_orders_gsi"));
    }

    @Test
    public void testForcedGsiCleanupRejectsPredicateColumnMissingFromGsi() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create global index cleanup_orders_gsi on " + TABLE_NAME
                + " (status) partition by hash(status) partitions 3");

        String cleanupSql = forceOmc30WithGsiHint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where remark like 'remove-%'";
        JdbcUtil.executeUpdateFailed(tddlConnection, cleanupSql, "Invalid cleanup predicate for GSI");

        Assert.assertEquals(12, queryCount("select count(*) from " + TABLE_NAME));
        Assert.assertEquals(4,
            queryCount("select count(*) from " + TABLE_NAME + " where remark like 'remove-%'"));
    }
}
