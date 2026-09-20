package com.alibaba.polardbx.qatest.ddl.auto.omc30.rebuildcleanup;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@CdcIgnore(ignoreReason = "REBUILD CLEANUP 不产生逐行 DELETE binlog，CDC 下游数据校验会不一致")
public class RebuildCleanupIncrementalTest extends RebuildCleanupTestBase {

    private static final String DATABASE_NAME = "rebuild_cleanup_incremental_test";
    private static final long CUTOVER_WAIT_TIMEOUT_MILLIS = 60_000L;
    private static final long DDL_WAIT_TIMEOUT_SECONDS = 180L;

    @Override
    protected String databaseName() {
        return DATABASE_NAME;
    }

    @Test
    public void testIncrementalInsertAppliesCleanupPredicate() throws Exception {
        executeDuringCutover(() -> JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + TABLE_NAME + " values "
                + "(101, 'active', 'incremental-keep'),"
                + "(102, 'deleted', 'incremental-cleanup')"));

        assertRemainingIds(1L, 3L, 4L, 6L, 8L, 9L, 10L, 12L, 101L);
        Assert.assertEquals(0, queryCount("select count(*) from " + TABLE_NAME + " where id = 102"));
    }

    @Test
    public void testIncrementalUpdateAcrossCleanupPredicate() throws Exception {
        executeDuringCutover(() -> {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "update " + TABLE_NAME + " set status = 'deleted' where id = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "update " + TABLE_NAME + " set status = 'active' where id = 2");
        });

        assertRemainingIds(2L, 3L, 4L, 6L, 8L, 9L, 10L, 12L);
    }

    @Test
    public void testIncrementalDeleteOfRetainedRow() throws Exception {
        executeDuringCutover(() -> JdbcUtil.executeUpdateSuccess(tddlConnection,
            "delete from " + TABLE_NAME + " where id = 6"));

        assertRemainingIds(1L, 3L, 4L, 8L, 9L, 10L, 12L);
    }

    private void executeDuringCutover(Runnable incrementalDml) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Void> ddlFuture = executor.submit(() -> {
            try (Connection connection = getPolardbxConnection()) {
                JdbcUtil.executeUpdateSuccess(connection, "use " + DATABASE_NAME);
                JdbcUtil.executeUpdateSuccess(connection,
                    RebuildCleanupTestSupport.forceOmc30WithCutoverSuspendHint()
                        + "alter table " + TABLE_NAME
                        + " rebuild cleanup where status = 'deleted'");
            }
            return null;
        });

        try {
            waitForCutover(ddlFuture);
            incrementalDml.run();
            ddlFuture.get(DDL_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            if (!ddlFuture.isDone()) {
                ddlFuture.cancel(true);
            }
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private void waitForCutover(Future<?> ddlFuture) throws Exception {
        long deadline = System.currentTimeMillis() + CUTOVER_WAIT_TIMEOUT_MILLIS;
        String sql = "select count(*) from information_schema.omc_progress"
            + " where table_schema = '" + DATABASE_NAME + "'"
            + " and table_name = '" + TABLE_NAME + "'"
            + " and omc_state = 'CUTOVER'";
        while (System.currentTimeMillis() < deadline) {
            if (queryCount(sql) > 0) {
                return;
            }
            if (ddlFuture.isDone()) {
                ddlFuture.get();
                Assert.fail("REBUILD CLEANUP completed before the CUTOVER suspension was observed");
            }
            Thread.sleep(100L);
        }
        Assert.fail("Timed out waiting for REBUILD CLEANUP to enter CUTOVER");
    }

    private void assertRemainingIds(Long... expectedIds) {
        List<Long> actualIds = new ArrayList<>();
        try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection,
            "select id from " + TABLE_NAME + " order by id")) {
            while (resultSet.next()) {
                actualIds.add(resultSet.getLong(1));
            }
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
        Assert.assertEquals(Arrays.asList(expectedIds), actualIds);
    }
}
