package com.alibaba.polardbx.qatest.ddl.auto.omc30.rebuildcleanup;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@CdcIgnore(ignoreReason = "REBUILD CLEANUP 不产生逐行 DELETE binlog，CDC 下游数据校验会不一致")
public class RebuildCleanupPredicateTest extends DDLBaseNewDBTestCase {

    private static final String DATABASE_NAME = "rebuild_cleanup_predicate_test";
    private static final String TABLE_NAME = "cleanup_events";

    @Before
    public void setUpRebuildCleanup() {
        RebuildCleanupTestSupport.dropDatabase(tddlConnection, DATABASE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + DATABASE_NAME + " mode = 'auto'");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + DATABASE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table " + TABLE_NAME + " ("
                + "id bigint primary key, "
                + "tenant_id int not null, "
                + "status varchar(32), "
                + "expire_date date, "
                + "expire_at datetime(6), "
                + "legal_hold tinyint, "
                + "priority int, "
                + "amount decimal(20, 4), "
                + "remark varchar(64)"
                + ") partition by hash(id) partitions 4");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + TABLE_NAME + " values "
                + "(1, 100, 'deleted', '2025-12-30', '2025-12-31 23:59:59.999999', 0, 1, -0.0001, "
                + "'temp-old'),"
                + "(2, 150, 'archived', '2025-12-31', '2026-01-01 00:00:00.000000', null, 5, 0, "
                + "'temp-archive'),"
                + "(3, 200, 'active', '2026-01-01', '2026-01-01 00:00:00.000001', 0, 0, 0.0001, "
                + "'temp-active'),"
                + "(4, 99, 'deleted', null, null, 0, 9, 9999999999999999.9999, 'temp-no-expiry'),"
                + "(5, 120, 'deleted', '2024-01-01', '2024-01-01 00:00:00.000000', 1, 2, 10, "
                + "'permanent'),"
                + "(6, 180, 'deleted', '2024-06-01', '2024-06-01 12:00:00.000000', null, 3, 20, "
                + "'temp-null-hold'),"
                + "(7, 200, 'deleted', '2025-12-31', '2026-01-01 00:00:00.000000', 0, 4, 30, "
                + "'temp-boundary'),"
                + "(8, 201, 'deleted', '2023-01-01', '2023-01-01 00:00:00.000000', 0, 5, 40, "
                + "'temp-outside-tenant'),"
                + "(9, 130, null, '2022-01-01', '2022-01-01 00:00:00.000000', 0, 6, 50, "
                + "'temp-null-status'),"
                + "(10, 160, 'archived', '2021-01-01', '2021-01-01 00:00:00.000000', 0, 7, 60, "
                + "'permanent'),"
                + "(11, 170, 'archived', '2020-01-01', '2020-01-01 00:00:00.000000', 0, 8, 70, "
                + "'temp-special'),"
                + "(12, 190, 'deleted', '9999-12-31', '9999-12-31 23:59:59.999999', 0, 9, 80, "
                + "'O''Reilly')");
    }

    @After
    public void tearDownRebuildCleanup() {
        RebuildCleanupTestSupport.dropDatabase(tddlConnection, DATABASE_NAME);
    }

    @Test
    public void testDatetimeExpirationHonorsMicrosecondBoundaryAndNull() {
        executeCleanup("expire_at < '2026-01-01 00:00:00.000000'");

        assertRemainingIds(2L, 3L, 4L, 7L, 12L);
        Assert.assertEquals("Rows exactly at the cutoff must be retained for a strict comparison",
            2, queryCount("expire_at = '2026-01-01 00:00:00.000000'"));
        Assert.assertEquals("A NULL expiration time makes the predicate UNKNOWN and must be retained",
            1, queryCount("expire_at is null"));
    }

    @Test
    public void testDateExpirationHonorsInclusiveBoundaryAndNull() {
        executeCleanup("expire_date <= '2025-12-31'");

        assertRemainingIds(3L, 4L, 12L);
        Assert.assertEquals("The inclusive date boundary must be cleaned",
            0, queryCount("expire_date = '2025-12-31'"));
        Assert.assertEquals("A NULL expiration date must be retained",
            1, queryCount("expire_date is null"));
    }

    @Test
    public void testCompositeBusinessCondition() {
        executeCleanup(
            "status in ('deleted', 'archived') "
                + "and tenant_id between 100 and 200 "
                + "and (legal_hold is null or legal_hold = 0) "
                + "and remark like 'temp-%'");

        assertRemainingIds(3L, 4L, 5L, 8L, 9L, 10L, 12L);
        Assert.assertEquals("A legal hold must override the other matching conditions",
            1, queryCount("id = 5 and legal_hold = 1"));
        Assert.assertEquals("BETWEEN must include the upper tenant boundary",
            0, queryCount("id = 7"));
        Assert.assertEquals("A tenant immediately outside the range must be retained",
            1, queryCount("id = 8"));
        Assert.assertEquals("A NULL status must not accidentally satisfy the cleanup predicate",
            1, queryCount("id = 9 and status is null"));
    }

    @Test
    public void testDecimalBooleanAndEscapedStringConditions() {
        executeCleanup("amount < 0 or (priority = 0 and status = 'active') or remark = 'O''Reilly'");

        assertRemainingIds(2L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);
        Assert.assertEquals("Decimal zero is the non-matching boundary and must be retained",
            1, queryCount("id = 2 and amount = 0"));
        Assert.assertEquals("The escaped quote literal must match and be cleaned",
            0, queryCount("remark = 'O''Reilly'"));
    }

    private void executeCleanup(String predicate) {
        String cleanupSql = RebuildCleanupTestSupport.forceOmc30Hint()
            + "alter table " + TABLE_NAME + " rebuild cleanup where " + predicate;
        JdbcUtil.executeUpdateSuccess(tddlConnection, cleanupSql);
    }

    private int queryCount(String predicate) {
        return RebuildCleanupTestSupport.queryCount(
            tddlConnection, "select count(*) from " + TABLE_NAME + " where " + predicate);
    }

    private void assertRemainingIds(Long... expectedIds) {
        List<Long> actualIds = new ArrayList<>();
        try (ResultSet resultSet =
            JdbcUtil.executeQuerySuccess(tddlConnection, "select id from " + TABLE_NAME + " order by id")) {
            while (resultSet.next()) {
                actualIds.add(resultSet.getLong(1));
            }
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
        Assert.assertEquals(Arrays.asList(expectedIds), actualIds);
    }
}
