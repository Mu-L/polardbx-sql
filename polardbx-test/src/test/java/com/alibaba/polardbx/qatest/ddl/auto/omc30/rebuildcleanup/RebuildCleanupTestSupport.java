package com.alibaba.polardbx.qatest.ddl.auto.omc30.rebuildcleanup;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

final class RebuildCleanupTestSupport {

    static final String TABLE_NAME = "cleanup_orders";

    private RebuildCleanupTestSupport() {
    }

    static void prepareTable(Connection connection, String databaseName) {
        JdbcUtil.executeUpdateSuccess(connection, "drop database if exists " + databaseName);
        JdbcUtil.executeUpdateSuccess(connection, "create database " + databaseName + " mode = 'auto'");
        JdbcUtil.executeUpdateSuccess(connection, "use " + databaseName);
        JdbcUtil.executeUpdateSuccess(connection,
            "create table " + TABLE_NAME + " ("
                + "id bigint primary key, "
                + "status varchar(32), "
                + "remark varchar(64)"
                + ") partition by hash(id) partitions 3");
        JdbcUtil.executeUpdateSuccess(connection,
            "insert into " + TABLE_NAME + " values "
                + "(1, 'active', 'keep-1'),"
                + "(2, 'deleted', 'remove-2'),"
                + "(3, null, 'keep-null-3'),"
                + "(4, 'pending', 'keep-4'),"
                + "(5, 'deleted', 'remove-5'),"
                + "(6, 'active', 'keep-6'),"
                + "(7, 'deleted', 'remove-7'),"
                + "(8, null, 'keep-null-8'),"
                + "(9, 'pending', 'keep-9'),"
                + "(10, 'active', 'keep-10'),"
                + "(11, 'deleted', 'remove-11'),"
                + "(12, 'active', 'keep-12')");
    }

    static void dropDatabase(Connection connection, String databaseName) {
        JdbcUtil.executeUpdateSuccess(connection, "drop database if exists " + databaseName);
    }

    static String forceOmc30Hint() {
        return "/*+TDDL:CMD_EXTRA(CDC_RANDOM_DDL_TOKEN=\"" + UUID.randomUUID()
            + "\",FORCE_USING_OMC_30=TRUE)*/ ";
    }

    static String forceOmc30WithGsiHint() {
        return "/*+TDDL:CMD_EXTRA(CDC_RANDOM_DDL_TOKEN=\"" + UUID.randomUUID()
            + "\",FORCE_USING_OMC_30=TRUE,FORCE_REBUILD_CLEANUP_WITH_GSI=TRUE)*/ ";
    }

    static String forceOmc30SkipCdcHint() {
        return "/*+TDDL:CMD_EXTRA(CDC_RANDOM_DDL_TOKEN=\"" + UUID.randomUUID()
            + "\",FORCE_USING_OMC_30=TRUE,REBUILD_CLEANUP_SKIP_CDC_TASK=TRUE)*/ ";
    }

    static String forceOmc30WithCutoverSuspendHint() {
        return "/*+TDDL:CMD_EXTRA(CDC_RANDOM_DDL_TOKEN=\"" + UUID.randomUUID()
            + "\",FORCE_USING_OMC_30=TRUE,FP_OMC_BEFORE_CUTOVER_SUSPEND=15000)*/ ";
    }

    static int queryCount(Connection connection, String sql) {
        try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(connection, sql)) {
            Assert.assertTrue(resultSet.next());
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    static void assertSampleColumn(String content, String expectedColumn) {
        boolean found = JSONObject.parseObject(content).keySet().stream()
            .anyMatch(column -> expectedColumn.equalsIgnoreCase(column));
        Assert.assertTrue("Sample does not contain column " + expectedColumn + ": " + content, found);
    }
}
