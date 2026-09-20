package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Regression test for AONE-85096607.
 * <p>
 * When the OptimizerContext of the target schema is temporarily unavailable
 * while {@code TableGroupSyncAction.sync()} runs, the sync action must report
 * an error carrying schemaName/tableGroupName context instead of letting a
 * message-less NPE degrade to "Failed to execute the DDL task. Caused by: null".
 * <p>
 * The "OptimizerContext unavailable" condition is deterministically injected
 * through fail point FP_TABLE_GROUP_SYNC_CONTEXT_NULL.
 */
@NotThreadSafe
public class TableGroupSyncNullContextGuardTest extends DDLBaseNewDBTestCase {

    private static final String DB_NAME = "tg_sync_null_ctx_db";
    private static final String TABLE_NAME = "tg_sync_null_ctx_order";
    private static final String GSI_NAME = "g_i_tg_sync_null_ctx";
    private static final String FP_KEY = "FP_TABLE_GROUP_SYNC_CONTEXT_NULL";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @After
    public void cleanUp() {
        rollbackRemainingJobsQuietly();
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set @" + FP_KEY + " = 'false'");
        } catch (Throwable ignored) {
        }
        JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, "drop database if exists " + DB_NAME);
    }

    @Test
    public void testSplitPartitionSyncWithNullContextReportsContextualError() {
        JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, "drop database if exists " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + DB_NAME + " mode = 'auto'");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + DB_NAME);

        String createTableSql = "create table " + TABLE_NAME + " ("
            + " id bigint not null auto_increment,"
            + " order_date datetime not null,"
            + " buyer_id bigint not null,"
            + " primary key(id, order_date)"
            + ") partition by range columns(order_date) ("
            + " partition p202512 values less than ('2025-12-01 00:00:00'),"
            + " partition pmax values less than (maxvalue)"
            + " )";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        String createIndexSql = "create global index " + GSI_NAME + " on " + TABLE_NAME + "(buyer_id)"
            + " partition by range columns(order_date) ("
            + " partition p202512 values less than ('2025-12-01 00:00:00'),"
            + " partition pmax values less than (maxvalue)"
            + " )";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndexSql);

        String tableGroupName = queryTableGroupName();
        Assert.assertNotNull("failed to resolve table group of " + TABLE_NAME, tableGroupName);

        // force the "OptimizerContext unavailable" branch inside TableGroupSyncAction.sync()
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set @" + FP_KEY + " = 'true'");

        String splitDdl = "/*+TDDL:cmd_extra(" + FP_KEY + "=true)*/ "
            + "alter index " + GSI_NAME + " on table " + TABLE_NAME
            + " split partition pmax into ("
            + " partition p202612 values less than ('2026-12-01 00:00:00'),"
            + " partition pmax values less than (maxvalue)"
            + " )";

        String errorMessage = JdbcUtil.executeUpdateFailedReturn(tddlConnection, splitDdl);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set @" + FP_KEY + " = 'false'");

        Assert.assertTrue(
            "DDL should report a contextual error instead of a meaningless one, but got: " + errorMessage,
            errorMessage != null && !errorMessage.toLowerCase().contains("caused by: null"));
        Assert.assertTrue(
            "DDL error message should contain the schema name [" + DB_NAME + "], but got: " + errorMessage,
            containsIgnoreCase(errorMessage, DB_NAME));
        Assert.assertTrue(
            "DDL error message should contain the table group name [" + tableGroupName + "], but got: "
                + errorMessage,
            containsIgnoreCase(errorMessage, tableGroupName));
    }

    private String queryTableGroupName() {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show tablegroup")) {
            String fallback = null;
            while (rs.next()) {
                String tgName = rs.getString("TABLE_GROUP_NAME");
                String tableName = tryGetString(rs, "TABLE_NAME");
                if (TABLE_NAME.equalsIgnoreCase(tableName)) {
                    return tgName;
                }
                if (fallback == null && tgName != null && !tgName.toLowerCase().startsWith("oss")) {
                    fallback = tgName;
                }
            }
            return fallback;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void rollbackRemainingJobsQuietly() {
        try {
            JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, "use " + DB_NAME);
            List<String> jobIds = new ArrayList<>();
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show ddl")) {
                while (rs.next()) {
                    if (DB_NAME.equalsIgnoreCase(rs.getString("OBJECT_SCHEMA"))) {
                        jobIds.add(rs.getString("JOB_ID"));
                    }
                }
            }
            for (String jobId : jobIds) {
                JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection, "rollback ddl " + jobId);
            }
            for (int i = 0; i < 60; i++) {
                boolean hasJob = false;
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show ddl")) {
                    while (rs.next()) {
                        if (DB_NAME.equalsIgnoreCase(rs.getString("OBJECT_SCHEMA"))) {
                            hasJob = true;
                        }
                    }
                }
                if (!hasJob) {
                    break;
                }
                Thread.sleep(2000);
            }
        } catch (Throwable ignored) {
        }
    }

    private static boolean containsIgnoreCase(String haystack, String needle) {
        return haystack != null && needle != null
            && haystack.toLowerCase().contains(needle.toLowerCase());
    }

    private static String tryGetString(ResultSet rs, String column) {
        try {
            return rs.getString(column);
        } catch (SQLException e) {
            return null;
        }
    }
}
