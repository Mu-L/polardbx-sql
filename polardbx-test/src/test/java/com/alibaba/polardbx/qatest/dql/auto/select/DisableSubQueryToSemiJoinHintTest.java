package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.google.common.truth.Truth.assertThat;

/**
 * AONE-85269649: verify the DISABLE_SUBQUERY_TO_SEMI_JOIN hint forces a correlated
 * subquery to be rewritten via buildCorrelateNode (CorrelateApply) instead of
 * SubQueryToSemiJoinRule's default SemiJoin/Join path.
 */
public class DisableSubQueryToSemiJoinHintTest extends BaseTestCase {

    private static final String DB_NAME = "disable_subquery_to_semi_join_test";

    private void createDbAndTable() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + DB_NAME);
            c.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            c.createStatement().execute("use " + DB_NAME);
            c.createStatement().execute("CREATE TABLE `host_tbl` (\n"
                + "\t`pk` bigint NOT NULL,\n"
                + "\t`integer_test` int DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`pk`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`pk`) PARTITIONS 4");
            c.createStatement().execute("CREATE TABLE `info_tbl` (\n"
                + "\t`pk` bigint NOT NULL,\n"
                + "\t`integer_test` int DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`pk`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`pk`) PARTITIONS 4");
        }
    }

    private String explainPlan(Connection c, String sql) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                plan.append("\n").append(rs.getString(1));
            }
        }
        return plan.toString();
    }

    @Test
    public void testDisableSubQueryToSemiJoinHintForIn() throws SQLException {
        createDbAndTable();
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            String baselineSql =
                "explain select pk, integer_test from host_tbl as host where pk in "
                    + "(select pk from info_tbl as info where info.integer_test = host.integer_test)";
            String baselinePlan = explainPlan(c, baselineSql);
            assertThat(baselinePlan).doesNotContain("CorrelateApply");

            String hintSql =
                "explain /*+TDDL:cmd_extra(DISABLE_SUBQUERY_TO_SEMI_JOIN=true)*/ select pk, integer_test "
                    + "from host_tbl as host where pk in "
                    + "(select pk from info_tbl as info where info.integer_test = host.integer_test)";
            String hintPlan = explainPlan(c, hintSql);
            assertThat(hintPlan).contains("CorrelateApply");
        }
    }

    @Test
    public void testDisableSubQueryToSemiJoinHintForScalar() throws SQLException {
        createDbAndTable();
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            String baselineSql =
                "explain select pk, integer_test from host_tbl as host where integer_test = "
                    + "(select pk from info_tbl as info where info.integer_test = host.integer_test)";
            String baselinePlan = explainPlan(c, baselineSql);
            assertThat(baselinePlan).doesNotContain("CorrelateApply");

            String hintSql =
                "explain /*+TDDL:cmd_extra(DISABLE_SUBQUERY_TO_SEMI_JOIN=true)*/ select pk, integer_test "
                    + "from host_tbl as host where integer_test = "
                    + "(select pk from info_tbl as info where info.integer_test = host.integer_test)";
            String hintPlan = explainPlan(c, hintSql);
            assertThat(hintPlan).contains("CorrelateApply");
        }
    }
}
