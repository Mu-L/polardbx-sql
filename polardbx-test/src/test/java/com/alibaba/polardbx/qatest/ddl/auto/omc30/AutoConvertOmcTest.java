package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.truth.Truth.assertWithMessage;

@ReplicaIgnore(ignoreReason = "set session variables")
public class AutoConvertOmcTest extends DDLBaseNewDBTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testAutoConvert() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        String tableName = "omc_30_" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int, c int) partition by hash(`a`)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Set<String> physicalTableNames = getPhysicalTableNames(tddlConnection, tableName);

        sql = String.format("alter table %s modify column b bigint, modify column c bigint, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Set<String> physicalTableNames2 = getPhysicalTableNames(tddlConnection, tableName);
        assertWithMessage("physical table names should be the same after omc30").that(physicalTableNames)
            .isEqualTo(physicalTableNames2);

        sql = String.format("alter table %s modify column a bigint, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Set<String> physicalTableNames3 = getPhysicalTableNames(tddlConnection, tableName);
        assertWithMessage("physical table names should not be the same after omc20").that(physicalTableNames)
            .isNotEqualTo(physicalTableNames3);
    }

    @Test
    public void testAutoConvertWithGsi() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        String tableName = "omc_30_gsi_" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int, c int, d int) partition by hash(`a`)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add global index gsi_b(b) covering(c) partition by key(b)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Set<String> physicalTableNames = getPhysicalTableNames(tddlConnection, tableName);

        // modify column only in primary table
        sql = String.format("alter table %s modify column d bigint, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Set<String> physicalTableNames2 = getPhysicalTableNames(tddlConnection, tableName);
        assertWithMessage("physical table names should be the same after omc30").that(physicalTableNames)
            .isEqualTo(physicalTableNames2);

        // modify gsi covering column
        sql = String.format("alter table %s modify column c bigint, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Set<String> physicalTableNames3 = getPhysicalTableNames(tddlConnection, tableName);
        assertWithMessage("physical table names should not be the same after omc20").that(physicalTableNames)
            .isNotEqualTo(physicalTableNames3);

        // modify gsi partition key
        sql = String.format("alter table %s modify column b bigint, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Set<String> physicalTableNames4 = getPhysicalTableNames(tddlConnection, tableName);
        assertWithMessage("physical table names should not be the same after omc20").that(physicalTableNames3)
            .isNotEqualTo(physicalTableNames4);

        // modify gsi covering column with hint
        String hint = "/*+TDDL:cmd_extra(ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI=true, FORCE_USING_OMC_30=true)*/";
        sql = hint + String.format("alter table %s modify column c bigint, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Set<String> physicalTableNames5 = getPhysicalTableNames(tddlConnection, tableName);
        assertWithMessage("physical table names should be the same after omc30").that(physicalTableNames4)
            .isEqualTo(physicalTableNames5);
    }

    @Test
    public void testModifyPartitionKeyButNoRepartition() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        String tableName = "omc_30_sk_" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int, b int, c int, d int) partition by hash(`a`)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Set<String> physicalTableNames = getPhysicalTableNames(tddlConnection, tableName);

        sql = String.format("alter table %s modify column a int default 1, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Set<String> physicalTableNames2 = getPhysicalTableNames(tddlConnection, tableName);
        assertWithMessage("physical table names should be the same after omc30").that(physicalTableNames)
            .isEqualTo(physicalTableNames2);
    }

    // float
    @Test
    public void testFloat() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        String tableName = "omc_30_float";
        dropTableIfExists(tableName);

        String sql =
            String.format("create table %s (a float primary key, b int, c int, d int) partition by hash(`d`)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Set<String> physicalTableNames = getPhysicalTableNames(tddlConnection, tableName);

        sql = String.format("alter table %s modify column b bigint, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Set<String> physicalTableNames2 = getPhysicalTableNames(tddlConnection, tableName);
        assertWithMessage("physical table names should not be the same after omc20").that(physicalTableNames)
            .isNotEqualTo(physicalTableNames2);
    }

    // timestamp(3)
    @Test
    public void testTimestamp3() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        String tableName = "omc_30_timestamp3";
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a timestamp(3) primary key, b int, c int, d int) partition by hash(`d`)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Set<String> physicalTableNames = getPhysicalTableNames(tddlConnection, tableName);

        sql = String.format("alter table %s modify column b bigint, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Set<String> physicalTableNames2 = getPhysicalTableNames(tddlConnection, tableName);
        assertWithMessage("physical table names should not be the same after omc20").that(physicalTableNames)
            .isNotEqualTo(physicalTableNames2);
    }

    public Set<String> getPhysicalTableNames(Connection tddlConnection, String tableName) throws SQLException {
        String showTopology = "show topology from " + tableName;
        ResultSet rs = JdbcUtil.executeQuery(showTopology, tddlConnection);
        Set<String> physicalTableNames = new TreeSet<>();
        while (rs.next()) {
            String physicalTableName = rs.getString("TABLE_NAME");
            physicalTableNames.add(physicalTableName);
        }
        return physicalTableNames;
    }
}
